package gcpfirestore

import (
	"slices"
	"strings"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	defaultListSize = 100
	maxListSize     = 300
	maxBatchWrites  = 500
	maxOrderParts   = 2
)

func applyMask(d *firestorepb.Document, m *firestorepb.DocumentMask) *firestorepb.Document {
	if m == nil {
		return d
	}

	return maskDoc(d, m.GetFieldPaths())
}

// GetDocument reads one document.
func (b *InMemoryBackend) GetDocument(req *firestorepb.GetDocumentRequest) (*firestorepb.Document, error) {
	if err := validateDocName(req.GetName()); err != nil {
		return nil, err
	}

	var out *firestorepb.Document

	err := b.read(selOf(req), func(rv *readView) error {
		d := b.docAt(req.GetName(), rv)
		if d == nil {
			return errNotFound("Document %q not found.", req.GetName())
		}

		out = applyMask(d, req.GetMask())

		return nil
	})

	return out, err
}

func selOf(req any) readSel {
	switch r := req.(type) {
	case *firestorepb.GetDocumentRequest:
		return readSel{txn: r.GetTransaction(), readTime: r.GetReadTime()}
	case *firestorepb.ListDocumentsRequest:
		return readSel{txn: r.GetTransaction(), readTime: r.GetReadTime()}
	case *firestorepb.BatchGetDocumentsRequest:
		return readSel{txn: r.GetTransaction(), newTxn: r.GetNewTransaction(), readTime: r.GetReadTime()}
	case *firestorepb.RunQueryRequest:
		return readSel{txn: r.GetTransaction(), newTxn: r.GetNewTransaction(), readTime: r.GetReadTime()}
	case *firestorepb.RunAggregationQueryRequest:
		return readSel{txn: r.GetTransaction(), newTxn: r.GetNewTransaction(), readTime: r.GetReadTime()}
	case *firestorepb.ListCollectionIdsRequest:
		return readSel{readTime: r.GetReadTime()}
	case *firestorepb.PartitionQueryRequest:
		return readSel{readTime: r.GetReadTime()}
	}

	return readSel{}
}

// BatchGetResult is one entry of a BatchGetDocuments stream.
type BatchGetResult struct {
	Found   *firestorepb.Document
	Missing string
}

// BatchGetDocuments reads documents by name at one snapshot.
func (b *InMemoryBackend) BatchGetDocuments(
	req *firestorepb.BatchGetDocumentsRequest,
) ([]BatchGetResult, []byte, *timestamppb.Timestamp, error) {
	if err := validateDatabase(req.GetDatabase()); err != nil {
		return nil, nil, nil, err
	}

	for _, n := range req.GetDocuments() {
		if err := validateDocName(n); err != nil {
			return nil, nil, nil, err
		}
	}

	var (
		results []BatchGetResult
		txn     []byte
		rt      *timestamppb.Timestamp
	)

	err := b.read(selOf(req), func(rv *readView) error {
		txn, rt = rv.newTxnID, timestamppb.New(rv.readTime)

		for _, n := range req.GetDocuments() {
			if d := b.docAt(n, rv); d != nil {
				results = append(results, BatchGetResult{Found: applyMask(d, req.GetMask())})
			} else {
				results = append(results, BatchGetResult{Missing: n})
			}
		}

		return nil
	})

	return results, txn, rt, err
}

func (b *InMemoryBackend) commitOne(w *firestorepb.Write) (*firestorepb.WriteResult, error) {
	defer b.flushDocEvents()

	b.mu.Lock("write")
	defer b.mu.Unlock()

	res, _, err := b.commitLocked([]*firestorepb.Write{w})
	if err != nil {
		return nil, err
	}

	return res[0], nil
}

// CreateDocument creates a document, generating an id when none is given.
func (b *InMemoryBackend) CreateDocument(req *firestorepb.CreateDocumentRequest) (*firestorepb.Document, error) {
	parent, err := parseParent(req.GetParent())
	if err != nil {
		return nil, err
	}

	if req.GetCollectionId() == "" || strings.Contains(req.GetCollectionId(), "/") {
		return nil, errInvalid("Invalid collection_id: %q", req.GetCollectionId())
	}

	id := req.GetDocumentId()
	if id == "" {
		id = newDocID()
	}

	if strings.Contains(id, "/") || id == "." || id == ".." ||
		strings.HasPrefix(id, "__") && strings.HasSuffix(id, "__") {
		return nil, errInvalid("Invalid document id: %q", id)
	}

	name := parent.String() + "/" + req.GetCollectionId() + "/" + id
	w := &firestorepb.Write{
		Operation: &firestorepb.Write_Update{
			Update: &firestorepb.Document{Name: name, Fields: req.GetDocument().GetFields()},
		},
		CurrentDocument: &firestorepb.Precondition{ConditionType: &firestorepb.Precondition_Exists{Exists: false}},
	}

	if _, cerr := b.commitOne(w); cerr != nil {
		return nil, cerr
	}

	return b.GetDocument(&firestorepb.GetDocumentRequest{Name: name, Mask: req.GetMask()})
}

// UpdateDocument writes a document, honouring the update mask and precondition.
func (b *InMemoryBackend) UpdateDocument(req *firestorepb.UpdateDocumentRequest) (*firestorepb.Document, error) {
	w := &firestorepb.Write{
		Operation:       &firestorepb.Write_Update{Update: req.GetDocument()},
		UpdateMask:      req.GetUpdateMask(),
		CurrentDocument: req.GetCurrentDocument(),
	}

	if _, err := b.commitOne(w); err != nil {
		return nil, err
	}

	return b.GetDocument(&firestorepb.GetDocumentRequest{Name: req.GetDocument().GetName(), Mask: req.GetMask()})
}

// DeleteDocument deletes a document; deleting an absent document succeeds unless a precondition says otherwise.
func (b *InMemoryBackend) DeleteDocument(req *firestorepb.DeleteDocumentRequest) error {
	_, err := b.commitOne(&firestorepb.Write{
		Operation:       &firestorepb.Write_Delete{Delete: req.GetName()},
		CurrentDocument: req.GetCurrentDocument(),
	})

	return err
}

// Commit applies writes atomically, optionally inside a transaction.
func (b *InMemoryBackend) Commit(req *firestorepb.CommitRequest) (*firestorepb.CommitResponse, error) {
	if err := validateDatabase(req.GetDatabase()); err != nil {
		return nil, err
	}

	defer b.flushDocEvents()

	b.mu.Lock("Commit")
	defer b.mu.Unlock()

	if len(req.GetTransaction()) > 0 {
		id := string(req.GetTransaction())

		t, ok := b.txns[id]
		if !ok {
			return nil, errInvalid("Transaction ID is invalid or has expired.")
		}

		delete(b.txns, id)

		if t.readOnly && len(req.GetWrites()) > 0 {
			return nil, errInvalid("Cannot modify data in a read-only transaction.")
		}

		if !t.readOnly && b.conflictLocked(t) {
			return nil, errAborted()
		}
	}

	results, ct, err := b.commitLocked(req.GetWrites())
	if err != nil {
		return nil, err
	}

	return &firestorepb.CommitResponse{WriteResults: results, CommitTime: timestamppb.New(ct)}, nil
}

// BatchWrite applies each write independently and reports a status per write.
func (b *InMemoryBackend) BatchWrite(req *firestorepb.BatchWriteRequest) (*firestorepb.BatchWriteResponse, error) {
	if err := validateDatabase(req.GetDatabase()); err != nil {
		return nil, err
	}

	if len(req.GetWrites()) > maxBatchWrites {
		return nil, errInvalid("A BatchWrite may contain at most %d writes.", maxBatchWrites)
	}

	seen := map[string]struct{}{}

	for _, w := range req.GetWrites() {
		name := writeTarget(w)
		if _, dup := seen[name]; dup {
			return nil, errInvalid("Duplicate writes to the same document in one BatchWrite: %s", name)
		}

		seen[name] = struct{}{}
	}

	resp := &firestorepb.BatchWriteResponse{}

	for _, w := range req.GetWrites() {
		res, err := b.commitOne(w)
		if err != nil {
			resp.WriteResults = append(resp.WriteResults, &firestorepb.WriteResult{})
			resp.Status = append(resp.Status, errStatus(err))

			continue
		}

		resp.WriteResults = append(resp.WriteResults, res)
		resp.Status = append(resp.Status, &spb.Status{Code: int32(codes.OK)})
	}

	return resp, nil
}

func writeTarget(w *firestorepb.Write) string {
	switch op := w.GetOperation().(type) {
	case *firestorepb.Write_Update:
		return op.Update.GetName()
	case *firestorepb.Write_Delete:
		return op.Delete
	case *firestorepb.Write_Transform:
		return op.Transform.GetDocument()
	}

	return ""
}

// ListDocuments lists the documents of one collection.
func (b *InMemoryBackend) ListDocuments(
	req *firestorepb.ListDocumentsRequest,
) ([]*firestorepb.Document, string, error) {
	parent, err := parseParent(req.GetParent())
	if err != nil {
		return nil, "", err
	}

	if req.GetCollectionId() == "" {
		return nil, "", errInvalid("collection_id is required.")
	}

	if req.GetShowMissing() && req.GetOrderBy() != "" {
		return nil, "", errInvalid("order_by cannot be combined with show_missing.")
	}

	q := &firestorepb.StructuredQuery{
		From: []*firestorepb.StructuredQuery_CollectionSelector{{CollectionId: req.GetCollectionId()}},
	}

	if q.OrderBy, err = parseOrderBy(req.GetOrderBy()); err != nil {
		return nil, "", err
	}

	var all []*firestorepb.Document

	err = b.read(selOf(req), func(rv *readView) error {
		docs, _, qerr := b.runQuery(parent.String(), q, rv)
		if qerr != nil {
			return qerr
		}

		all = docs

		if req.GetShowMissing() {
			all = b.withMissing(parent.String()+"/"+req.GetCollectionId(), docs, rv)
		}

		return nil
	})
	if err != nil {
		return nil, "", err
	}

	size := min(int(req.GetPageSize()), maxListSize)

	pg := page.New(all, req.GetPageToken(), size, defaultListSize)

	out := make([]*firestorepb.Document, 0, len(pg.Data))
	for _, d := range pg.Data {
		out = append(out, applyMask(d, req.GetMask()))
	}

	return out, pg.Next, nil
}

func parseOrderBy(s string) ([]*firestorepb.StructuredQuery_Order, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}

	var out []*firestorepb.StructuredQuery_Order

	for part := range strings.SplitSeq(s, ",") {
		f := strings.Fields(part)
		if len(f) == 0 || len(f) > maxOrderParts {
			return nil, errInvalid("Invalid order_by: %q", s)
		}

		o := &firestorepb.StructuredQuery_Order{
			Field:     &firestorepb.StructuredQuery_FieldReference{FieldPath: f[0]},
			Direction: firestorepb.StructuredQuery_ASCENDING,
		}

		if len(f) == maxOrderParts {
			switch strings.ToLower(f[1]) {
			case "desc":
				o.Direction = firestorepb.StructuredQuery_DESCENDING
			case "asc":
			default:
				return nil, errInvalid("Invalid order_by direction: %q", f[1])
			}
		}

		out = append(out, o)
	}

	return out, nil
}

// withMissing adds name-only entries for ids that have descendants but no document of their own.
func (b *InMemoryBackend) withMissing(
	collection string, docs []*firestorepb.Document, rv *readView,
) []*firestorepb.Document {
	have := make(map[string]struct{}, len(docs))
	for _, d := range docs {
		have[d.GetName()] = struct{}{}
	}

	out := slices.Clone(docs)

	for name, rec := range b.docs {
		rest, ok := strings.CutPrefix(name, collection+"/")
		if !ok || rec.at(rv.at) == nil {
			continue
		}

		id, _, deeper := strings.Cut(rest, "/")
		if !deeper {
			continue
		}

		full := collection + "/" + id
		if _, dup := have[full]; !dup {
			have[full] = struct{}{}
			out = append(out, &firestorepb.Document{Name: full})
		}
	}

	slices.SortFunc(out, func(a, c *firestorepb.Document) int { return compareRefs(a.GetName(), c.GetName()) })

	return out
}

// ListCollectionIDs lists the collection ids directly under a document or the database root.
func (b *InMemoryBackend) ListCollectionIDs(
	req *firestorepb.ListCollectionIdsRequest,
) ([]string, string, error) {
	parent, err := parseParent(req.GetParent())
	if err != nil {
		return nil, "", err
	}

	set := map[string]struct{}{}

	err = b.read(selOf(req), func(rv *readView) error {
		prefix := parent.String() + "/"

		for name, rec := range b.docs {
			rest, ok := strings.CutPrefix(name, prefix)
			if !ok || rec.at(rv.at) == nil {
				continue
			}

			set[strings.SplitN(rest, "/", 2)[0]] = struct{}{} //nolint:mnd // collection id and remainder
		}

		return nil
	})
	if err != nil {
		return nil, "", err
	}

	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}

	slices.Sort(ids)

	pg := page.New(ids, req.GetPageToken(), int(req.GetPageSize()), len(ids)+1)

	return pg.Data, pg.Next, nil
}

// PartitionQuery splits a collection-group query into cursors over document names.
func (b *InMemoryBackend) PartitionQuery(
	req *firestorepb.PartitionQueryRequest,
) ([]*firestorepb.Cursor, string, error) {
	q := req.GetStructuredQuery()
	if len(q.GetFrom()) != 1 || !q.GetFrom()[0].GetAllDescendants() {
		return nil, "", errInvalid("Partition queries must be collection group queries.")
	}

	if req.GetPartitionCount() < 1 {
		return nil, "", errInvalid("partition_count must be positive.")
	}

	var names []string

	err := b.read(selOf(req), func(rv *readView) error {
		cq, err := compileQuery(req.GetParent(), &firestorepb.StructuredQuery{From: q.GetFrom()})
		if err != nil {
			return err
		}

		docs, err := b.filtered(cq, rv)
		for _, d := range docs {
			names = append(names, d.GetName())
		}

		return err
	})
	if err != nil {
		return nil, "", err
	}

	n := int(req.GetPartitionCount())

	var cursors []*firestorepb.Cursor

	for i := 1; i < n && len(names) > 0; i++ {
		idx := i * len(names) / n
		if idx == 0 || (len(cursors) > 0 && cursors[len(cursors)-1].GetValues()[0].GetReferenceValue() == names[idx]) {
			continue
		}

		cursors = append(cursors, &firestorepb.Cursor{Values: []*firestorepb.Value{refVal(names[idx])}, Before: true})
	}

	pg := page.New(cursors, req.GetPageToken(), int(req.GetPageSize()), len(cursors)+1)

	return pg.Data, pg.Next, nil
}

func errStatus(err error) *spb.Status {
	return status.Convert(err).Proto()
}
