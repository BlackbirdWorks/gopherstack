package bedrockagentcore

import (
	"cmp"
	"context"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	maxBatchRecords = 100
	recordIDLen     = 32
	defaultTopK     = 10
	maxTopK         = 100
	errBadRequest   = 400
	errNotFoundCode = 404
	maxRecordText   = 1 << 20
)

func recordOps() []string {
	return []string{
		opEquals, opExists, opNotExists, opBefore, opAfter, "CONTAINS",
		"GREATER_THAN", "GREATER_THAN_OR_EQUALS", "LESS_THAN", "LESS_THAN_OR_EQUALS",
	}
}

func validateRecordMetadata(m doc) error {
	if len(m) > maxMetadataItems {
		return invalid("metadata may hold at most %d entries", maxMetadataItems)
	}

	for k, v := range m {
		vm, _ := v.(map[string]any)
		if _, ok := exactlyOne(vm, "stringValue", "numberValue", "stringListValue", "dateTimeValue"); !ok {
			return invalid("metadata value %q must set exactly one value member", k)
		}
	}

	return nil
}

func (b *InMemoryBackend) strategyExists(m MemoryInfo, id string) bool {
	return id == "" || slices.ContainsFunc(m.Strategies, func(s StrategyInfo) bool { return s.ID == id })
}

func recordText(content doc) (string, bool) {
	t, ok := content[keyText].(string)

	return t, ok && len(t) <= maxRecordText
}

func recordView(r *MemoryRecord) doc {
	out := doc{
		keyRecordID: r.ID, "content": doc{keyText: r.Text}, "namespaces": r.Namespaces,
		keyCreatedAt: epoch(r.Created),
	}

	if r.StrategyID != "" {
		out["memoryStrategyId"] = r.StrategyID
	}

	if len(r.Metadata) > 0 {
		out["metadata"] = r.Metadata
	}

	return out
}

func failedRecord(id, requestID string, code int, msg string) doc {
	out := doc{keyStatus: "FAILED", "errorCode": code, "errorMessage": msg}
	if id != "" {
		out[keyRecordID] = id
	}

	if requestID != "" {
		out["requestIdentifier"] = requestID
	}

	return out
}

func (b *InMemoryBackend) newRecord(sc Scope, m MemoryInfo, in doc) (*MemoryRecord, error) {
	text, ok := recordText(objOf(in, "content"))
	if !ok {
		return nil, invalid("content.text is required")
	}

	ns := strList(in, "namespaces")
	if len(ns) == 0 {
		return nil, invalid("namespaces must not be empty")
	}

	if strOf(in, "requestIdentifier") == "" {
		return nil, invalid("requestIdentifier is required")
	}

	ts, ok := fromEpoch(in["timestamp"])
	if !ok {
		return nil, invalid("timestamp is required")
	}

	sid := strOf(in, "memoryStrategyId")
	if !b.strategyExists(m, sid) {
		return nil, notFound("memory strategy %q not found", sid)
	}

	if err := validateRecordMetadata(objOf(in, "metadata")); err != nil {
		return nil, err
	}

	return &MemoryRecord{
		Account: sc.Account, Region: sc.Region, MemoryID: m.ID, ID: awsid.NewN("mem-", recordIDLen),
		StrategyID: sid, Text: text, Namespaces: ns, Metadata: objOf(in, "metadata"), Created: ts,
	}, nil
}

func (b *InMemoryBackend) batchCreateRecords(ctx context.Context, req *restroute.Request) (any, int, error) {
	items, err := batchInput(req)
	if err != nil {
		return nil, 0, err
	}

	sc := b.scopeOf(ctx)

	m, err := b.memoryOf(sc, req.Params["memoryId"])
	if err != nil {
		return nil, 0, err
	}

	b.mu.Lock("BatchCreateMemoryRecords")
	defer b.mu.Unlock()

	ok, failed := []doc{}, []doc{}

	for _, it := range items {
		item, _ := it.(map[string]any)

		rec, rerr := b.newRecord(sc, m, item)
		if rerr != nil {
			failed = append(failed, failedRecord("", strOf(item, "requestIdentifier"), errBadRequest, rerr.Error()))

			continue
		}

		b.seq++
		rec.Seq = b.seq
		b.records.Put(rec)

		ok = append(ok, doc{
			keyRecordID: rec.ID, keyStatus: statusSucceeded, "requestIdentifier": strOf(item, "requestIdentifier"),
		})
	}

	return doc{keySucceeded: ok, keyFailed: failed}, 0, nil
}

func batchInput(req *restroute.Request) ([]any, error) {
	in, err := decodeDoc(req)
	if err != nil {
		return nil, err
	}

	items := listOf(in, "records")
	if len(items) == 0 || len(items) > maxBatchRecords {
		return nil, invalid("records must contain between 1 and %d entries", maxBatchRecords)
	}

	return items, nil
}

func (b *InMemoryBackend) batchUpdateRecords(ctx context.Context, req *restroute.Request) (any, int, error) {
	items, err := batchInput(req)
	if err != nil {
		return nil, 0, err
	}

	sc := b.scopeOf(ctx)

	m, err := b.memoryOf(sc, req.Params["memoryId"])
	if err != nil {
		return nil, 0, err
	}

	b.mu.Lock("BatchUpdateMemoryRecords")
	defer b.mu.Unlock()

	ok, failed := []doc{}, []doc{}

	for _, it := range items {
		item, _ := it.(map[string]any)
		id := strOf(item, keyRecordID)

		rec, found := b.records.Get(recordKey(sc.Account, sc.Region, m.ID, id))
		if !found {
			failed = append(failed, failedRecord(id, "", errNotFoundCode, "memory record not found"))

			continue
		}

		if uerr := b.applyRecordUpdate(m, rec, item); uerr != nil {
			failed = append(failed, failedRecord(id, "", errBadRequest, uerr.Error()))

			continue
		}

		ok = append(ok, doc{keyRecordID: id, keyStatus: statusSucceeded})
	}

	return doc{keySucceeded: ok, keyFailed: failed}, 0, nil
}

func (b *InMemoryBackend) applyRecordUpdate(m MemoryInfo, rec *MemoryRecord, in doc) error {
	next := *rec

	if c := objOf(in, "content"); c != nil {
		text, ok := recordText(c)
		if !ok {
			return invalid("content.text is required")
		}

		next.Text = text
	}

	if ns := strList(in, "namespaces"); len(ns) > 0 {
		next.Namespaces = ns
	}

	if sid := strOf(in, "memoryStrategyId"); sid != "" {
		if !b.strategyExists(m, sid) {
			return notFound("memory strategy %q not found", sid)
		}

		next.StrategyID = sid
	}

	if md := objOf(in, "metadata"); md != nil {
		if err := validateRecordMetadata(md); err != nil {
			return err
		}

		next.Metadata = md
	}

	*rec = next

	return nil
}

func (b *InMemoryBackend) batchDeleteRecords(ctx context.Context, req *restroute.Request) (any, int, error) {
	items, err := batchInput(req)
	if err != nil {
		return nil, 0, err
	}

	sc := b.scopeOf(ctx)

	if _, err = b.memoryOf(sc, req.Params["memoryId"]); err != nil {
		return nil, 0, err
	}

	b.mu.Lock("BatchDeleteMemoryRecords")
	defer b.mu.Unlock()

	ok, failed := []doc{}, []doc{}

	for _, it := range items {
		item, _ := it.(map[string]any)
		id := strOf(item, keyRecordID)

		if b.records.Delete(recordKey(sc.Account, sc.Region, req.Params["memoryId"], id)) {
			ok = append(ok, doc{keyRecordID: id, keyStatus: statusSucceeded})
		} else {
			failed = append(failed, failedRecord(id, "", errNotFoundCode, "memory record not found"))
		}
	}

	return doc{keySucceeded: ok, keyFailed: failed}, 0, nil
}

func (b *InMemoryBackend) findRecord(sc Scope, req *restroute.Request) (*MemoryRecord, error) {
	if _, err := b.memoryOf(sc, req.Params["memoryId"]); err != nil {
		return nil, err
	}

	rec, ok := b.records.Get(recordKey(sc.Account, sc.Region, req.Params["memoryId"], req.Params[keyRecordID]))
	if !ok || !hasNamespace(rec.Namespaces, req.Query.Get("namespace")) {
		return nil, notFound("memory record %q not found", req.Params[keyRecordID])
	}

	return rec, nil
}

func (b *InMemoryBackend) getRecord(ctx context.Context, req *restroute.Request) (any, int, error) {
	b.mu.RLock("GetMemoryRecord")
	defer b.mu.RUnlock()

	rec, err := b.findRecord(b.scopeOf(ctx), req)
	if err != nil {
		return nil, 0, err
	}

	return doc{"memoryRecord": recordView(rec)}, 0, nil
}

func (b *InMemoryBackend) deleteRecord(ctx context.Context, req *restroute.Request) (any, int, error) {
	b.mu.Lock("DeleteMemoryRecord")
	defer b.mu.Unlock()

	sc := b.scopeOf(ctx)

	rec, err := b.findRecord(sc, req)
	if err != nil {
		return nil, 0, err
	}

	b.records.Delete(recordKey(sc.Account, sc.Region, rec.MemoryID, rec.ID))

	return doc{keyRecordID: rec.ID}, 0, nil
}

type recordQuery struct {
	namespace string
	strategy  string
	filters   []metaCond
}

func parseRecordQuery(in doc, filters []any) (recordQuery, error) {
	q := recordQuery{namespace: strOf(in, "namespace"), strategy: strOf(in, "memoryStrategyId")}
	if q.namespace == "" {
		q.namespace = strOf(in, "namespacePath")
	}

	for _, f := range filters {
		c, err := parseCond(f, recordOps())
		if err != nil {
			return q, err
		}

		q.filters = append(q.filters, c)
	}

	return q, nil
}

func (q recordQuery) match(r *MemoryRecord) bool {
	if q.strategy != "" && r.StrategyID != q.strategy {
		return false
	}

	if !hasNamespace(r.Namespaces, q.namespace) {
		return false
	}

	return slices.IndexFunc(q.filters, func(c metaCond) bool { return !c.matchRecord(r.Metadata) }) < 0
}

func (b *InMemoryBackend) candidates(sc Scope, memory string, q recordQuery) []*MemoryRecord {
	var out []*MemoryRecord

	for _, r := range b.records.All() {
		if r.Account == sc.Account && r.Region == sc.Region && r.MemoryID == memory && q.match(r) {
			out = append(out, r)
		}
	}

	slices.SortFunc(out, func(x, y *MemoryRecord) int {
		return cmp.Or(x.Created.Compare(y.Created), cmp.Compare(x.Seq, y.Seq))
	})

	return out
}

func (b *InMemoryBackend) listRecords(ctx context.Context, req *restroute.Request) (any, int, error) {
	in, err := decodeDoc(req)
	if err != nil {
		return nil, 0, err
	}

	q, err := parseRecordQuery(in, listOf(in, "metadataFilters"))
	if err != nil {
		return nil, 0, err
	}

	b.mu.RLock("ListMemoryRecords")
	defer b.mu.RUnlock()

	sc := b.scopeOf(ctx)

	if _, err = b.memoryOf(sc, req.Params["memoryId"]); err != nil {
		return nil, 0, err
	}

	all := b.candidates(sc, req.Params["memoryId"], q)

	pg, err := restroute.Paginate(all, strOf(in, "nextToken"), intOf(in, "maxResults"), defaultPageSize, maxPageSize)
	if err != nil {
		return nil, 0, err
	}

	return pageBody("memoryRecordSummaries", summaries(pg.Data, nil), pg.Next), 0, nil
}

func summaries(recs []*MemoryRecord, scores map[string]float64) []doc {
	out := make([]doc, 0, len(recs))

	for _, r := range recs {
		v := recordView(r)
		if scores != nil {
			v["score"] = scores[r.ID]
		}

		out = append(out, v)
	}

	return out
}

func (b *InMemoryBackend) retrieveRecords(ctx context.Context, req *restroute.Request) (any, int, error) {
	in, err := decodeDoc(req)
	if err != nil {
		return nil, 0, err
	}

	crit := objOf(in, "searchCriteria")
	if strings.TrimSpace(strOf(crit, "searchQuery")) == "" {
		return nil, 0, invalid("searchCriteria.searchQuery is required")
	}

	q, err := parseRecordQuery(doc{
		"namespace": strOf(in, "namespace"), "namespacePath": strOf(in, "namespacePath"),
		"memoryStrategyId": strOf(crit, "memoryStrategyId"),
	}, listOf(crit, "metadataFilters"))
	if err != nil {
		return nil, 0, err
	}

	topK := intOf(crit, "topK")
	if topK == 0 {
		topK = defaultTopK
	}

	if topK < 1 || topK > maxTopK {
		return nil, 0, invalid("topK must be between 1 and %d", maxTopK)
	}

	b.mu.RLock("RetrieveMemoryRecords")
	defer b.mu.RUnlock()

	sc := b.scopeOf(ctx)

	if _, err = b.memoryOf(sc, req.Params["memoryId"]); err != nil {
		return nil, 0, err
	}

	ranked, scores := rankRecords(b.candidates(sc, req.Params["memoryId"], q), strOf(crit, "searchQuery"), topK)

	pg, err := restroute.Paginate(ranked, strOf(in, "nextToken"), intOf(in, "maxResults"), defaultPageSize, maxPageSize)
	if err != nil {
		return nil, 0, err
	}

	return pageBody("memoryRecordSummaries", summaries(pg.Data, scores), pg.Next), 0, nil
}

func rankRecords(cands []*MemoryRecord, query string, topK int) ([]*MemoryRecord, map[string]float64) {
	texts := make([]string, len(cands))
	for i, c := range cands {
		texts[i] = c.Text
	}

	raw := bm25(query, texts)
	scores := map[string]float64{}
	ranked := make([]*MemoryRecord, 0, len(cands))

	for i, c := range cands {
		if raw[i] > 0 {
			scores[c.ID] = raw[i]
			ranked = append(ranked, c)
		}
	}

	slices.SortStableFunc(ranked, func(x, y *MemoryRecord) int { return cmp.Compare(scores[y.ID], scores[x.ID]) })

	if len(ranked) > topK {
		ranked = ranked[:topK]
	}

	return ranked, scores
}

func (c metaCond) matchRecord(meta map[string]any) bool {
	v, has := meta[c.key]

	switch c.op {
	case opExists:
		return has
	case opNotExists:
		return !has
	}

	if !has {
		return false
	}

	return compareMeta(c.op, objMap(v), objMap(c.right))
}

func objMap(v any) doc {
	m, _ := v.(map[string]any)

	return m
}

func compareMeta(op string, left, right doc) bool {
	switch op {
	case opEquals:
		return jsonEqual(left, right)
	case "CONTAINS":
		return containsMeta(left, right)
	case opBefore, opAfter:
		l, lok := fromEpoch(left["dateTimeValue"])
		r, rok := fromEpoch(right["dateTimeValue"])

		return lok && rok && ((op == opBefore && l.Before(r)) || (op == opAfter && l.After(r)))
	}

	return compareNumber(op, left, right)
}

func containsMeta(left, right doc) bool {
	want := strOf(right, "stringValue")

	if s, ok := left["stringValue"].(string); ok {
		return strings.Contains(s, want)
	}

	return slices.Contains(strList(left, "stringListValue"), want)
}

func compareNumber(op string, left, right doc) bool {
	l, lok := left["numberValue"].(float64)
	r, rok := right["numberValue"].(float64)

	if !lok || !rok {
		return false
	}

	switch op {
	case "GREATER_THAN":
		return l > r
	case "GREATER_THAN_OR_EQUALS":
		return l >= r
	case "LESS_THAN":
		return l < r
	case "LESS_THAN_OR_EQUALS":
		return l <= r
	}

	return false
}
