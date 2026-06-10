package dataplane

import (
	"bytes"
	"maps"
	"sort"

	awsddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func newByteReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }

func newByteBuffer() *bytes.Buffer { return &bytes.Buffer{} }

// Request optional-parameter ordinals (subset), matching the DAX client.
const (
	reqParamConsistentRead = 2
	reqParamReturnValues   = 7
)

// Response body ordinals (subset), matching the DAX client.
const (
	respParamItem       = 0
	respParamAttributes = 2
)

// DAX returnValue ordinals.
const (
	rvNone = 1 + iota
	rvAllOld
	rvUpdatedOld
	rvAllNew
	rvUpdatedNew
)

// itemOpParams holds the optional parameters decoded from an item operation.
type itemOpParams struct {
	consistentRead bool
	returnValues   int
}

// readItemOptionalParams decodes the indefinite-length optional-params map sent
// after the key for item operations, extracting the fields gopherstack acts on
// and safely skipping the rest (including pre-parsed expression blobs).
func readItemOptionalParams(r *Reader) (itemOpParams, error) {
	p := itemOpParams{returnValues: rvNone}

	hdr, err := r.PeekHeader()
	if err != nil {
		return p, err
	}

	// The client always writes an indefinite-length map (0xbf).
	if hdr != majorMap+minorTypeMask {
		// Defensive: a definite map is also valid CBOR.
		return readDefiniteItemParams(r, p)
	}

	if _, err = r.r.ReadByte(); err != nil { // consume map-stream header
		return p, err
	}

	for {
		next, e := r.PeekHeader()
		if e != nil {
			return p, e
		}

		if next == streamBreak {
			_, e = r.r.ReadByte()

			return p, e
		}

		if err = decodeOneItemParam(r, &p); err != nil {
			return p, err
		}
	}
}

func readDefiniteItemParams(r *Reader, p itemOpParams) (itemOpParams, error) {
	n, err := r.ReadMapLength()
	if err != nil {
		return p, err
	}

	for range n {
		if err = decodeOneItemParam(r, &p); err != nil {
			return p, err
		}
	}

	return p, nil
}

func decodeOneItemParam(r *Reader, p *itemOpParams) error {
	key, err := r.ReadInt()
	if err != nil {
		return err
	}

	switch key {
	case reqParamConsistentRead:
		b, e := r.readBoolOrInt()
		if e != nil {
			return e
		}

		p.consistentRead = b
	case reqParamReturnValues:
		v, e := r.ReadInt()
		if e != nil {
			return e
		}

		p.returnValues = v
	default:
		return r.skip()
	}

	return nil
}

// readBoolOrInt accepts either a CBOR bool or an int (the client has used both
// encodings for consistentRead across versions).
func (r *Reader) readBoolOrInt() (bool, error) {
	hdr, err := r.PeekHeader()
	if err != nil {
		return false, err
	}

	if hdr == simpleTrue || hdr == simpleFalse {
		_, e := r.r.ReadByte()

		return hdr == simpleTrue, e
	}

	v, e := r.ReadInt()

	return v != 0, e
}

// handleGetItem decodes a GetItem request, delegates to the backend, and writes
// the DAX-shaped response.
func (s *Server) handleGetItem(r *Reader, w *Writer) error {
	table, key, _, err := s.readKeyedRequest(r)
	if err != nil {
		return s.writeError(w, statusBadRequest, "ValidationException", err.Error())
	}

	ctx, cancel := requestContext()
	defer cancel()

	out, err := s.backend.GetItem(ctx, &awsddb.GetItemInput{TableName: &table, Key: key})
	if err != nil {
		return s.writeBackendError(w, err)
	}

	if err = writeOK(w); err != nil {
		return err
	}

	ks, err := s.schemaFor(ctx, table)
	if err != nil {
		return err
	}

	if len(out.Item) == 0 {
		return w.WriteNull()
	}

	return s.writeItemMap(w, respParamItem, out.Item, ks)
}

// handlePutItem decodes a PutItem request (key bytes + non-key attributes) and
// delegates to the backend.
func (s *Server) handlePutItem(r *Reader, w *Writer) error {
	table, err := readTable(r)
	if err != nil {
		return s.writeError(w, statusBadRequest, "ValidationException", err.Error())
	}

	ctx, cancel := requestContext()
	defer cancel()

	ks, err := s.schemaFor(ctx, table)
	if err != nil {
		return s.writeError(w, statusBadRequest, "ResourceNotFoundException", err.Error())
	}

	key, err := decodeItemKey(r, ks)
	if err != nil {
		return s.writeError(w, statusBadRequest, "ValidationException", err.Error())
	}

	nonKey, err := s.readNonKeyAttributes(r)
	if err != nil {
		return s.writeError(w, statusBadRequest, "ValidationException", err.Error())
	}

	item := mergeItem(key, nonKey)

	params, err := readItemOptionalParams(r)
	if err != nil {
		return err
	}

	in := &awsddb.PutItemInput{TableName: &table, Item: item}
	if params.returnValues == rvAllOld {
		in.ReturnValues = types.ReturnValueAllOld
	}

	out, err := s.backend.PutItem(ctx, in)
	if err != nil {
		return s.writeBackendError(w, err)
	}

	if err = writeOK(w); err != nil {
		return err
	}

	if len(out.Attributes) == 0 {
		return w.WriteNull()
	}

	return s.writeAttributesResponse(w, out.Attributes, ks)
}

// handleDeleteItem decodes a DeleteItem request and delegates to the backend.
func (s *Server) handleDeleteItem(r *Reader, w *Writer) error {
	table, key, params, err := s.readKeyedRequest(r)
	if err != nil {
		return s.writeError(w, statusBadRequest, "ValidationException", err.Error())
	}

	ctx, cancel := requestContext()
	defer cancel()

	in := &awsddb.DeleteItemInput{TableName: &table, Key: key}
	if params.returnValues == rvAllOld {
		in.ReturnValues = types.ReturnValueAllOld
	}

	out, err := s.backend.DeleteItem(ctx, in)
	if err != nil {
		return s.writeBackendError(w, err)
	}

	if err = writeOK(w); err != nil {
		return err
	}

	ks, err := s.schemaFor(ctx, table)
	if err != nil {
		return err
	}

	if len(out.Attributes) == 0 {
		return w.WriteNull()
	}

	return s.writeAttributesResponse(w, out.Attributes, ks)
}

// readKeyedRequest reads the common [table, key, optionalParams] prefix shared
// by GetItem and DeleteItem.
func (s *Server) readKeyedRequest(r *Reader) (string, map[string]types.AttributeValue, itemOpParams, error) {
	table, err := readTable(r)
	if err != nil {
		return "", nil, itemOpParams{}, err
	}

	ctx, cancel := requestContext()
	defer cancel()

	ks, err := s.schemaFor(ctx, table)
	if err != nil {
		return "", nil, itemOpParams{}, err
	}

	key, err := decodeItemKey(r, ks)
	if err != nil {
		return "", nil, itemOpParams{}, err
	}

	params, err := readItemOptionalParams(r)
	if err != nil {
		return "", nil, itemOpParams{}, err
	}

	return table, key, params, nil
}

// readNonKeyAttributes decodes the byte-wrapped [attrListId, values...] payload
// that PutItem sends for an item's non-key attributes.
func (s *Server) readNonKeyAttributes(r *Reader) (map[string]types.AttributeValue, error) {
	blob, err := r.ReadBytes()
	if err != nil {
		return nil, err
	}

	inner := NewReader(newByteReader(blob))

	id, err := inner.ReadInt64()
	if err != nil {
		return nil, err
	}

	names := s.attrListNames(id)

	attrs := make(map[string]types.AttributeValue, len(names))

	for _, name := range names {
		av, e := decodeAttributeValue(inner)
		if e != nil {
			return nil, e
		}

		attrs[name] = av
	}

	return attrs, nil
}

// writeItemMap writes a single-entry response map {ordinal: nonKeyAttrsBlob}.
func (s *Server) writeItemMap(
	w *Writer, ordinal int, item map[string]types.AttributeValue, ks keySchema,
) error {
	if err := w.WriteMapHeader(1); err != nil {
		return err
	}

	if err := w.WriteInt(ordinal); err != nil {
		return err
	}

	return s.writeNonKeyAttributes(w, item, ks)
}

// writeAttributesResponse writes the {responseParamAttributes: blob} map used by
// Put/Delete/Update responses that return prior item attributes.
func (s *Server) writeAttributesResponse(
	w *Writer, attrs map[string]types.AttributeValue, ks keySchema,
) error {
	return s.writeItemMap(w, respParamAttributes, attrs, ks)
}

// writeNonKeyAttributes encodes the non-key attributes of an item as the DAX
// byte-wrapped [attrListId, values...] payload. Key attributes are excluded
// because the client reconstructs them from the request key.
func (s *Server) writeNonKeyAttributes(w *Writer, item map[string]types.AttributeValue, ks keySchema) error {
	keyNames := ks.keyNames()

	names := make([]string, 0, len(item))

	for name := range item {
		if _, isKey := keyNames[name]; !isKey {
			names = append(names, name)
		}
	}

	sort.Strings(names)

	id, sorted := s.idForAttrNames(names)

	buf := newByteBuffer()
	inner := NewWriter(buf)

	if err := inner.WriteInt64(id); err != nil {
		return err
	}

	for _, name := range sorted {
		if err := encodeAttributeValue(inner, item[name]); err != nil {
			return err
		}
	}

	if err := inner.Flush(); err != nil {
		return err
	}

	return w.WriteBytes(buf.Bytes())
}

// writeBackendError maps a DynamoDB backend error onto a DAX error response.
func (s *Server) writeBackendError(w *Writer, err error) error {
	return s.writeError(w, statusBadRequest, "ValidationException", err.Error())
}

func mergeItem(key, nonKey map[string]types.AttributeValue) map[string]types.AttributeValue {
	item := make(map[string]types.AttributeValue, len(key)+len(nonKey))
	maps.Copy(item, nonKey)
	maps.Copy(item, key)

	return item
}

func readTable(r *Reader) (string, error) {
	b, err := r.ReadBytes()
	if err != nil {
		return "", err
	}

	return string(b), nil
}
