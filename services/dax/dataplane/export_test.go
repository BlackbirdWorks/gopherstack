package dataplane

import (
	"bufio"
	"bytes"
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// This file exports internal codec entry points for the external test package
// (dataplane_test) so that protocol-level encoding can be exercised without
// loosening the package's public API.

// TestWriter is a CBOR writer exposed for tests.
type TestWriter = Writer

// TestReader is a CBOR reader exposed for tests.
type TestReader = Reader

// NewTestWriter wraps w in a CBOR writer for tests.
func NewTestWriter(w io.Writer) *TestWriter { return NewWriter(w) }

// NewTestReader wraps r in a CBOR reader for tests.
func NewTestReader(r io.Reader) *TestReader { return NewReader(r) }

// WriteInt64ForTest writes a signed 64-bit integer in CBOR form.
func WriteInt64ForTest(w *TestWriter, v int64) error { return w.WriteInt64(v) }

// ReadInt64ForTest reads a signed 64-bit integer from CBOR.
func ReadInt64ForTest(r *TestReader) (int64, error) { return r.ReadInt64() }

// EncodeAttributeValueForTest encodes a single AttributeValue in DAX CBOR form.
func EncodeAttributeValueForTest(w *TestWriter, v types.AttributeValue) error {
	return encodeAttributeValue(w, v)
}

// DecodeAttributeValueForTest decodes a single AttributeValue from DAX CBOR.
func DecodeAttributeValueForTest(r *TestReader) (types.AttributeValue, error) {
	return decodeAttributeValue(r)
}

// KeyPart describes one primary-key component for tests.
type KeyPart struct {
	Name string
	Type types.ScalarAttributeType
}

// EncodeItemKeyForTest encodes an item's primary key against a key schema.
func EncodeItemKeyForTest(w *TestWriter, item map[string]types.AttributeValue, parts []KeyPart) error {
	return encodeItemKey(w, item, toKeySchema(parts))
}

// DecodeItemKeyForTest decodes a primary key against a key schema.
func DecodeItemKeyForTest(r *TestReader, parts []KeyPart) (map[string]types.AttributeValue, error) {
	return decodeItemKey(r, toKeySchema(parts))
}

func toKeySchema(parts []KeyPart) keySchema {
	ks := make(keySchema, len(parts))
	for i, p := range parts {
		ks[i] = keyAttr{name: p.Name, typ: p.Type}
	}

	return ks
}

// LexDecimalRoundTripForTest encodes a DynamoDB number literal as a
// lexicographic-decimal range key and decodes it back, returning the recovered
// literal.
func LexDecimalRoundTripForTest(literal string) (string, error) {
	d := &decimal{}
	if !d.setString(literal) {
		return "", errBadLexDecimal
	}

	var buf bytes.Buffer
	bw := bufio.NewWriter(&buf)

	if err := encodeLexDecimal(d, bw); err != nil {
		return "", err
	}

	if err := bw.Flush(); err != nil {
		return "", err
	}

	got, err := decodeLexDecimal(bufio.NewReader(bytes.NewReader(buf.Bytes())))
	if err != nil {
		return "", err
	}

	return got.String(), nil
}

// LexDecimalEncodeForTest returns the lexicographic-decimal byte encoding of a
// DynamoDB number literal, for sort-order assertions.
func LexDecimalEncodeForTest(literal string) ([]byte, error) {
	d := &decimal{}
	if !d.setString(literal) {
		return nil, errBadLexDecimal
	}

	var buf bytes.Buffer
	bw := bufio.NewWriter(&buf)

	if err := encodeLexDecimal(d, bw); err != nil {
		return nil, err
	}

	if err := bw.Flush(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// DecimalNormalizeForTest parses and re-renders a number literal through the
// codec's decimal type, for value comparisons that tolerate exponent form.
func DecimalNormalizeForTest(literal string) (string, bool) {
	d := &decimal{}
	if !d.setString(literal) {
		return "", false
	}

	return d.String(), true
}

// Expression kind constants exported for tests.
const (
	KindUpdate     = int(kindUpdate)
	KindCondition  = int(kindCondition)
	KindProjection = int(kindProjection)
)

// DecodedExpression is the test-visible result of decoding one or more DAX
// expression blobs.
type DecodedExpression struct {
	Names  map[string]string
	Values map[string]types.AttributeValue
	Expr   string
}

// DecodeExpressionBlobForTest decodes a single encoded expression blob of the
// given kind and returns the reconstructed expression and attribute maps.
func DecodeExpressionBlobForTest(blob []byte, kind int) (DecodedExpression, error) {
	dec := newDecodedExpression()

	expr, err := dec.decodeBlob(blob, exprKind(kind))
	if err != nil {
		return DecodedExpression{}, err
	}

	return DecodedExpression{Expr: expr, Names: dec.names, Values: dec.values}, nil
}

// WriteTransactWriteResponseForTest emits the TransactWriteItems success body.
func WriteTransactWriteResponseForTest(w *TestWriter) error {
	return writeTransactWriteResponse(w)
}

// AttrListServer wraps a Server so tests can emit an attribute-projection
// payload and then resolve the ordinals it used via the registered name list.
type AttrListServer struct {
	s *Server
}

// NewAttrListServerForTest builds a server wrapper whose attribute-list id
// allocations can be inspected by tests.
func NewAttrListServerForTest() *AttrListServer {
	return &AttrListServer{s: NewServer(context.TODO(), nil)}
}

// WriteAttributeProjection emits an attribute-projection payload via the wrapped
// server, so the test can then resolve ordinals through Names.
func (a *AttrListServer) WriteAttributeProjection(w *TestWriter, attrs map[string]types.AttributeValue) error {
	return a.s.writeAttributeProjection(w, attrs)
}

// Names returns the attribute names registered for an id.
func (a *AttrListServer) Names(id int64) []string { return a.s.attrListNames(id) }

// AllocID registers an attribute-name list and returns its id, exposing the
// internal allocator so tests can exercise the dictionary capacity bound.
func (a *AttrListServer) AllocID(names []string) int64 { return a.s.attrListID(names) }

// DictLen reports the number of entries in the attribute-list dictionary.
func (a *AttrListServer) DictLen() int { return len(a.s.attrToID) }

// EmptyAttributeListIDForTest exposes the reserved empty-list id.
func EmptyAttributeListIDForTest() int64 { return emptyAttributeListID }

// AttrListMaxCapForTest exposes the dictionary capacity bound.
func AttrListMaxCapForTest() int { return attrListMaxCap }

// ProjectedItemForTest decodes a ProjectionExpression blob, then projects the
// given item against it and returns the ordinal/value entries that a Query/Scan
// projected response would emit, along with the reconstructed projection-
// expression string. Used to verify the projection response sub-protocol.
func ProjectedItemForTest(
	blob []byte, item map[string]types.AttributeValue,
) (string, map[int]types.AttributeValue, error) {
	dec := newDecodedExpression()

	proj, err := dec.decodeProjectionBlob(blob)
	if err != nil {
		return "", nil, err
	}

	out := map[int]types.AttributeValue{}
	for _, e := range projectedEntries(item, proj.ordinals) {
		out[e.ordinal] = e.value
	}

	return proj.expression, out, nil
}

// Expression op-code constants exported for building test blobs.
const (
	OpEqual        = opEqual
	OpBeginsWith   = opBeginsWith
	OpBetween      = opBetween
	OpVariable     = opVariable
	OpDocumentPath = opDocumentPath
	OpSetAction    = opSetAction
	OpRemoveAction = opRemoveAction
	ExprVersion    = exprEncodingVersion

	// TagDocumentPathOrdinal is the CBOR tag for a list-access index in a
	// document path.
	TagDocumentPathOrdinal = tagDocumentPathOrdinal
)
