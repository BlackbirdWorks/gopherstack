package dataplane_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dax/dataplane"
)

// These tests build encoded expression blobs in the exact CBOR s-expression
// shape produced by github.com/aws/aws-dax-go/dax/internal/parser and verify the
// decoder reconstructs an equivalent DynamoDB expression string plus the
// expression attribute name/value maps. End-to-end correctness against the real
// client encoder is covered by the integration tests.

// blobBuilder assembles an encoded expression blob using the package's CBOR
// writer.
type blobBuilder struct {
	w   *dataplane.TestWriter
	buf bytes.Buffer
}

func newBlobBuilder() *blobBuilder {
	b := &blobBuilder{}
	b.w = dataplane.NewTestWriter(&b.buf)

	return b
}

func (b *blobBuilder) bytes(t *testing.T) []byte {
	t.Helper()

	if err := b.w.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	return b.buf.Bytes()
}

// writeFullExpr writes [version, tree, [values...]].
func writeFullExpr(t *testing.T, tree func(w *dataplane.TestWriter), vals []types.AttributeValue) []byte {
	t.Helper()

	b := newBlobBuilder()
	mustWrite(t, b.w.WriteArrayHeader(3))
	mustWrite(t, b.w.WriteInt(dataplane.ExprVersion))
	tree(b.w)
	mustWrite(t, b.w.WriteArrayHeader(len(vals)))

	for _, v := range vals {
		mustWrite(t, dataplane.EncodeAttributeValueForTest(b.w, v))
	}

	return b.bytes(t)
}

func mustWrite(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatalf("write: %v", err)
	}
}

// docPath writes an opDocumentPath node for a path of one or more attribute
// name elements (e.g. "a" or "b","c" for b.c).
func docPath(t *testing.T, w *dataplane.TestWriter, names ...string) {
	t.Helper()
	mustWrite(t, w.WriteArrayHeader(1+len(names)))
	mustWrite(t, w.WriteInt(dataplane.OpDocumentPath))

	for _, n := range names {
		mustWrite(t, w.WriteString(n))
	}
}

// docPathIndexed writes an opDocumentPath node for name[idx], encoding the index
// as the tag-3324 list-access atom the DAX parser emits.
func docPathIndexed(t *testing.T, w *dataplane.TestWriter, name string, idx int64) {
	t.Helper()
	mustWrite(t, w.WriteArrayHeader(3))
	mustWrite(t, w.WriteInt(dataplane.OpDocumentPath))
	mustWrite(t, w.WriteString(name))
	mustWrite(t, w.WriteTag(dataplane.TagDocumentPathOrdinal))
	mustWrite(t, dataplane.WriteInt64ForTest(w, idx))
}

// variable writes an opVariable node referencing value index id.
func variable(t *testing.T, w *dataplane.TestWriter, id int) {
	t.Helper()
	mustWrite(t, w.WriteArrayHeader(2))
	mustWrite(t, w.WriteInt(dataplane.OpVariable))
	mustWrite(t, w.WriteInt(id))
}

func num(v string) types.AttributeValue { return &types.AttributeValueMemberN{Value: v} }
func str(v string) types.AttributeValue { return &types.AttributeValueMemberS{Value: v} }

func TestDecodeConditionExpression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantVals map[string]string
		name     string
		wantExpr string
		wantName string
		blob     []byte
	}{
		{
			name: "equality",
			blob: writeFullExpr(t, func(w *dataplane.TestWriter) {
				mustWrite(t, w.WriteArrayHeader(3))
				mustWrite(t, w.WriteInt(dataplane.OpEqual))
				docPath(t, w, "score")
				variable(t, w, 0)
			}, []types.AttributeValue{num("10")}),
			wantExpr: "#n0 = :v0",
			wantVals: map[string]string{":v0": "10"},
			wantName: "score",
		},
		{
			name: "begins_with function",
			blob: writeFullExpr(t, func(w *dataplane.TestWriter) {
				mustWrite(t, w.WriteArrayHeader(3))
				mustWrite(t, w.WriteInt(dataplane.OpBeginsWith))
				docPath(t, w, "name")
				variable(t, w, 0)
			}, []types.AttributeValue{str("Ad")}),
			wantExpr: "begins_with(#n0, :v0)",
			wantVals: map[string]string{":v0": "Ad"},
			wantName: "name",
		},
		{
			name: "between",
			blob: writeFullExpr(t, func(w *dataplane.TestWriter) {
				mustWrite(t, w.WriteArrayHeader(4))
				mustWrite(t, w.WriteInt(dataplane.OpBetween))
				docPath(t, w, "n")
				variable(t, w, 0)
				variable(t, w, 1)
			}, []types.AttributeValue{num("1"), num("9")}),
			wantExpr: "#n0 BETWEEN :v0 AND :v1",
			wantVals: map[string]string{":v0": "1", ":v1": "9"},
			wantName: "n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := dataplane.DecodeExpressionBlobForTest(tc.blob, dataplane.KindCondition)
			if err != nil {
				t.Fatalf("DecodeExpressionBlobForTest: %v", err)
			}

			if got.Expr != tc.wantExpr {
				t.Fatalf("expr: got %q want %q", got.Expr, tc.wantExpr)
			}

			if name := got.Names["#n0"]; name != tc.wantName {
				t.Fatalf("name #n0: got %q want %q", name, tc.wantName)
			}

			assertDecodedValues(t, got.Values, tc.wantVals)
		})
	}
}

func TestDecodeUpdateExpression(t *testing.T) {
	t.Parallel()

	// SET score = :v0
	blob := writeFullExpr(t, func(w *dataplane.TestWriter) {
		mustWrite(t, w.WriteArrayHeader(1)) // one action
		mustWrite(t, w.WriteArrayHeader(3)) // [opSetAction, path, value]
		mustWrite(t, w.WriteInt(dataplane.OpSetAction))
		docPath(t, w, "score")
		variable(t, w, 0)
	}, []types.AttributeValue{num("99")})

	got, err := dataplane.DecodeExpressionBlobForTest(blob, dataplane.KindUpdate)
	if err != nil {
		t.Fatalf("DecodeExpressionBlobForTest: %v", err)
	}

	const want = "SET #n0 = :v0"
	if got.Expr != want {
		t.Fatalf("update expr: got %q want %q", got.Expr, want)
	}

	if got.Names["#n0"] != "score" {
		t.Fatalf("name #n0: got %q want %q", got.Names["#n0"], "score")
	}

	assertDecodedValues(t, got.Values, map[string]string{":v0": "99"})
}

func TestDecodeUpdateExpressionMultipleClauses(t *testing.T) {
	t.Parallel()

	// SET a = :v0 REMOVE b
	blob := writeFullExpr(t, func(w *dataplane.TestWriter) {
		mustWrite(t, w.WriteArrayHeader(2))

		mustWrite(t, w.WriteArrayHeader(3))
		mustWrite(t, w.WriteInt(dataplane.OpSetAction))
		docPath(t, w, "a")
		variable(t, w, 0)

		mustWrite(t, w.WriteArrayHeader(2))
		mustWrite(t, w.WriteInt(dataplane.OpRemoveAction))
		docPath(t, w, "b")
	}, []types.AttributeValue{num("1")})

	got, err := dataplane.DecodeExpressionBlobForTest(blob, dataplane.KindUpdate)
	if err != nil {
		t.Fatalf("DecodeExpressionBlobForTest: %v", err)
	}

	const want = "SET #n0 = :v0 REMOVE #n1"
	if got.Expr != want {
		t.Fatalf("update expr: got %q want %q", got.Expr, want)
	}
}

// TestDecodeProjectionBlob verifies that a ProjectionExpression blob decodes to
// the expected expression string and that projecting an item against it yields
// the ordinal-keyed entries the response sub-protocol emits, including nested
// map paths and list-index paths.
func TestDecodeProjectionBlob(t *testing.T) {
	t.Parallel()

	// projection blob: [version, [path0, path1, path2]] for "a, b.c, d[1]".
	b := newBlobBuilder()
	mustWrite(t, b.w.WriteArrayHeader(2))
	mustWrite(t, b.w.WriteInt(dataplane.ExprVersion))
	mustWrite(t, b.w.WriteArrayHeader(3))
	docPath(t, b.w, "a")
	docPath(t, b.w, "b", "c")
	docPathIndexed(t, b.w, "d", 1)

	item := map[string]types.AttributeValue{
		"a": &types.AttributeValueMemberS{Value: "av"},
		"b": &types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
			"c": &types.AttributeValueMemberN{Value: "7"},
		}},
		"d": &types.AttributeValueMemberL{Value: []types.AttributeValue{
			&types.AttributeValueMemberN{Value: "0"},
			&types.AttributeValueMemberS{Value: "d1"},
		}},
		"ignored": &types.AttributeValueMemberS{Value: "no"},
	}

	expr, entries, err := dataplane.ProjectedItemForTest(b.bytes(t), item)
	if err != nil {
		t.Fatalf("ProjectedItemForTest: %v", err)
	}

	const wantExpr = "#n0, #n1.#n2, #n3[1]"
	if expr != wantExpr {
		t.Fatalf("projection expr: got %q want %q", expr, wantExpr)
	}

	wantOrdinals := map[int]string{0: "av", 1: "7", 2: "d1"}
	if len(entries) != len(wantOrdinals) {
		t.Fatalf("entry count: got %d want %d", len(entries), len(wantOrdinals))
	}

	for ord, want := range wantOrdinals {
		got := renderAttrValue(t, entries[ord])
		if got != want {
			t.Fatalf("ordinal %d: got %q want %q", ord, got, want)
		}
	}
}

// TestDecodeProjectionMissingPath verifies that a projection path that does not
// resolve in the item is omitted from the response entries (matching the real
// service), rather than emitting a null.
func TestDecodeProjectionMissingPath(t *testing.T) {
	t.Parallel()

	b := newBlobBuilder()
	mustWrite(t, b.w.WriteArrayHeader(2))
	mustWrite(t, b.w.WriteInt(dataplane.ExprVersion))
	mustWrite(t, b.w.WriteArrayHeader(2))
	docPath(t, b.w, "present")
	docPath(t, b.w, "absent")

	item := map[string]types.AttributeValue{
		"present": &types.AttributeValueMemberS{Value: "yes"},
	}

	_, entries, err := dataplane.ProjectedItemForTest(b.bytes(t), item)
	if err != nil {
		t.Fatalf("ProjectedItemForTest: %v", err)
	}

	if len(entries) != 1 {
		t.Fatalf("entry count: got %d want 1", len(entries))
	}

	if got := renderAttrValue(t, entries[0]); got != "yes" {
		t.Fatalf("ordinal 0: got %q want %q", got, "yes")
	}
}

func assertDecodedValues(t *testing.T, values map[string]types.AttributeValue, want map[string]string) {
	t.Helper()

	if len(values) != len(want) {
		t.Fatalf("value count: got %d want %d", len(values), len(want))
	}

	for ph, wantVal := range want {
		av, ok := values[ph]
		if !ok {
			t.Fatalf("missing value placeholder %q", ph)
		}

		got := renderAttrValue(t, av)
		if got != wantVal {
			t.Fatalf("value %q: got %q want %q", ph, got, wantVal)
		}
	}
}

func renderAttrValue(t *testing.T, av types.AttributeValue) string {
	t.Helper()

	switch v := av.(type) {
	case *types.AttributeValueMemberN:
		return v.Value
	case *types.AttributeValueMemberS:
		return v.Value
	default:
		t.Fatalf("unexpected attribute value type %T", av)

		return ""
	}
}
