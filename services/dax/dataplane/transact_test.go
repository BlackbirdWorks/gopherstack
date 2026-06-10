package dataplane_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dax/dataplane"
)

// TestWriteTransactWriteResponse verifies the TransactWriteItems success body is
// the [returnValues, consumedCapacity, itemCollectionMetrics] shape the client
// decodes: a 3-element array of (empty array, empty array, empty map).
func TestWriteTransactWriteResponse(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	w := dataplane.NewTestWriter(&buf)
	if err := dataplane.WriteTransactWriteResponseForTest(w); err != nil {
		t.Fatalf("write: %v", err)
	}

	if err := w.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	r := dataplane.NewTestReader(&buf)

	n, err := r.ReadArrayLength()
	if err != nil {
		t.Fatalf("outer array: %v", err)
	}

	if n != 3 {
		t.Fatalf("outer arity: got %d want 3", n)
	}

	if got, e := r.ReadArrayLength(); e != nil || got != 0 {
		t.Fatalf("returnValues: got %d err %v want 0", got, e)
	}

	if got, e := r.ReadArrayLength(); e != nil || got != 0 {
		t.Fatalf("consumedCapacity: got %d err %v want 0", got, e)
	}

	if got, e := r.ReadMapLength(); e != nil || got != 0 {
		t.Fatalf("itemCollectionMetrics: got %d err %v want 0", got, e)
	}
}

// TestWriteAttributeProjection verifies the UpdateItem UPDATED_* attribute-
// projection payload: a byte string wrapping [attrListId, {ord: value}] where
// the ordinals index the registered, sorted attribute-name list.
func TestWriteAttributeProjection(t *testing.T) {
	t.Parallel()

	attrs := map[string]types.AttributeValue{
		"zeta":  &types.AttributeValueMemberN{Value: "2"},
		"alpha": &types.AttributeValueMemberS{Value: "x"},
	}

	srv := dataplane.NewAttrListServerForTest()

	var buf bytes.Buffer

	w := dataplane.NewTestWriter(&buf)
	if err := srv.WriteAttributeProjection(w, attrs); err != nil {
		t.Fatalf("write: %v", err)
	}

	if err := w.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// The payload is a single byte string; read it, then decode the inner CBOR.
	outer := dataplane.NewTestReader(&buf)

	blob, err := outer.ReadBytes()
	if err != nil {
		t.Fatalf("read outer bytes: %v", err)
	}

	inner := dataplane.NewTestReader(bytes.NewReader(blob))

	id, err := dataplane.ReadInt64ForTest(inner)
	if err != nil {
		t.Fatalf("read attrListId: %v", err)
	}

	names := srv.Names(id)
	if len(names) != 2 {
		t.Fatalf("registered names: got %d want 2", len(names))
	}

	mapLen, err := inner.ReadMapLength()
	if err != nil {
		t.Fatalf("read map len: %v", err)
	}

	if mapLen != 2 {
		t.Fatalf("projection map len: got %d want 2", mapLen)
	}

	got := map[string]string{}

	for range mapLen {
		ord, e := inner.ReadInt()
		if e != nil {
			t.Fatalf("read ordinal: %v", e)
		}

		if ord < 0 || ord >= len(names) {
			t.Fatalf("ordinal %d out of range for %v", ord, names)
		}

		av, e := dataplane.DecodeAttributeValueForTest(inner)
		if e != nil {
			t.Fatalf("decode value: %v", e)
		}

		got[names[ord]] = renderAttrValue(t, av)
	}

	want := map[string]string{"alpha": "x", "zeta": "2"}
	for k, wv := range want {
		if got[k] != wv {
			t.Fatalf("attr %q: got %q want %q", k, got[k], wv)
		}
	}
}
