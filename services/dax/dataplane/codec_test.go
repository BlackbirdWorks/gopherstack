package dataplane_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dax/dataplane"
)

// roundTripAttr encodes then decodes an AttributeValue and returns the result.
func roundTripAttr(t *testing.T, in types.AttributeValue) types.AttributeValue {
	t.Helper()

	var buf bytes.Buffer

	w := dataplane.NewTestWriter(&buf)
	if err := dataplane.EncodeAttributeValueForTest(w, in); err != nil {
		t.Fatalf("encode: %v", err)
	}

	if err := w.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	out, err := dataplane.DecodeAttributeValueForTest(dataplane.NewTestReader(&buf))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	return out
}

func TestAttributeValueRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   types.AttributeValue
		want types.AttributeValue
		name string
	}{
		{&types.AttributeValueMemberS{Value: "hello"}, &types.AttributeValueMemberS{Value: "hello"}, "string"},
		{&types.AttributeValueMemberS{Value: ""}, &types.AttributeValueMemberS{Value: ""}, "empty string"},
		{&types.AttributeValueMemberN{Value: "42"}, &types.AttributeValueMemberN{Value: "42"}, "int"},
		{&types.AttributeValueMemberN{Value: "-7"}, &types.AttributeValueMemberN{Value: "-7"}, "negative int"},
		{
			&types.AttributeValueMemberN{Value: "123456789012345678901234567890"},
			&types.AttributeValueMemberN{Value: "123456789012345678901234567890"},
			"big int",
		},
		// Decimals are normalised to the unscaled*10^-scale form on the wire.
		{&types.AttributeValueMemberN{Value: "3.14"}, &types.AttributeValueMemberN{Value: "314E-2"}, "decimal"},
		{
			&types.AttributeValueMemberB{Value: []byte{1, 2, 3}},
			&types.AttributeValueMemberB{Value: []byte{1, 2, 3}},
			"binary",
		},
		{&types.AttributeValueMemberBOOL{Value: true}, &types.AttributeValueMemberBOOL{Value: true}, "bool true"},
		{&types.AttributeValueMemberBOOL{Value: false}, &types.AttributeValueMemberBOOL{Value: false}, "bool false"},
		{&types.AttributeValueMemberNULL{Value: true}, &types.AttributeValueMemberNULL{Value: true}, "null"},
		{
			&types.AttributeValueMemberSS{Value: []string{"a", "b"}},
			&types.AttributeValueMemberSS{Value: []string{"a", "b"}},
			"string set",
		},
		{
			&types.AttributeValueMemberNS{Value: []string{"1", "2"}},
			&types.AttributeValueMemberNS{Value: []string{"1", "2"}},
			"number set",
		},
		{
			&types.AttributeValueMemberBS{Value: [][]byte{{1}, {2}}},
			&types.AttributeValueMemberBS{Value: [][]byte{{1}, {2}}},
			"binary set",
		},
		{
			&types.AttributeValueMemberL{Value: []types.AttributeValue{
				&types.AttributeValueMemberS{Value: "x"},
				&types.AttributeValueMemberN{Value: "1"},
			}},
			&types.AttributeValueMemberL{Value: []types.AttributeValue{
				&types.AttributeValueMemberS{Value: "x"},
				&types.AttributeValueMemberN{Value: "1"},
			}},
			"list",
		},
		{
			&types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				"k": &types.AttributeValueMemberS{Value: "v"},
			}},
			&types.AttributeValueMemberM{Value: map[string]types.AttributeValue{
				"k": &types.AttributeValueMemberS{Value: "v"},
			}},
			"map",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := roundTripAttr(t, tt.in)
			if !attrEqual(got, tt.want) {
				t.Fatalf("round trip mismatch: got %#v want %#v", got, tt.want)
			}
		})
	}
}

func TestItemKeyRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		item map[string]types.AttributeValue
		name string
		ks   []dataplane.KeyPart
	}{
		{
			item: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "user#1"}},
			name: "string hash only",
			ks:   []dataplane.KeyPart{{Name: "pk", Type: types.ScalarAttributeTypeS}},
		},
		{
			item: map[string]types.AttributeValue{"id": &types.AttributeValueMemberN{Value: "12345"}},
			name: "number hash only",
			ks:   []dataplane.KeyPart{{Name: "id", Type: types.ScalarAttributeTypeN}},
		},
		{
			item: map[string]types.AttributeValue{"b": &types.AttributeValueMemberB{Value: []byte{9, 8, 7}}},
			name: "binary hash only",
			ks:   []dataplane.KeyPart{{Name: "b", Type: types.ScalarAttributeTypeB}},
		},
		{
			item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "tenant#7"},
				"sk": &types.AttributeValueMemberS{Value: "order#42"},
			},
			name: "string hash + string range",
			ks: []dataplane.KeyPart{
				{Name: "pk", Type: types.ScalarAttributeTypeS},
				{Name: "sk", Type: types.ScalarAttributeTypeS},
			},
		},
		{
			item: map[string]types.AttributeValue{
				"h": &types.AttributeValueMemberB{Value: []byte{1, 2}},
				"r": &types.AttributeValueMemberB{Value: []byte{3, 4, 5}},
			},
			name: "binary hash + binary range",
			ks: []dataplane.KeyPart{
				{Name: "h", Type: types.ScalarAttributeTypeB},
				{Name: "r", Type: types.ScalarAttributeTypeB},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			w := dataplane.NewTestWriter(&buf)
			if err := dataplane.EncodeItemKeyForTest(w, tt.item, tt.ks); err != nil {
				t.Fatalf("encode key: %v", err)
			}

			if err := w.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}

			got, err := dataplane.DecodeItemKeyForTest(dataplane.NewTestReader(&buf), tt.ks)
			if err != nil {
				t.Fatalf("decode key: %v", err)
			}

			if len(got) != len(tt.item) {
				t.Fatalf("key attr count: got %d want %d", len(got), len(tt.item))
			}

			for k, want := range tt.item {
				if !attrEqual(got[k], want) {
					t.Fatalf("key %q: got %#v want %#v", k, got[k], want)
				}
			}
		})
	}
}

func TestIntegerEncodingBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		val  int64
	}{
		{"zero", 0},
		{"small positive", 23},
		{"size8 boundary", 24},
		{"size16 boundary", 256},
		{"size32 boundary", 65536},
		{"large", 4294967296},
		{"small negative", -1},
		{"method id", -2106490455},
		{"max int64", 1<<63 - 1},
		{"min int64", -1 << 63},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer

			w := dataplane.NewTestWriter(&buf)
			if err := dataplane.WriteInt64ForTest(w, tt.val); err != nil {
				t.Fatalf("write: %v", err)
			}

			if err := w.Flush(); err != nil {
				t.Fatalf("flush: %v", err)
			}

			got, err := dataplane.ReadInt64ForTest(dataplane.NewTestReader(&buf))
			if err != nil {
				t.Fatalf("read: %v", err)
			}

			if got != tt.val {
				t.Fatalf("got %d want %d", got, tt.val)
			}
		})
	}
}

func attrEqual(a, b types.AttributeValue) bool {
	switch av := a.(type) {
	case *types.AttributeValueMemberS:
		bv, ok := b.(*types.AttributeValueMemberS)

		return ok && av.Value == bv.Value
	case *types.AttributeValueMemberN:
		bv, ok := b.(*types.AttributeValueMemberN)

		return ok && av.Value == bv.Value
	case *types.AttributeValueMemberB:
		bv, ok := b.(*types.AttributeValueMemberB)

		return ok && bytes.Equal(av.Value, bv.Value)
	case *types.AttributeValueMemberBOOL:
		bv, ok := b.(*types.AttributeValueMemberBOOL)

		return ok && av.Value == bv.Value
	case *types.AttributeValueMemberNULL:
		bv, ok := b.(*types.AttributeValueMemberNULL)

		return ok && av.Value == bv.Value
	case *types.AttributeValueMemberSS:
		bv, ok := b.(*types.AttributeValueMemberSS)

		return ok && stringSliceEqual(av.Value, bv.Value)
	case *types.AttributeValueMemberNS:
		bv, ok := b.(*types.AttributeValueMemberNS)

		return ok && stringSliceEqual(av.Value, bv.Value)
	case *types.AttributeValueMemberBS:
		bv, ok := b.(*types.AttributeValueMemberBS)

		return ok && byteSliceSliceEqual(av.Value, bv.Value)
	case *types.AttributeValueMemberL:
		bv, ok := b.(*types.AttributeValueMemberL)

		return ok && listEqual(av.Value, bv.Value)
	case *types.AttributeValueMemberM:
		bv, ok := b.(*types.AttributeValueMemberM)

		return ok && mapEqual(av.Value, bv.Value)
	default:
		return false
	}
}

func stringSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

func byteSliceSliceEqual(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}

	return true
}

func listEqual(a, b []types.AttributeValue) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if !attrEqual(a[i], b[i]) {
			return false
		}
	}

	return true
}

func mapEqual(a, b map[string]types.AttributeValue) bool {
	if len(a) != len(b) {
		return false
	}

	for k, av := range a {
		if !attrEqual(av, b[k]) {
			return false
		}
	}

	return true
}
