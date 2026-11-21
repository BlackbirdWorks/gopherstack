package gremlin

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGraphBinarySubGraph(t *testing.T) {
	t.Parallel()

	str := func(s string) []byte {
		return append([]byte{0, 0, 0, byte(len(s))}, s...)
	}
	fqStr := func(s string) []byte { return append([]byte{gbString, 0}, str(s)...) }
	cat := func(parts ...[]byte) []byte {
		var out []byte
		for _, p := range parts {
			out = append(out, p...)
		}

		return out
	}
	i32 := func(n byte) []byte { return []byte{0, 0, 0, n} }
	null := []byte{gbNull, 1}
	fqLong := func(n byte) []byte { return []byte{gbLong, 0, 0, 0, 0, 0, 0, 0, 0, n} }

	tests := []struct {
		name string
		in   *SubGraph
		want []byte
	}{
		{
			name: "empty",
			in:   &SubGraph{},
			want: cat([]byte{0x10, 0}, i32(0), i32(0)),
		},
		{
			name: "vertex and edge",
			in: &SubGraph{
				Vertices: []*GraphVertex{{
					ID: "a", Label: "p",
					Props: []GraphProp{{Key: "k", Vals: []any{int64(7)}, IDs: []int64{5}}},
				}},
				Edges: []*GraphEdge{{
					Edge:  Edge{ID: "e", Label: "l", InID: "b", OutID: "a"},
					Props: []GraphProp{{Key: "w", Vals: []any{0.5}}},
				}},
			},
			want: cat(
				[]byte{0x10, 0},
				i32(1), fqStr("a"), str("p"), i32(1),
				fqLong(5), str("k"), fqLong(7), null, i32(0),
				i32(1), fqStr("e"), str("l"), fqStr("b"), null, fqStr("a"), null, null,
				i32(1), []byte{gbProperty, 0}, str("w"), []byte{gbDouble, 0, 0x3F, 0xE0, 0, 0, 0, 0, 0, 0}, null,
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := &bbWriter{}
			w.fq(tt.in)
			assert.Equal(t, tt.want, w.buf.Bytes())
		})
	}
}

func TestEncodeFrameBinaryGraph(t *testing.T) {
	t.Parallel()

	msg := &message{id: "00000000-0000-0000-0000-000000000001", codec: codecBinary}
	_, payload := encodeFrame(msg, StatusSuccess, "", []any{&SubGraph{}}, true)

	want := []byte{gbList, 0, 0, 0, 0, 1, gbGraph, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	require.GreaterOrEqual(t, len(payload), len(want))
	assert.Equal(t, want, payload[len(payload)-len(want):])
}
