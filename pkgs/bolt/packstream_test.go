package bolt_test

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/bolt"
)

func TestPackStreamRoundTrip(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("x", 70000)

	for _, tc := range []struct {
		in   any
		want any
		name string
	}{
		{nil, nil, "nil"},
		{true, true, "true"},
		{int64(-16), int64(-16), "tinyint"},
		{int64(-100), int64(-100), "int8"},
		{int64(-30000), int64(-30000), "int16"},
		{int64(2000000000), int64(2000000000), "int32"},
		{int64(math.MinInt64), int64(math.MinInt64), "int64"},
		{int64(math.MaxInt64), int64(math.MaxInt64), "max"},
		{1.5, 1.5, "float"},
		{"héllo", "héllo", "string"},
		{long, long, "long string"},
		{[]byte{1, 2, 3}, []byte{1, 2, 3}, "bytes"},
		{[]any{int64(1), "a", nil}, []any{int64(1), "a", nil}, "list"},
		{[]string{"a", "b"}, []any{"a", "b"}, "strings"},
		{map[string]any{"k": []any{int64(300)}}, map[string]any{"k": []any{int64(300)}}, "map"},
		{&bolt.Struct{Tag: 0x01, Fields: []any{"a"}}, &bolt.Struct{Tag: 0x01, Fields: []any{"a"}}, "struct"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := bolt.NewEncoder(bolt.Version{Major: 5})
			require.NoError(t, e.Encode(tc.in))

			d := bolt.NewDecoder(e.Bytes())
			got, err := d.Decode()
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
			assert.Zero(t, d.Remaining())
		})
	}
}

func TestPackStreamGraphStructs(t *testing.T) {
	t.Parallel()

	n := bolt.Node{ID: 1, ElementID: "e1", Labels: []string{"L"}}

	for _, tc := range []struct {
		in     any
		name   string
		fields int
		major  byte
	}{
		{n, "node4", 3, 4},
		{n, "node5", 4, 5},
		{bolt.Relationship{}, "rel4", 5, 4},
		{bolt.Relationship{}, "rel5", 8, 5},
		{bolt.Path{Nodes: []bolt.Node{n}, Indices: []int64{1, 1}}, "path", 3, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := bolt.NewEncoder(bolt.Version{Major: tc.major})
			require.NoError(t, e.Encode(tc.in))

			v, err := bolt.NewDecoder(e.Bytes()).Decode()
			require.NoError(t, err)

			s, ok := v.(*bolt.Struct)
			require.True(t, ok)
			assert.Len(t, s.Fields, tc.fields)
		})
	}
}

func TestPackStreamBounds(t *testing.T) {
	t.Parallel()

	deep := strings.Repeat("\x91", 100) + "\x01"

	for _, tc := range []struct {
		want error
		name string
		in   string
	}{
		{bolt.ErrTruncated, "empty", ""},
		{bolt.ErrTruncated, "short string", "\x85ab"},
		{bolt.ErrTooLarge, "huge list", "\xD6\x7F\xFF\xFF\xFF"},
		{bolt.ErrTooLarge, "huge map", "\xDA\x7F\xFF\xFF\xFF"},
		{bolt.ErrTooLarge, "oversized length", "\xD2\xFF\xFF\xFF\xFF"},
		{bolt.ErrTruncated, "huge bytes", "\xCE\x7F\xFF\xFF\xFF"},
		{bolt.ErrTooDeep, "deep", deep},
		{bolt.ErrBadMarker, "bad marker", "\xC4"},
		{bolt.ErrUnsupported, "non-string key", "\xA1\x01\x01"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := bolt.NewDecoder([]byte(tc.in)).Decode()
			require.ErrorIs(t, err, tc.want)
		})
	}
}

func TestEncodeUnsupported(t *testing.T) {
	t.Parallel()

	require.ErrorIs(t, bolt.NewEncoder(bolt.Version{}).Encode(struct{}{}), bolt.ErrUnsupported)
}
