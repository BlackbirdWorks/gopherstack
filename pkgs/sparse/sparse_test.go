package sparse_test

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sparse"
)

func TestExtents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		build      func() sparse.Extents
		name       string
		wantRead   string
		wantRanges []sparse.Range
		readOff    int64
		readLen    int64
	}{
		{
			name:       "overlapping write wins",
			build:      func() sparse.Extents { return sparse.Extents(nil).Write(0, []byte("aaaaaa")).Write(2, []byte("BB")) },
			wantRanges: []sparse.Range{{Start: 0, End: 5}},
			readLen:    6,
			wantRead:   "aaBBaa",
		},
		{
			name:  "gap stays sparse",
			build: func() sparse.Extents { return sparse.Extents(nil).Write(0, []byte("ab")).Write(5, []byte("cd")) },
			wantRanges: []sparse.Range{
				{Start: 0, End: 1},
				{Start: 5, End: 6},
			},
			readLen:  7,
			wantRead: "ab\x00\x00\x00cd",
		},
		{
			name:       "clear splits",
			build:      func() sparse.Extents { return sparse.Extents(nil).Write(0, []byte("abcdef")).Clear(2, 2) },
			wantRanges: []sparse.Range{{Start: 0, End: 1}, {Start: 4, End: 5}}, readLen: 6, wantRead: "ab\x00\x00ef",
		},
		{
			name: "truncate cuts straddling run",
			build: func() sparse.Extents {
				return sparse.Extents(nil).Write(0, []byte("abcdef")).Write(10, []byte("zz")).Truncate(4)
			},
			wantRanges: []sparse.Range{{Start: 0, End: 3}}, readLen: 6, wantRead: "abcd\x00\x00",
		},
		{
			name:       "adjacent runs merge",
			build:      func() sparse.Extents { return sparse.Extents(nil).Write(0, []byte("ab")).Write(2, []byte("cd")) },
			wantRanges: []sparse.Range{{Start: 0, End: 3}},
			readLen:    4,
			wantRead:   "abcd",
		},
		{
			name:       "empty write is a no-op",
			build:      func() sparse.Extents { return sparse.Extents(nil).Write(3, nil) },
			wantRanges: nil, readLen: 2, wantRead: "\x00\x00",
		},
		{
			name:       "partial read window",
			build:      func() sparse.Extents { return sparse.Extents(nil).Write(10, []byte("hello")) },
			wantRanges: []sparse.Range{{Start: 10, End: 14}}, readOff: 12, readLen: 6, wantRead: "llo\x00\x00\x00",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			x := tt.build()
			assert.Equal(t, tt.wantRanges, x.Ranges(0, 1<<20))
			assert.Equal(t, tt.wantRead, string(x.Read(tt.readOff, tt.readLen)))
		})
	}
}

func TestExtentsCloneIsolated(t *testing.T) {
	t.Parallel()

	orig := sparse.Extents(nil).Write(0, []byte("abcd"))
	clone := orig.Clone()
	orig = orig.Write(1, []byte("ZZ"))

	assert.Equal(t, "abcd", string(clone.Read(0, 4)))
	assert.Equal(t, "aZZd", string(orig.Read(0, 4)))
}

func TestExtentsDiff(t *testing.T) {
	t.Parallel()

	prev := sparse.Extents(nil).Write(0, []byte("aaaa")).Write(10, []byte("bbbb"))
	cur := sparse.Extents(nil).Write(0, []byte("aXaa")).Write(6, []byte("new"))

	changed, cleared := cur.Diff(prev, 14)
	assert.Equal(t, []sparse.Range{{Start: 1, End: 1}, {Start: 6, End: 8}}, changed)
	assert.Equal(t, []sparse.Range{{Start: 10, End: 13}}, cleared)
}

func TestExtentReader(t *testing.T) {
	t.Parallel()

	x := sparse.Extents(nil).Write(2, []byte("ab")).Write(8, []byte("cd"))

	tests := []struct {
		name string
		want string
		off  int64
		end  int64
	}{
		{name: "whole", off: 0, end: 11, want: "\x00\x00ab\x00\x00\x00\x00cd\x00\x00"},
		{name: "inside run", off: 3, end: 3, want: "b"},
		{name: "gap only", off: 4, end: 7, want: "\x00\x00\x00\x00"},
		{name: "past data", off: 10, end: 12, want: "\x00\x00\x00"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := io.ReadAll(sparse.NewReader(x, tt.off, tt.end))
			require.NoError(t, err)
			assert.Equal(t, tt.want, string(got))
		})
	}
}
