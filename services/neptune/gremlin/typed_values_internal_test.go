package gremlin

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func TestDateGraphBinaryRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ms   int64
	}{
		{"epoch", 0},
		{"positive", 1_700_000_000_123},
		{"negative", -86_400_000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := time.UnixMilli(tt.ms).UTC()

			w := &bbWriter{}
			w.fq(in)
			require.Equal(t, byte(gbDate), w.buf.Bytes()[0])

			got, err := (&bbReader{b: w.buf.Bytes()}).fq()
			require.NoError(t, err)
			assert.Equal(t, in, got)
		})
	}
}

func TestDateGraphSONRoundTrip(t *testing.T) {
	t.Parallel()

	in := time.UnixMilli(1_700_000_000_123).UTC()
	enc := graphsonWriter{}.value(in)
	assert.Equal(t, map[string]any{"@type": "g:Date", "@value": int64(1_700_000_000_123)}, enc)

	got, err := decodeTyped("g:Date", jsonx.Number("1700000000123"))
	require.NoError(t, err)
	assert.Equal(t, in, got)
}

func TestDateValueSemantics(t *testing.T) {
	t.Parallel()

	a := time.UnixMilli(1000).UTC()
	b := time.UnixMilli(2000).UTC()

	assert.True(t, equalValues(a, time.UnixMilli(1000).UTC()))
	assert.False(t, equalValues(a, b))
	assert.False(t, equalValues(a, int64(1000)))

	c, ok := compareValues(a, b)
	assert.True(t, ok)
	assert.Negative(t, c)
	assert.True(t, storableValue(a))
}

func TestUUIDValues(t *testing.T) {
	t.Parallel()

	const id = "48af8178-50ce-971a-fc41-8c9a954cea62"

	got, err := decodeTyped("g:UUID", id)
	require.NoError(t, err)
	assert.Equal(t, UUID(id), got)
	assert.False(t, storableValue(got))
	assert.Equal(t, map[string]any{"@type": "g:UUID", "@value": id}, graphsonWriter{}.value(got))
	assert.False(t, equalValues(got, id))

	w := &bbWriter{}
	w.fq(UUID(id))
	require.Equal(t, byte(gbUUID), w.buf.Bytes()[0])

	back, err := (&bbReader{b: w.buf.Bytes()}).fq()
	require.NoError(t, err)
	assert.Equal(t, UUID(id), back)
}
