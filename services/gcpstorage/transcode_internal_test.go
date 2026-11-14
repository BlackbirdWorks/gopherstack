package gcpstorage

import (
	"bytes"
	"compress/gzip"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGunzipCapped(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(bytes.Repeat([]byte("a"), 100))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	valid := buf.Bytes()

	tests := []struct {
		name   string
		data   []byte
		limit  int64
		wantOK bool
	}{
		{name: "under cap", data: valid, limit: 1000, wantOK: true},
		{name: "exactly cap", data: valid, limit: 100, wantOK: true},
		{name: "over cap", data: valid, limit: 99},
		{name: "corrupt stream", data: valid[:len(valid)-6], limit: 1000},
		{name: "not gzip", data: []byte("plain"), limit: 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := gunzipCapped(tt.data, tt.limit)
			assert.Equal(t, tt.wantOK, ok)

			if tt.wantOK {
				assert.Len(t, got, 100)
			} else {
				assert.Nil(t, got)
			}
		})
	}
}
