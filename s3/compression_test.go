package s3_test

import (
	"testing"

	"Gopherstack/s3"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGzipCompressor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "empty data",
			data: []byte{},
		},
		{
			name: "small text",
			data: []byte("hello world"),
		},
		{
			name: "large repeated data",
			data: []byte("abcdefghij" + "abcdefghij" + "abcdefghij"),
		},
		{
			name: "binary data",
			data: []byte{0x00, 0x01, 0xFF, 0xFE, 0x80},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			compressor := &s3.GzipCompressor{}

			compressed, err := compressor.Compress(tt.data)
			require.NoError(t, err)

			decompressed, err := compressor.Decompress(compressed)
			require.NoError(t, err)

			assert.Equal(t, tt.data, decompressed)
		})
	}
}

func TestGzipCompressor_DecompressInvalidData(t *testing.T) {
	t.Parallel()

	compressor := &s3.GzipCompressor{}

	_, err := compressor.Decompress([]byte("not gzip data"))
	require.Error(t, err)
}
