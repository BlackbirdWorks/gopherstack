package s3_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/blackbirdworks/gopherstack/services/s3"

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

	tests := []struct {
		name string
		data []byte
	}{
		{name: "not gzip data", data: []byte("not gzip data")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			compressor := &s3.GzipCompressor{}
			_, err := compressor.Decompress(tt.data)
			require.Error(t, err)
		})
	}
}

// TestInMemoryBackend_WithCompressionMinBytes verifies that the compression
// threshold clamps negative values to zero and leaves objects below the
// configured minimum uncompressed (but still readable).
func TestInMemoryBackend_WithCompressionMinBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bucket   string
		key      string
		body     []byte
		minBytes int
	}{
		{
			name:     "negative_clamped_to_zero",
			minBytes: -1,
			bucket:   "comp-neg",
			key:      "key",
			body:     []byte("small"),
		},
		{
			name:     "above_threshold_not_compressed",
			minBytes: 1000,
			bucket:   "comp-thresh",
			key:      "small-obj",
			body:     []byte("small"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := s3.NewInMemoryBackend(&s3.GzipCompressor{}).WithCompressionMinBytes(tt.minBytes)
			mustCreateBucket(t, backend, tt.bucket)

			_, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
				Bucket: aws.String(tt.bucket),
				Key:    aws.String(tt.key),
				Body:   bytes.NewReader(tt.body),
			})
			require.NoError(t, err)

			out, err := backend.GetObject(t.Context(), &sdk_s3.GetObjectInput{
				Bucket: aws.String(tt.bucket),
				Key:    aws.String(tt.key),
			})
			require.NoError(t, err)
			defer out.Body.Close()
		})
	}
}
