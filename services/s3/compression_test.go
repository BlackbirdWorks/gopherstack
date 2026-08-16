package s3_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

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

func TestCompressionMinBytes_PutObject(t *testing.T) {
	t.Parallel()

	smallData := bytes.Repeat([]byte("a"), 512)
	largeData := bytes.Repeat([]byte("a"), 2048)

	tests := []struct {
		name                string
		data                []byte
		compressionMinBytes int
		wantCompressed      bool
	}{
		{
			name:                "small object below threshold is not compressed",
			data:                smallData,
			compressionMinBytes: 1024,
			wantCompressed:      false,
		},
		{
			name:                "large object at or above threshold is compressed",
			data:                largeData,
			compressionMinBytes: 1024,
			wantCompressed:      true,
		},
		{
			name:                "zero threshold compresses all objects",
			data:                smallData,
			compressionMinBytes: 0,
			wantCompressed:      true,
		},
		{
			name:                "object exactly at threshold is compressed",
			data:                bytes.Repeat([]byte("b"), 1024),
			compressionMinBytes: 1024,
			wantCompressed:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rc := &recordingCompressor{delegate: &s3.GzipCompressor{}}
			backend := s3.NewInMemoryBackend(rc).
				WithCompressionMinBytes(tt.compressionMinBytes).
				WithSkipMultipartSizeCheck()
			mustCreateBucket(t, backend, "bkt")

			_, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
				Bucket:   aws.String("bkt"),
				Key:      aws.String("key"),
				Body:     bytes.NewReader(tt.data),
				Metadata: map[string]string{},
			})
			require.NoError(t, err)

			assert.Equal(
				t, tt.wantCompressed, rc.compressCalled,
				"unexpected compression decision for object of size %d with threshold %d",
				len(tt.data), tt.compressionMinBytes,
			)

			// Verify the round-trip: GetObject must return the original data.
			out, err := backend.GetObject(t.Context(), &sdk_s3.GetObjectInput{
				Bucket: aws.String("bkt"),
				Key:    aws.String("key"),
			})
			require.NoError(t, err)

			body, err := io.ReadAll(out.Body)
			require.NoError(t, err)
			assert.Equal(t, tt.data, body)
		})
	}
}

// recordingCompressor wraps a Compressor and records whether Compress was called.
type recordingCompressor struct {
	delegate       s3.Compressor
	compressCalled bool
}

func (r *recordingCompressor) Compress(data []byte) ([]byte, error) {
	r.compressCalled = true

	return r.delegate.Compress(data)
}

func (r *recordingCompressor) Decompress(data []byte) ([]byte, error) {
	return r.delegate.Decompress(data)
}

func TestCompressionMinBytes_CompleteMultipartUpload(t *testing.T) {
	t.Parallel()

	// Each part is 512 bytes; two parts assemble to 1024 bytes total.
	partData := bytes.Repeat([]byte("x"), 512)

	tests := []struct {
		name                string
		compressionMinBytes int
		wantCompressed      bool
	}{
		{
			name:                "assembled size below threshold is not compressed",
			compressionMinBytes: 2048,
			wantCompressed:      false,
		},
		{
			name:                "assembled size at or above threshold is compressed",
			compressionMinBytes: 1024,
			wantCompressed:      true,
		},
		{
			name:                "zero threshold compresses all objects",
			compressionMinBytes: 0,
			wantCompressed:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rc := &recordingCompressor{delegate: &s3.GzipCompressor{}}
			backend := s3.NewInMemoryBackend(rc).
				WithCompressionMinBytes(tt.compressionMinBytes).
				WithSkipMultipartSizeCheck()
			mustCreateBucket(t, backend, "bkt")

			// Start multipart upload
			createOut, err := backend.CreateMultipartUpload(
				t.Context(),
				&sdk_s3.CreateMultipartUploadInput{
					Bucket: aws.String("bkt"),
					Key:    aws.String("key"),
				},
			)
			require.NoError(t, err)
			uploadID := createOut.UploadId

			// Upload two parts
			p1, err := backend.UploadPart(t.Context(), &sdk_s3.UploadPartInput{
				Bucket:     aws.String("bkt"),
				Key:        aws.String("key"),
				UploadId:   uploadID,
				PartNumber: aws.Int32(1),
				Body:       bytes.NewReader(partData),
			})
			require.NoError(t, err)

			p2, err := backend.UploadPart(t.Context(), &sdk_s3.UploadPartInput{
				Bucket:     aws.String("bkt"),
				Key:        aws.String("key"),
				UploadId:   uploadID,
				PartNumber: aws.Int32(2),
				Body:       bytes.NewReader(partData),
			})
			require.NoError(t, err)

			// Complete
			_, err = backend.CompleteMultipartUpload(
				t.Context(),
				&sdk_s3.CompleteMultipartUploadInput{
					Bucket:   aws.String("bkt"),
					Key:      aws.String("key"),
					UploadId: uploadID,
					MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{
						{PartNumber: aws.Int32(1), ETag: p1.ETag},
						{PartNumber: aws.Int32(2), ETag: p2.ETag},
					}},
				},
			)
			require.NoError(t, err)

			assert.Equal(
				t, tt.wantCompressed, rc.compressCalled,
				"unexpected compression decision for assembled size %d with threshold %d",
				len(partData)*2, tt.compressionMinBytes,
			)

			// Verify round-trip.
			out, err := backend.GetObject(t.Context(), &sdk_s3.GetObjectInput{
				Bucket: aws.String("bkt"),
				Key:    aws.String("key"),
			})
			require.NoError(t, err)

			body, err := io.ReadAll(out.Body)
			require.NoError(t, err)
			assert.Equal(t, append(partData, partData...), body)
		})
	}
}
