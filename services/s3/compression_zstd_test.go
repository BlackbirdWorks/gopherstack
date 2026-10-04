package s3_test

import (
	"bytes"
	"io"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	sdk_s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/blackbirdworks/gopherstack/services/s3"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	zstdMagic = "\x28\xb5\x2f\xfd"
	gzipMagic = "\x1f\x8b"
)

func TestZstdCompressor_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"small", []byte("hello world")},
		{"json", benchPayload("json", 300<<10)},
		{"random", benchPayload("random", 128<<10)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := &s3.ZstdCompressor{}
			enc, err := c.Compress(tt.data)
			require.NoError(t, err)
			assert.True(t, bytes.HasPrefix(enc, []byte(zstdMagic)))

			dec, err := c.Decompress(enc)
			require.NoError(t, err)
			assert.Len(t, dec, len(tt.data))
			assert.True(t, bytes.Equal(tt.data, dec))
		})
	}
}

func TestZstdCompressor_DecodesLegacyGzip(t *testing.T) {
	t.Parallel()

	data := benchPayload("json", 50<<10)
	gz, err := (&s3.GzipCompressor{}).Compress(data)
	require.NoError(t, err)
	require.True(t, bytes.HasPrefix(gz, []byte(gzipMagic)))

	z, err := (&s3.ZstdCompressor{}).Compress(data)
	require.NoError(t, err)

	tests := []struct {
		name string
		c    s3.Compressor
		in   []byte
	}{
		{"zstd decodes gzip", &s3.ZstdCompressor{}, gz},
		{"gzip decodes zstd", &s3.GzipCompressor{}, z},
		{"gzip decodes gzip", &s3.GzipCompressor{}, gz},
		{"zstd decodes zstd", &s3.ZstdCompressor{}, z},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, dErr := tt.c.Decompress(tt.in)
			require.NoError(t, dErr)
			assert.True(t, bytes.Equal(data, out))
		})
	}
}

func TestZstdCompressor_DecompressInvalid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data []byte
	}{
		{"garbage", []byte("not compressed at all")},
		{"truncated zstd", append([]byte(zstdMagic), 0x01, 0x02)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := (&s3.ZstdCompressor{}).Decompress(tt.data)
			require.Error(t, err)
		})
	}
}

func TestZstdCompressor_CompressForStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		parts          [][]byte
		wantCompressed bool
	}{
		{"compressible", [][]byte{benchPayload("json", 200<<10)}, true},
		{"incompressible large", [][]byte{benchPayload("random", 200<<10)}, false},
		{"incompressible small", [][]byte{benchPayload("random", 2<<10)}, false},
		{"multipart streaming", [][]byte{benchPayload("json", 100<<10), benchPayload("json", 70<<10)}, true},
		{"empty", [][]byte{{}}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := &s3.ZstdCompressor{}
			out, compressed, err := c.CompressForStore(tt.parts)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCompressed, compressed)

			want := bytes.Join(tt.parts, nil)
			if !compressed {
				assert.True(t, bytes.Equal(want, out))

				return
			}

			assert.Less(t, len(out), len(want))

			dec, err := c.Decompress(out)
			require.NoError(t, err)
			assert.True(t, bytes.Equal(want, dec))
		})
	}
}

func TestZstdCompressor_Concurrent(t *testing.T) {
	t.Parallel()

	c := &s3.ZstdCompressor{}
	payloads := [][]byte{benchPayload("json", 90<<10), benchPayload("json", 300<<10), []byte("tiny")}

	var wg sync.WaitGroup

	for i := range 16 {
		wg.Go(func() {
			data := payloads[i%len(payloads)]
			parts := [][]byte{data[:len(data)/2], data[len(data)/2:]}

			for range 20 {
				var enc []byte
				var err error

				if i%2 == 0 {
					enc, err = c.Compress(data)
				} else {
					enc, err = c.CompressParts(parts)
				}

				if err != nil {
					t.Error(err)

					return
				}

				dec, err := c.Decompress(enc)
				if err != nil || !bytes.Equal(data, dec) {
					t.Errorf("round trip mismatch: %v", err)

					return
				}
			}
		})
	}

	wg.Wait()
}

func TestBackend_LegacyGzipSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data []byte
	}{
		{"compressible", benchPayload("json", 200<<10)},
		{"small raw", []byte("tiny object")},
		{"random", benchPayload("random", 100<<10)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			old := s3.NewInMemoryBackend(&s3.GzipCompressor{}).WithCompressionMinBytes(1024)
			_, err := old.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String("b")})
			require.NoError(t, err)
			_, err = old.PutObject(t.Context(), &sdk_s3.PutObjectInput{
				Bucket: aws.String("b"), Key: aws.String("k"), Body: bytes.NewReader(tt.data),
			})
			require.NoError(t, err)

			snap := old.Snapshot(t.Context())
			require.NotEmpty(t, snap)

			fresh := s3.NewInMemoryBackend(&s3.ZstdCompressor{}).WithCompressionMinBytes(1024)
			require.NoError(t, fresh.Restore(t.Context(), snap))

			_, err = fresh.PutObject(t.Context(), &sdk_s3.PutObjectInput{
				Bucket: aws.String("b"), Key: aws.String("new"), Body: bytes.NewReader(tt.data),
			})
			require.NoError(t, err)

			for _, key := range []string{"k", "new"} {
				out, gErr := fresh.GetObject(t.Context(), &sdk_s3.GetObjectInput{
					Bucket: aws.String("b"), Key: aws.String(key),
				})
				require.NoError(t, gErr)

				got, rErr := io.ReadAll(out.Body)
				require.NoError(t, rErr)
				assert.True(t, bytes.Equal(tt.data, got), key)
			}
		})
	}
}

func TestBackend_ZstdMultipartRoundTrip(t *testing.T) {
	t.Parallel()

	backend := s3.NewInMemoryBackend(&s3.ZstdCompressor{}).WithSkipMultipartSizeCheck()
	_, err := backend.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String("b")})
	require.NoError(t, err)

	p1, p2 := benchPayload("json", 120<<10), benchPayload("random", 60<<10)
	up, err := backend.CreateMultipartUpload(t.Context(), &sdk_s3.CreateMultipartUploadInput{
		Bucket: aws.String("b"), Key: aws.String("mp"),
	})
	require.NoError(t, err)

	parts := make([]sdkCompletedPart, 0, 2)

	for i, p := range [][]byte{p1, p2} {
		out, uErr := backend.UploadPart(t.Context(), &sdk_s3.UploadPartInput{
			Bucket: aws.String("b"), Key: aws.String("mp"), UploadId: up.UploadId,
			PartNumber: aws.Int32(int32(i + 1)), Body: bytes.NewReader(p),
		})
		require.NoError(t, uErr)

		parts = append(parts, completedPart(int32(i+1), out.ETag))
	}

	_, err = backend.CompleteMultipartUpload(t.Context(), completeInput("b", "mp", up.UploadId, parts))
	require.NoError(t, err)

	out, err := backend.GetObject(t.Context(), &sdk_s3.GetObjectInput{Bucket: aws.String("b"), Key: aws.String("mp")})
	require.NoError(t, err)

	got, err := io.ReadAll(out.Body)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(append(bytes.Clone(p1), p2...), got))
}

type sdkCompletedPart = sdk_s3types.CompletedPart

func completedPart(n int32, etag *string) sdkCompletedPart {
	return sdkCompletedPart{PartNumber: aws.Int32(n), ETag: etag}
}

func completeInput(bucket, key string, id *string, parts []sdkCompletedPart) *sdk_s3.CompleteMultipartUploadInput {
	return &sdk_s3.CompleteMultipartUploadInput{
		Bucket: aws.String(bucket), Key: aws.String(key), UploadId: id,
		MultipartUpload: &sdk_s3types.CompletedMultipartUpload{Parts: parts},
	}
}
