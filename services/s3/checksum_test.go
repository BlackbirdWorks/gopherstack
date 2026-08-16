package s3_test

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

func TestCRC64NVME_HashInterface(t *testing.T) {
	t.Parallel()

	h := s3.NewCRC64NVME()

	// Size must be 8 bytes (64-bit).
	assert.Equal(t, 8, h.Size())

	// BlockSize must be 1 (byte-oriented).
	assert.Equal(t, 1, h.BlockSize())

	// Write some data and verify reset clears state.
	_, _ = h.Write([]byte("data"))
	sumBefore := h.Sum(nil)

	h.Reset()
	sumAfter := h.Sum(nil)

	// After reset, should match the empty hash.
	empty := s3.NewCRC64NVME()
	emptySum := empty.Sum(nil)
	assert.Equal(t, emptySum, sumAfter)
	assert.NotEqual(t, sumBefore, sumAfter, "hash after reset should differ from hash of 'data'")
}

func TestCRC64NVME_Hash_IsConsistent(t *testing.T) {
	t.Parallel()

	data := []byte("The quick brown fox jumps over the lazy dog")
	got1 := s3.CalculateCRC64NVME(data)
	got2 := s3.CalculateCRC64NVME(data)

	assert.Equal(t, got1, got2)
	assert.NotEmpty(t, got1)
}

func TestCRC64NVME_EmptyData(t *testing.T) {
	t.Parallel()

	got := s3.CalculateCRC64NVME([]byte{})
	assert.NotEmpty(t, got)

	// Verify base64 decodes to 8 bytes (64-bit hash).
	b, err := base64.StdEncoding.DecodeString(got)
	require.NoError(t, err)
	assert.Len(t, b, 8)
}

func TestCRC64NVME_DifferentData_DifferentHash(t *testing.T) {
	t.Parallel()

	a := s3.CalculateCRC64NVME([]byte("hello"))
	b := s3.CalculateCRC64NVME([]byte("world"))

	assert.NotEqual(t, a, b)
}

func TestCalculateChecksum(t *testing.T) {
	t.Parallel()

	data := []byte("hello world")
	tests := []struct {
		name      string
		algorithm string
		want      string
	}{
		{name: "CRC32", algorithm: "CRC32", want: "DUoRhQ=="},
		{name: "CRC32C", algorithm: "CRC32C", want: "yZRlqg=="},
		{name: "SHA1", algorithm: "SHA1", want: "Kq5sNclPz7QV2+lfQIuc6R7oRu0="},
		{name: "SHA256", algorithm: "SHA256", want: "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek="},
		{name: "Unknown", algorithm: "UNKNOWN", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, s3.CalculateChecksum(data, tt.algorithm))
		})
	}
}

func TestHandler_ChecksumSupport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "SHA256 checksum header roundtrip"},
		{name: "existing checksum preserved on get with checksum mode"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			switch tt.name {
			case "SHA256 checksum header roundtrip":
				body := "checksum test data"
				// Correct SHA256 checksum for "checksum test data"
				correctSHA256 := s3.CalculateChecksum([]byte(body), "SHA256")
				req := httptest.NewRequest(http.MethodPut, "/bkt/check", strings.NewReader(body))
				req.Header.Set("X-Amz-Checksum-Algorithm", "SHA256")
				req.Header.Set("X-Amz-Checksum-Sha256", correctSHA256)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				assert.Equal(t, http.StatusOK, rec.Code)

				expectedHash := md5.Sum([]byte(body))
				expectedETag := "\"" + hex.EncodeToString(expectedHash[:]) + "\""
				assert.Equal(t, expectedETag, rec.Header().Get("ETag"))
				assert.Equal(t, correctSHA256, rec.Header().Get("X-Amz-Checksum-Sha256"))

				req = httptest.NewRequest(http.MethodGet, "/bkt/check", nil)
				rec = httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Equal(t, correctSHA256, rec.Header().Get("X-Amz-Checksum-Sha256"))
				assert.Equal(t, "SHA256", rec.Header().Get("X-Amz-Checksum-Algorithm"))

			case "existing checksum preserved on get with checksum mode":
				_, err := backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
					Bucket:         aws.String("bkt"),
					Key:            aws.String("key2"),
					Body:           strings.NewReader("data"),
					ChecksumSHA256: aws.String("fake-sha256"),
				})
				require.NoError(t, err)

				req := httptest.NewRequest(http.MethodGet, "/bkt/key2", nil)
				req.Header.Set("X-Amz-Checksum-Mode", "ENABLED")
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				assert.Equal(t, "SHA256", rec.Header().Get("X-Amz-Checksum-Algorithm"))
				assert.Equal(t, "fake-sha256", rec.Header().Get("X-Amz-Checksum-Sha256"))
			}
		})
	}
}

func TestHandler_ChecksumAlgorithms_CRC32AndSHA1(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		header string
	}{
		{name: "CRC32", header: "X-Amz-Checksum-Crc32"},
		{name: "CRC32C", header: "X-Amz-Checksum-Crc32c"},
		{name: "SHA1", header: "X-Amz-Checksum-Sha1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			body := []byte("data")
			correctChecksum := s3.CalculateChecksum(body, tt.name)

			key := "check-" + tt.name
			req := httptest.NewRequest(http.MethodPut, "/bkt/"+key, bytes.NewReader(body))
			req.Header.Set("X-Amz-Checksum-Algorithm", tt.name)
			req.Header.Set(tt.header, correctChecksum)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)

			req = httptest.NewRequest(http.MethodGet, "/bkt/"+key, nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, correctChecksum, rec.Header().Get(tt.header))
			assert.Equal(t, tt.name, rec.Header().Get("X-Amz-Checksum-Algorithm"))
		})
	}
}
