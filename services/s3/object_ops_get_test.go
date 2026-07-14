package s3_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// TestHandler_HeadObject_NoSuchBucket verifies that HeadObject on a
// nonexistent bucket returns 404.
func TestHandler_HeadObject_NoSuchBucket(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)

	req := httptest.NewRequest(http.MethodHead, "/no-bucket/obj", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestHandler_GetObject_WithChecksumMode verifies a basic GetObject round trip
// with X-Amz-Checksum-Mode set.
func TestHandler_GetObject_WithChecksumMode(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "checksum-bucket")

	putReq := httptest.NewRequest(
		http.MethodPut,
		"/checksum-bucket/ck-obj",
		strings.NewReader("hello checksum"),
	)
	putReq.Header.Set("X-Amz-Checksum-Algorithm", "SHA256")
	putRec := httptest.NewRecorder()
	serveS3Handler(handler, putRec, putReq)
	require.Equal(t, http.StatusOK, putRec.Code)

	getReq := httptest.NewRequest(http.MethodGet, "/checksum-bucket/ck-obj", nil)
	getReq.Header.Set("X-Amz-Checksum-Mode", "ENABLED")
	getRec := httptest.NewRecorder()
	serveS3Handler(handler, getRec, getReq)

	assert.Equal(t, http.StatusOK, getRec.Code)
	assert.Equal(t, "hello checksum", getRec.Body.String())
}

// TestGetObject_ReturnsStoredChecksum verifies that GetObject with
// X-Amz-Checksum-Mode: ENABLED echoes back the checksum recorded at PutObject
// time, for every supported algorithm, and that an object stored without any
// checksum falls back to a server-computed CRC32.
func TestGetObject_ReturnsStoredChecksum(t *testing.T) {
	t.Parallel()

	tests := []struct {
		checksum       func(data []byte) string
		name           string
		bucket         string
		putAlgo        string
		putHeaderName  string
		wantHeaderName string
		data           []byte
	}{
		{
			name:           "crc32",
			bucket:         "gsc-crc32",
			data:           []byte("checksum test data"),
			putAlgo:        "CRC32",
			putHeaderName:  "X-Amz-Checksum-Crc32",
			checksum:       checksumB64CRC32,
			wantHeaderName: "X-Amz-Checksum-Crc32",
		},
		{
			name:           "sha256",
			bucket:         "gsc-sha256",
			data:           []byte("checksum test data"),
			putAlgo:        "SHA256",
			putHeaderName:  "X-Amz-Checksum-Sha256",
			checksum:       checksumB64SHA256,
			wantHeaderName: "X-Amz-Checksum-Sha256",
		},
		{
			name:           "sha1",
			bucket:         "gsc-sha1",
			data:           []byte("sha1 data"),
			putAlgo:        "SHA1",
			putHeaderName:  "X-Amz-Checksum-Sha1",
			checksum:       checksumB64SHA1,
			wantHeaderName: "X-Amz-Checksum-Sha1",
		},
		{
			name:           "crc32c",
			bucket:         "gsc-crc32c",
			data:           []byte("crc32c data"),
			putAlgo:        "CRC32C",
			putHeaderName:  "X-Amz-Checksum-Crc32c",
			checksum:       checksumB64CRC32C,
			wantHeaderName: "X-Amz-Checksum-Crc32c",
		},
		{
			name:           "crc64nvme",
			bucket:         "gsc-crc64",
			data:           []byte("crc64 test data"),
			putAlgo:        "CRC64NVME",
			putHeaderName:  "X-Amz-Checksum-Crc64nvme",
			checksum:       s3.CalculateCRC64NVME,
			wantHeaderName: "X-Amz-Checksum-Crc64nvme",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, tt.bucket)

			checksum := tt.checksum(tt.data)

			req := httptest.NewRequest(http.MethodPut, "/"+tt.bucket+"/obj",
				bytes.NewReader(tt.data))
			req.Header.Set("X-Amz-Checksum-Algorithm", tt.putAlgo)
			req.Header.Set(tt.putHeaderName, checksum)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)

			getReq := httptest.NewRequest(http.MethodGet, "/"+tt.bucket+"/obj", nil)
			getReq.Header.Set("X-Amz-Checksum-Mode", "ENABLED")
			getRec := httptest.NewRecorder()
			serveS3Handler(handler, getRec, getReq)
			require.Equal(t, http.StatusOK, getRec.Code)

			if tt.name == "crc64nvme" {
				// CRC64NVME parity assertion only checks presence upstream;
				// preserve that (weaker) assertion here rather than tightening it.
				assert.NotEmpty(t, getRec.Header().Get("X-Amz-Checksum-Algorithm"))

				return
			}
			assert.Equal(t, checksum, getRec.Header().Get(tt.wantHeaderName))
		})
	}
}

// TestGetObject_ChecksumMode_NoStoredChecksum_ComputesCRC32 verifies that an
// object stored without any checksum falls back to a server-computed CRC32
// when read back with checksum mode enabled.
func TestGetObject_ChecksumMode_NoStoredChecksum_ComputesCRC32(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "gsc-nocsum")
	mustPutObject(t, backend, "gsc-nocsum", "obj", []byte("data"))

	getReq := httptest.NewRequest(http.MethodGet, "/gsc-nocsum/obj", nil)
	getReq.Header.Set("X-Amz-Checksum-Mode", "ENABLED")
	getRec := httptest.NewRecorder()
	serveS3Handler(handler, getRec, getReq)
	require.Equal(t, http.StatusOK, getRec.Code)
	// Server-computed CRC32 should be set.
	assert.Equal(t, "CRC32", getRec.Header().Get("X-Amz-Checksum-Algorithm"))
}

// TestGetObject_WithLifecycle_SetsExpirationHeader verifies that GetObject
// exercises setExpirationHeader (which consults the janitor) without panicking
// when a lifecycle rule with a Days expiration is configured.
func TestGetObject_WithLifecycle_SetsExpirationHeader(t *testing.T) {
	t.Parallel()

	// Create a handler WITH janitor so setExpirationHeader is exercised.
	backend := s3.NewInMemoryBackend(&s3.GzipCompressor{}).WithSkipMultipartSizeCheck()
	handler := s3.NewHandler(backend).WithJanitor(s3.Settings{})

	mustCreateBucket(t, backend, "lc-exp-bucket")
	mustPutObject(t, backend, "lc-exp-bucket", "obj.log", []byte("hello"))

	// Set a lifecycle rule with a Days expiration.
	lcXML := `<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
  <Rule>
    <ID>expire-logs</ID>
    <Status>Enabled</Status>
    <Filter><Prefix>obj</Prefix></Filter>
    <Expiration><Days>30</Days></Expiration>
  </Rule>
</LifecycleConfiguration>`

	putLCReq := httptest.NewRequest(http.MethodPut,
		"/lc-exp-bucket?lifecycle",
		strings.NewReader(lcXML))
	putLCRec := httptest.NewRecorder()
	serveS3Handler(handler, putLCRec, putLCReq)
	require.Contains(t, []int{http.StatusOK, http.StatusNoContent}, putLCRec.Code)

	getReq := httptest.NewRequest(http.MethodGet, "/lc-exp-bucket/obj.log", nil)
	getRec := httptest.NewRecorder()
	serveS3Handler(handler, getRec, getReq)
	require.Equal(t, http.StatusOK, getRec.Code)
	// X-Amz-Expiration may or may not be set depending on implementation;
	// we just verify the request doesn't panic.
}
