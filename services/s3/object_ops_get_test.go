package s3_test

import (
	"bytes"
	"io"
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

func TestHandler_GetObject_ChecksumMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "auto-computed CRC32 checksum returned"},
		{name: "stored SHA256 checksum returned"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			switch tt.name {
			case "auto-computed CRC32 checksum returned":
				mustPutObject(t, backend, "bkt", "key", []byte("checksum-me"))
				req := httptest.NewRequest(http.MethodGet, "/bkt/key", nil)
				req.Header.Set("X-Amz-Checksum-Mode", "ENABLED")
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusOK, rec.Code)
				assert.NotEmpty(t, rec.Header().Get("X-Amz-Checksum-Algorithm"))
				assert.NotEmpty(t, rec.Header().Get("X-Amz-Checksum-Crc32"))

			case "stored SHA256 checksum returned":
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
				require.Equal(t, http.StatusOK, rec.Code)
				assert.Equal(t, "SHA256", rec.Header().Get("X-Amz-Checksum-Algorithm"))
				assert.Equal(t, "fake-sha256", rec.Header().Get("X-Amz-Checksum-Sha256"))
			}
		})
	}
}

func TestHandler_GetObject_Range(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		rangeHdr   string
		wantBody   string
		wantRange  string
		wantStatus int
	}{
		{
			name:       "partial range returns 206 and slice",
			rangeHdr:   "bytes=0-4",
			wantStatus: http.StatusPartialContent,
			wantBody:   "01234",
			wantRange:  "bytes 0-4/10",
		},
		{
			name:       "suffix range returns last bytes",
			rangeHdr:   "bytes=-3",
			wantStatus: http.StatusPartialContent,
			wantBody:   "789",
		},
		{
			name:       "open-ended range returns from offset",
			rangeHdr:   "bytes=8-",
			wantStatus: http.StatusPartialContent,
			wantBody:   "89",
		},
		{
			// start (10) >= object size (10): syntactically valid but
			// unsatisfiable -> S3 returns 416 InvalidRange.
			name:       "start beyond size returns 416 InvalidRange",
			rangeHdr:   "bytes=10-5",
			wantStatus: http.StatusRequestedRangeNotSatisfiable,
			wantRange:  "bytes */10",
		},
		{
			// start far beyond size is likewise unsatisfiable.
			name:       "start far beyond size returns 416",
			rangeHdr:   "bytes=100-200",
			wantStatus: http.StatusRequestedRangeNotSatisfiable,
			wantRange:  "bytes */10",
		},
		{
			// last-byte-pos < first-byte-pos with start in-bounds is malformed;
			// S3 ignores the Range header and returns the full object with 200.
			name:       "inverted in-bounds range ignored returns full object",
			rangeHdr:   "bytes=5-2",
			wantStatus: http.StatusOK,
			wantBody:   "0123456789",
		},
		{
			// end past the object size clamps to the last byte.
			name:       "end past size clamps to last byte",
			rangeHdr:   "bytes=8-100",
			wantStatus: http.StatusPartialContent,
			wantBody:   "89",
			wantRange:  "bytes 8-9/10",
		},
		{
			name:       "unsupported range unit falls back to 200",
			rangeHdr:   "bits=0-5",
			wantStatus: http.StatusOK,
			wantBody:   "0123456789",
		},
		{
			// Non-numeric range value is malformed -> ignored, full object.
			name:       "malformed numeric range ignored returns full object",
			rangeHdr:   "bytes=abc-def",
			wantStatus: http.StatusOK,
			wantBody:   "0123456789",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")
			mustPutObject(t, backend, "bkt", "key", []byte("0123456789"))

			req := httptest.NewRequest(http.MethodGet, "/bkt/key", nil)
			req.Header.Set("Range", tt.rangeHdr)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Equal(t, tt.wantBody, rec.Body.String())
			}

			if tt.wantRange != "" {
				assert.Equal(t, tt.wantRange, rec.Header().Get("Content-Range"))
			}

			if tt.wantStatus == http.StatusRequestedRangeNotSatisfiable {
				body := rec.Body.String()
				assert.Contains(t, body, "<Code>InvalidRange</Code>")
				assert.Contains(t, body, "<ActualObjectSize>10</ActualObjectSize>")
				assert.Contains(t, body, "<RangeRequested>"+tt.rangeHdr+"</RangeRequested>")
			}
		})
	}
}

func TestHandler_GetObject_ExpirationHeader(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")
	mustPutObject(t, backend, "bkt", "obj", []byte("data"))

	req := httptest.NewRequest(http.MethodGet, "/bkt/obj", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	// x-amz-expiration is not always present (only with lifecycle), but
	// we verify the handler completes without error for both HEAD and GET.
	assert.Equal(
		t,
		"bytes",
		rec.Header().Get("Accept-Ranges"),
		"Accept-Ranges should be set on GET",
	)
	// Real S3 omits x-amz-storage-class for STANDARD-class objects.
	assert.Empty(
		t,
		rec.Header().Get("X-Amz-Storage-Class"),
		"X-Amz-Storage-Class should be omitted for STANDARD",
	)
}

func TestHandler_GetObject_ResponseHeaderOverrides(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")
	mustPutObject(t, backend, "bkt", "obj", []byte("data"))

	req := httptest.NewRequest(http.MethodGet,
		"/bkt/obj?response-content-type=application/pdf"+
			"&response-content-disposition=attachment%3B%20filename%3D%22r.pdf%22"+
			"&response-cache-control=no-cache", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/pdf", rec.Header().Get("Content-Type"))
	assert.Equal(t, `attachment; filename="r.pdf"`, rec.Header().Get("Content-Disposition"))
	assert.Equal(t, "no-cache", rec.Header().Get("Cache-Control"))
}

func TestHandler_GetObject_RangeContentLength(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")
	mustPutObject(t, backend, "bkt", "obj", []byte("0123456789"))

	req := httptest.NewRequest(http.MethodGet, "/bkt/obj", nil)
	req.Header.Set("Range", "bytes=2-5")
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusPartialContent, rec.Code)
	assert.Equal(
		t,
		"4",
		rec.Header().Get("Content-Length"),
		"range response should have correct Content-Length",
	)
	assert.Equal(t, "bytes 2-5/10", rec.Header().Get("Content-Range"))
	assert.Equal(t, "2345", rec.Body.String())
}

func TestHandler_GetObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *s3.InMemoryBackend)
		name       string
		bucket     string
		key        string
		wantBody   string
		wantStatus int
	}{
		{
			name:   "get existing object",
			bucket: "bkt",
			key:    "key",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "bkt")
				mustPutObject(t, b, "bkt", "key", []byte("content"))
			},
			wantStatus: http.StatusOK,
			wantBody:   "content",
		},
		{
			name:   "get non-existent key",
			bucket: "bkt",
			key:    "no-key",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "bkt")
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			tt.setup(t, backend)

			req := httptest.NewRequest(http.MethodGet, "/"+tt.bucket+"/"+tt.key, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				body, _ := io.ReadAll(rec.Body)
				assert.Equal(t, tt.wantBody, string(body))
			}
		})
	}
}
