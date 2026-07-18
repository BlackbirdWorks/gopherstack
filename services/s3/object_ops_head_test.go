package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_HeadObject_StorageClassAndAcceptRanges(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")
	mustPutObject(t, backend, "bkt", "obj", []byte("data"))

	req := httptest.NewRequest(http.MethodHead, "/bkt/obj", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "bytes", rec.Header().Get("Accept-Ranges"))
	// Real S3 omits x-amz-storage-class for STANDARD-class objects.
	assert.Empty(t, rec.Header().Get("X-Amz-Storage-Class"))
}

// TestHandler_GetObject_ResponseHeaderOverrides verifies the AWS response-*
// query parameters override the corresponding response headers on GET (used
// heavily via presigned URLs, e.g. forcing a download filename).

func TestHandler_HeadObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *s3.InMemoryBackend)
		name       string
		bucket     string
		key        string
		wantStatus int
	}{
		{
			name:   "existing object",
			bucket: "bkt",
			key:    "key",
			setup: func(t *testing.T, b *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, b, "bkt")
				mustPutObject(t, b, "bkt", "key", []byte("data"))
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "non-existent object",
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

			req := httptest.NewRequest(http.MethodHead, "/"+tt.bucket+"/"+tt.key, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_HeadObjectWithMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "head and get object return metadata headers"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "bkt")

			req := httptest.NewRequest(http.MethodPut, "/bkt/meta", strings.NewReader("data"))
			req.Header.Set("Content-Type", "text/plain")
			req.Header.Set("X-Amz-Meta-Author", "Antigravity")
			req.Header.Set("X-Amz-Meta-Priority", "High")
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)

			req = httptest.NewRequest(http.MethodHead, "/bkt/meta", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "text/plain", rec.Header().Get("Content-Type"))
			assert.Equal(t, "Antigravity", rec.Header().Get("X-Amz-Meta-Author"))
			assert.Equal(t, "High", rec.Header().Get("X-Amz-Meta-Priority"))

			req = httptest.NewRequest(http.MethodGet, "/bkt/meta", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "text/plain", rec.Header().Get("Content-Type"))
			assert.Equal(t, "Antigravity", rec.Header().Get("X-Amz-Meta-Author"))
			assert.Equal(t, "High", rec.Header().Get("X-Amz-Meta-Priority"))
		})
	}
}
