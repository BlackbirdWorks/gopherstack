package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_GetObject_IfRange verifies If-Range semantics: a matching ETag
// serves the partial (206) range, while a non-matching If-Range ETag causes the
// full object (200) to be returned.
func TestParity_GetObject_IfRange(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "ir-bucket")

	req := httptest.NewRequest(http.MethodPut, "/ir-bucket/k", strings.NewReader("0123456789"))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	etag := rec.Header().Get("ETag")
	require.NotEmpty(t, etag)

	// Matching If-Range → 206 partial.
	req = httptest.NewRequest(http.MethodGet, "/ir-bucket/k", nil)
	req.Header.Set("Range", "bytes=0-3")
	req.Header.Set("If-Range", etag)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusPartialContent, rec.Code)
	assert.Equal(t, "0123", rec.Body.String())

	// Non-matching If-Range → full 200.
	req = httptest.NewRequest(http.MethodGet, "/ir-bucket/k", nil)
	req.Header.Set("Range", "bytes=0-3")
	req.Header.Set("If-Range", `"deadbeefdeadbeefdeadbeefdeadbeef"`)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "0123456789", rec.Body.String())
}
