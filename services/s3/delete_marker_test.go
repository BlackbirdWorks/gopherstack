package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_DeleteMarker_GetHeadSemantics verifies AWS delete-marker behavior:
// an unversioned GET/HEAD of a key whose latest version is a delete marker returns
// 404 with x-amz-delete-marker: true, and a versioned GET of the delete-marker
// version returns 405 (MethodNotAllowed) with the same header.
func TestParity_DeleteMarker_GetHeadSemantics(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "dm-bucket")

	// Enable versioning.
	req := httptest.NewRequest(http.MethodPut, "/dm-bucket?versioning",
		strings.NewReader(`<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Put an object.
	req = httptest.NewRequest(http.MethodPut, "/dm-bucket/k", strings.NewReader("hello"))
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete it → creates a delete marker; capture its version id.
	req = httptest.NewRequest(http.MethodDelete, "/dm-bucket/k", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)
	assert.Equal(t, "true", rec.Header().Get("X-Amz-Delete-Marker"))
	dmVersion := rec.Header().Get("X-Amz-Version-Id")
	require.NotEmpty(t, dmVersion)

	// Unversioned GET → 404 + x-amz-delete-marker.
	req = httptest.NewRequest(http.MethodGet, "/dm-bucket/k", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Equal(t, "true", rec.Header().Get("X-Amz-Delete-Marker"))
	assert.Contains(t, rec.Body.String(), "NoSuchKey")

	// Unversioned HEAD → 404 + x-amz-delete-marker.
	req = httptest.NewRequest(http.MethodHead, "/dm-bucket/k", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Equal(t, "true", rec.Header().Get("X-Amz-Delete-Marker"))

	// Versioned GET of the delete-marker version → 405 + x-amz-delete-marker.
	req = httptest.NewRequest(http.MethodGet, "/dm-bucket/k?versionId="+dmVersion, nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	assert.Equal(t, "true", rec.Header().Get("X-Amz-Delete-Marker"))
}
