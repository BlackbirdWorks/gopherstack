package s3_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandler_Tagging(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	bucket := "tag-bucket"
	key := "tag-key"

	mustCreateBucket(t, backend, bucket)
	mustPutObject(t, backend, bucket, key, []byte("data"))

	// 1. Put Tagging
	taggingXML := `<Tagging><TagSet><Tag><Key>testkey</Key><Value>testval</Value></Tag></TagSet></Tagging>`
	req := httptest.NewRequest(http.MethodPut, fmt.Sprintf("/%s/%s?tagging", bucket, key), strings.NewReader(taggingXML))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// 2. Get Tagging
	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/%s/%s?tagging", bucket, key), nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "testkey")
	assert.Contains(t, rec.Body.String(), "testval")

	// 3. Delete Tagging
	req = httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/%s/%s?tagging", bucket, key), nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// 4. Verify Deleted
	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/%s/%s?tagging", bucket, key), nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "testkey")
}

func TestHandler_Versioning(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	bucket := "ver-bucket"
	mustCreateBucket(t, backend, bucket)

	// Get Versioning (Default)
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/%s?versioning", bucket), nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "VersioningConfiguration")
}

func TestHandler_MultipartUpload_ErrorsExtra(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)
	bucket := "mp-bucket"
	// Create bucket implicitly via handler or backend later?
	// Let's use backend to avoid noise.
	// Actually, just test invalid upload ID

	req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/%s/key?uploadId=invalid", bucket), nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	// Should fail with 404 or 400 because upload ID doesn't exist
	assert.NotEqual(t, http.StatusOK, rec.Code)
}
