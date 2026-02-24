package s3_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/s3"
)

func TestHandler_ListMultipartUploads_Empty(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "mpu-empty")

	req := httptest.NewRequest(http.MethodGet, "/mpu-empty?uploads", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var result s3.ListMultipartUploadsResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))
	assert.Equal(t, "mpu-empty", result.Bucket)
	assert.Empty(t, result.Uploads)
}

func TestHandler_ListMultipartUploads_WithUploads(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "mpu-list")

	// Start two multipart uploads
	req := httptest.NewRequest(http.MethodPost, "/mpu-list/key1?uploads", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	req = httptest.NewRequest(http.MethodPost, "/mpu-list/key2?uploads", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// List uploads
	req = httptest.NewRequest(http.MethodGet, "/mpu-list?uploads", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var result s3.ListMultipartUploadsResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))
	assert.Equal(t, "mpu-list", result.Bucket)
	assert.Len(t, result.Uploads, 2)
}

func TestHandler_ListMultipartUploads_WithPrefix(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "mpu-prefix")

	// Start uploads with different key prefixes
	for _, key := range []string{"logs/2024/a", "logs/2024/b", "data/x"} {
		req := httptest.NewRequest(http.MethodPost, "/mpu-prefix/"+key+"?uploads", nil)
		rec := httptest.NewRecorder()
		serveS3Handler(handler, rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// List uploads with prefix=logs/
	req := httptest.NewRequest(http.MethodGet, "/mpu-prefix?uploads&prefix=logs/", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var result s3.ListMultipartUploadsResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))
	assert.Len(t, result.Uploads, 2)
	for _, u := range result.Uploads {
		assert.True(t, strings.HasPrefix(u.Key, "logs/"))
	}
}

func TestHandler_ListMultipartUploads_NoSuchBucket(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/no-such-bucket?uploads", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_ListParts_WithParts(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "lp-bucket")

	// Create a multipart upload
	req := httptest.NewRequest(http.MethodPost, "/lp-bucket/myobj?uploads", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	start := strings.Index(body, "<UploadId>") + len("<UploadId>")
	end := strings.Index(body, "</UploadId>")
	uploadID := body[start:end]

	// Upload two parts
	for _, partNum := range []string{"1", "2"} {
		req = httptest.NewRequest(
			http.MethodPut,
			"/lp-bucket/myobj?partNumber="+partNum+"&uploadId="+uploadID,
			strings.NewReader("part-data-"+partNum),
		)
		rec = httptest.NewRecorder()
		serveS3Handler(handler, rec, req)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// List parts
	req = httptest.NewRequest(http.MethodGet, "/lp-bucket/myobj?uploadId="+uploadID, nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var result s3.ListPartsResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))
	assert.Equal(t, "lp-bucket", result.Bucket)
	assert.Equal(t, "myobj", result.Key)
	assert.Equal(t, uploadID, result.UploadID)
	require.Len(t, result.Parts, 2)
	assert.Equal(t, 1, result.Parts[0].PartNumber)
	assert.Equal(t, 2, result.Parts[1].PartNumber)
	assert.Greater(t, result.Parts[0].Size, int64(0))
	assert.NotEmpty(t, result.Parts[0].ETag)
}

func TestHandler_ListParts_NoSuchUpload(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "lp-nosuchupload")

	req := httptest.NewRequest(http.MethodGet, "/lp-nosuchupload/obj?uploadId=nonexistent", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_ListParts_Empty(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "lp-empty")

	// Create upload but don't add any parts
	req := httptest.NewRequest(http.MethodPost, "/lp-empty/obj?uploads", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	start := strings.Index(body, "<UploadId>") + len("<UploadId>")
	end := strings.Index(body, "</UploadId>")
	uploadID := body[start:end]

	req = httptest.NewRequest(http.MethodGet, "/lp-empty/obj?uploadId="+uploadID, nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	var result s3.ListPartsResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))
	assert.Empty(t, result.Parts)
}
