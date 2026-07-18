package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_DeleteMarker_GetHeadSemantics verifies AWS delete-marker behavior:
// an unversioned GET/HEAD of a key whose latest version is a delete marker returns
// 404 with x-amz-delete-marker: true, and a versioned GET of the delete-marker
// version returns 405 (MethodNotAllowed) with the same header.
func TestDeleteMarker_GetHeadSemantics(t *testing.T) {
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

func TestDeleteMarker_Versioning_GetReturns404WithHeader(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "ver-bucket")
	enableVersioning(t, handler, "ver-bucket")

	mustPutObject(t, backend, "ver-bucket", "obj", []byte("v1"))

	// Delete without versionId → creates delete marker.
	delRec := doRequest(handler, http.MethodDelete, "/ver-bucket/obj", nil, nil)
	assert.Equal(t, http.StatusNoContent, delRec.Code)
	assert.Equal(t, "true", delRec.Header().Get("X-Amz-Delete-Marker"))

	// GET now returns 404 with x-amz-delete-marker: true.
	getRec := doRequest(handler, http.MethodGet, "/ver-bucket/obj", nil, nil)
	assert.Equal(t, http.StatusNotFound, getRec.Code)
}

func TestDeleteMarker_Versioning_ListVersionsEmitsMarker(t *testing.T) {
	t.Parallel()

	_, backend := newTestHandler(t)

	_, err := backend.CreateBucket(
		t.Context(),
		&sdk_s3.CreateBucketInput{Bucket: aws.String("lv-bucket")},
	)
	require.NoError(t, err)

	_, err = backend.PutBucketVersioning(t.Context(), &sdk_s3.PutBucketVersioningInput{
		Bucket: aws.String("lv-bucket"),
		VersioningConfiguration: &types.VersioningConfiguration{
			Status: types.BucketVersioningStatusEnabled,
		},
	})
	require.NoError(t, err)

	_, err = backend.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String("lv-bucket"),
		Key:    aws.String("obj"),
		Body:   strings.NewReader("v1"),
	})
	require.NoError(t, err)

	_, err = backend.DeleteObject(t.Context(), &sdk_s3.DeleteObjectInput{
		Bucket: aws.String("lv-bucket"),
		Key:    aws.String("obj"),
	})
	require.NoError(t, err)

	out, err := backend.ListObjectVersions(t.Context(), &sdk_s3.ListObjectVersionsInput{
		Bucket: aws.String("lv-bucket"),
	})
	require.NoError(t, err)

	assert.Len(t, out.Versions, 1, "should have one version")
	assert.Len(t, out.DeleteMarkers, 1, "should have one delete marker")
}

// ─── Object Lock enforcement on delete/overwrite ─────────────────────────────
