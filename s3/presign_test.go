package s3_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// presignedURL returns a fake presigned URL query string for testing.
// dateStr is in "20060102T150405Z" format. expiresSeconds is the lifetime.
func presignedQuery(dateStr string, expiresSeconds int) string {
	return fmt.Sprintf(
		"?X-Amz-Algorithm=AWS4-HMAC-SHA256"+
			"&X-Amz-Credential=test%%2F20240101%%2Fus-east-1%%2Fs3%%2Faws4_request"+
			"&X-Amz-Date=%s"+
			"&X-Amz-Expires=%d"+
			"&X-Amz-SignedHeaders=host"+
			"&X-Amz-Signature=fakesig",
		dateStr,
		expiresSeconds,
	)
}

func TestHandler_PresignedGet_ValidURL(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "my-bucket")
	mustPutObject(t, backend, "my-bucket", "file.txt", []byte("hello presigned"))

	// Use a date 5 minutes in the past, with 1 hour expiry → still valid.
	dateStr := time.Now().UTC().Add(-5 * time.Minute).Format("20060102T150405Z")
	query := presignedQuery(dateStr, 3600)

	req := httptest.NewRequest(http.MethodGet, "/my-bucket/file.txt"+query, nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	body, err := io.ReadAll(rec.Body)
	require.NoError(t, err)
	assert.Equal(t, "hello presigned", string(body))
}

func TestHandler_PresignedGet_ExpiredURL(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "my-bucket")
	mustPutObject(t, backend, "my-bucket", "file.txt", []byte("hello"))

	// Use a date 2 hours in the past, with 1 hour expiry → expired.
	dateStr := time.Now().UTC().Add(-2 * time.Hour).Format("20060102T150405Z")
	query := presignedQuery(dateStr, 3600)

	req := httptest.NewRequest(http.MethodGet, "/my-bucket/file.txt"+query, nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Contains(t, rec.Body.String(), "AccessDenied")
}

func TestHandler_PresignedPut_ValidURL(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "my-bucket")

	// Use a date 1 minute in the past, with 1 hour expiry → still valid.
	dateStr := time.Now().UTC().Add(-1 * time.Minute).Format("20060102T150405Z")
	query := presignedQuery(dateStr, 3600)

	req := httptest.NewRequest(http.MethodPut, "/my-bucket/uploaded.txt"+query,
		io.NopCloser(http.NoBody))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	// Verify the object exists with a regular (non-presigned) GET.
	req2 := httptest.NewRequest(http.MethodGet, "/my-bucket/uploaded.txt", nil)
	rec2 := httptest.NewRecorder()
	serveS3Handler(handler, rec2, req2)
	assert.Equal(t, http.StatusOK, rec2.Code)
}

func TestHandler_PresignedGet_MissingDate(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "my-bucket")
	mustPutObject(t, backend, "my-bucket", "file.txt", []byte("hello"))

	// Missing X-Amz-Date → should be rejected.
	req := httptest.NewRequest(http.MethodGet,
		"/my-bucket/file.txt?X-Amz-Signature=fakesig&X-Amz-Expires=3600", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Contains(t, rec.Body.String(), "AccessDenied")
}

func TestHandler_PresignedGet_InvalidDate(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "my-bucket")
	mustPutObject(t, backend, "my-bucket", "file.txt", []byte("hello"))

	// Malformed X-Amz-Date → should be rejected.
	req := httptest.NewRequest(http.MethodGet,
		"/my-bucket/file.txt?X-Amz-Signature=fakesig&X-Amz-Date=NOTADATE&X-Amz-Expires=3600", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "AuthorizationQueryParametersError")
}

func TestHandler_PresignedGet_InvalidExpires(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "my-bucket")
	mustPutObject(t, backend, "my-bucket", "file.txt", []byte("hello"))

	dateStr := time.Now().UTC().Format("20060102T150405Z")

	// X-Amz-Expires is not a number → should be rejected.
	req := httptest.NewRequest(http.MethodGet,
		fmt.Sprintf("/my-bucket/file.txt?X-Amz-Signature=fakesig&X-Amz-Date=%s&X-Amz-Expires=notanumber", dateStr),
		nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "AuthorizationQueryParametersError")
}

func TestHandler_NonPresigned_Unaffected(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "my-bucket")
	mustPutObject(t, backend, "my-bucket", "file.txt", []byte("regular"))

	// Normal (non-presigned) GET should be unaffected.
	req := httptest.NewRequest(http.MethodGet, "/my-bucket/file.txt", nil)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusOK, rec.Code)

	body, err := io.ReadAll(rec.Body)
	require.NoError(t, err)
	assert.Equal(t, "regular", string(body))
}
