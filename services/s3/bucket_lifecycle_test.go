package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/blackbirdworks/gopherstack/services/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3BucketLifecycleCRUD(t *testing.T) {
	t.Parallel()
	handler, sdkClient := newTestHandler(t)
	bucket := "lifecycle-test-bucket"

	_, err := sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: &bucket})
	require.NoError(t, err)

	lifecycleXML := `<LifecycleConfiguration><Rule><ID>expire-old</ID>` +
		`<Status>Enabled</Status><Expiration><Days>30</Days></Expiration></Rule></LifecycleConfiguration>`

	// PutBucketLifecycleConfiguration
	req := httptest.NewRequest(
		http.MethodPut,
		"/"+bucket+"?lifecycle",
		strings.NewReader(lifecycleXML),
	)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// GetBucketLifecycleConfiguration
	req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?lifecycle", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "expire-old")

	// DeleteBucketLifecycleConfiguration
	req = httptest.NewRequest(http.MethodDelete, "/"+bucket+"?lifecycle", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// GetBucketLifecycleConfiguration after delete → NoSuchLifecycleConfiguration
	req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?lifecycle", nil)
	rec = httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestS3BucketNotificationCRUD verifies put/get bucket notification configuration.

func TestS3_DeleteBucketLifecycle(t *testing.T) {
	t.Parallel()

	lifecycleXML := `<LifecycleConfiguration><Rule><ID>rule1</ID>` +
		`<Status>Enabled</Status><Expiration><Days>7</Days></Expiration></Rule></LifecycleConfiguration>`

	tests := []struct {
		name       string
		setup      func(t *testing.T, handler *s3.S3Handler, backend *s3.InMemoryBackend)
		method     string
		path       string
		wantStatus int
	}{
		{
			name:   "DeleteBucketLifecycle clears lifecycle config",
			method: http.MethodDelete,
			path:   "/lifecycle-bucket?lifecycle",
			setup: func(t *testing.T, handler *s3.S3Handler, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "lifecycle-bucket")
				req := httptest.NewRequest(
					http.MethodPut,
					"/lifecycle-bucket?lifecycle",
					strings.NewReader(lifecycleXML),
				)
				rec := httptest.NewRecorder()
				serveS3Handler(handler, rec, req)
				require.Equal(t, http.StatusNoContent, rec.Code)
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "DeleteBucketLifecycle on missing bucket returns 404",
			method:     http.MethodDelete,
			path:       "/no-such-bucket?lifecycle",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, handler, backend)
			}

			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestS3_BucketMetadataConfig verifies create/get/delete bucket metadata configuration.
