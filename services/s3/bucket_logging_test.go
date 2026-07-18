package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3BucketLoggingCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		configXML string
		wantBody  string
		wantPut   int
		wantGet   int
	}{
		{
			name: "logging-enabled",
			configXML: `<BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
				`<LoggingEnabled>` +
				`<TargetBucket>my-logs-bucket</TargetBucket>` +
				`<TargetPrefix>logs/</TargetPrefix>` +
				`</LoggingEnabled>` +
				`</BucketLoggingStatus>`,
			wantPut:  http.StatusOK,
			wantGet:  http.StatusOK,
			wantBody: "my-logs-bucket",
		},
		{
			name: "logging-disabled",
			configXML: `<BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
				`</BucketLoggingStatus>`,
			wantPut:  http.StatusOK,
			wantGet:  http.StatusOK,
			wantBody: "BucketLoggingStatus",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			handler, sdkClient := newTestHandler(t)
			bucket := "logging-test-" + tt.name

			_, err := sdkClient.CreateBucket(
				t.Context(),
				&sdk_s3.CreateBucketInput{Bucket: &bucket},
			)
			require.NoError(t, err)

			// GetBucketLogging before put → empty BucketLoggingStatus (not an error)
			req := httptest.NewRequest(http.MethodGet, "/"+bucket+"?logging", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), "BucketLoggingStatus")

			// PutBucketLogging
			req = httptest.NewRequest(
				http.MethodPut,
				"/"+bucket+"?logging",
				strings.NewReader(tt.configXML),
			)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantPut, rec.Code)

			// Second PutBucketLogging
			req = httptest.NewRequest(
				http.MethodPut,
				"/"+bucket+"?logging",
				strings.NewReader(tt.configXML),
			)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantPut, rec.Code)

			// GetBucketLogging
			req = httptest.NewRequest(http.MethodGet, "/"+bucket+"?logging", nil)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantGet, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

// TestS3BucketLogging_MalformedXML verifies that PutBucketLogging rejects invalid XML.

func TestS3BucketLogging_MalformedXML(t *testing.T) {
	t.Parallel()
	handler, sdkClient := newTestHandler(t)
	bucket := "logging-malformed-bucket"

	_, err := sdkClient.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: &bucket})
	require.NoError(t, err)

	req := httptest.NewRequest(
		http.MethodPut,
		"/"+bucket+"?logging",
		strings.NewReader("not-valid-xml"),
	)
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "MalformedXML")
}

// TestS3BucketReplicationCRUD verifies put/get/delete bucket replication configuration.
