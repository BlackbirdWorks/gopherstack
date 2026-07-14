package s3_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3"
)

// TestCompleteMultipartUpload_EmptyParts verifies that CompleteMultipartUpload
// rejects a request with no <Parts> (or an empty <Parts> list).
func TestCompleteMultipartUpload_EmptyParts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name:     "nil_parts_rejected",
			body:     `<CompleteMultipartUpload/>`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty_parts_list_rejected",
			body:     `<CompleteMultipartUpload><Parts></Parts></CompleteMultipartUpload>`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "test-bucket")

			// Initiate multipart upload.
			req := httptest.NewRequest(http.MethodPost, "/test-bucket/my-key?uploads", nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			require.Equal(t, http.StatusOK, rec.Code)

			var initResult s3.InitiateMultipartUploadResult
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &initResult))
			uploadID := initResult.UploadID
			require.NotEmpty(t, uploadID)

			// Complete with empty parts.
			req = httptest.NewRequest(
				http.MethodPost,
				"/test-bucket/my-key?uploadId="+uploadID,
				strings.NewReader(tt.body),
			)
			rec = httptest.NewRecorder()
			serveS3Handler(handler, rec, req)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
