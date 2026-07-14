package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_BucketEncryption verifies the PutBucketEncryption /
// GetBucketEncryption / DeleteBucketEncryption HTTP handlers, including error
// paths for missing configuration and malformed XML.
func TestHandler_BucketEncryption(t *testing.T) {
	t.Parallel()

	const encryptionXML = `<ServerSideEncryptionConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">` +
		`<Rule><ApplyServerSideEncryptionByDefault>` +
		`<SSEAlgorithm>AES256</SSEAlgorithm>` +
		`</ApplyServerSideEncryptionByDefault></Rule>` +
		`</ServerSideEncryptionConfiguration>`

	tests := []struct {
		name     string
		method   string
		url      string
		body     string
		setup    bool
		wantCode int
	}{
		{
			name:     "put_encryption_config",
			method:   http.MethodPut,
			url:      "/enc-test-bucket?encryption",
			body:     encryptionXML,
			wantCode: http.StatusOK,
		},
		{
			name:     "get_encryption_config",
			method:   http.MethodGet,
			url:      "/enc-test-bucket?encryption",
			setup:    true,
			wantCode: http.StatusOK,
		},
		{
			name:     "delete_encryption_config",
			method:   http.MethodDelete,
			url:      "/enc-test-bucket?encryption",
			setup:    true,
			wantCode: http.StatusNoContent,
		},
		{
			name:     "get_encryption_config_not_found",
			method:   http.MethodGet,
			url:      "/enc-notfound-bucket?encryption",
			wantCode: http.StatusNotFound,
		},
		{
			name:     "put_encryption_invalid_xml",
			method:   http.MethodPut,
			url:      "/enc-test-bucket?encryption",
			body:     "not-xml",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "enc-test-bucket")

			if tt.setup {
				putReq := httptest.NewRequest(
					http.MethodPut,
					"/enc-test-bucket?encryption",
					strings.NewReader(encryptionXML),
				)
				putRec := httptest.NewRecorder()
				serveS3Handler(handler, putRec, putReq)
				require.Equal(t, http.StatusOK, putRec.Code)
			}

			var reqBody *strings.Reader
			if tt.body != "" {
				reqBody = strings.NewReader(tt.body)
			} else {
				reqBody = strings.NewReader("")
			}

			req := httptest.NewRequest(tt.method, tt.url, reqBody)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
