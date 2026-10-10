package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestS3ExpressSession_ReadOnlyMode(t *testing.T) {
	t.Parallel()

	const bucket = "ro--use1-az4--x-s3"

	tests := []struct {
		name     string
		mode     types.SessionMode
		method   string
		wantCode string
	}{
		{"readonly_put", types.SessionModeReadOnly, http.MethodPut, "AccessDenied"},
		{"readonly_delete", types.SessionModeReadOnly, http.MethodDelete, "AccessDenied"},
		{"readonly_get", types.SessionModeReadOnly, http.MethodGet, "SignatureDoesNotMatch"},
		{"readwrite_put", types.SessionModeReadWrite, http.MethodPut, "SignatureDoesNotMatch"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			handler.PresignSecret = "secret"
			mustCreateBucket(t, backend, bucket)

			creds, err := backend.CreateSession(t.Context(), bucket, tt.mode)
			require.NoError(t, err)

			now := time.Now().UTC()
			req := httptest.NewRequest(tt.method, "/"+bucket+"/k", strings.NewReader(""))
			req.Header.Set("X-Amz-Date", now.Format("20060102T150405Z"))
			req.Header.Set("X-Amz-S3session-Token", creds.SessionToken)
			req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+creds.AccessKeyID+"/"+
				now.Format("20060102")+"/us-east-1/s3express/aws4_request, SignedHeaders=host, Signature=00")

			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, http.StatusForbidden, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantCode)
		})
	}
}
