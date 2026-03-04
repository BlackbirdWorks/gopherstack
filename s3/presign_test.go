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

// presignedQuery returns a fake presigned URL query string for testing.
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

func TestHandler_PresignedGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		urlFn       func() string
		wantCode    int
		wantBody    string
		wantContain string
	}{
		{
			name: "valid url still within expiry window",
			urlFn: func() string {
				dateStr := time.Now().UTC().Add(-5 * time.Minute).Format("20060102T150405Z")
				return "/my-bucket/file.txt" + presignedQuery(dateStr, 3600)
			},
			wantCode: http.StatusOK,
			wantBody: "hello presigned",
		},
		{
			name: "expired url",
			urlFn: func() string {
				dateStr := time.Now().UTC().Add(-2 * time.Hour).Format("20060102T150405Z")
				return "/my-bucket/file.txt" + presignedQuery(dateStr, 3600)
			},
			wantCode:    http.StatusForbidden,
			wantContain: "AccessDenied",
		},
		{
			name: "missing X-Amz-Date rejected",
			urlFn: func() string {
				return "/my-bucket/file.txt?X-Amz-Signature=fakesig&X-Amz-Expires=3600"
			},
			wantCode:    http.StatusForbidden,
			wantContain: "AccessDenied",
		},
		{
			name: "invalid X-Amz-Date rejected",
			urlFn: func() string {
				return "/my-bucket/file.txt?X-Amz-Signature=fakesig&X-Amz-Date=NOTADATE&X-Amz-Expires=3600"
			},
			wantCode:    http.StatusBadRequest,
			wantContain: "AuthorizationQueryParametersError",
		},
		{
			name: "non-numeric X-Amz-Expires rejected",
			urlFn: func() string {
				dateStr := time.Now().UTC().Format("20060102T150405Z")
				return fmt.Sprintf("/my-bucket/file.txt?X-Amz-Signature=fakesig&X-Amz-Date=%s&X-Amz-Expires=notanumber", dateStr)
			},
			wantCode:    http.StatusBadRequest,
			wantContain: "AuthorizationQueryParametersError",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "my-bucket")
			mustPutObject(t, backend, "my-bucket", "file.txt", []byte("hello presigned"))

			req := httptest.NewRequest(http.MethodGet, tt.urlFn(), nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantBody != "" {
				body, err := io.ReadAll(rec.Body)
				require.NoError(t, err)
				assert.Equal(t, tt.wantBody, string(body))
			}

			if tt.wantContain != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContain)
			}
		})
	}
}

func TestHandler_PresignedPut(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		urlFn         func() string
		wantPutCode   int
		wantGetCode   int
		verifyGetPath string
	}{
		{
			name: "valid url uploads object successfully",
			urlFn: func() string {
				dateStr := time.Now().UTC().Add(-1 * time.Minute).Format("20060102T150405Z")
				return "/my-bucket/uploaded.txt" + presignedQuery(dateStr, 3600)
			},
			wantPutCode:   http.StatusOK,
			wantGetCode:   http.StatusOK,
			verifyGetPath: "/my-bucket/uploaded.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "my-bucket")

			req := httptest.NewRequest(http.MethodPut, tt.urlFn(), io.NopCloser(http.NoBody))
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			require.Equal(t, tt.wantPutCode, rec.Code)

			if tt.verifyGetPath != "" {
				req2 := httptest.NewRequest(http.MethodGet, tt.verifyGetPath, nil)
				rec2 := httptest.NewRecorder()
				serveS3Handler(handler, rec2, req2)
				assert.Equal(t, tt.wantGetCode, rec2.Code)
			}
		})
	}
}

func TestHandler_NonPresigned_Unaffected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		method   string
		path     string
		body     []byte
		wantCode int
		wantBody string
	}{
		{
			name:     "regular GET bypasses presign checks",
			method:   http.MethodGet,
			path:     "/my-bucket/file.txt",
			body:     []byte("regular"),
			wantCode: http.StatusOK,
			wantBody: "regular",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			mustCreateBucket(t, backend, "my-bucket")
			mustPutObject(t, backend, "my-bucket", "file.txt", tt.body)

			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			require.Equal(t, tt.wantCode, rec.Code)

			body, err := io.ReadAll(rec.Body)
			require.NoError(t, err)
			assert.Equal(t, tt.wantBody, string(body))
		})
	}
}
