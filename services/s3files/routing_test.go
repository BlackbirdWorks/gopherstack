package s3files_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/s3files"
)

func TestRouteMatcherAndExtraction(t *testing.T) {
	t.Parallel()

	const signed = "AWS4-HMAC-SHA256 Credential=AKIA/20260101/us-east-1/s3files/aws4_request, " +
		"SignedHeaders=host, Signature=00"

	tests := []struct {
		name         string
		method       string
		path         string
		auth         string
		host         string
		wantOp       string
		wantResource string
		wantMatch    bool
	}{
		{
			name:      "create",
			method:    http.MethodPut,
			path:      "/file-systems",
			auth:      signed,
			wantMatch: true,
			wantOp:    "CreateFileSystem",
		},
		{
			name:         "get",
			method:       http.MethodGet,
			path:         "/file-systems/fs-1",
			auth:         signed,
			wantMatch:    true,
			wantOp:       "GetFileSystem",
			wantResource: "fs-1",
		},
		{
			name:         "arn id",
			method:       http.MethodGet,
			path:         "/file-systems/arn:aws:s3files:us-east-1:1:file-system%2Ffs-1",
			auth:         signed,
			wantMatch:    true,
			wantOp:       "GetFileSystem",
			wantResource: "arn:aws:s3files:us-east-1:1:file-system/fs-1",
		},
		{
			name:         "policy",
			method:       http.MethodDelete,
			path:         "/file-systems/fs-1/policy",
			auth:         signed,
			wantMatch:    true,
			wantOp:       "DeleteFileSystemPolicy",
			wantResource: "fs-1",
		},
		{
			name:         "tags",
			method:       http.MethodPost,
			path:         "/resource-tags/fs-1",
			auth:         signed,
			wantMatch:    true,
			wantOp:       "TagResource",
			wantResource: "fs-1",
		},
		{
			name:      "endpoint host unsigned",
			method:    http.MethodGet,
			path:      "/mount-targets",
			host:      "s3files.us-east-1.amazonaws.com",
			wantMatch: true,
			wantOp:    "ListMountTargets",
		},
		{name: "unsigned", method: http.MethodGet, path: "/file-systems", wantOp: "ListFileSystems"},
		{
			name:   "other service signature",
			method: http.MethodGet,
			path:   "/file-systems",
			auth:   "AWS4-HMAC-SHA256 Credential=AKIA/20260101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=00",
			wantOp: "ListFileSystems",
		},
		{name: "wrong method", method: http.MethodPatch, path: "/file-systems", auth: signed, wantOp: "Unknown"},
		{name: "unknown path", method: http.MethodGet, path: "/buckets", auth: signed, wantOp: "Unknown"},
	}

	h := s3files.NewHandler(s3files.NewInMemoryBackend(testAccount, testRegion))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, http.NoBody)
			if tt.auth != "" {
				req.Header.Set("Authorization", tt.auth)
			}

			if tt.host != "" {
				req.Host = tt.host
			}

			c := echo.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
			assert.Equal(t, tt.wantResource, h.ExtractResource(c))
		})
	}
}

func TestSupportedOperations(t *testing.T) {
	t.Parallel()

	h := s3files.NewHandler(s3files.NewInMemoryBackend(testAccount, testRegion))
	assert.Len(t, h.GetSupportedOperations(), 21)
}
