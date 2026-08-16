package s3_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/s3"
)

func TestHandler_VirtualHostedStyle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		endpoint   string
		host       string
		path       string
		bucket     string
		wantStatus int
	}{
		{
			name:       "valid virtual hosted bucket returns 404 for missing key",
			endpoint:   "localhost:8080",
			host:       "my-bucket.localhost:8080",
			path:       "/key",
			bucket:     "my-bucket",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "invalid bucket name in host falls back to path style",
			endpoint:   "localhost:8080",
			host:       "invalid_bucket.localhost:8080",
			path:       "/key",
			bucket:     "my-bucket",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "host not matching endpoint falls back to path style",
			endpoint:   "localhost:8080",
			host:       "my-bucket.otherhost.com",
			path:       "/key",
			bucket:     "my-bucket",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "host without port matches endpoint",
			endpoint:   "s3.amazonaws.com",
			host:       "mybucket.s3.amazonaws.com",
			path:       "/key",
			bucket:     "mybucket",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			backend := s3.NewInMemoryBackend(&s3.GzipCompressor{})
			handler := s3.NewHandler(backend)
			handler.Endpoint = tt.endpoint
			mustCreateBucket(t, backend, tt.bucket)

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			req.Host = tt.host
			rec := httptest.NewRecorder()

			// Use Echo handler
			ctx := logger.Save(req.Context(), slog.Default())
			req = req.WithContext(ctx)
			e := echo.New()
			c := e.NewContext(req, rec)
			_ = handler.Handler()(c)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_VirtualHostedStyle_Final(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		host       string
		path       string
		wantBody   string
		wantStatus int
	}{
		{
			name:       "valid virtual hosted bucket key not found",
			host:       "my-vh-bucket.localhost:9000",
			path:       "/somekey",
			wantStatus: http.StatusNotFound,
			wantBody:   "NoSuchKey",
		},
		{
			name:       "invalid bucket in host falls back to path style",
			host:       "invalid_bucket.localhost:9000",
			path:       "/",
			wantStatus: http.StatusOK,
		},
		{
			name:       "host not matching endpoint falls back",
			host:       "my-vh-bucket.other:9000",
			path:       "/",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			handler.Endpoint = "localhost:9000"
			mustCreateBucket(t, backend, "my-vh-bucket")

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			req.Host = tt.host
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}
