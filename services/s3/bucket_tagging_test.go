package s3_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/s3"
	"github.com/stretchr/testify/assert"
)

func TestHandler_BucketTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, backend *s3.InMemoryBackend)
		name       string
		method     string
		body       string
		wantBody   string
		wantStatus int
	}{
		{
			name:   "PUT bucket tagging succeeds",
			method: http.MethodPut,
			setup: func(t *testing.T, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "bkt")
			},
			body:       `<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>`,
			wantStatus: http.StatusNoContent,
		},
		{
			name:   "GET bucket tagging returns tags",
			method: http.MethodGet,
			setup: func(t *testing.T, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "bkt")

				// pre-populate a tag
				req := httptest.NewRequest(http.MethodPut, "/bkt?tagging", strings.NewReader(
					`<Tagging><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tagging>`,
				))
				rec := httptest.NewRecorder()
				handler, _ := newTestHandler(t)
				handler.Backend = backend
				serveS3Handler(handler, rec, req)
			},
			wantStatus: http.StatusOK,
			wantBody:   "env",
		},
		{
			name:   "GET bucket tagging returns 404 when no tags set",
			method: http.MethodGet,
			setup: func(t *testing.T, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "bkt")
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name:   "DELETE bucket tagging succeeds",
			method: http.MethodDelete,
			setup: func(t *testing.T, backend *s3.InMemoryBackend) {
				t.Helper()
				mustCreateBucket(t, backend, "bkt")
			},
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, backend := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, backend)
			}

			var reqBody io.Reader
			if tt.body != "" {
				reqBody = strings.NewReader(tt.body)
			}

			req := httptest.NewRequest(tt.method, "/bkt?tagging", reqBody)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_BucketTaggingMissingBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		body       string
		wantStatus int
	}{
		{
			name:       "PUT bucket tagging without bucket returns 404",
			method:     http.MethodPut,
			body:       `<Tagging><TagSet><Tag><Key>k</Key><Value>v</Value></Tag></TagSet></Tagging>`,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "GET bucket tagging without bucket returns 404",
			method:     http.MethodGet,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, _ := newTestHandler(t)

			var body io.Reader
			if tt.body != "" {
				body = strings.NewReader(tt.body)
			}

			req := httptest.NewRequest(tt.method, "/bkt?tagging", body)
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
