package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

func TestGCPStorageProxy(t *testing.T) {
	t.Parallel()

	const base = "/dashboard/api/v1/gcp/storage"

	tests := []struct {
		name     string
		method   string
		path     string
		body     string
		wantBody string
		wantCode int
		seed     bool
	}{
		{
			name: "create bucket", method: http.MethodPost, path: base + "/storage/v1/b?project=p",
			body: `{"name":"bkt1"}`, wantCode: http.StatusOK, wantBody: `"bkt1"`,
		},
		{
			name: "list buckets", method: http.MethodGet, path: base + "/storage/v1/b?project=p",
			seed: true, wantCode: http.StatusOK, wantBody: `"bkt1"`,
		},
		{
			name: "media upload", method: http.MethodPost,
			path: base + "/upload/storage/v1/b/bkt1/o?uploadType=media&name=dir%2Fa.txt",
			body: "hello", seed: true, wantCode: http.StatusOK, wantBody: `"dir/a.txt"`,
		},
		{
			name: "list objects delimiter", method: http.MethodGet,
			path: base + "/storage/v1/b/bkt1/o?delimiter=/", seed: true, wantCode: http.StatusOK, wantBody: `"dir/"`,
		},
		{
			name: "get encoded object", method: http.MethodGet, path: base + "/storage/v1/b/bkt1/o/dir%2Fa.txt",
			seed: true, wantCode: http.StatusOK, wantBody: `"dir/a.txt"`,
		},
		{
			name: "download", method: http.MethodGet,
			path: base + "/download/storage/v1/b/bkt1/o/dir%2Fa.txt?alt=media",
			seed: true, wantCode: http.StatusOK, wantBody: "hello",
		},
		{
			name: "missing object", method: http.MethodGet, path: base + "/storage/v1/b/bkt1/o/nope",
			seed: true, wantCode: http.StatusNotFound,
		},
		{
			name: "traversal blocked", method: http.MethodGet,
			path: base + "/storage/v1/b/bkt1/o/%2E%2E", wantCode: http.StatusBadRequest,
		},
		{
			name: "xml path blocked", method: http.MethodGet,
			path: base + "/bkt1/dir/a.txt", wantCode: http.StatusBadRequest,
		},
		{
			name: "compose blocked", method: http.MethodPost,
			path: base + "/storage/v1/b/bkt1/o/x/compose", wantCode: http.StatusBadRequest,
		},
		{name: "empty", method: http.MethodGet, path: base + "/", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := dashboard.NewHandler(dashboard.Config{
				GCPStorageOps: gcpstorage.NewHandler(gcpstorage.NewInMemoryBackend()),
			})

			do := func(method, path, body string) *httptest.ResponseRecorder {
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(method, path, strings.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				require.NoError(t, h.Handler()(echo.NewContext(req, rec)))

				return rec
			}

			if tt.seed {
				do(http.MethodPost, base+"/storage/v1/b?project=p", `{"name":"bkt1"}`)
				do(http.MethodPost, base+"/upload/storage/v1/b/bkt1/o?uploadType=media&name=dir%2Fa.txt", "hello")
			}

			rec := do(tt.method, tt.path, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

func TestGCPStorageProxyDisabled(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/dashboard/api/v1/gcp/storage/storage/v1/b", nil)
	require.NoError(t, h.Handler()(echo.NewContext(req, rec)))

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}
