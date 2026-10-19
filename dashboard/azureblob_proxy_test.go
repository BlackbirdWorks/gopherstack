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
	"github.com/blackbirdworks/gopherstack/services/azureblob"
)

func TestAzureBlobProxy(t *testing.T) {
	t.Parallel()

	const base = "/dashboard/api/v1/azure/blob/devstoreaccount1"

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
			name: "create container", method: http.MethodPut, path: base + "/c1?restype=container",
			wantCode: http.StatusCreated,
		},
		{
			name: "list containers", method: http.MethodGet, path: base + "?comp=list",
			seed: true, wantCode: http.StatusOK, wantBody: "c1",
		},
		{
			name: "put blob", method: http.MethodPut, path: base + "/c1/dir/a.txt", body: "hello",
			seed: true, wantCode: http.StatusCreated,
		},
		{
			name: "list blobs", method: http.MethodGet, path: base + "/c1?restype=container&comp=list",
			seed: true, wantCode: http.StatusOK, wantBody: "dir/a.txt",
		},
		{
			name: "missing blob", method: http.MethodGet, path: base + "/c1/nope",
			seed: true, wantCode: http.StatusNotFound,
		},
		{name: "traversal blocked", method: http.MethodGet, path: base + "/c1/../x", wantCode: http.StatusBadRequest},
		{
			name: "no account", method: http.MethodGet, path: "/dashboard/api/v1/azure/blob/",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			blob := azureblob.NewHandler(azureblob.NewInMemoryBackend())
			h := dashboard.NewHandler(dashboard.Config{AzureBlobOps: blob})

			if tt.seed {
				seed := httptest.NewRequest(http.MethodPut, base+"/c1?restype=container", nil)
				require.NoError(t, h.Handler()(echo.NewContext(seed, httptest.NewRecorder())))

				put := httptest.NewRequest(http.MethodPut, base+"/c1/dir/a.txt", strings.NewReader("hello"))
				put.Header.Set("X-Ms-Blob-Type", "BlockBlob")
				require.NoError(t, h.Handler()(echo.NewContext(put, httptest.NewRecorder())))
			}

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			req.Header.Set("X-Ms-Blob-Type", "BlockBlob")
			require.NoError(t, h.Handler()(echo.NewContext(req, rec)))

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

func TestAzureBlobProxyDisabled(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/dashboard/api/v1/azure/blob/a?comp=list", nil)
	require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}
