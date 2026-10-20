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
	"github.com/blackbirdworks/gopherstack/services/gcpbigquery"
)

func TestGCPBigQueryProxy(t *testing.T) {
	t.Parallel()

	const base = "/dashboard/api/v1/gcp/bigquery/bigquery/v2/projects"

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
			name: "create dataset", method: http.MethodPost, path: base + "/p/datasets",
			body: `{"datasetReference":{"datasetId":"ds1"}}`, wantCode: http.StatusOK, wantBody: `"ds1"`,
		},
		{name: "list datasets", method: http.MethodGet, path: base + "/p/datasets", seed: true,
			wantCode: http.StatusOK, wantBody: `"ds1"`},
		{
			name: "query", method: http.MethodPost, path: base + "/p/queries", seed: true,
			body:     `{"query":"SELECT 1 AS x","useLegacySql":false}`,
			wantCode: http.StatusOK, wantBody: `"jobComplete":true`,
		},
		{name: "query error", method: http.MethodPost, path: base + "/p/queries", seed: true,
			body: `{"query":"SELEC nope"}`, wantCode: http.StatusBadRequest},
		{name: "traversal blocked", method: http.MethodGet, path: base + "/p/datasets/%2E%2E",
			wantCode: http.StatusBadRequest},
		{name: "delete job blocked", method: http.MethodDelete, path: base + "/p/jobs/j1",
			wantCode: http.StatusBadRequest},
		{name: "put dataset blocked", method: http.MethodPut, path: base + "/p/datasets/ds1",
			wantCode: http.StatusMethodNotAllowed},
		{name: "upload blocked", method: http.MethodPost,
			path:     "/dashboard/api/v1/gcp/bigquery/upload/bigquery/v2/projects/p/jobs",
			wantCode: http.StatusBadRequest},
		{name: "unknown shape", method: http.MethodGet, path: base + "/p/models", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := dashboard.NewHandler(dashboard.Config{
				GCPBigQueryOps: gcpbigquery.NewHandler(gcpbigquery.NewInMemoryBackend()),
			})

			do := func(method, path, body string) *httptest.ResponseRecorder {
				rec := httptest.NewRecorder()
				req := httptest.NewRequest(method, path, strings.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				require.NoError(t, h.Handler()(echo.NewContext(req, rec)))

				return rec
			}

			if tt.seed {
				do(http.MethodPost, base+"/p/datasets", `{"datasetReference":{"datasetId":"ds1"}}`)
			}

			rec := do(tt.method, tt.path, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

func TestGCPBigQueryProxyDisabled(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/dashboard/api/v1/gcp/bigquery/bigquery/v2/projects", nil)
	require.NoError(t, h.Handler()(echo.NewContext(req, rec)))

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}
