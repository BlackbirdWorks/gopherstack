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
	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

func TestAzureCosmosProxy(t *testing.T) {
	t.Parallel()

	const base = "/dashboard/api/v1/azure/cosmos"

	type step struct{ method, path, body string }

	seed := []step{
		{http.MethodPost, base + "/dbs", `{"id":"d1"}`},
		{http.MethodPost, base + "/dbs/d1/colls", `{"id":"c1","partitionKey":{"paths":["/pk"],"kind":"Hash"}}`},
		{http.MethodPost, base + "/dbs/d1/colls/c1/docs", `{"id":"a","pk":"x","n":1}`},
	}

	tests := []struct {
		name     string
		method   string
		path     string
		body     string
		header   map[string]string
		wantBody string
		wantCode int
		seed     bool
	}{
		{
			name: "create db", method: http.MethodPost, path: base + "/dbs",
			body: `{"id":"d1"}`, wantCode: http.StatusCreated,
		},
		{
			name: "list dbs", method: http.MethodGet, path: base + "/dbs",
			seed: true, wantCode: http.StatusOK, wantBody: "d1",
		},
		{
			name: "list colls", method: http.MethodGet, path: base + "/dbs/d1/colls",
			seed: true, wantCode: http.StatusOK, wantBody: "/pk",
		},
		{
			name: "list docs", method: http.MethodGet, path: base + "/dbs/d1/colls/c1/docs",
			seed: true, wantCode: http.StatusOK, wantBody: `"id":"a"`,
		},
		{
			name: "query with param", method: http.MethodPost, path: base + "/dbs/d1/colls/c1/docs",
			body:   `{"query":"SELECT * FROM c WHERE c.n = @n","parameters":[{"name":"@n","value":1}]}`,
			header: map[string]string{"X-Ms-Documentdb-Isquery": "true", "Content-Type": "application/query+json"},
			seed:   true, wantCode: http.StatusOK, wantBody: `"id":"a"`,
		},
		{
			name: "get doc", method: http.MethodGet, path: base + "/dbs/d1/colls/c1/docs/a",
			header: map[string]string{"X-Ms-Documentdb-Partitionkey": `["x"]`},
			seed:   true, wantCode: http.StatusOK, wantBody: "_etag",
		},
		{
			name: "bad authorization stripped", method: http.MethodGet, path: base + "/dbs",
			header: map[string]string{"Authorization": "garbage"}, wantCode: http.StatusOK,
		},
		{
			name: "account root blocked", method: http.MethodGet, path: base + "/",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "table api blocked", method: http.MethodGet, path: base + "/Tables",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "traversal blocked", method: http.MethodGet, path: base + "/dbs/../x",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "bad colls seg", method: http.MethodGet, path: base + "/dbs/d1/other",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "bad docs seg", method: http.MethodGet, path: base + "/dbs/d1/colls/c1/x",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "too deep", method: http.MethodGet, path: base + "/dbs/d1/colls/c1/docs/a/b",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing db", method: http.MethodDelete, path: base + "/dbs/nope",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cosmos := cosmosdb.NewHandler(cosmosdb.NewInMemoryBackend())
			h := dashboard.NewHandler(dashboard.Config{AzureCosmosOps: cosmos})

			if tt.seed {
				for _, s := range seed {
					req := httptest.NewRequest(s.method, s.path, strings.NewReader(s.body))
					require.NoError(t, h.Handler()(echo.NewContext(req, httptest.NewRecorder())))
				}
			}

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))

			for k, v := range tt.header {
				req.Header.Set(k, v)
			}

			require.NoError(t, h.Handler()(echo.NewContext(req, rec)))

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

func TestAzureCosmosProxyDisabled(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/dashboard/api/v1/azure/cosmos/dbs", nil)
	require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}
