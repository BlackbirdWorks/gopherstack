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
	"github.com/blackbirdworks/gopherstack/services/azuretable"
)

func TestAzureTableProxy(t *testing.T) {
	t.Parallel()

	const base = "/dashboard/api/v1/azure/table/devstoreaccount1"

	const entity = `{"PartitionKey":"p","RowKey":"r","Name":"bob"}`

	const item = base + "/tbl1(PartitionKey='p',RowKey='r')"

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
			name: "create table", method: http.MethodPost, path: base + "/Tables",
			body: `{"TableName":"tbl1"}`, wantCode: http.StatusCreated, wantBody: "tbl1",
		},
		{
			name:     "list tables",
			method:   http.MethodGet,
			path:     base + "/Tables",
			seed:     true,
			wantCode: 200,
			wantBody: "tbl1",
		},
		{
			name: "insert entity", method: http.MethodPost, path: base + "/tbl1",
			body: entity, seed: true, wantCode: http.StatusConflict, wantBody: "EntityAlreadyExists",
		},
		{
			name:     "query entities",
			method:   http.MethodGet,
			path:     base + "/tbl1?$top=1",
			seed:     true,
			wantCode: 200,
			wantBody: "bob",
		},
		{name: "get entity", method: http.MethodGet, path: item, seed: true, wantCode: 200, wantBody: "bob"},
		{
			name: "merge entity", method: http.MethodPatch, path: item,
			body: `{"Age":3}`, seed: true, wantCode: http.StatusNoContent,
		},
		{
			name: "replace entity", method: http.MethodPut, path: item,
			body: `{"Age":3}`, seed: true, wantCode: http.StatusNoContent,
		},
		{name: "delete entity", method: http.MethodDelete, path: item, seed: true, wantCode: http.StatusBadRequest},
		{name: "delete table", method: http.MethodDelete, path: base + "/Tables('tbl1')", seed: true, wantCode: 204},
		{name: "missing table", method: http.MethodGet, path: base + "/nope", wantCode: http.StatusNotFound},
		{name: "batch blocked", method: http.MethodPost, path: base + "/$batch", wantCode: http.StatusBadRequest},
		{name: "traversal blocked", method: http.MethodGet, path: base + "/tbl1/../x", wantCode: http.StatusBadRequest},
		{name: "too deep", method: http.MethodGet, path: base + "/tbl1/x", wantCode: http.StatusBadRequest},
		{name: "account only", method: http.MethodGet, path: base, wantCode: http.StatusBadRequest},
		{
			name:     "put on collection",
			method:   http.MethodPut,
			path:     base + "/tbl1?comp=acl",
			wantCode: http.StatusBadRequest,
		},
		{name: "delete collection", method: http.MethodDelete, path: base + "/tbl1", wantCode: http.StatusBadRequest},
		{
			name: "no account", method: http.MethodGet,
			path: "/dashboard/api/v1/azure/table/", wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := dashboard.NewHandler(dashboard.Config{
				AzureTableOps: azuretable.NewHandler(azuretable.NewInMemoryBackend()),
			})

			if tt.seed {
				for _, s := range []struct{ path, body string }{
					{base + "/Tables", `{"TableName":"tbl1"}`},
					{base + "/tbl1", entity},
				} {
					req := httptest.NewRequest(http.MethodPost, s.path, strings.NewReader(s.body))
					require.NoError(t, h.Handler()(echo.NewContext(req, httptest.NewRecorder())))
				}
			}

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			require.NoError(t, h.Handler()(echo.NewContext(req, rec)))

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}
