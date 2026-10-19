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
	"github.com/blackbirdworks/gopherstack/pkgs/aadauth"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

func TestAzureARMProxy(t *testing.T) {
	t.Parallel()

	const rg = "/dashboard/api/v1/azure/arm/subscriptions/00000000-0000-0000-0000-000000000000/resourcegroups/demo"

	const base = "/dashboard/api/v1/azure/arm"

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
			name: "put rg", method: http.MethodPut, path: rg + "?api-version=2021-04-01",
			body: `{"location":"eastus"}`, wantCode: http.StatusCreated, wantBody: "demo",
		},
		{
			name: "get rg", method: http.MethodGet, path: rg + "?api-version=2021-04-01",
			seed: true, wantCode: http.StatusOK, wantBody: "demo",
		},
		{
			name: "missing rg", method: http.MethodGet, path: rg + "x?api-version=2021-04-01",
			wantCode: http.StatusNotFound,
		},
		{
			name: "delete rg", method: http.MethodDelete, path: rg + "?api-version=2021-04-01",
			seed: true, wantCode: http.StatusOK,
		},
		{
			name: "token endpoint blocked", method: http.MethodGet, path: base + "/metadata/endpoints",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "traversal blocked", method: http.MethodGet, path: base + "/subscriptions/../metadata/endpoints",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "post non-listkeys blocked", method: http.MethodPost, path: rg, body: "{}",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := azurearm.NewInMemoryBackend()
			registry := azurearm.NewRegistry(backend)
			registry.Register(azurearm.NewStorageProvider(azurearm.StorageEndpointConfig{}, nil))
			issuer, err := aadauth.NewIssuer()
			require.NoError(t, err)

			arm := azurearm.NewHandler(backend, registry, issuer, azurearm.DefaultSettings())
			h := dashboard.NewHandler(dashboard.Config{AzureARMOps: arm})

			if tt.seed {
				seed := httptest.NewRequest(http.MethodPut, rg+"?api-version=2021-04-01",
					strings.NewReader(`{"location":"eastus"}`))
				require.NoError(t, h.Handler()(echo.NewContext(seed, httptest.NewRecorder())))
			}

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			require.NoError(t, h.Handler()(echo.NewContext(req, rec)))

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}
