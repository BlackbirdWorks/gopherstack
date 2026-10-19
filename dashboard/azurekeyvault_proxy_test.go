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
	"github.com/blackbirdworks/gopherstack/services/azurekeyvault"
)

func TestAzureKeyVaultProxy(t *testing.T) {
	t.Parallel()

	const base = "/dashboard/api/v1/azure/keyvault"

	tests := []struct {
		name     string
		method   string
		path     string
		body     string
		wantBody string
		wantCode int
	}{
		{
			name: "create vault", method: http.MethodPost, path: base + "/vaults", body: `{"name":"v1"}`,
			wantCode: 400,
		},
		{
			name: "create vault ok", method: http.MethodPost, path: base + "/vaults",
			body: `{"name":"vault1"}`, wantCode: http.StatusCreated,
		},
		{
			name: "list vaults", method: http.MethodGet, path: base + "/vaults",
			wantCode: 200, wantBody: `"vaults"`,
		},
		{
			name: "set secret", method: http.MethodPut, path: base + "/vaults/vault1/secrets/s1",
			body: `{"value":"hush"}`, wantCode: 200, wantBody: "hush",
		},
		{
			name: "list secrets", method: http.MethodGet, path: base + "/vaults/vault1/secrets",
			wantCode: 200,
		},
		{
			name: "create key", method: http.MethodPost, path: base + "/vaults/vault1/keys/k1/create",
			body: `{"kty":"RSA"}`, wantCode: http.StatusOK,
		},
		{
			name: "list deleted", method: http.MethodGet, path: base + "/vaults/vault1/deletedkeys",
			wantCode: 200,
		},
		{
			name: "backup blocked", method: http.MethodPost, path: base + "/vaults/vault1/secrets/s1/backup",
			wantCode: 400,
		},
		{
			name: "restore blocked", method: http.MethodPost, path: base + "/vaults/vault1/secrets/restore",
			wantCode: 400,
		},
		{
			name: "traversal", method: http.MethodGet, path: base + "/vaults/vault1/secrets/../keys",
			wantCode: 400,
		},
		{
			name: "no vaults prefix", method: http.MethodGet, path: base + "/secrets/s1",
			wantCode: 400,
		},
		{
			name: "unknown kind", method: http.MethodGet, path: base + "/vaults/vault1/rng",
			wantCode: 400,
		},
		{
			name: "bad vault", method: http.MethodGet, path: base + "/vaults/a/secrets",
			wantCode: 400,
		},
		{
			name: "too deep", method: http.MethodGet, path: base + "/vaults/vault1/keys/a/b/c/d",
			wantCode: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			kv := azurekeyvault.NewHandler(azurekeyvault.NewInMemoryBackend(true), azurekeyvault.DefaultSettings())
			h := dashboard.NewHandler(dashboard.Config{AzureKeyVaultOps: kv})

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			require.NoError(t, h.Handler()(echo.NewContext(req, rec)))

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

func TestAzureKeyVaultProxyRespectsTokenValidation(t *testing.T) {
	t.Parallel()

	settings := azurekeyvault.DefaultSettings()
	settings.ValidateTokens = true
	kv := azurekeyvault.NewHandler(azurekeyvault.NewInMemoryBackend(true), settings)
	h := dashboard.NewHandler(dashboard.Config{AzureKeyVaultOps: kv})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/dashboard/api/v1/azure/keyvault/vaults/vault1/secrets", nil)
	require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAzureKeyVaultProxyDisabled(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/dashboard/api/v1/azure/keyvault/vaults/vault1/secrets", nil)
	require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}
