package azurekeyvault_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurekeyvault"
)

func TestAuth_Challenge(t *testing.T) {
	t.Parallel()

	validating := azurekeyvault.DefaultSettings()
	validating.ValidateTokens = true

	tests := []struct {
		name      string
		auth      string
		query     string
		settings  azurekeyvault.Settings
		wantCode  int
		wantChall bool
	}{
		{"no_auth_challenged", "", "?api-version=7.5", azurekeyvault.DefaultSettings(), http.StatusUnauthorized, true},
		{"any_bearer", "Bearer abc", "?api-version=7.5", azurekeyvault.DefaultSettings(), http.StatusOK, false},
		{"missing_api_version", "Bearer abc", "", azurekeyvault.DefaultSettings(), http.StatusBadRequest, false},
		{"validate_rejects_garbage", "Bearer abc", "?api-version=7.5", validating, http.StatusUnauthorized, true},
		{"validate_accepts_jwt", "Bearer " + unsignedJWT(), "?api-version=7.5", validating, http.StatusOK, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newTestServer(t, tt.settings)
			req, err := http.NewRequestWithContext(
				t.Context(),
				http.MethodGet,
				s.vaultURL("v")+"/secrets"+tt.query,
				nil,
			)
			require.NoError(t, err)

			if tt.auth != "" {
				req.Header.Set("Authorization", tt.auth)
			}

			resp, err := s.srv.Client().Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			assert.Equal(t, tt.wantCode, resp.StatusCode)

			chall := resp.Header.Get("WWW-Authenticate")
			if tt.wantChall {
				assert.True(t, strings.HasPrefix(chall, "Bearer authorization="), chall)
				assert.Contains(t, chall, `resource="https://vault.azure.net"`)
			} else {
				assert.Empty(t, chall)
			}
		})
	}
}

func unsignedJWT() string {
	return "eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJleHAiOjQxMDI0NDQ4MDAsImF1ZCI6Imh0dHBzOi8vdmF1bHQuYXp1cmUubmV0In0.sig"
}

func TestVaultAddressing(t *testing.T) {
	t.Parallel()

	s := defaultServer(t)
	ctx := t.Context()

	tests := []struct {
		name string
		url  string
	}{
		{"path_prefix", s.vaultURL("alpha")},
		{"host_label", strings.Replace(s.srv.URL, "127.0.0.1", "alpha.vault.azure.net", 1)},
	}

	ids := make([]string, 0, len(tests))

	for _, tt := range tests {
		opts := s.clientOptions()
		if tt.name == "host_label" {
			opts.Transport = s.hostTransport()
		}

		c, err := azsecrets.NewClient(tt.url, fakeCredential{}, &azsecrets.ClientOptions{
			ClientOptions:                        opts,
			DisableChallengeResourceVerification: tt.name == "path_prefix",
		})
		require.NoError(t, err, tt.name)

		if tt.name == "path_prefix" {
			_, err = c.SetSecret(ctx, "shared", azsecrets.SetSecretParameters{Value: new("v")}, nil)
			require.NoError(t, err)
		}

		got, err := c.GetSecret(ctx, "shared", "", nil)
		require.NoError(t, err, tt.name)

		ids = append(ids, string(*got.ID))
	}

	assert.Contains(t, ids[0], "/secrets/shared/")
	assert.NotContains(t, ids[0], "/vaults/")
	assert.Contains(t, ids[1], "https://alpha.vault.azure.net:")
}
