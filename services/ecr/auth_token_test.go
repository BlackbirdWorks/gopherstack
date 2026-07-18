package ecr_test

// auth_token_test.go — verifies GetAuthorizationToken (handler_auth_token.go):
// base64-encoded AWS:<password> tokens, one token per requested registryId,
// non-zero future expiresAt, and a proxyEndpoint sourced from the backend.

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

func TestGetAuthorizationToken_RegistryIds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantTokens int
	}{
		{
			name:       "no_registry_ids",
			body:       map[string]any{},
			wantTokens: 1,
		},
		{
			name:       "multiple_registry_ids",
			body:       map[string]any{"registryIds": []string{"111", "222"}},
			wantTokens: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doECRRequest(t, h, "GetAuthorizationToken", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			out := parseAccuracy(t, rec)
			authData, authOK := out["authorizationData"].([]any)
			require.True(t, authOK)
			assert.Len(t, authData, tt.wantTokens)

			for _, raw := range authData {
				entry, entryOK := raw.(map[string]any)
				require.True(t, entryOK)
				tokenRaw, tokenOK := entry["authorizationToken"].(string)
				require.True(t, tokenOK)
				require.NotEmpty(t, tokenRaw)

				decoded, decErr := base64.StdEncoding.DecodeString(tokenRaw)
				require.NoError(t, decErr, "token must be valid base64")
				parts := strings.SplitN(string(decoded), ":", 2)
				require.Len(t, parts, 2, "decoded token must be user:password")
				assert.NotEmpty(t, parts[1], "password must not be empty")
				assert.NotZero(t, entry["expiresAt"])
			}
		})
	}
}

func TestGetAuthorizationToken_UniquePerCall(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		registryIDs []string
		wantCount   int
	}{
		{
			name:      "default_single_token",
			wantCount: 1,
		},
		{
			name:        "one_token_per_registry_id",
			registryIDs: []string{"111111111111", "222222222222"},
			wantCount:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandlerWithBackend()

			body := map[string]any{}
			if len(tt.registryIDs) > 0 {
				body["registryIds"] = tt.registryIDs
			}

			rec := doAccuracy(t, h, "GetAuthorizationToken", body)
			require.Equal(t, http.StatusOK, rec.Code)

			out := parseAccuracy(t, rec)
			authData, _ := out["authorizationData"].([]any)
			require.Len(t, authData, tt.wantCount)

			// Each token must decode to AWS:<non-empty-password>.
			for _, entry := range authData {
				e, _ := entry.(map[string]any)
				tokenRaw, _ := e["authorizationToken"].(string)
				require.NotEmpty(t, tokenRaw)

				decoded, err := base64.StdEncoding.DecodeString(tokenRaw)
				require.NoError(t, err, "token must be valid base64")

				parts := strings.SplitN(string(decoded), ":", 2)
				require.Len(t, parts, 2)
				assert.Equal(t, "AWS", parts[0])
				assert.Equal(t, "dummy-password", parts[1],
					"emulator returns a stable AWS:dummy-password credential")
			}

			// The emulator returns a stable token across calls.
			rec2 := doAccuracy(t, h, "GetAuthorizationToken", body)
			require.Equal(t, http.StatusOK, rec2.Code)

			out2 := parseAccuracy(t, rec2)
			authData2, _ := out2["authorizationData"].([]any)
			e1, _ := authData[0].(map[string]any)
			e2, _ := authData2[0].(map[string]any)
			assert.Equal(t, e1["authorizationToken"], e2["authorizationToken"],
				"consecutive calls return the stable token")
		})
	}
}

func TestGetAuthorizationToken_NoRegistryIds_ReturnsSingleToken(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "GetAuthorizationToken", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	data, _ := out["authorizationData"].([]any)
	require.Len(t, data, 1, "default call returns exactly one token")

	token := data[0].(map[string]any)
	assert.NotEmpty(t, token["authorizationToken"], "authorizationToken must be non-empty")
	assert.Greater(t, token["expiresAt"].(float64), float64(0), "expiresAt must be future unix timestamp")
}

func TestGetAuthorizationToken_MultipleRegistryIds(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "GetAuthorizationToken", map[string]any{
		"registryIds": []string{"111111111111", "222222222222", "333333333333"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	data, _ := out["authorizationData"].([]any)
	assert.Len(t, data, 3,
		"when registryIds has N elements, N authorization tokens must be returned")
}

func TestGetAuthorizationToken_SingleRegistryId(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "GetAuthorizationToken", map[string]any{
		"registryIds": []string{"123456789012"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	data, _ := out["authorizationData"].([]any)
	require.Len(t, data, 1)

	token := data[0].(map[string]any)
	assert.NotEmpty(t, token["authorizationToken"])
}

func TestGetAuthorizationToken_ProxyEndpoint_HasHTTPSPrefix(t *testing.T) {
	t.Parallel()

	b := ecr.NewInMemoryBackend("123456789012", "us-east-1", "myregistry.example.com:5000")
	h := ecr.NewHandler(b, nil)

	rec := doAccuracy(t, h, "GetAuthorizationToken", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	data, _ := out["authorizationData"].([]any)
	require.Len(t, data, 1)

	token := data[0].(map[string]any)
	proxyEndpoint, _ := token["proxyEndpoint"].(string)
	assert.True(t,
		len(proxyEndpoint) == 0 ||
			proxyEndpoint[:8] == "https://" ||
			proxyEndpoint[:7] == "http://",
		"proxyEndpoint must use https:// or http:// scheme, got %q", proxyEndpoint)
}

func TestGetAuthorizationToken_ExpiresAt_InFuture(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "GetAuthorizationToken", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	data, _ := out["authorizationData"].([]any)
	require.Len(t, data, 1)

	expiresAt, _ := data[0].(map[string]any)["expiresAt"].(float64)
	assert.Greater(t, expiresAt, float64(1_700_000_000),
		"expiresAt must be a Unix timestamp well in the future")
}

func TestGetAuthorizationToken_Base64Format(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "GetAuthorizationToken", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	data, _ := out["authorizationData"].([]any)
	require.Len(t, data, 1)
	token, _ := data[0].(map[string]any)["authorizationToken"].(string)
	assert.NotEmpty(t, token)

	_, err := base64.StdEncoding.DecodeString(token)
	assert.NoError(t, err, "authorizationToken must be valid base64")
}

func TestGetAuthorizationToken_ExpiresAt_IsFloat(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "GetAuthorizationToken", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	data, _ := out["authorizationData"].([]any)
	expiresAt, ok := data[0].(map[string]any)["expiresAt"].(float64)
	assert.True(t, ok, "expiresAt must be a JSON number")
	assert.Greater(t, expiresAt, float64(0), "expiresAt must be positive")
}

func TestECR_GetAuthorizationToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECRRequest(t, h, "GetAuthorizationToken", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	authData, ok := resp["authorizationData"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, authData)

	entry, ok := authData[0].(map[string]any)
	require.True(t, ok)

	tokenRaw, ok := entry["authorizationToken"].(string)
	require.True(t, ok)
	require.NotEmpty(t, tokenRaw)

	decoded, err := base64.StdEncoding.DecodeString(tokenRaw)
	require.NoError(t, err)
	parts := strings.SplitN(string(decoded), ":", 2)
	require.Len(t, parts, 2, "token must be user:password")
	assert.Equal(t, "AWS", parts[0])
	assert.NotEmpty(t, parts[1], "password must be non-empty")

	assert.NotZero(t, entry["expiresAt"])
}
