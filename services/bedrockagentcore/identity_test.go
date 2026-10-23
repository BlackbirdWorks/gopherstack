package bedrockagentcore_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcore"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcore/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrockagentcore"
)

const (
	keyARN    = "arn:aws:secretsmanager:us-east-1:000000000000:secret:k-1"
	secretARN = "arn:aws:secretsmanager:us-east-1:000000000000:secret:c-1"
)

func tokenServer(t *testing.T) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, _ := r.BasicAuth()
		if r.FormValue("grant_type") != "client_credentials" || user != "cid" || pass != "shh" {
			http.Error(w, "denied", http.StatusUnauthorized)

			return
		}

		_ = json.NewEncoder(w).Encode(map[string]string{"access_token": "tok-for-" + r.FormValue("scope")})
	}))
	t.Cleanup(srv.Close)

	return srv
}

func identityControl(tokenURL string) fakeControl {
	c := defaultControl()
	c.providers["apiKey|keyp"] = bedrockagentcore.ProviderInfo{Name: "keyp", SecretARN: keyARN, SecretJSONID: "apiKey"}
	c.providers["oauth2|oa"] = bedrockagentcore.ProviderInfo{
		Name: "oa", Vendor: "customOauth2ProviderConfig", SecretARN: secretARN, SecretJSONID: "clientSecret",
		Config: map[string]any{
			"clientId": "cid",
			"oauthDiscovery": map[string]any{
				"authorizationServerMetadata": map[string]any{"tokenEndpoint": tokenURL},
			},
		},
	}
	c.providers["oauth2|nodisc"] = bedrockagentcore.ProviderInfo{Name: "nodisc", SecretARN: secretARN}
	c.secrets[keyARN] = "the-key"
	c.secrets[secretARN] = "shh"

	return c
}

func workloadToken(t *testing.T, f *fixture) string {
	t.Helper()

	out, err := f.client.GetWorkloadAccessTokenForUserId(t.Context(), &sdk.GetWorkloadAccessTokenForUserIdInput{
		WorkloadName: new(testWorkld), UserId: new("u1"),
	})
	require.NoError(t, err)

	return *out.WorkloadAccessToken
}

func TestIdentity_WorkloadTokens(t *testing.T) {
	t.Parallel()

	f := newFixture(t, defaultControl())

	plain, err := f.client.GetWorkloadAccessToken(
		t.Context(),
		&sdk.GetWorkloadAccessTokenInput{WorkloadName: new(testWorkld)},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, *plain.WorkloadAccessToken)

	payload, _ := json.Marshal(map[string]string{"sub": "user-9"})
	jwt := "e30." + base64.RawURLEncoding.EncodeToString(payload) + ".sig"

	viaJWT, err := f.client.GetWorkloadAccessTokenForJWT(t.Context(), &sdk.GetWorkloadAccessTokenForJWTInput{
		WorkloadName: new(testWorkld), UserToken: &jwt,
	})
	require.NoError(t, err)
	assert.NotEqual(t, *plain.WorkloadAccessToken, *viaJWT.WorkloadAccessToken)

	tests := []struct {
		name string
		call func() error
		code string
	}{
		{name: "unknown workload", code: "ResourceNotFoundException", call: func() error {
			_, e := f.client.GetWorkloadAccessToken(
				t.Context(),
				&sdk.GetWorkloadAccessTokenInput{WorkloadName: new("nope")},
			)

			return e
		}},
		{name: "not a jwt", code: "ValidationException", call: func() error {
			_, e := f.client.GetWorkloadAccessTokenForJWT(t.Context(), &sdk.GetWorkloadAccessTokenForJWTInput{
				WorkloadName: new(testWorkld), UserToken: new("opaque"),
			})

			return e
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.code, apiCode(tt.call()))
		})
	}
}

func TestIdentity_ResourceCredentials(t *testing.T) {
	t.Parallel()

	srv := tokenServer(t)
	f := newFixture(t, identityControl(srv.URL))
	tok := workloadToken(t, f)

	key, err := f.client.GetResourceApiKey(t.Context(), &sdk.GetResourceApiKeyInput{
		ResourceCredentialProviderName: new("keyp"), WorkloadIdentityToken: &tok,
	})
	require.NoError(t, err)
	assert.Equal(t, "the-key", *key.ApiKey)

	oa, err := f.client.GetResourceOauth2Token(t.Context(), &sdk.GetResourceOauth2TokenInput{
		ResourceCredentialProviderName: new("oa"), WorkloadIdentityToken: &tok,
		Oauth2Flow: types.Oauth2FlowTypeM2m, Scopes: []string{"read", "write"},
	})
	require.NoError(t, err)
	assert.Equal(t, "tok-for-read write", *oa.AccessToken)

	tests := []struct {
		name string
		call func() error
		code string
	}{
		{name: "bad token", code: "UnauthorizedException", call: func() error {
			_, e := f.client.GetResourceApiKey(t.Context(), &sdk.GetResourceApiKeyInput{
				ResourceCredentialProviderName: new("keyp"), WorkloadIdentityToken: new("bogus"),
			})

			return e
		}},
		{name: "unknown provider", code: "ResourceNotFoundException", call: func() error {
			_, e := f.client.GetResourceApiKey(t.Context(), &sdk.GetResourceApiKeyInput{
				ResourceCredentialProviderName: new("zzz"), WorkloadIdentityToken: &tok,
			})

			return e
		}},
		{name: "user federation not served", code: "ValidationException", call: func() error {
			_, e := f.client.GetResourceOauth2Token(t.Context(), &sdk.GetResourceOauth2TokenInput{
				ResourceCredentialProviderName: new("oa"), WorkloadIdentityToken: &tok,
				Oauth2Flow: types.Oauth2FlowTypeUserFederation, Scopes: []string{"x"},
			})

			return e
		}},
		{name: "no reachable token endpoint", code: "ValidationException", call: func() error {
			_, e := f.client.GetResourceOauth2Token(t.Context(), &sdk.GetResourceOauth2TokenInput{
				ResourceCredentialProviderName: new("nodisc"), WorkloadIdentityToken: &tok,
				Oauth2Flow: types.Oauth2FlowTypeM2m, Scopes: []string{"x"},
			})

			return e
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.code, apiCode(tt.call()))
		})
	}
}

func TestIdentity_TokenExpiry(t *testing.T) {
	t.Parallel()

	f := newFixture(t, identityControl("http://127.0.0.1:1"))
	tok := workloadToken(t, f)

	f.clock.advance(2 * time.Hour)

	_, err := f.client.GetResourceApiKey(t.Context(), &sdk.GetResourceApiKeyInput{
		ResourceCredentialProviderName: new("keyp"), WorkloadIdentityToken: &tok,
	})
	assert.Equal(t, "UnauthorizedException", apiCode(err), "tokens expire on the injected clock")
}
