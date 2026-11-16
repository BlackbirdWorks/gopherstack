package bedrockagentcorecontrol_test

import (
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/bedrockagentcorecontrol"
)

type codeIDP struct {
	*httptest.Server
	grants []string
	mu     sync.Mutex
}

func newCodeIDP(t *testing.T) *codeIDP {
	t.Helper()

	idp := &codeIDP{}
	idp.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		form, _ := url.ParseQuery(string(raw))

		idp.mu.Lock()
		idp.grants = append(idp.grants, form.Get("grant_type")+":"+form.Get("code")+form.Get("refresh_token"))
		idp.mu.Unlock()

		if form.Get("code") == "denied" {
			w.WriteHeader(http.StatusBadRequest)

			return
		}

		out, _ := json.Marshal(map[string]any{
			"access_token":  "at-" + form.Get("code") + form.Get("refresh_token"),
			"refresh_token": "rt-1", "expires_in": 3600,
		}, jsonx.V1())
		_, _ = w.Write(out)
	}))
	t.Cleanup(idp.Close)

	return idp
}

func (m *mcpFixture) codeProvider(t *testing.T, idp *codeIDP) (string, string) {
	t.Helper()

	m.backend.SetReferences(&memorySecrets{functions: map[string]bool{lambdaARN: true}, values: map[string]string{}})

	out, err := m.client.CreateOauth2CredentialProvider(t.Context(), &sdk.CreateOauth2CredentialProviderInput{
		Name: aws.String("idp"), CredentialProviderVendor: types.CredentialProviderVendorTypeCustomOauth2,
		Oauth2ProviderConfigInput: &types.Oauth2ProviderConfigInputMemberCustomOauth2ProviderConfig{
			Value: types.CustomOauth2ProviderConfigInput{
				ClientId: aws.String("cid"), ClientSecret: aws.String("csecret"),
				OauthDiscovery: &types.Oauth2DiscoveryMemberAuthorizationServerMetadata{
					Value: types.Oauth2AuthorizationServerMetadata{
						Issuer: aws.String(idp.URL), AuthorizationEndpoint: aws.String(idp.URL + "/authorize"),
						TokenEndpoint: aws.String(idp.URL + "/token"),
					},
				},
			},
		},
	})
	require.NoError(t, err)

	return aws.ToString(out.CredentialProviderArn), aws.ToString(out.CallbackUrl)
}

func codeCredential(providerARN string) []types.CredentialProviderConfiguration {
	return []types.CredentialProviderConfiguration{{
		CredentialProviderType: types.CredentialProviderTypeOauth,
		CredentialProvider: &types.CredentialProviderMemberOauthCredentialProvider{Value: types.OAuthCredentialProvider{
			ProviderArn: aws.String(providerARN), Scopes: []string{"a", "b"},
			GrantType: types.OAuthGrantTypeAuthorizationCode, DefaultReturnUrl: aws.String("https://app.example/cb"),
		}},
	}}
}

func noRedirect(t *testing.T) *http.Client {
	t.Helper()

	c := *testclient.New(t)
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	return &c
}

func TestGatewayTargetAuthorizationCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		code       string
		wantStatus types.TargetStatus
		wantHTTP   int
	}{
		{name: "completes", code: "abc", wantStatus: types.TargetStatusReady, wantHTTP: http.StatusFound},
		{
			name: "denied stays pending", code: "denied", wantStatus: types.TargetStatusCreatePendingAuth,
			wantHTTP: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			idp := newCodeIDP(t)
			m := newMCPFixture(t)
			providerARN, callback := m.codeProvider(t, idp)
			gid := aws.ToString(createGateway(t, m.client, "Code-Gw").GatewayId)

			var apiAuth string

			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				apiAuth = r.Header.Get("Authorization")
				_, _ = w.Write([]byte(`{}`))
			}))
			t.Cleanup(api.Close)

			created, err := m.client.CreateGatewayTarget(t.Context(), &sdk.CreateGatewayTargetInput{
				GatewayIdentifier: aws.String(gid), Name: aws.String("code"),
				TargetConfiguration:              openAPISpecTarget(api.URL),
				CredentialProviderConfigurations: codeCredential(providerARN),
			})
			require.NoError(t, err)

			tid := aws.ToString(created.TargetId)
			get := func() *sdk.GetGatewayTargetOutput {
				out, gerr := m.client.GetGatewayTarget(t.Context(), &sdk.GetGatewayTargetInput{
					GatewayIdentifier: aws.String(gid), TargetId: aws.String(tid),
				})
				require.NoError(t, gerr)

				return out
			}

			pending := get()
			require.Equal(t, types.TargetStatusCreatePendingAuth, pending.Status)

			authz, ok := pending.AuthorizationData.(*types.AuthorizationDataMemberOauth2)
			require.True(t, ok)

			authURL, err := url.Parse(aws.ToString(authz.Value.AuthorizationUrl))
			require.NoError(t, err)
			assert.Equal(t, idp.URL+"/authorize", authURL.Scheme+"://"+authURL.Host+authURL.Path)
			assert.Equal(t, "cid", authURL.Query().Get("client_id"))
			assert.Equal(t, callback, authURL.Query().Get("redirect_uri"))
			assert.Equal(t, "a b", authURL.Query().Get("scope"))

			cb := m.url + callbackPath(callback) +
				"?code=" + tt.code + "&state=" + authURL.Query().Get("state")

			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, cb, http.NoBody)
			require.NoError(t, err)

			resp, err := noRedirect(t).Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			assert.Equal(t, tt.wantHTTP, resp.StatusCode)
			assert.Equal(t, tt.wantStatus, get().Status)

			if tt.wantStatus != types.TargetStatusReady {
				return
			}

			assert.Nil(t, get().AuthorizationData)

			call := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"code___ping"}}`
			status, raw := m.rpc(t, gid, nil, call)
			assert.Equal(t, http.StatusOK, status, raw)
			assert.Equal(t, "Bearer at-abc", apiAuth)
			assert.Equal(t, "https://app.example/cb", resp.Header.Get("Location"))
		})
	}
}

func TestOAuthCallbackUnknownState(t *testing.T) {
	t.Parallel()

	m := newMCPFixture(t)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		m.url+"/identities/oauth2/callback/zzz?code=x&state=nope", http.NoBody)
	require.NoError(t, err)

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestAuthSessionsPersist(t *testing.T) {
	t.Parallel()

	idp := newCodeIDP(t)
	m := newMCPFixture(t)
	providerARN, _ := m.codeProvider(t, idp)
	gid := aws.ToString(createGateway(t, m.client, "Persist-Gw").GatewayId)

	_, err := m.client.CreateGatewayTarget(t.Context(), &sdk.CreateGatewayTargetInput{
		GatewayIdentifier: aws.String(gid), Name: aws.String("code"),
		TargetConfiguration:              openAPISpecTarget("http://127.0.0.1:1"),
		CredentialProviderConfigurations: codeCredential(providerARN),
	})
	require.NoError(t, err)

	assert.Equal(t, 1, m.backend.AuthSessionCount())

	peer := bedrockagentcorecontrol.NewInMemoryBackend(testAccount, "us-east-1")
	require.NoError(t, peer.Restore(t.Context(), m.backend.Snapshot(t.Context())))
	assert.Equal(t, 1, peer.AuthSessionCount())
}

func callbackPath(callback string) string {
	_, tail, _ := strings.Cut(callback, "/identities/")

	return "/identities/" + tail
}
