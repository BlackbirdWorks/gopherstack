package cognitoidp_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

// oauth2OnlyIDP models Facebook / Login with Amazon: an authorization code and an opaque access
// token, with the profile served from a userinfo endpoint and no id_token.
type oauth2OnlyIDP struct {
	srv     *httptest.Server
	profile map[string]any
}

func newOAuth2OnlyIDP(t *testing.T, profile map[string]any) *oauth2OnlyIDP {
	t.Helper()

	idp := &oauth2OnlyIDP{profile: profile}
	idp.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/authorize":
			q := r.URL.Query()
			back, _ := url.Parse(q.Get("redirect_uri"))
			bq := back.Query()
			bq.Set("code", "code-1")
			bq.Set("state", q.Get("state"))
			back.RawQuery = bq.Encode()
			http.Redirect(w, r, back.String(), http.StatusFound)
		case "/token":
			_ = r.ParseForm()
			if r.PostForm.Get("code") != "code-1" || r.PostForm.Get("client_secret") != fedSecret {
				http.Error(w, `{"error":"invalid_grant"}`, http.StatusBadRequest)

				return
			}

			_ = json.NewEncoder(w).Encode(map[string]string{"access_token": "opaque-token", "token_type": "bearer"})
		case "/me":
			if r.Header.Get("Authorization") != "Bearer opaque-token" {
				http.Error(w, "unauthorized", http.StatusUnauthorized)

				return
			}

			_ = json.NewEncoder(w).Encode(idp.profile)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(idp.srv.Close)

	return idp
}

func newProviderEnv(t *testing.T, ptype string, details map[string]string, mapping map[string]string) *fedEnv {
	t.Helper()

	env := newOAuthEnv(t)

	_, err := env.backend.CreateIdentityProviderFull(env.poolID, fedProvider, ptype, details, mapping, nil)
	require.NoError(t, err)

	client, err := env.backend.CreateUserPoolClientWithOpts(env.poolID, "fed", cognitoidp.UserPoolClientOptions{
		AllowedOAuthFlows: []string{"code"}, AllowedOAuthFlowsUserPoolClient: true,
		AllowedOAuthScopes: []string{"openid", "email", "profile"}, CallbackURLs: []string{oauthRedirect},
		SupportedIdentityProviders: []string{fedProvider},
	})
	require.NoError(t, err)

	return &fedEnv{oauthEnv: env, clientID: client.ClientID}
}

func TestSocialProviderFederation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		profile map[string]any
		mapping map[string]string
		name    string
		ptype   string
		subject string
	}{
		{
			name: "facebook", ptype: "Facebook", subject: "fb-77",
			profile: map[string]any{"id": "fb-77", "name": "Fay", "email": "fay@fb.example"},
			mapping: map[string]string{"email": "email", "name": "name"},
		},
		{
			name: "login with amazon", ptype: "LoginWithAmazon", subject: "amzn1.account.X",
			profile: map[string]any{"user_id": "amzn1.account.X", "name": "Amy", "email": "amy@amzn.example"},
			mapping: map[string]string{"email": "email", "name": "name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			idp := newOAuth2OnlyIDP(t, tt.profile)
			env := newProviderEnv(t, tt.ptype, map[string]string{
				"client_id": fedClientID, "client_secret": fedSecret, "authorize_scopes": "email",
				"authorize_url": idp.srv.URL + "/authorize", "token_url": idp.srv.URL + "/token",
				"attributes_url": idp.srv.URL + "/me",
			}, tt.mapping)

			q := authorizeQuery(env.clientID, "")
			q.Set("identity_provider", fedProvider)

			authResp, _ := env.get(t, "/oauth2/authorize?"+q.Encode(), nil)
			cb := env.viaIDP(t, authResp)
			resp, _ := env.get(t, cb, nil)
			appQ := locationQuery(t, resp)
			require.NotEmpty(t, appQ.Get("code"), appQ.Encode())

			claims := env.verifyAgainstJWKS(t, env.redeem(t, appQ.Get("code"))["id_token"].(string))
			assert.Equal(t, fedProvider+"_"+tt.subject, claims["cognito:username"])
			assert.Equal(t, tt.profile["email"], claims["email"])

			idents := claims["identities"].([]any)
			assert.Equal(t, tt.ptype, idents[0].(map[string]any)["providerType"])
			assert.Equal(t, tt.subject, idents[0].(map[string]any)["userId"])
		})
	}
}

func TestGoogleFederationUsesIDToken(t *testing.T) {
	t.Parallel()

	idp := newFakeIDP(t)
	env := newProviderEnv(t, "Google", map[string]string{
		"client_id": fedClientID, "client_secret": fedSecret, "authorize_scopes": "openid email profile",
		"oidc_issuer": idp.srv.URL, "authorize_url": idp.srv.URL + "/authorize",
		"token_url": idp.srv.URL + "/token", "jwks_uri": idp.srv.URL + "/jwks",
	}, map[string]string{"email": "email", "name": "name", "username": "sub"})
	env.idp = idp

	resp := env.signIn(t)
	appQ := locationQuery(t, resp)
	require.NotEmpty(t, appQ.Get("code"))

	claims := env.verifyAgainstJWKS(t, env.redeem(t, appQ.Get("code"))["id_token"].(string))
	assert.Equal(t, "bob@corp.example", claims["email"])
	assert.Equal(t, "Google", claims["identities"].([]any)[0].(map[string]any)["providerType"])
}

func TestAppleFederationSignsClientSecretAndAcceptsFormPost(t *testing.T) {
	t.Parallel()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	der, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)

	pemKey := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))

	idp := newFakeIDP(t)
	idp.secretOK = func(secret string) bool {
		tok, perr := jwt.Parse(secret, func(*jwt.Token) (any, error) { return &key.PublicKey, nil },
			jwt.WithValidMethods([]string{"ES256"}), jwt.WithAudience("https://appleid.apple.com"),
			jwt.WithIssuer("TEAM123"), jwt.WithSubject(fedClientID), jwt.WithExpirationRequired())

		return perr == nil && tok.Header["kid"] == "KEY456"
	}

	env := newProviderEnv(t, "SignInWithApple", map[string]string{
		"client_id": fedClientID, "team_id": "TEAM123", "key_id": "KEY456", "private_key": pemKey,
		"authorize_scopes": "email name", "oidc_issuer": idp.srv.URL, "authorize_url": idp.srv.URL + "/authorize",
		"token_url": idp.srv.URL + "/token", "jwks_uri": idp.srv.URL + "/jwks",
	}, map[string]string{"email": "email", "username": "sub"})
	env.idp = idp

	q := authorizeQuery(env.clientID, "")
	q.Set("identity_provider", fedProvider)

	authResp, _ := env.get(t, "/oauth2/authorize?"+q.Encode(), nil)
	target, err := url.Parse(authResp.Header.Get("Location"))
	require.NoError(t, err)
	assert.Equal(t, "form_post", target.Query().Get("response_mode"))

	cb, err := url.Parse(env.viaIDP(t, authResp))
	require.NoError(t, err)

	resp, _ := env.postForm(t, "/oauth2/idpresponse", cb.Query(), nil)
	appQ := locationQuery(t, resp)
	require.NotEmpty(t, appQ.Get("code"), appQ.Encode())
}

func TestFederationRejectsBadConfigurations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		details map[string]string
		name    string
		ptype   string
	}{
		{name: "saml without metadata", ptype: "SAML", details: map[string]string{}},
		{name: "unknown type", ptype: "Mystery", details: map[string]string{"client_id": "x"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newProviderEnv(t, tt.ptype, tt.details, nil)
			q := authorizeQuery(env.clientID, "")
			q.Set("identity_provider", fedProvider)

			resp, _ := env.get(t, "/oauth2/authorize?"+q.Encode(), nil)
			appQ := locationQuery(t, resp)
			assert.NotEmpty(t, appQ.Get("error"))
			assert.Empty(t, appQ.Get("code"))
		})
	}
}
