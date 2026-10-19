package cognitoidp_test

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

func TestFederatedOIDCCodeFlow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		query    url.Values
		name     string
		explicit bool
	}{
		{name: "discovery by name", query: url.Values{"identity_provider": {fedProvider}}},
		{name: "explicit endpoints", explicit: true, query: url.Values{"identity_provider": {fedProvider}}},
		{name: "idp identifier", query: url.Values{"idp_identifier": {"corp-sso"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newFedEnv(t, newFakeIDP(t), []string{fedProvider}, tt.explicit)

			authResp := env.authorizeToIDP(t, tt.query)
			require.Equal(t, http.StatusFound, authResp.StatusCode)

			target, err := url.Parse(authResp.Header.Get("Location"))
			require.NoError(t, err)
			assert.Equal(t, env.idp.srv.URL+"/authorize", target.Scheme+"://"+target.Host+target.Path)

			q := target.Query()
			assert.Equal(t, fedClientID, q.Get("client_id"))
			assert.Equal(t, "openid email profile", q.Get("scope"))
			assert.Equal(t, env.srv.URL+"/oauth2/idpresponse", q.Get("redirect_uri"))
			assert.NotEmpty(t, q.Get("nonce"))
			assert.NotEqual(t, "st8", q.Get("state"))

			cb := env.viaIDP(t, authResp)
			resp, _ := env.get(t, cb, nil)
			appQ := locationQuery(t, resp)
			assert.Equal(t, "st8", appQ.Get("state"))
			require.NotEmpty(t, appQ.Get("code"))

			tok := env.redeem(t, appQ.Get("code"))
			claims := env.verifyAgainstJWKS(t, tok["id_token"].(string))
			assert.Equal(t, fedProvider+"_"+fedSubject, claims["cognito:username"])
			assert.Equal(t, "bob@corp.example", claims["email"])
			assert.Equal(t, "Bob", claims["name"])

			identities, ok := claims["identities"].([]any)
			require.True(t, ok, "identities claim: %v", claims["identities"])
			require.Len(t, identities, 1)

			ident := identities[0].(map[string]any)
			assert.Equal(t, fedSubject, ident["userId"])
			assert.Equal(t, fedProvider, ident["providerName"])
			assert.Equal(t, "OIDC", ident["providerType"])
			assert.Equal(t, "true", ident["primary"])

			user := env.federatedUser(t)
			require.NotNil(t, user)
			assert.Equal(t, "EXTERNAL_PROVIDER", user.Status)
			assert.Equal(t, "Bob", user.Attributes["name"])
			assert.Equal(t, "true", user.Attributes["email_verified"])

			var stored []map[string]string
			require.NoError(t, json.Unmarshal([]byte(user.Attributes["identities"]), &stored))
			assert.Equal(t, fedSubject, stored[0]["userId"])
		})
	}
}

func TestFederatedOIDCPKCEAndRelogin(t *testing.T) {
	t.Parallel()

	idp := newFakeIDP(t)
	env := newFedEnv(t, idp, []string{fedProvider}, false)

	verifier, challenge := pkcePair()
	cb := env.viaIDP(t, env.authorizeToIDP(t, url.Values{
		"identity_provider": {fedProvider}, "code_challenge": {challenge}, "code_challenge_method": {"S256"},
	}))
	resp, _ := env.get(t, cb, nil)
	code := locationQuery(t, resp).Get("code")

	bad, _ := env.postForm(t, "/oauth2/token", url.Values{
		"grant_type": {"authorization_code"}, "code": {code}, "redirect_uri": {oauthRedirect},
		"client_id": {env.clientID}, "code_verifier": {"wrong-verifier-wrong-verifier-wrong-verifier-xx"},
	}, nil)
	assert.Equal(t, http.StatusBadRequest, bad.StatusCode)

	resp, _ = env.get(t, env.viaIDP(t, env.authorizeToIDP(t, url.Values{
		"identity_provider": {fedProvider}, "code_challenge": {challenge}, "code_challenge_method": {"S256"},
	})), nil)
	code = locationQuery(t, resp).Get("code")

	ok, body := env.postForm(t, "/oauth2/token", url.Values{
		"grant_type": {"authorization_code"}, "code": {code}, "redirect_uri": {oauthRedirect},
		"client_id": {env.clientID}, "code_verifier": {verifier},
	}, nil)
	require.Equal(t, http.StatusOK, ok.StatusCode, string(body))

	idp.claims["name"] = "Robert"
	idp.claims["email"] = "robert@corp.example"

	resp = env.signIn(t)
	appQ := locationQuery(t, resp)
	tok := env.redeem(t, appQ.Get("code"))
	claims := env.verifyAgainstJWKS(t, tok["id_token"].(string))
	assert.Equal(t, "Robert", claims["name"])
	assert.Equal(t, "robert@corp.example", claims["email"])
	assert.Len(t, claims["identities"], 1)

	users, err := env.backend.ListUsers(env.poolID)
	require.NoError(t, err)

	federated := 0

	for _, u := range users {
		if u.Status == "EXTERNAL_PROVIDER" {
			federated++
		}
	}

	assert.Equal(t, 1, federated)
}

func TestFederatedOIDCCallbackFailures(t *testing.T) {
	t.Parallel()

	otherKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	tests := []struct {
		name    string
		tweak   func(*fakeIDP)
		wantErr string
	}{
		{name: "bad signature", tweak: func(f *fakeIDP) { f.signKey = otherKey }, wantErr: "invalid_request"},
		{name: "wrong audience", tweak: func(f *fakeIDP) { f.audience = "someone-else" }, wantErr: "invalid_request"},
		{name: "expired id token", tweak: func(f *fakeIDP) { f.expiresIn = -time.Hour }, wantErr: "invalid_request"},
		{
			name:    "wrong issuer",
			tweak:   func(f *fakeIDP) { f.claims["iss"] = "https://evil.example" },
			wantErr: "invalid_request",
		},
		{name: "nonce mismatch", tweak: func(f *fakeIDP) { f.claims["nonce"] = "forged" }, wantErr: "invalid_request"},
		{
			name:    "token endpoint error",
			tweak:   func(f *fakeIDP) { f.tokenCode = http.StatusBadRequest },
			wantErr: "invalid_request",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			idp := newFakeIDP(t)
			tt.tweak(idp)
			env := newFedEnv(t, idp, []string{fedProvider}, false)

			q := locationQuery(t, env.signIn(t))
			assert.Equal(t, tt.wantErr, q.Get("error"))
			assert.NotEmpty(t, q.Get("error_description"))
			assert.Equal(t, "st8", q.Get("state"))
			assert.Empty(t, q.Get("code"))
			assert.Nil(t, env.federatedUser(t))
		})
	}
}

func TestFederatedOIDCStateHandling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		callback func(cb string) string
		name     string
		want     int
		replay   bool
	}{
		{
			name:     "unknown state",
			callback: func(string) string { return "/oauth2/idpresponse?code=x&state=bogus" },
			want:     400,
		},
		{name: "missing state", callback: func(string) string { return "/oauth2/idpresponse?code=x" }, want: 400},
		{name: "replayed state", callback: func(cb string) string { return cb }, replay: true, want: 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newFedEnv(t, newFakeIDP(t), []string{fedProvider}, false)
			cb := env.viaIDP(t, env.authorizeToIDP(t, url.Values{"identity_provider": {fedProvider}}))

			if tt.replay {
				first, _ := env.get(t, cb, nil)
				require.Equal(t, http.StatusFound, first.StatusCode)
			}

			resp, body := env.get(t, tt.callback(cb), nil)
			assert.Equal(t, tt.want, resp.StatusCode)
			assert.Contains(t, string(body), "invalid_request")

			if !tt.replay {
				assert.Nil(t, env.federatedUser(t))
			}
		})
	}
}

func TestFederatedOIDCIdPErrorRedirects(t *testing.T) {
	t.Parallel()

	env := newFedEnv(t, newFakeIDP(t), []string{fedProvider}, false)
	cb := env.viaIDP(t, env.authorizeToIDP(t, url.Values{"identity_provider": {fedProvider}}))

	u, err := url.Parse(cb)
	require.NoError(t, err)

	q := url.Values{
		"state":             {u.Query().Get("state")},
		"error":             {"access_denied"},
		"error_description": {"user said no"},
	}
	resp, _ := env.get(t, "/oauth2/idpresponse?"+q.Encode(), nil)

	got := locationQuery(t, resp)
	assert.Equal(t, "access_denied", got.Get("error"))
	assert.Equal(t, "user said no", got.Get("error_description"))
	assert.Equal(t, "st8", got.Get("state"))
}

func TestFederatedOIDCAuthorizeRejections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		query     url.Values
		name      string
		supported []string
	}{
		{
			name:      "provider not in supported list",
			supported: []string{"COGNITO"},
			query:     url.Values{"identity_provider": {fedProvider}},
		},
		{name: "no supported providers", query: url.Values{"identity_provider": {fedProvider}}},
		{name: "unknown provider", supported: []string{"Nope"}, query: url.Values{"identity_provider": {"Nope"}}},
		{name: "unknown identifier", supported: []string{fedProvider}, query: url.Values{"idp_identifier": {"nope"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newFedEnv(t, newFakeIDP(t), tt.supported, false)
			q := locationQuery(t, env.authorizeToIDP(t, tt.query))
			assert.Equal(t, "invalid_request", q.Get("error"))
			assert.Equal(t, "st8", q.Get("state"))
		})
	}
}

func TestFederatedOIDCLinkedAndDisabledUsers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		link    bool
		disable bool
	}{
		{name: "linked native user", link: true},
		{name: "disabled federated user", disable: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newFedEnv(t, newFakeIDP(t), []string{fedProvider}, false)

			if tt.link {
				require.NoError(t, env.backend.AdminLinkProviderForUser(
					env.poolID, oauthUser, fedProvider, "Cognito_Subject", fedSubject))
			}

			first := locationQuery(t, env.signIn(t))
			require.NotEmpty(t, first.Get("code"))

			if tt.disable {
				require.NoError(t, env.backend.AdminDisableUser(env.poolID, fedProvider+"_"+fedSubject))

				q := locationQuery(t, env.signIn(t))
				assert.Equal(t, "invalid_request", q.Get("error"))

				return
			}

			claims := env.verifyAgainstJWKS(t, env.redeem(t, first.Get("code"))["id_token"].(string))
			assert.Equal(t, oauthUser, claims["cognito:username"])
			assert.Nil(t, env.federatedUser(t))
		})
	}
}

func TestFederatedOIDCBrowserBinding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cookie     string
		wantStatus int
	}{
		{name: "valid cookie", cookie: "good", wantStatus: http.StatusFound},
		{name: "no cookie", cookie: "", wantStatus: http.StatusBadRequest},
		{name: "wrong cookie", cookie: "bad", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newFedEnv(t, newFakeIDP(t), []string{fedProvider}, false)
			authResp := env.authorizeToIDP(t, url.Values{"identity_provider": {fedProvider}})

			ck := bindingCookie(authResp)
			require.NotNil(t, ck)
			assert.True(t, ck.HttpOnly)
			assert.Equal(t, http.SameSiteLaxMode, ck.SameSite)
			assert.Equal(t, "/oauth2/idpresponse", ck.Path)

			cb := env.viaIDP(t, authResp)
			req, err := http.NewRequest(http.MethodGet, env.srv.URL+cb, nil)
			require.NoError(t, err)

			switch tt.cookie {
			case "good":
				req.AddCookie(ck)
			case "bad":
				req.AddCookie(&http.Cookie{Name: ck.Name, Value: "forged"})
			}

			assert.Equal(t, tt.wantStatus, bareDo(t, req).StatusCode)
		})
	}
}

func TestFederatedUsernameNamespaceReserved(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		username string
		wantErr  bool
	}{
		{name: "provider prefix", username: "Corp_abc", wantErr: true},
		{name: "prefix case-insensitive", username: "corp_abc", wantErr: true},
		{name: "other prefix", username: "Other_abc"},
		{name: "no underscore", username: "Corpabc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			pool, err := b.CreateUserPool("p")
			require.NoError(t, err)

			_, err = b.CreateIdentityProviderFull(pool.ID, fedProvider, "OIDC", map[string]string{}, nil, nil)
			require.NoError(t, err)

			client, err := b.CreateUserPoolClient(pool.ID, "c")
			require.NoError(t, err)

			_, signErr := b.SignUp(client.ClientID, tt.username, "Pass1234!", nil)
			_, adminErr := b.AdminCreateUser(pool.ID, tt.username+"x", "Pass1234!", nil)

			if tt.wantErr {
				require.ErrorIs(t, signErr, cognitoidp.ErrInvalidParameter)
				require.ErrorIs(t, adminErr, cognitoidp.ErrInvalidParameter)

				return
			}

			require.NoError(t, signErr)
			require.NoError(t, adminErr)
		})
	}
}
