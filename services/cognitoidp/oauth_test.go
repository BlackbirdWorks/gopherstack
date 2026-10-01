package cognitoidp_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOAuthAuthorizationCodeFlow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		confidential bool
		pkce         bool
	}{
		{name: "public client with pkce", pkce: true},
		{name: "confidential client basic auth", confidential: true},
		{name: "confidential client with pkce", confidential: true, pkce: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newOAuthEnv(t)
			clientID, auth := env.pubID, map[string]string(nil)

			if tt.confidential {
				clientID, auth = env.webID, basic(env.webID, env.webSec)
			}

			verifier, challenge := pkcePair()
			if !tt.pkce {
				verifier, challenge = "", ""
			}

			code := codeFrom(t, env.login(t, authorizeQuery(clientID, challenge)))
			require.NotEmpty(t, code)

			form := url.Values{
				"grant_type": {"authorization_code"}, "code": {code}, "redirect_uri": {oauthRedirect},
				"client_id": {clientID},
			}
			if verifier != "" {
				form.Set("code_verifier", verifier)
			}

			resp, body := env.postForm(t, "/oauth2/token", form, auth)
			require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
			assert.Equal(t, "*", resp.Header.Get("Access-Control-Allow-Origin"))

			tok := jsonMap(t, body)
			assert.Equal(t, "Bearer", tok["token_type"])
			assert.InDelta(t, 3600, tok["expires_in"], 0)

			idClaims := env.verifyAgainstJWKS(t, tok["id_token"].(string))
			accessClaims := env.verifyAgainstJWKS(t, tok["access_token"].(string))
			assert.Equal(t, "http://localhost:8000/"+env.poolID, idClaims["iss"])
			assert.Equal(t, idClaims["iss"], accessClaims["iss"])
			assert.Equal(t, clientID, idClaims["aud"])
			assert.Equal(t, "email openid profile", scopeSorted(accessClaims["scope"].(string)))

			env.assertUserInfoAndRefresh(t, clientID, auth, tok)
		})
	}
}

func scopeSorted(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func (e *oauthEnv) assertUserInfoAndRefresh(t *testing.T, clientID string, auth map[string]string, tok map[string]any) {
	t.Helper()

	resp, body := e.get(
		t,
		"/oauth2/userInfo",
		map[string]string{"Authorization": "Bearer " + tok["access_token"].(string)},
	)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

	info := jsonMap(t, body)
	assert.Equal(t, oauthUser, info["username"])
	assert.Equal(t, "alice@example.com", info[keyEmail])
	assert.Equal(t, "Alice", info[keyName])
	assert.NotContains(t, info, "phone_number", "phone scope not granted")
	assert.NotEmpty(t, info[keySub])

	refresh := tok["refresh_token"].(string)
	resp, body = e.postForm(t, "/oauth2/token", url.Values{
		"grant_type": {"refresh_token"}, "refresh_token": {refresh}, "client_id": {clientID},
	}, auth)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

	refreshed := jsonMap(t, body)
	assert.NotEmpty(t, refreshed["id_token"])
	assert.Equal(
		t,
		"email openid profile",
		scopeSorted(e.verifyAgainstJWKS(t, refreshed["access_token"].(string))["scope"].(string)),
	)

	rotated := refreshed["refresh_token"].(string)

	resp, _ = e.postForm(t, "/oauth2/token", url.Values{
		"grant_type": {"refresh_token"}, "refresh_token": {refresh}, "client_id": {clientID},
	}, auth)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "rotated-out refresh token must fail")

	e.revokeAndExpectRefreshFailure(t, clientID, auth, rotated)
}

func (e *oauthEnv) revokeAndExpectRefreshFailure(
	t *testing.T,
	clientID string,
	auth map[string]string,
	refresh string,
) {
	t.Helper()

	resp, body := e.postForm(t, "/oauth2/revoke", url.Values{"token": {refresh}, "client_id": {clientID}}, auth)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	assert.Empty(t, body)

	resp, body = e.postForm(t, "/oauth2/token", url.Values{
		"grant_type": {"refresh_token"}, "refresh_token": {refresh}, "client_id": {clientID},
	}, auth)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "invalid_grant", jsonMap(t, body)["error"])

	resp, _ = e.postForm(t, "/oauth2/revoke", url.Values{"token": {refresh}, "client_id": {clientID}}, auth)
	assert.Equal(t, http.StatusOK, resp.StatusCode, "re-revoking is a no-op success")
}

func TestOAuthTokenCodeErrors(t *testing.T) {
	t.Parallel()

	verifier, challenge := pkcePair()

	tests := []struct {
		name      string
		challenge string
		mutate    func(env *oauthEnv, f url.Values)
		wantErr   string
		wantDesc  string
	}{
		{name: "wrong verifier", challenge: challenge, wantErr: "invalid_grant",
			mutate: setForm("code_verifier", strings.Repeat("x", 50))},
		{name: "missing verifier", challenge: challenge, wantErr: "invalid_request",
			mutate: delForm("code_verifier")},
		{name: "short verifier", challenge: challenge, wantErr: "invalid_grant",
			mutate: setForm("code_verifier", "short")},
		{name: "redirect mismatch", challenge: challenge, wantErr: "unauthorized_client", wantDesc: "invalid_redirect",
			mutate: setForm("redirect_uri", oauthRedirect+"/x")},
		{name: "missing redirect", challenge: challenge, wantErr: "invalid_request",
			mutate: delForm("redirect_uri")},
		{name: "unknown code", challenge: challenge, wantErr: "invalid_grant",
			mutate: setForm("code", "nope")},
		{name: "missing code", challenge: challenge, wantErr: "invalid_request",
			mutate: delForm("code")},
		{name: "other client", challenge: challenge, wantErr: "invalid_grant",
			mutate: func(env *oauthEnv, f url.Values) { f.Set("client_id", env.noRevoke) }},
		{name: "unsupported grant", challenge: challenge, wantErr: "unsupported_grant_type",
			mutate: setForm("grant_type", "password")},
		{name: "missing grant", challenge: challenge, wantErr: "invalid_request",
			mutate: delForm("grant_type")},
		{name: "unknown client", challenge: challenge, wantErr: "invalid_client",
			mutate: setForm("client_id", "nonexistent")},
		{name: "no client id", challenge: challenge, wantErr: "invalid_request",
			mutate: delForm("client_id")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newOAuthEnv(t)
			code := codeFrom(t, env.login(t, authorizeQuery(env.pubID, tt.challenge)))
			form := url.Values{
				"grant_type": {"authorization_code"}, "code": {code}, "redirect_uri": {oauthRedirect},
				"client_id": {env.pubID}, "code_verifier": {verifier},
			}
			tt.mutate(env, form)

			resp, body := env.postForm(t, "/oauth2/token", form, nil)
			require.Equal(t, http.StatusBadRequest, resp.StatusCode, string(body))

			got := jsonMap(t, body)
			assert.Equal(t, tt.wantErr, got["error"])

			if tt.wantDesc != "" {
				assert.Equal(t, tt.wantDesc, got["error_description"])
			}

			// A failed redemption must still burn the code (single use).
			good := url.Values{
				"grant_type": {"authorization_code"}, "code": {code}, "redirect_uri": {oauthRedirect},
				"client_id": {env.pubID}, "code_verifier": {verifier},
			}
			if form.Get("code") == code && tt.wantErr != "invalid_client" && tt.wantErr != "unsupported_grant_type" &&
				tt.wantErr != "invalid_request" {
				resp, _ = env.postForm(t, "/oauth2/token", good, nil)
				assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "code must be single-use")
			}
		})
	}
}

func TestOAuthCodeReplayAndBinding(t *testing.T) {
	t.Parallel()

	env := newOAuthEnv(t)
	_, challenge := pkcePair()
	verifier, _ := pkcePair()
	code := codeFrom(t, env.login(t, authorizeQuery(env.pubID, challenge)))
	form := url.Values{
		"grant_type": {"authorization_code"}, "code": {code}, "redirect_uri": {oauthRedirect},
		"client_id": {env.pubID}, "code_verifier": {verifier},
	}

	resp, _ := env.postForm(t, "/oauth2/token", form, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp, body := env.postForm(t, "/oauth2/token", form, nil)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "invalid_grant", jsonMap(t, body)["error"])
}

func TestOAuthClientAuthentication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		hdr     func(env *oauthEnv) map[string]string
		body    func(env *oauthEnv) url.Values
		wantErr string
	}{
		{name: "no secret", wantErr: "invalid_client",
			hdr:  func(*oauthEnv) map[string]string { return nil },
			body: func(env *oauthEnv) url.Values { return url.Values{"client_id": {env.m2mID}} }},
		{name: "wrong basic secret", wantErr: "invalid_client",
			hdr:  func(env *oauthEnv) map[string]string { return basic(env.m2mID, "wrong") },
			body: func(*oauthEnv) url.Values { return url.Values{} }},
		{name: "wrong body secret", wantErr: "invalid_client",
			hdr: func(*oauthEnv) map[string]string { return nil },
			body: func(env *oauthEnv) url.Values {
				return url.Values{"client_id": {env.m2mID}, "client_secret": {"wrong"}}
			}},
		{name: "empty secret", wantErr: "invalid_client",
			hdr:  func(env *oauthEnv) map[string]string { return basic(env.m2mID, "") },
			body: func(*oauthEnv) url.Values { return url.Values{} }},
		{name: "header and body id differ", wantErr: "invalid_client",
			hdr:  func(env *oauthEnv) map[string]string { return basic(env.m2mID, env.m2mSec) },
			body: func(env *oauthEnv) url.Values { return url.Values{"client_id": {env.webID}} }},
		{name: "public client cannot client_credentials", wantErr: "unauthorized_client",
			hdr:  func(*oauthEnv) map[string]string { return nil },
			body: func(env *oauthEnv) url.Values { return url.Values{"client_id": {env.pubID}} }},
		{name: "code client cannot client_credentials", wantErr: "unauthorized_client",
			hdr:  func(env *oauthEnv) map[string]string { return basic(env.webID, env.webSec) },
			body: func(*oauthEnv) url.Values { return url.Values{} }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newOAuthEnv(t)
			form := tt.body(env)
			form.Set("grant_type", "client_credentials")
			form.Set("scope", "api/read")

			resp, body := env.postForm(t, "/oauth2/token", form, tt.hdr(env))
			require.Equal(t, http.StatusBadRequest, resp.StatusCode, string(body))
			assert.Equal(t, tt.wantErr, jsonMap(t, body)["error"])
		})
	}
}

func TestOAuthClientCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		scope      string
		wantScope  string
		wantErr    string
		wantStatus int
		basicAuth  bool
	}{
		{name: "basic one scope", scope: "api/read", basicAuth: true, wantScope: "api/read", wantStatus: 200},
		{name: "body secret two scopes", scope: "api/write api/read", wantScope: "api/read api/write", wantStatus: 200},
		{name: "no scope grants all custom", basicAuth: true, wantScope: "api/read api/write", wantStatus: 200},
		{name: "unknown scope", scope: "api/nope", basicAuth: true, wantStatus: 400, wantErr: "invalid_scope"},
		{name: "unknown server", scope: "other/read", basicAuth: true, wantStatus: 400, wantErr: "invalid_scope"},
		{name: "reserved scope not allowed for client", scope: "openid", basicAuth: true, wantStatus: 400,
			wantErr: "invalid_grant"},
		{name: "mixed allowed and unknown", scope: "api/read api/nope", basicAuth: true, wantStatus: 400,
			wantErr: "invalid_scope"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newOAuthEnv(t)
			form := url.Values{"grant_type": {"client_credentials"}}
			if tt.scope != "" {
				form.Set("scope", tt.scope)
			}

			var hdr map[string]string
			if tt.basicAuth {
				hdr = basic(env.m2mID, env.m2mSec)
			} else {
				form.Set("client_id", env.m2mID)
				form.Set("client_secret", env.m2mSec)
			}

			resp, body := env.postForm(t, "/oauth2/token", form, hdr)
			require.Equal(t, tt.wantStatus, resp.StatusCode, string(body))

			got := jsonMap(t, body)
			if tt.wantErr != "" {
				assert.Equal(t, tt.wantErr, got["error"])

				return
			}

			assert.Equal(t, "Bearer", got["token_type"])
			assert.NotContains(t, got, "id_token")
			assert.NotContains(t, got, "refresh_token")

			claims := env.verifyAgainstJWKS(t, got["access_token"].(string))
			assert.Equal(t, tt.wantScope, scopeSorted(claims["scope"].(string)))
			assert.Equal(t, env.m2mID, claims["client_id"])
			assert.Equal(t, env.m2mID, claims[keySub])
			assert.Equal(t, "access", claims["token_use"])
			assert.Equal(t, "http://localhost:8000/"+env.poolID, claims["iss"])

			resp, _ = env.get(
				t,
				"/oauth2/userInfo",
				map[string]string{"Authorization": "Bearer " + got["access_token"].(string)},
			)
			assert.Equal(t, http.StatusUnauthorized, resp.StatusCode, "M2M token has no user")
		})
	}
}

func TestOAuthAuthorizeErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mutate   func(env *oauthEnv, q url.Values)
		wantPage string
		wantErr  string
	}{
		{name: "unknown client", wantPage: "invalid_request",
			mutate: func(_ *oauthEnv, q url.Values) { q.Set("client_id", "nope") }},
		{name: "unregistered redirect", wantPage: "redirect_mismatch",
			mutate: func(_ *oauthEnv, q url.Values) { q.Set("redirect_uri", "https://evil.example.com/cb") }},
		{name: "redirect prefix is not a match", wantPage: "redirect_mismatch",
			mutate: func(_ *oauthEnv, q url.Values) { q.Set("redirect_uri", oauthRedirect+"/../x") }},
		{name: "redirect with extra query", wantPage: "redirect_mismatch",
			mutate: func(_ *oauthEnv, q url.Values) { q.Set("redirect_uri", oauthRedirect+"?next=https://evil") }},
		{name: "redirect case differs", wantPage: "redirect_mismatch",
			mutate: func(_ *oauthEnv, q url.Values) { q.Set("redirect_uri", strings.ToUpper(oauthRedirect)) }},
		{name: "missing redirect", wantPage: "redirect_mismatch",
			mutate: func(_ *oauthEnv, q url.Values) { q.Del("redirect_uri") }},
		{name: "fragment redirect", wantPage: "redirect_mismatch",
			mutate: func(_ *oauthEnv, q url.Values) { q.Set("redirect_uri", oauthRedirect+"#x") }},
		{name: "missing response_type", wantErr: "invalid_request",
			mutate: func(_ *oauthEnv, q url.Values) { q.Del("response_type") }},
		{name: "bad response_type", wantErr: "invalid_request",
			mutate: func(_ *oauthEnv, q url.Values) { q.Set("response_type", "id_token") }},
		{name: "implicit not allowed", wantErr: "unauthorized_client",
			mutate: func(env *oauthEnv, q url.Values) {
				q.Set("response_type", "token")
				q.Set("client_id", env.pubID)
			}},
		{name: "unknown scope", wantErr: "invalid_scope",
			mutate: func(_ *oauthEnv, q url.Values) { q.Set("scope", "openid bogus") }},
		{name: "profile without openid", wantErr: "invalid_scope",
			mutate: func(_ *oauthEnv, q url.Values) { q.Set("scope", "profile") }},
		{name: "challenge without method", wantErr: "invalid_request",
			mutate: func(_ *oauthEnv, q url.Values) { q.Set("code_challenge", strings.Repeat("a", 43)) }},
		{name: "plain method rejected", wantErr: "invalid_request",
			mutate: func(_ *oauthEnv, q url.Values) {
				q.Set("code_challenge", strings.Repeat("a", 43))
				q.Set("code_challenge_method", "plain")
			}},
		{name: "method without challenge", wantErr: "invalid_request",
			mutate: func(_ *oauthEnv, q url.Values) { q.Set("code_challenge_method", "S256") }},
		{name: "short challenge", wantErr: "invalid_request",
			mutate: func(_ *oauthEnv, q url.Values) {
				q.Set("code_challenge", "abc")
				q.Set("code_challenge_method", "S256")
			}},
		{name: "prompt none", wantErr: "login_required",
			mutate: func(_ *oauthEnv, q url.Values) { q.Set("prompt", "none") }},
		{name: "client credentials client", wantErr: "unauthorized_client",
			mutate: func(env *oauthEnv, q url.Values) { q.Set("client_id", env.m2mID) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newOAuthEnv(t)
			q := authorizeQuery(env.webID, "")
			tt.mutate(env, q)

			for _, path := range []string{"/oauth2/authorize?", "/login?"} {
				resp, body := env.get(t, path+q.Encode(), nil)

				if tt.wantPage != "" {
					require.Equal(t, http.StatusBadRequest, resp.StatusCode, path)
					assert.Contains(t, string(body), tt.wantPage)
					assert.Empty(t, resp.Header.Get("Location"), "must never redirect to an unvalidated URI")

					continue
				}

				require.Equal(t, http.StatusFound, resp.StatusCode, path)

				loc, err := url.Parse(resp.Header.Get("Location"))
				require.NoError(t, err)
				assert.Equal(t, "app.example.com", loc.Host)
				assert.Equal(t, tt.wantErr, loc.Query().Get("error"))
				assert.Equal(t, "st8", loc.Query().Get("state"))
			}
		})
	}
}

func TestOAuthLoginNegative(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		user       string
		pass       string
		csrf       string
		wantText   string
		wantStatus int
	}{
		{
			name:       "wrong password",
			user:       oauthUser,
			pass:       "Wrong1234!",
			wantStatus: 200,
			wantText:   "Incorrect username or password.",
		},
		{name: "unknown user", user: "ghost", pass: oauthPassword, wantStatus: 200,
			wantText: "Incorrect username or password."},
		{name: "empty credentials", wantStatus: 200, wantText: "Incorrect username or password."},
		{
			name:       "bad csrf",
			user:       oauthUser,
			pass:       oauthPassword,
			csrf:       "forged",
			wantStatus: 403,
			wantText:   "session expired",
		},
		{
			name:       "missing csrf",
			user:       oauthUser,
			pass:       oauthPassword,
			csrf:       "-",
			wantStatus: 403,
			wantText:   "session expired",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newOAuthEnv(t)
			loginURL := "/login?" + authorizeQuery(env.webID, "").Encode()

			resp, body := env.get(t, loginURL, nil)
			require.Equal(t, http.StatusOK, resp.StatusCode)

			csrf := between(string(body), `name="_csrf" value="`, `"`)
			switch tt.csrf {
			case "":
			case "-":
				csrf = ""
			default:
				csrf = tt.csrf
			}

			resp, body = env.postForm(
				t,
				loginURL,
				url.Values{"username": {tt.user}, "password": {tt.pass}, "_csrf": {csrf}},
				nil,
			)
			assert.Equal(t, tt.wantStatus, resp.StatusCode)
			assert.Empty(t, resp.Header.Get("Location"))
			assert.Contains(t, strings.ToLower(string(body)), strings.ToLower(tt.wantText))
		})
	}
}

func TestOAuthLoginHTMLEscapes(t *testing.T) {
	t.Parallel()

	env := newOAuthEnv(t)
	q := authorizeQuery(env.webID, "")
	q.Set("login_hint", `"><script>alert(1)</script>`)

	_, body := env.get(t, "/login?"+q.Encode(), nil)
	assert.NotContains(t, string(body), "<script>")
}

func TestOAuthLoginTriggersAndStates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		prepare func(t *testing.T, env *oauthEnv)
		name    string
	}{
		{name: "disabled user", prepare: func(t *testing.T, env *oauthEnv) {
			t.Helper()
			require.NoError(t, env.backend.AdminDisableUser(env.poolID, oauthUser))
		}},
		{name: "force change password", prepare: func(t *testing.T, env *oauthEnv) {
			t.Helper()
			require.NoError(t, env.backend.AdminResetUserPassword(env.poolID, oauthUser))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newOAuthEnv(t)
			tt.prepare(t, env)

			resp := env.login(t, authorizeQuery(env.webID, ""))
			assert.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Empty(t, resp.Header.Get("Location"))
		})
	}
}

func TestOAuthImplicitFlow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		scope   string
		wantAcc string
		wantID  bool
	}{
		{name: "with openid", scope: "openid email", wantID: true, wantAcc: "email openid"},
		{name: "without openid", scope: "aws.cognito.signin.user.admin", wantAcc: "aws.cognito.signin.user.admin"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newOAuthEnv(t)
			q := authorizeQuery(env.webID, "")
			q.Set("response_type", "token")
			q.Set("scope", tt.scope)

			resp := env.login(t, q)
			require.Equal(t, http.StatusFound, resp.StatusCode)

			loc, err := url.Parse(resp.Header.Get("Location"))
			require.NoError(t, err)
			assert.Empty(t, loc.RawQuery, "tokens must be in the fragment, never the query")

			frag, err := url.ParseQuery(loc.Fragment)
			require.NoError(t, err)
			assert.Equal(t, "st8", frag.Get("state"))
			assert.Equal(t, "bearer", frag.Get("token_type"))
			assert.Empty(t, frag.Get("refresh_token"))
			assert.Equal(t, tt.wantID, frag.Get("id_token") != "")

			claims := env.verifyAgainstJWKS(t, frag.Get("access_token"))
			assert.Equal(t, tt.wantAcc, scopeSorted(claims["scope"].(string)))
		})
	}
}

func TestOAuthUserInfoErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		auth       func(env *oauthEnv) string
		name       string
		wantErr    string
		wantStatus int
	}{
		{name: "no header", auth: func(*oauthEnv) string { return "" }, wantStatus: 400, wantErr: "invalid_request"},
		{
			name:       "wrong scheme",
			auth:       func(*oauthEnv) string { return "Basic abc" },
			wantStatus: 400,
			wantErr:    "invalid_request",
		},
		{name: "garbage token", auth: func(*oauthEnv) string { return "Bearer abc.def.ghi" },
			wantStatus: 401, wantErr: "invalid_token"},
		{name: "api token without openid", wantStatus: 401, wantErr: "invalid_token", auth: func(env *oauthEnv) string {
			c, err := env.backend.CreateUserPoolClient(env.poolID, "api-only")
			if err != nil {
				return ""
			}

			res, err := env.backend.InitiateAuth(c.ClientID, "USER_PASSWORD_AUTH", oauthUser, oauthPassword)
			if err != nil {
				return ""
			}

			return "Bearer " + res.Tokens.AccessToken
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newOAuthEnv(t)

			hdr := map[string]string{}
			if a := tt.auth(env); a != "" {
				hdr["Authorization"] = a
			}

			resp, _ := env.get(t, "/oauth2/userInfo", hdr)
			assert.Equal(t, tt.wantStatus, resp.StatusCode)
			assert.Contains(t, resp.Header.Get("WWW-Authenticate"), tt.wantErr)
		})
	}
}

func TestOAuthRevokeErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		form       func(env *oauthEnv) url.Values
		hdr        func(env *oauthEnv) map[string]string
		name       string
		wantErr    string
		wantStatus int
	}{
		{name: "missing token", wantStatus: 400, wantErr: "invalid_request",
			form: func(env *oauthEnv) url.Values { return url.Values{"client_id": {env.pubID}} }},
		{name: "revocation disabled", wantStatus: 400, wantErr: "invalid_request",
			form: func(env *oauthEnv) url.Values { return url.Values{"client_id": {env.noRevoke}, "token": {"x"}} }},
		{name: "jwt is not a refresh token", wantStatus: 400, wantErr: "unsupported_token_type",
			form: func(env *oauthEnv) url.Values { return url.Values{"client_id": {env.pubID}, "token": {"a.b.c"}} }},
		{name: "bad client secret", wantStatus: 401, wantErr: "invalid_client",
			form: func(*oauthEnv) url.Values { return url.Values{"token": {"x"}} },
			hdr:  func(env *oauthEnv) map[string]string { return basic(env.webID, "wrong") }},
		{name: "unknown client", wantStatus: 401, wantErr: "invalid_client",
			form: func(*oauthEnv) url.Values { return url.Values{"client_id": {"nope"}, "token": {"x"}} }},
		{name: "other clients refresh token", wantStatus: 400, wantErr: "invalid_request",
			form: func(env *oauthEnv) url.Values {
				res, err := env.backend.InitiateAuth(env.pubID, "USER_PASSWORD_AUTH", oauthUser, oauthPassword)
				if err != nil {
					return nil
				}

				return url.Values{"client_id": {env.webID}, "token": {res.Tokens.RefreshToken}}
			},
			hdr: func(env *oauthEnv) map[string]string { return basic(env.webID, env.webSec) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newOAuthEnv(t)

			var hdr map[string]string
			if tt.hdr != nil {
				hdr = tt.hdr(env)
			}

			resp, body := env.postForm(t, "/oauth2/revoke", tt.form(env), hdr)
			require.Equal(t, tt.wantStatus, resp.StatusCode, string(body))
			assert.Equal(t, tt.wantErr, jsonMap(t, body)["error"])
		})
	}
}

func TestOAuthLogout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		query      func(env *oauthEnv) url.Values
		name       string
		wantLoc    string
		wantStatus int
		wantLogin  bool
	}{
		{name: "registered logout uri", wantStatus: 302, wantLoc: oauthLogout,
			query: func(env *oauthEnv) url.Values {
				return url.Values{"client_id": {env.webID}, "logout_uri": {oauthLogout}}
			}},
		{name: "unregistered logout uri", wantStatus: 400,
			query: func(env *oauthEnv) url.Values {
				return url.Values{"client_id": {env.webID}, "logout_uri": {"https://evil.example.com/"}}
			}},
		{name: "logout uri prefix", wantStatus: 400,
			query: func(env *oauthEnv) url.Values {
				return url.Values{"client_id": {env.webID}, "logout_uri": {oauthLogout + "/x"}}
			}},
		{name: "unknown client", wantStatus: 400,
			query: func(*oauthEnv) url.Values {
				return url.Values{"client_id": {"nope"}, "logout_uri": {oauthLogout}}
			}},
		{name: "neither uri", wantStatus: 400,
			query: func(env *oauthEnv) url.Values { return url.Values{"client_id": {env.webID}} }},
		{name: "redirect to login", wantStatus: 302, wantLogin: true,
			query: func(env *oauthEnv) url.Values { return authorizeQuery(env.webID, "") }},
		{name: "unregistered redirect", wantStatus: 400,
			query: func(env *oauthEnv) url.Values {
				q := authorizeQuery(env.webID, "")
				q.Set("redirect_uri", "https://evil.example.com/")

				return q
			}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newOAuthEnv(t)
			resp, _ := env.get(t, "/logout?"+tt.query(env).Encode(), nil)
			require.Equal(t, tt.wantStatus, resp.StatusCode)

			switch {
			case tt.wantLoc != "":
				assert.Equal(t, tt.wantLoc, resp.Header.Get("Location"))
			case tt.wantLogin:
				assert.True(t, strings.HasPrefix(resp.Header.Get("Location"), "/login?"))
			default:
				assert.Empty(t, resp.Header.Get("Location"))
			}
		})
	}
}

func TestOAuthDiscoveryMatchesIssuer(t *testing.T) {
	t.Parallel()

	env := newOAuthEnv(t)
	_, err := env.backend.CreateUserPoolDomain(env.poolID, "mydomain")
	require.NoError(t, err)

	tests := []struct {
		name string
		path string
		host string
		code int
	}{
		{name: "path based", path: "/" + env.poolID + "/.well-known/openid-configuration", code: 200},
		{
			name: "pool domain host",
			path: "/.well-known/openid-configuration",
			host: "mydomain.auth.us-east-1.amazoncognito.com",
			code: 200,
		},
		{name: "unknown pool", path: "/us-east-1_nope/.well-known/openid-configuration", code: 404},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req, reqErr := http.NewRequest(http.MethodGet, env.srv.URL+tt.path, nil)
			require.NoError(t, reqErr)

			if tt.host != "" {
				req.Host = tt.host
			}

			resp, body := env.do(t, req)
			require.Equal(t, tt.code, resp.StatusCode, string(body))

			if tt.code != 200 {
				return
			}

			doc := jsonMap(t, body)
			iss := "http://localhost:8000/" + env.poolID
			assert.Equal(t, iss, doc["issuer"])
			assert.Equal(t, "http://localhost:8000/oauth2/token", doc["token_endpoint"])
			assert.Equal(t, "http://localhost:8000/oauth2/authorize", doc["authorization_endpoint"])
			assert.Equal(t, "http://localhost:8000/oauth2/userInfo", doc["userinfo_endpoint"])
			assert.Equal(t, iss+"/.well-known/jwks.json", doc["jwks_uri"])
			assert.Contains(t, doc["id_token_signing_alg_values_supported"], "RS256")

			form := url.Values{"grant_type": {"client_credentials"}, "scope": {"api/read"}}
			_, tokBody := env.postForm(t, "/oauth2/token", form, basic(env.m2mID, env.m2mSec))
			claims := env.verifyAgainstJWKS(t, jsonMap(t, tokBody)["access_token"].(string))
			assert.Equal(t, doc["issuer"], claims["iss"])
		})
	}
}

func TestOAuthDomainHostRouting(t *testing.T) {
	t.Parallel()

	env := newOAuthEnv(t)
	_, err := env.backend.CreateUserPoolDomain(env.poolID, "mydomain")
	require.NoError(t, err)

	other, err := env.backend.CreateUserPool("other")
	require.NoError(t, err)

	otherClient, err := env.backend.CreateUserPoolClient(other.ID, "o")
	require.NoError(t, err)

	tests := []struct {
		name     string
		host     string
		clientID string
		wantCode int
	}{
		{
			name:     "client in domain pool",
			host:     "mydomain.auth.us-east-1.amazoncognito.com",
			clientID: env.webID,
			wantCode: 302,
		},
		{name: "custom host path-based", host: "", clientID: env.webID, wantCode: 302},
		{
			name:     "client from another pool",
			host:     "mydomain.auth.us-east-1.amazoncognito.com",
			clientID: otherClient.ClientID,
			wantCode: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			target := env.srv.URL + "/oauth2/authorize?" + authorizeQuery(tt.clientID, "").Encode()
			req, reqErr := http.NewRequest(http.MethodGet, target, nil)
			require.NoError(t, reqErr)

			if tt.host != "" {
				req.Host = tt.host
			}

			resp, _ := env.do(t, req)
			assert.Equal(t, tt.wantCode, resp.StatusCode)
		})
	}

	req, err := http.NewRequest(http.MethodGet, env.srv.URL+"/.well-known/jwks.json", nil)
	require.NoError(t, err)

	req.Host = "mydomain.auth.us-east-1.amazoncognito.com"

	resp, body := env.do(t, req)
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))
	assert.Contains(t, string(body), "keys")
}

func TestOAuthRouteMatcher(t *testing.T) {
	t.Parallel()

	env := newOAuthEnv(t)
	_, err := env.backend.CreateUserPoolDomain(env.poolID, "mydomain")
	require.NoError(t, err)

	tests := []struct {
		hdr    map[string]string
		name   string
		method string
		target string
		host   string
		want   bool
	}{
		{name: "token post", method: "POST", target: "/oauth2/token", want: true},
		{name: "token get", method: "GET", target: "/oauth2/token", want: false},
		{name: "revoke post", method: "POST", target: "/oauth2/revoke", want: true},
		{name: "userinfo get", method: "GET", target: "/oauth2/userInfo", want: true},
		{name: "authorize with client", method: "GET", target: "/oauth2/authorize?client_id=x", want: true},
		{name: "authorize without client", method: "GET", target: "/oauth2/authorize", want: false},
		{name: "login post", method: "POST", target: "/login?client_id=x", want: true},
		{name: "login put", method: "PUT", target: "/login?client_id=x", want: false},
		{name: "s3 style bucket login anonymous", method: "PUT", target: "/login", want: false},
		{name: "sigv4 signed token path", method: "POST", target: "/oauth2/token", want: false,
			hdr: map[string]string{"Authorization": "AWS4-HMAC-SHA256 Credential=x"}},
		{name: "logout", method: "GET", target: "/logout?client_id=x", want: true},
		{name: "logout without client", method: "GET", target: "/logout", want: false},
		{name: "discovery", method: "GET", target: "/us-east-1_abc/.well-known/openid-configuration", want: true},
		{name: "nested discovery", method: "GET", target: "/a/b/.well-known/openid-configuration", want: false},
		{name: "root discovery unknown host", method: "GET", target: "/.well-known/openid-configuration", want: false},
		{name: "root discovery domain host", method: "GET", target: "/.well-known/openid-configuration",
			host: "mydomain.auth.us-east-1.amazoncognito.com", want: true},
		{name: "preflight", method: "OPTIONS", target: "/oauth2/token", want: true},
		{name: "unrelated", method: "GET", target: "/some-bucket/key", want: false},
	}

	e := echo.New()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.target, nil)
			if tt.host != "" {
				req.Host = tt.host
			}

			for k, v := range tt.hdr {
				req.Header.Set(k, v)
			}

			assert.Equal(t, tt.want, env.handler.RouteMatcher()(e.NewContext(req, httptest.NewRecorder())))
		})
	}
}

func setForm(key, val string) func(*oauthEnv, url.Values) {
	return func(_ *oauthEnv, f url.Values) { f.Set(key, val) }
}

func delForm(key string) func(*oauthEnv, url.Values) {
	return func(_ *oauthEnv, f url.Values) { f.Del(key) }
}

func TestHostedLoginFlowNotSelectableViaAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   func(env *oauthEnv, flow string) map[string]any
		name   string
		target string
	}{
		{name: "InitiateAuth", target: "InitiateAuth",
			body: func(env *oauthEnv, flow string) map[string]any {
				return map[string]any{
					"ClientId": env.webID, "AuthFlow": flow,
					"AuthParameters": map[string]string{"USERNAME": oauthUser, "PASSWORD": oauthPassword},
				}
			}},
		{name: "AdminInitiateAuth", target: "AdminInitiateAuth",
			body: func(env *oauthEnv, flow string) map[string]any {
				return map[string]any{
					"UserPoolId": env.poolID, "ClientId": env.webID, "AuthFlow": flow,
					"AuthParameters": map[string]string{"USERNAME": oauthUser, "PASSWORD": oauthPassword},
				}
			}},
	}

	for _, tt := range tests {
		for _, flow := range []string{"HOSTED_UI_LOGIN", "BOGUS", ""} {
			t.Run(tt.name+" "+flow, func(t *testing.T) {
				t.Parallel()

				env := newOAuthEnv(t)
				rec := doCognitoRequest(t, env.handler, tt.target, tt.body(env, flow))
				require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())

				got := jsonMap(t, rec.Body.Bytes())
				assert.Equal(t, "InvalidParameterException", got["__type"])
				assert.NotContains(t, got, "AuthenticationResult")
			})
		}
	}

	t.Run("web client without USER_PASSWORD_AUTH rejects the API flow", func(t *testing.T) {
		t.Parallel()

		env := newOAuthEnv(t)
		_, err := env.backend.InitiateAuth(env.webID, "USER_PASSWORD_AUTH", oauthUser, oauthPassword)
		require.Error(t, err)
	})
}

func TestOAuthLoginRedirectUsesRegisteredURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		state string
	}{
		{name: "plain state", state: "st8"},
		{name: "state needing encoding", state: "a b&c=d"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newOAuthEnv(t)
			q := authorizeQuery(env.webID, "")
			q.Set("state", tt.state)

			resp := env.login(t, q)
			require.Equal(t, http.StatusFound, resp.StatusCode)

			loc := resp.Header.Get("Location")
			code := between(loc, "code=", "&")
			require.NotEmpty(t, code)

			want := oauthRedirect + "?" + url.Values{"code": {code}, "state": {tt.state}}.Encode()
			assert.Equal(t, want, loc)
		})
	}
}

func TestOAuthLoginCookieAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		host       string
		wantSecure bool
	}{
		{name: "loopback ip", host: "127.0.0.1:4566", wantSecure: true},
		{name: "localhost", host: "localhost:4566", wantSecure: true},
		{name: "other plain http host", host: "gopherstack.internal:4566", wantSecure: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newOAuthEnv(t)
			req, err := http.NewRequest(
				http.MethodGet, env.srv.URL+"/login?"+authorizeQuery(env.webID, "").Encode(), nil,
			)
			require.NoError(t, err)

			req.Host = tt.host

			resp, _ := env.do(t, req)
			require.Equal(t, http.StatusOK, resp.StatusCode)

			cookies := resp.Cookies()
			require.Len(t, cookies, 1)
			assert.Equal(t, "XSRF-TOKEN", cookies[0].Name)
			assert.True(t, cookies[0].HttpOnly)
			assert.Equal(t, http.SameSiteLaxMode, cookies[0].SameSite)
			assert.Equal(t, tt.wantSecure, cookies[0].Secure)
		})
	}
}
