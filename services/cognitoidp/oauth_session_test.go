package cognitoidp_test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

const sessionCookieName = "cognito"

func (e *oauthEnv) sessionCookie(t *testing.T) string {
	t.Helper()

	u, err := url.Parse(e.srv.URL)
	require.NoError(t, err)

	for _, c := range e.client.Jar.Cookies(u) {
		if c.Name == sessionCookieName {
			return c.Value
		}
	}

	return ""
}

func (e *oauthEnv) authorize(t *testing.T, q url.Values, cookie string) *http.Response {
	t.Helper()

	hdr := map[string]string{}
	if cookie != "" {
		hdr["Cookie"] = sessionCookieName + "=" + cookie
	}

	resp, _ := e.get(t, "/oauth2/authorize?"+q.Encode(), hdr)

	return resp
}

func (e *oauthEnv) exchange(t *testing.T, code string) map[string]any {
	t.Helper()

	resp, body := e.postForm(t, "/oauth2/token", url.Values{
		"grant_type": {"authorization_code"}, "code": {code}, "redirect_uri": {oauthRedirect},
		"client_id": {e.webID},
	}, basic(e.webID, e.webSec))
	require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

	return jsonMap(t, body)
}

func (e *oauthEnv) userTokens(t *testing.T) *cognitoidp.TokenResult {
	t.Helper()

	res, err := e.backend.InitiateAuth(e.pubID, "USER_PASSWORD_AUTH", oauthUser, oauthPassword)
	require.NoError(t, err)
	require.NotNil(t, res.Tokens)

	return res.Tokens
}

func TestOAuthNonceRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		nonce    string
		implicit bool
	}{
		{name: "code flow", nonce: "n-0S6_WzA2Mj"},
		{name: "implicit flow", nonce: "n-0S6_WzA2Mj", implicit: true},
		{name: "code flow without nonce"},
		{name: "implicit flow without nonce", implicit: true},
		{name: "nonce with markup", nonce: `"><b>x&y=z`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newOAuthEnv(t)
			q := authorizeQuery(env.webID, "")

			if tt.nonce != "" {
				q.Set("nonce", tt.nonce)
			}

			var idToken string

			if tt.implicit {
				q.Set("response_type", "token")

				loc, err := url.Parse(env.login(t, q).Header.Get("Location"))
				require.NoError(t, err)

				frag, err := url.ParseQuery(loc.Fragment)
				require.NoError(t, err)

				idToken = frag.Get("id_token")
			} else {
				idToken, _ = env.exchange(t, codeFrom(t, env.login(t, q)))["id_token"].(string)
			}

			claims := env.verifyAgainstJWKS(t, idToken)
			if tt.nonce == "" {
				assert.NotContains(t, claims, "nonce")

				return
			}

			assert.Equal(t, tt.nonce, claims["nonce"])
		})
	}
}

func TestOAuthSessionAuthorize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		prompt    string
		wantErr   string
		loggedIn  bool
		wantCode  bool
		wantLogin bool
	}{
		{name: "session skips the form", loggedIn: true, wantCode: true},
		{name: "prompt none with session", loggedIn: true, prompt: "none", wantCode: true},
		{name: "prompt none without session", prompt: "none", wantErr: "login_required"},
		{name: "prompt login forces the form", loggedIn: true, prompt: "login", wantLogin: true},
		{name: "no session shows the form", wantLogin: true},
		{name: "prompt none and login conflict", loggedIn: true, prompt: "none login", wantErr: "invalid_request"},
		{name: "prompt consent still reuses session", loggedIn: true, prompt: "consent", wantCode: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newOAuthEnv(t)
			q := authorizeQuery(env.webID, "")

			if tt.loggedIn {
				require.Equal(t, http.StatusFound, env.login(t, q).StatusCode)
				require.NotEmpty(t, env.sessionCookie(t))
			}

			if tt.prompt != "" {
				q.Set("prompt", tt.prompt)
			}

			resp := env.authorize(t, q, "")
			require.Equal(t, http.StatusFound, resp.StatusCode)

			loc := resp.Header.Get("Location")

			switch {
			case tt.wantCode:
				code := codeFrom(t, resp)
				require.NotEmpty(t, code)
				claims := env.verifyAgainstJWKS(t, env.exchange(t, code)["id_token"].(string))
				assert.Equal(t, oauthUser, claims["cognito:username"])
			case tt.wantLogin:
				assert.True(t, strings.HasPrefix(loc, "/login?"), loc)
			default:
				u, err := url.Parse(loc)
				require.NoError(t, err)
				assert.Equal(t, "app.example.com", u.Host)
				assert.Equal(t, tt.wantErr, u.Query().Get("error"))
				assert.Equal(t, "st8", u.Query().Get("state"))
			}
		})
	}
}

func TestOAuthSessionCookieAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		host       string
		wantSecure bool
	}{
		{name: "loopback", host: "127.0.0.1:4566", wantSecure: true},
		{name: "plain http host", host: "gopherstack.internal:4566", wantSecure: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newOAuthEnv(t)
			q := authorizeQuery(env.webID, "")

			resp, body := env.get(t, "/login?"+q.Encode(), map[string]string{"Host": tt.host})
			require.Equal(t, http.StatusOK, resp.StatusCode)

			csrf := between(string(body), `name="_csrf" value="`, `"`)
			req, err := http.NewRequest(http.MethodPost, env.srv.URL+"/login?"+q.Encode(), strings.NewReader(url.Values{
				"username": {oauthUser}, "password": {oauthPassword}, "_csrf": {csrf},
			}.Encode()))
			require.NoError(t, err)

			req.Host = tt.host
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set("Cookie", "XSRF-TOKEN="+csrf)

			resp, _ = env.do(t, req)
			require.Equal(t, http.StatusFound, resp.StatusCode)

			var sess *http.Cookie

			for _, c := range resp.Cookies() {
				if c.Name == sessionCookieName {
					sess = c
				}
			}

			require.NotNil(t, sess)
			assert.True(t, sess.HttpOnly)
			assert.Equal(t, http.SameSiteLaxMode, sess.SameSite)
			assert.Equal(t, tt.wantSecure, sess.Secure)
			assert.Equal(t, int(time.Hour.Seconds()), sess.MaxAge)
			assert.GreaterOrEqual(t, len(sess.Value), 43, "at least 256 bits of entropy")
		})
	}
}

func TestOAuthSessionIDsAreUnique(t *testing.T) {
	t.Parallel()

	env := newOAuthEnv(t)
	seen := map[string]bool{}

	for range 5 {
		q := authorizeQuery(env.webID, "")
		q.Set("prompt", "login")
		require.Equal(t, http.StatusFound, env.login(t, q).StatusCode)

		id := env.sessionCookie(t)
		require.NotEmpty(t, id)
		assert.False(t, seen[id])

		seen[id] = true
	}
}

func TestOAuthSessionInvalidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		act  func(t *testing.T, env *oauthEnv)
		name string
	}{
		{name: "logout", act: func(t *testing.T, env *oauthEnv) {
			t.Helper()

			resp, _ := env.get(
				t,
				"/logout?"+url.Values{"client_id": {env.webID}, "logout_uri": {oauthLogout}}.Encode(),
				nil,
			)
			require.Equal(t, http.StatusFound, resp.StatusCode)
		}},
		{name: "admin disable", act: func(t *testing.T, env *oauthEnv) {
			t.Helper()
			require.NoError(t, env.backend.AdminDisableUser(env.poolID, oauthUser))
		}},
		{name: "disable then re-enable", act: func(t *testing.T, env *oauthEnv) {
			t.Helper()
			require.NoError(t, env.backend.AdminDisableUser(env.poolID, oauthUser))
			require.NoError(t, env.backend.AdminEnableUser(env.poolID, oauthUser))
		}},
		{name: "admin global sign out", act: func(t *testing.T, env *oauthEnv) {
			t.Helper()
			require.NoError(t, env.backend.AdminUserGlobalSignOut(env.poolID, oauthUser))
		}},
		{name: "global sign out", act: func(t *testing.T, env *oauthEnv) {
			t.Helper()
			require.NoError(t, env.backend.GlobalSignOut(env.userTokens(t).AccessToken))
		}},
		{name: "password change", act: func(t *testing.T, env *oauthEnv) {
			t.Helper()
			require.NoError(t, env.backend.ChangePassword(env.userTokens(t).AccessToken, oauthPassword, "NewPass99!x"))
		}},
		{name: "admin set password", act: func(t *testing.T, env *oauthEnv) {
			t.Helper()
			require.NoError(t, env.backend.AdminSetUserPassword(env.poolID, oauthUser, "Other1234!x", true))
		}},
		{name: "delete user", act: func(t *testing.T, env *oauthEnv) {
			t.Helper()
			require.NoError(t, env.backend.AdminDeleteUser(env.poolID, oauthUser))
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newOAuthEnv(t)
			q := authorizeQuery(env.webID, "")
			require.Equal(t, http.StatusFound, env.login(t, q).StatusCode)

			stolen := env.sessionCookie(t)
			require.NotEmpty(t, stolen)
			require.Contains(t, env.authorize(t, q, stolen).Header.Get("Location"), "code=")

			tt.act(t, env)

			q.Set("prompt", "none")

			for _, cookie := range []string{"", stolen} {
				resp := env.authorize(t, q, cookie)
				require.Equal(t, http.StatusFound, resp.StatusCode)
				assert.Contains(t, resp.Header.Get("Location"), "error=login_required", "cookie %q", cookie)
			}
		})
	}
}

func TestOAuthLogoutClearsCookie(t *testing.T) {
	t.Parallel()

	env := newOAuthEnv(t)
	require.Equal(t, http.StatusFound, env.login(t, authorizeQuery(env.webID, "")).StatusCode)

	resp, _ := env.get(t, "/logout?"+url.Values{"client_id": {env.webID}, "logout_uri": {oauthLogout}}.Encode(), nil)
	require.Equal(t, http.StatusFound, resp.StatusCode)
	assert.Equal(t, oauthLogout, resp.Header.Get("Location"))

	var cleared *http.Cookie

	for _, c := range resp.Cookies() {
		if c.Name == sessionCookieName {
			cleared = c
		}
	}

	require.NotNil(t, cleared)
	assert.Empty(t, cleared.Value)
	assert.Negative(t, cleared.MaxAge)
	assert.Empty(t, env.sessionCookie(t))
}

func TestOAuthSessionIsPoolScoped(t *testing.T) {
	t.Parallel()

	env := newOAuthEnv(t)
	require.Equal(t, http.StatusFound, env.login(t, authorizeQuery(env.webID, "")).StatusCode)

	other, err := env.backend.CreateUserPool("other-pool")
	require.NoError(t, err)

	c, err := env.backend.CreateUserPoolClientWithOpts(other.ID, "other", cognitoidp.UserPoolClientOptions{
		AllowedOAuthFlows: []string{"code"}, AllowedOAuthFlowsUserPoolClient: true,
		AllowedOAuthScopes: []string{"openid"}, CallbackURLs: []string{oauthRedirect},
	})
	require.NoError(t, err)

	q := authorizeQuery(c.ClientID, "")
	q.Set("scope", "openid")
	q.Set("prompt", "none")

	resp := env.authorize(t, q, "")
	assert.Contains(t, resp.Header.Get("Location"), "error=login_required")
}

func totpEnv(t *testing.T) (*oauthEnv, string) {
	t.Helper()

	env := newOAuthEnv(t)
	tokens := env.userTokens(t)

	secret, _, err := env.backend.AssociateSoftwareToken(tokens.AccessToken, "")
	require.NoError(t, err)

	code, err := cognitoidp.GenerateTOTPCode(secret, time.Now())
	require.NoError(t, err)

	_, err = env.backend.VerifySoftwareToken(tokens.AccessToken, "", code)
	require.NoError(t, err)
	require.NoError(t, env.backend.SetUserPoolMfaConfig(env.poolID, "ON"))

	return env, secret
}

func validTOTP(secret string) string {
	code, _ := cognitoidp.GenerateTOTPCode(secret, time.Now())

	return code
}

type loginStep struct {
	loginURL string
	csrf     string
	session  string
}

func (e *oauthEnv) startChallenge(t *testing.T, q url.Values, user, pass string) (loginStep, string) {
	t.Helper()

	loginURL := "/login?" + q.Encode()
	_, body := e.get(t, loginURL, nil)
	csrf := between(string(body), `name="_csrf" value="`, `"`)

	resp, body := e.postForm(t, loginURL, url.Values{
		"username": {user}, "password": {pass}, "_csrf": {csrf},
	}, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Empty(t, resp.Header.Get("Location"))

	html := string(body)
	require.Contains(t, html, `name="challenge"`)

	return loginStep{
		loginURL: loginURL,
		csrf:     between(html, `name="_csrf" value="`, `"`),
		session:  between(html, `name="session" value="`, `"`),
	}, between(html, `name="challenge" value="`, `"`)
}

func TestOAuthHostedMFAStep(t *testing.T) {
	t.Parallel()

	tests := []struct {
		code       func(secret string) string
		name       string
		csrf       string
		wantText   string
		wantStatus int
		wantCode   bool
	}{
		{name: "valid totp completes the flow", wantStatus: http.StatusFound, wantCode: true,
			code: validTOTP},
		{name: "wrong code returns to sign in", wantStatus: http.StatusOK, wantText: "sign in again",
			code: func(string) string { return "000000" }},
		{name: "empty code", wantStatus: http.StatusOK, wantText: "sign in again",
			code: func(string) string { return "" }},
		{name: "forged csrf", csrf: "forged", wantStatus: http.StatusForbidden, wantText: "session expired",
			code: validTOTP},
		{name: "missing csrf", csrf: "-", wantStatus: http.StatusForbidden, wantText: "session expired",
			code: validTOTP},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env, secret := totpEnv(t)
			q := authorizeQuery(env.webID, "")
			step, name := env.startChallenge(t, q, oauthUser, oauthPassword)
			assert.Equal(t, "SOFTWARE_TOKEN_MFA", name)

			csrf := step.csrf

			switch tt.csrf {
			case "":
			case "-":
				csrf = ""
			default:
				csrf = tt.csrf
			}

			form := url.Values{
				"_csrf": {csrf}, "session": {step.session}, "challenge": {name}, "code": {tt.code(secret)},
			}
			resp, body := env.postForm(t, step.loginURL, form, nil)
			require.Equal(t, tt.wantStatus, resp.StatusCode, string(body))

			if tt.wantCode {
				assert.NotEmpty(t, codeFrom(t, resp))
				assert.NotEmpty(t, env.sessionCookie(t))

				resp, _ = env.postForm(t, step.loginURL, form, nil)
				assert.Equal(t, http.StatusOK, resp.StatusCode, "answered session cannot be replayed")

				return
			}

			assert.Empty(t, resp.Header.Get("Location"))
			assert.Empty(t, env.sessionCookie(t), "no session before the challenge is answered")
			assert.Contains(t, strings.ToLower(string(body)), strings.ToLower(tt.wantText))

			if tt.csrf == "" {
				return
			}

			good := url.Values{
				"_csrf": {
					between(string(body), `name="_csrf" value="`, `"`),
				},
				"session":   {step.session},
				"challenge": {name},
				"code":      {tt.code(secret)},
			}
			resp, _ = env.postForm(t, step.loginURL, good, nil)
			assert.Equal(t, http.StatusFound, resp.StatusCode, "csrf rejection must not burn the challenge")
		})
	}
}

func TestOAuthHostedMFASessionBinding(t *testing.T) {
	t.Parallel()

	env, secret := totpEnv(t)
	q := authorizeQuery(env.webID, "")
	step, name := env.startChallenge(t, q, oauthUser, oauthPassword)

	code, err := cognitoidp.GenerateTOTPCode(secret, time.Now())
	require.NoError(t, err)

	other := authorizeQuery(env.pubID, "")
	_, body := env.get(t, "/login?"+other.Encode(), nil)
	csrf := between(string(body), `name="_csrf" value="`, `"`)

	resp, _ := env.postForm(t, "/login?"+other.Encode(), url.Values{
		"_csrf": {csrf}, "session": {step.session}, "challenge": {name}, "code": {code},
	}, nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Empty(t, resp.Header.Get("Location"), "a challenge session is bound to its client")
}

func TestOAuthHostedNewPasswordStep(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		newPassword  string
		wantComplete bool
		wantRetry    bool
	}{
		{name: "new password completes the flow", newPassword: "Brand-New99!", wantComplete: true},
		{name: "empty new password keeps the challenge", newPassword: "", wantRetry: true},
		{name: "policy violation keeps the challenge", newPassword: "x", wantRetry: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newOAuthEnvWith(t, cognitoidp.UserPoolOptions{
				PasswordPolicy: &cognitoidp.PasswordPolicy{MinimumLength: 11},
			}, nil)
			_, err := env.backend.AdminCreateUser(
				env.poolID,
				"bob",
				"Temp1234!xyz",
				map[string]string{keyEmail: "b@x.com"},
			)
			require.NoError(t, err)

			q := authorizeQuery(env.webID, "")
			step, name := env.startChallenge(t, q, "bob", "Temp1234!xyz")
			assert.Equal(t, "NEW_PASSWORD_REQUIRED", name)

			resp, body := env.postForm(t, step.loginURL, url.Values{
				"_csrf": {step.csrf}, "session": {step.session}, "challenge": {name}, "new_password": {tt.newPassword},
			}, nil)

			if tt.wantComplete {
				require.NotEmpty(t, codeFrom(t, resp))
				assert.NotEmpty(t, env.sessionCookie(t))

				_, err = env.backend.InitiateAuth(env.pubID, "USER_PASSWORD_AUTH", "bob", tt.newPassword)
				require.NoError(t, err, "the new password is the permanent one")

				return
			}

			require.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Empty(t, resp.Header.Get("Location"))

			assert.Contains(t, string(body), `value="NEW_PASSWORD_REQUIRED"`)
		})
	}
}

func TestOAuthHostedChallengeHTMLEscapes(t *testing.T) {
	t.Parallel()

	env, _ := totpEnv(t)
	q := authorizeQuery(env.webID, "")
	q.Set("state", `"><script>alert(1)</script>`)

	_, body := env.get(t, "/login?"+q.Encode(), nil)
	csrf := between(string(body), `name="_csrf" value="`, `"`)

	_, body = env.postForm(t, "/login?"+q.Encode(), url.Values{
		"username": {oauthUser}, "password": {oauthPassword}, "_csrf": {csrf},
	}, nil)
	assert.NotContains(t, string(body), "<script>")
}

func TestOAuthClientCredentialsPreTokenTrigger(t *testing.T) {
	t.Parallel()

	arn := "arn:aws:lambda:us-east-1:000000000000:function:pre"
	v3 := map[string]any{"PreTokenGenerationConfig": map[string]any{"LambdaArn": arn, "LambdaVersion": "V3_0"}}
	v2 := map[string]any{"PreTokenGenerationConfig": map[string]any{"LambdaArn": arn, "LambdaVersion": "V2_0"}}
	legacy := map[string]any{"PreTokenGeneration": arn}

	respond := func(string, map[string]any) (map[string]any, error) {
		return map[string]any{"claimsAndScopeOverrideDetails": map[string]any{
			"accessTokenGeneration": map[string]any{
				"claimsToAddOrOverride": map[string]any{
					"tenant": "acme", "tier": float64(3), "client_id": "forged", "scope": "forged",
				},
				"claimsToSuppress": []any{"auth_time", "iss", "tier"},
				"scopesToAdd":      []any{"api/extra", "bad scope"},
				"scopesToSuppress": []any{"api/write"},
			},
		}}, nil
	}

	tests := []struct {
		cfg        map[string]any
		name       string
		wantCalled bool
	}{
		{name: "v3 applies to machine tokens", cfg: v3, wantCalled: true},
		{name: "v2 does not apply to machine tokens", cfg: v2},
		{name: "v1 bare arn does not apply", cfg: legacy},
		{name: "no trigger"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			inv := &fakeInvoker{respond: respond}
			env := newOAuthEnvWith(t, cognitoidp.UserPoolOptions{LambdaConfig: tt.cfg}, inv)

			resp, body := env.postForm(t, "/oauth2/token", url.Values{
				"grant_type": {"client_credentials"}, "scope": {"api/read api/write"},
				"aws_client_metadata": {`{"environment":"dev"}`},
			}, basic(env.m2mID, env.m2mSec))
			require.Equal(t, http.StatusOK, resp.StatusCode, string(body))

			claims := env.verifyAgainstJWKS(t, jsonMap(t, body)["access_token"].(string))

			if !tt.wantCalled {
				assert.Zero(t, inv.callCount())
				assert.Equal(t, "api/read api/write", scopeSorted(claims["scope"].(string)))
				assert.NotContains(t, claims, "tenant")

				return
			}

			require.Equal(t, 1, inv.callCount())

			ev := inv.lastCall().event
			assert.Equal(t, "TokenGeneration_ClientCredentials", ev["triggerSource"])
			assert.Equal(t, "3", ev["version"])

			req, _ := ev["request"].(map[string]any)
			assert.ElementsMatch(t, []any{"api/read", "api/write"}, req["scopes"])
			assert.Equal(t, map[string]any{"environment": "dev"}, req["clientMetadata"])

			assert.Equal(t, "acme", claims["tenant"])
			assert.NotContains(t, claims, "tier", "suppress wins over add")
			assert.Equal(t, env.m2mID, claims["client_id"], "protected claims cannot be overridden")
			assert.Equal(t, "api/extra api/read", scopeSorted(claims["scope"].(string)))
			assert.Contains(t, claims, "auth_time", "protected claims cannot be suppressed")
			assert.Contains(t, claims, "iss")
		})
	}
}

func TestOAuthClientCredentialsTriggerErrors(t *testing.T) {
	t.Parallel()

	v3 := map[string]any{"PreTokenGenerationConfig": map[string]any{
		"LambdaArn": "arn:aws:lambda:us-east-1:000000000000:function:pre", "LambdaVersion": "V3_0",
	}}

	tests := []struct {
		respond    func(string, map[string]any) (map[string]any, error)
		name       string
		meta       string
		wantStatus int
	}{
		{name: "malformed metadata", meta: "not-json", wantStatus: http.StatusBadRequest},
		{name: "lambda failure", wantStatus: http.StatusInternalServerError,
			respond: func(string, map[string]any) (map[string]any, error) { return nil, errPreAuthenticationBlocked }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newOAuthEnvWith(t, cognitoidp.UserPoolOptions{LambdaConfig: v3}, &fakeInvoker{respond: tt.respond})
			form := url.Values{"grant_type": {"client_credentials"}, "scope": {"api/read"}}

			if tt.meta != "" {
				form.Set("aws_client_metadata", tt.meta)
			}

			resp, _ := env.postForm(t, "/oauth2/token", form, basic(env.m2mID, env.m2mSec))
			assert.Equal(t, tt.wantStatus, resp.StatusCode)
		})
	}
}
