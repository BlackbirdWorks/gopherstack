package gcpidentitytoolkit_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSignUpAndSignIn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		email  string
		pw     string
		prep   func(*testing.T, *env)
		msg    string
		status int
	}{
		{name: "ok", email: "a@example.com", pw: "secret1", status: http.StatusOK},
		{name: "email case folded", email: "Mixed@Example.com", pw: "secret1", status: http.StatusOK},
		{
			name:   "weak password",
			email:  "b@example.com",
			pw:     "123",
			status: 400,
			msg:    "WEAK_PASSWORD : Password should be at least 6 characters",
		},
		{name: "bad email", email: "not-an-email", pw: "secret1", status: 400, msg: "INVALID_EMAIL"},
		{name: "missing password", email: "c@example.com", status: 400, msg: "MISSING_PASSWORD"},
		{
			name: "duplicate", email: "d@example.com", pw: "secret1", status: 400, msg: "EMAIL_EXISTS",
			prep: func(t *testing.T, e *env) {
				t.Helper()
				e.signUp(t, "D@example.com")
			},
		},
		{
			name: "provider disabled", email: "e@example.com", pw: "secret1", status: 400,
			msg: "OPERATION_NOT_ALLOWED : Password sign-in is disabled for this project.",
			prep: func(t *testing.T, e *env) {
				t.Helper()

				e.setConfig(t, map[string]any{"signIn": map[string]any{"email": map[string]any{"enabled": false}}})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			if tt.prep != nil {
				tt.prep(t, e)
			}

			r := e.client(t, "signUp", map[string]any{"email": tt.email, "password": tt.pw, "returnSecureToken": true})
			assert.Equal(t, tt.status, r.status, r.body)

			if tt.msg != "" {
				assert.Equal(t, tt.msg, r.errMsg())

				return
			}

			assert.Equal(t, "identitytoolkit#SignupNewUserResponse", r.str("kind"))
			assert.NotEmpty(t, r.str("idToken"))
			assert.NotEmpty(t, r.str("refreshToken"))
			assert.Equal(t, "3600", r.str("expiresIn"))

			in := e.client(
				t,
				"signInWithPassword",
				map[string]any{"email": tt.email, "password": tt.pw, "returnSecureToken": true},
			)
			require.Equal(t, http.StatusOK, in.status, in.body)
			assert.Equal(t, r.str("localId"), in.str("localId"))
			assert.Equal(t, true, in.body["registered"])
		})
	}
}

func TestSignInErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		email   string
		pw      string
		msg     string
		privacy bool
		disable bool
	}{
		{
			name:    "wrong password with protection",
			email:   "u@example.com",
			pw:      "wrongpw",
			privacy: true,
			msg:     "INVALID_LOGIN_CREDENTIALS",
		},
		{
			name:    "unknown email with protection",
			email:   "x@example.com",
			pw:      "wrongpw",
			privacy: true,
			msg:     "INVALID_LOGIN_CREDENTIALS",
		},
		{name: "wrong password without protection", email: "u@example.com", pw: "wrongpw", msg: "INVALID_PASSWORD"},
		{name: "unknown email without protection", email: "x@example.com", pw: "wrongpw", msg: "EMAIL_NOT_FOUND"},
		{
			name:    "disabled user",
			email:   "u@example.com",
			pw:      "secret1",
			privacy: true,
			disable: true,
			msg:     "USER_DISABLED : The user account has been disabled by an administrator.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			up := e.signUp(t, "u@example.com")

			e.setConfig(
				t,
				map[string]any{"emailPrivacyConfig": map[string]any{"enableImprovedEmailPrivacy": tt.privacy}},
			)

			if tt.disable {
				r := e.admin(t, "update", map[string]any{"localId": up.str("localId"), "disableUser": true})
				require.Equal(t, http.StatusOK, r.status, r.body)
			}

			r := e.client(t, "signInWithPassword", map[string]any{"email": tt.email, "password": tt.pw})
			e.assertErr(t, r, http.StatusBadRequest, tt.msg)
		})
	}
}

func TestAuthRequirements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		auth   string
		msg    string
		status int
	}{
		{
			name:   "client call without key",
			path:   "/v1/accounts:signUp",
			status: http.StatusForbidden,
			msg:    "The request is missing a valid API key.",
		},
		{name: "client call with key", path: "/v1/accounts:signUp?key=abc", status: http.StatusOK},
		{
			name:   "admin path without bearer",
			path:   "/v1/projects/p1/accounts:batchGet?key=abc",
			status: http.StatusUnauthorized,
		},
		{name: "tenants without bearer", path: "/v2/projects/p1/tenants?key=abc", status: http.StatusUnauthorized},
		{
			name:   "emulator prefix",
			path:   "/identitytoolkit.googleapis.com/v1/accounts:signUp?key=abc",
			status: http.StatusOK,
		},
		{name: "unknown route", path: "/v1/nope", status: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			method := http.MethodPost
			if tt.path == "/v1/projects/p1/accounts:batchGet?key=abc" || tt.path == "/v2/projects/p1/tenants?key=abc" {
				method = http.MethodGet
			}

			e := newEnv(t)
			r := e.do(t, method, tt.path, tt.auth, map[string]any{})
			assert.Equal(t, tt.status, r.status, r.body)

			if tt.msg != "" {
				assert.Equal(t, tt.msg, r.errMsg())
			}
		})
	}
}

func TestAnonymousSignUp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		enabled bool
		status  int
	}{
		{name: "enabled", enabled: true, status: http.StatusOK},
		{name: "disabled", enabled: false, status: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.setConfig(t, map[string]any{"signIn": map[string]any{"anonymous": map[string]any{"enabled": tt.enabled}}})

			r := e.client(t, "signUp", map[string]any{"returnSecureToken": true})
			require.Equal(t, tt.status, r.status, r.body)

			if !tt.enabled {
				assert.Equal(t, "ADMIN_ONLY_OPERATION", r.errMsg())

				return
			}

			lk := e.client(t, "lookup", map[string]any{"idToken": r.str("idToken")})
			require.Equal(t, http.StatusOK, lk.status, lk.body)

			users, _ := lk.body["users"].([]any)
			require.Len(t, users, 1)
			assert.NotContains(t, users[0], "email")

			link := e.client(
				t,
				"signUp",
				map[string]any{"idToken": r.str("idToken"), "email": "anon@example.com", "password": "secret1"},
			)
			require.Equal(t, http.StatusOK, link.status, link.body)
			assert.Equal(t, r.str("localId"), link.str("localId"))
		})
	}
}

func TestLookupRedactsHashForClients(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	up := e.signUp(t, "h@example.com")

	client := e.client(t, "lookup", map[string]any{"idToken": up.str("idToken")})
	require.Equal(t, http.StatusOK, client.status, client.body)

	cu := client.body["users"].([]any)[0].(map[string]any)
	assert.Equal(t, "UkVEQUNURUQ=", cu["passwordHash"])
	assert.NotContains(t, cu, "salt")

	admin := e.admin(t, "lookup", map[string]any{"email": []string{"h@example.com"}})
	require.Equal(t, http.StatusOK, admin.status, admin.body)

	au := admin.body["users"].([]any)[0].(map[string]any)
	assert.NotEqual(t, "UkVEQUNURUQ=", au["passwordHash"])
	assert.NotEmpty(t, au["salt"])
	assert.Equal(t, "password", au["providerUserInfo"].([]any)[0].(map[string]any)["providerId"])

	none := e.admin(t, "lookup", map[string]any{"localId": []string{"missing"}})
	require.Equal(t, http.StatusOK, none.status)
	assert.NotContains(t, none.body, "users")

	noAuth := e.client(t, "lookup", map[string]any{"localId": []string{"x"}})
	e.assertErr(t, noAuth, http.StatusBadRequest, "MISSING_ID_TOKEN")
}

func TestUpdateAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   func(idToken string) map[string]any
		check  func(*testing.T, result)
		name   string
		msg    string
		second bool
	}{
		{
			name: "display name and photo",
			body: func(id string) map[string]any {
				return map[string]any{"idToken": id, "displayName": "Ann", "photoUrl": "http://x/y.png"}
			},
			check: func(t *testing.T, r result) {
				t.Helper()

				assert.Equal(t, "Ann", r.str("displayName"))
				assert.Equal(t, "http://x/y.png", r.str("photoUrl"))
			},
		},
		{
			name: "delete attributes",
			body: func(id string) map[string]any {
				return map[string]any{"idToken": id, "deleteAttribute": []string{"DISPLAY_NAME", "PHOTO_URL"}}
			},
			check: func(t *testing.T, r result) {
				t.Helper()

				assert.Empty(t, r.str("displayName"))
				assert.Empty(t, r.str("photoUrl"))
			},
		},
		{
			name: "change email unverifies",
			body: func(id string) map[string]any {
				return map[string]any{"idToken": id, "email": "new@example.com", "returnSecureToken": true}
			},
			check: func(t *testing.T, r result) {
				t.Helper()

				assert.Equal(t, "new@example.com", r.str("email"))
				assert.NotEmpty(t, r.str("idToken"))
			},
		},
		{
			name: "weak password",
			body: func(id string) map[string]any { return map[string]any{"idToken": id, "password": "abc"} },
			msg:  "WEAK_PASSWORD : Password should be at least 6 characters",
		},
		{
			name: "email taken",
			body: func(id string) map[string]any { return map[string]any{"idToken": id, "email": "other@example.com"} },
			msg:  "EMAIL_EXISTS",
		},
		{
			name: "bad token",
			body: func(string) map[string]any { return map[string]any{"idToken": "junk"} },
			msg:  "INVALID_ID_TOKEN",
		},
		{
			name: "client cannot disable",
			body: func(id string) map[string]any { return map[string]any{"idToken": id, "disableUser": true} },
			check: func(t *testing.T, r result) {
				t.Helper()
				assert.Equal(t, "identitytoolkit#SetAccountInfoResponse", r.str("kind"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			up := e.signUp(t, "me@example.com")
			e.signUp(t, "other@example.com")

			r := e.client(t, "update", tt.body(up.str("idToken")))
			if tt.msg != "" {
				e.assertErr(t, r, http.StatusBadRequest, tt.msg)

				return
			}

			require.Equal(t, http.StatusOK, r.status, r.body)
			tt.check(t, r)
		})
	}
}

func TestClientDisableIsIgnored(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	up := e.signUp(t, "me@example.com")
	e.client(t, "update", map[string]any{"idToken": up.str("idToken"), "disableUser": true})

	in := e.client(t, "signInWithPassword", map[string]any{"email": "me@example.com", "password": "secret1"})
	assert.Equal(t, http.StatusOK, in.status, in.body)
}

func TestCustomAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		attrs string
		msg   string
	}{
		{name: "valid", attrs: `{"role":"admin","level":3}`},
		{name: "reserved claim", attrs: `{"sub":"x"}`, msg: "FORBIDDEN_CLAIM : sub"},
		{name: "invalid json", attrs: `{nope`, msg: "INVALID_CLAIMS"},
		{
			name:  "too large",
			attrs: `{"k":"` + string(make([]byte, 0)) + repeat('x', 1001) + `"}`,
			msg:   "CLAIMS_TOO_LARGE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			up := e.signUp(t, "c@example.com")

			r := e.admin(t, "update", map[string]any{"localId": up.str("localId"), "customAttributes": tt.attrs})
			if tt.msg != "" {
				e.assertErr(t, r, http.StatusBadRequest, tt.msg)

				return
			}

			require.Equal(t, http.StatusOK, r.status, r.body)

			lk := e.admin(t, "lookup", map[string]any{"localId": []string{up.str("localId")}})
			assert.JSONEq(t, tt.attrs, lk.body["users"].([]any)[0].(map[string]any)["customAttributes"].(string))
		})
	}
}

func repeat(c byte, n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = c
	}

	return string(b)
}

func TestDeleteAccount(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	up := e.signUp(t, "del@example.com")

	r := e.client(t, "delete", map[string]any{"idToken": up.str("idToken")})
	require.Equal(t, http.StatusOK, r.status, r.body)

	again := e.client(t, "delete", map[string]any{"idToken": up.str("idToken")})
	e.assertErr(t, again, http.StatusBadRequest, "USER_NOT_FOUND")

	adm := e.admin(t, "delete", map[string]any{"localId": "ghost"})
	e.assertErr(t, adm, http.StatusBadRequest, "USER_NOT_FOUND")

	in := e.client(t, "signInWithPassword", map[string]any{"email": "del@example.com", "password": "secret1"})
	e.assertErr(t, in, http.StatusBadRequest, "INVALID_LOGIN_CREDENTIALS")
}

func TestTokenExpiryAndRefresh(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	up := e.signUp(t, "tok@example.com")

	refresh := func(tok string) result {
		return e.do(
			t,
			http.MethodPost,
			"/v1/token?key=k",
			"",
			map[string]any{"grant_type": "refresh_token", "refresh_token": tok},
		)
	}

	e.clock.Advance(time.Hour + time.Minute)

	expired := e.client(t, "lookup", map[string]any{"idToken": up.str("idToken")})
	e.assertErr(t, expired, http.StatusBadRequest, "TOKEN_EXPIRED")

	r := refresh(up.str("refreshToken"))
	require.Equal(t, http.StatusOK, r.status, r.body)
	assert.Equal(t, "Bearer", r.str("token_type"))
	assert.Equal(t, "3600", r.str("expires_in"))
	assert.Equal(t, up.str("localId"), r.str("user_id"))

	fresh := e.client(t, "lookup", map[string]any{"idToken": r.str("id_token")})
	assert.Equal(t, http.StatusOK, fresh.status, fresh.body)

	bad := refresh("garbage")
	assert.Equal(t, "INVALID_REFRESH_TOKEN", bad.errMsg())

	none := refresh("")
	assert.Equal(t, "MISSING_REFRESH_TOKEN", none.errMsg())

	grant := e.do(t, http.MethodPost, "/v1/token?key=k", "", map[string]any{"grant_type": "x", "refresh_token": "y"})
	assert.Equal(t, "INVALID_GRANT_TYPE", grant.errMsg())

	e.admin(t, "update", map[string]any{"localId": up.str("localId"), "disableUser": true})
	disabled := refresh(up.str("refreshToken"))
	assert.Equal(t, "USER_DISABLED", disabled.errMsg())
}

func TestRevokeRefreshTokens(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	up := e.signUp(t, "rev@example.com")

	e.clock.Advance(2 * time.Second)

	r := e.admin(t, "update", map[string]any{"localId": up.str("localId"), "validSince": e.clock.Now().Unix()})
	require.Equal(t, http.StatusOK, r.status, r.body)

	lk := e.client(t, "lookup", map[string]any{"idToken": up.str("idToken")})
	e.assertErr(t, lk, http.StatusBadRequest, "TOKEN_EXPIRED")

	rf := e.do(
		t,
		http.MethodPost,
		"/v1/token?key=k",
		"",
		map[string]any{"grant_type": "refresh_token", "refresh_token": up.str("refreshToken")},
	)
	assert.Equal(t, "TOKEN_EXPIRED", rf.errMsg())

	in := e.client(t, "signInWithPassword", map[string]any{"email": "rev@example.com", "password": "secret1"})
	require.Equal(t, http.StatusOK, in.status)

	ok := e.client(t, "lookup", map[string]any{"idToken": in.str("idToken")})
	assert.Equal(t, http.StatusOK, ok.status, ok.body)
}
