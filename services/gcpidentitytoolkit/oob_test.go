package gcpidentitytoolkit_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *env) oobCodes(t *testing.T) []map[string]any {
	t.Helper()

	path := "/emulator/v1/projects/" + e.project + "/oobCodes"

	r := e.do(t, http.MethodGet, path, "", nil)
	require.Equal(t, http.StatusOK, r.status, r.body)

	raw, _ := r.body["oobCodes"].([]any)
	out := make([]map[string]any, 0, len(raw))

	for _, c := range raw {
		out = append(out, c.(map[string]any))
	}

	return out
}

func TestPasswordResetFlow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		email   string
		wantErr string
		wantOOB int
		privacy bool
	}{
		{name: "known email", email: "rp@example.com", privacy: true, wantOOB: 1},
		{name: "unknown email with protection is silent", email: "ghost@example.com", privacy: true},
		{name: "unknown email without protection", email: "ghost@example.com", wantErr: "EMAIL_NOT_FOUND"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.signUp(t, "rp@example.com")
			e.setConfig(
				t,
				map[string]any{"emailPrivacyConfig": map[string]any{"enableImprovedEmailPrivacy": tt.privacy}},
			)

			r := e.client(t, "sendOobCode", map[string]any{"requestType": "PASSWORD_RESET", "email": tt.email})
			if tt.wantErr != "" {
				e.assertErr(t, r, http.StatusBadRequest, tt.wantErr)

				return
			}

			require.Equal(t, http.StatusOK, r.status, r.body)
			assert.Equal(t, tt.email, r.str("email"))
			assert.Empty(t, r.str("oobCode"), "client calls never see the code")

			codes := e.oobCodes(t)
			require.Len(t, codes, tt.wantOOB)

			if tt.wantOOB == 0 {
				return
			}

			code := codes[0]["oobCode"].(string)
			assert.Equal(t, "PASSWORD_RESET", codes[0]["requestType"])
			assert.Contains(t, codes[0]["oobLink"], "mode=resetPassword")

			check := e.client(t, "resetPassword", map[string]any{"oobCode": code})
			require.Equal(t, http.StatusOK, check.status, check.body)
			assert.Equal(t, "PASSWORD_RESET", check.str("requestType"))

			weak := e.client(t, "resetPassword", map[string]any{"oobCode": code, "newPassword": "123"})
			e.assertErr(t, weak, http.StatusBadRequest, "WEAK_PASSWORD : Password should be at least 6 characters")

			done := e.client(t, "resetPassword", map[string]any{"oobCode": code, "newPassword": "brandnew1"})
			require.Equal(t, http.StatusOK, done.status, done.body)

			again := e.client(t, "resetPassword", map[string]any{"oobCode": code, "newPassword": "brandnew2"})
			e.assertErr(t, again, http.StatusBadRequest, "INVALID_OOB_CODE")

			old := e.client(t, "signInWithPassword", map[string]any{"email": "rp@example.com", "password": "secret1"})
			assert.Equal(t, http.StatusBadRequest, old.status)

			in := e.client(t, "signInWithPassword", map[string]any{"email": "rp@example.com", "password": "brandnew1"})
			assert.Equal(t, http.StatusOK, in.status, in.body)
		})
	}
}

func TestOobExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		msg     string
		advance time.Duration
	}{
		{name: "within ttl", advance: 59 * time.Minute},
		{name: "after ttl", advance: 61 * time.Minute, msg: "EXPIRED_OOB_CODE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.signUp(t, "x@example.com")
			e.client(t, "sendOobCode", map[string]any{"requestType": "PASSWORD_RESET", "email": "x@example.com"})

			code := e.oobCodes(t)[0]["oobCode"].(string)
			e.clock.Advance(tt.advance)

			r := e.client(t, "resetPassword", map[string]any{"oobCode": code})
			if tt.msg != "" {
				e.assertErr(t, r, http.StatusBadRequest, tt.msg)

				return
			}

			assert.Equal(t, http.StatusOK, r.status, r.body)
		})
	}
}

func TestVerifyEmailFlow(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	up := e.signUp(t, "v@example.com")

	r := e.client(t, "sendOobCode", map[string]any{"requestType": "VERIFY_EMAIL", "idToken": up.str("idToken")})
	require.Equal(t, http.StatusOK, r.status, r.body)

	code := e.oobCodes(t)[0]["oobCode"].(string)

	upd := e.client(t, "update", map[string]any{"oobCode": code})
	require.Equal(t, http.StatusOK, upd.status, upd.body)
	assert.Equal(t, true, upd.body["emailVerified"])

	lk := e.admin(t, "lookup", map[string]any{"localId": []string{up.str("localId")}})
	assert.Equal(t, true, lk.body["users"].([]any)[0].(map[string]any)["emailVerified"])
	assert.Empty(t, e.oobCodes(t))
}

func TestEmailLinkSignIn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		email    string
		msg      string
		existing bool
		link     bool
		isNew    bool
	}{
		{name: "new user", link: true, email: "new@example.com", isNew: true},
		{name: "existing user", existing: true, link: true, email: "old@example.com"},
		{name: "wrong email", link: true, email: "other@example.com", msg: "INVALID_EMAIL"},
		{
			name:  "link sign-in disabled",
			link:  false,
			email: "new@example.com",
			msg:   "OPERATION_NOT_ALLOWED : Email link sign-in is disabled for this project.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			if tt.existing {
				e.signUp(t, "old@example.com")
			}

			target := tt.email
			if tt.msg == "INVALID_EMAIL" {
				target = "a@example.com"
			}

			if !tt.link {
				e.setConfig(
					t,
					map[string]any{"signIn": map[string]any{"email": map[string]any{"passwordRequired": true}}},
				)

				r := e.client(t, "sendOobCode", map[string]any{"requestType": "EMAIL_SIGNIN", "email": target})
				e.assertErr(t, r, http.StatusBadRequest, tt.msg)

				return
			}

			r := e.client(t, "sendOobCode", map[string]any{"requestType": "EMAIL_SIGNIN", "email": target})
			require.Equal(t, http.StatusOK, r.status, r.body)

			code := e.oobCodes(t)[0]["oobCode"].(string)

			in := e.client(t, "signInWithEmailLink", map[string]any{"email": tt.email, "oobCode": code})
			if tt.msg != "" {
				e.assertErr(t, in, http.StatusBadRequest, tt.msg)

				return
			}

			require.Equal(t, http.StatusOK, in.status, in.body)
			assert.NotEmpty(t, in.str("idToken"))
			assert.Equal(t, tt.isNew, in.body["isNewUser"] == true)

			lk := e.client(t, "lookup", map[string]any{"idToken": in.str("idToken")})
			assert.Equal(t, true, lk.body["users"].([]any)[0].(map[string]any)["emailVerified"])
		})
	}
}

func TestAdminOobLink(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body map[string]any
		mode string
		msg  string
	}{
		{
			name: "password reset",
			body: map[string]any{"requestType": "PASSWORD_RESET", "email": "a@example.com"},
			mode: "mode=resetPassword",
		},
		{
			name: "verify email",
			body: map[string]any{"requestType": "VERIFY_EMAIL", "email": "a@example.com"},
			mode: "mode=verifyEmail",
		},
		{
			name: "email signin",
			body: map[string]any{
				"requestType": "EMAIL_SIGNIN",
				"email":       "z@example.com",
				"continueUrl": "https://app.example.com/done",
			},
			mode: "mode=signIn",
		},
		{
			name: "change email",
			body: map[string]any{
				"requestType": "VERIFY_AND_CHANGE_EMAIL",
				"email":       "a@example.com",
				"newEmail":    "b@example.com",
			},
			mode: "mode=verifyAndChangeEmail",
		},
		{
			name: "unknown user",
			body: map[string]any{"requestType": "PASSWORD_RESET", "email": "no@example.com"},
			msg:  "EMAIL_NOT_FOUND",
		},
		{
			name: "bad type",
			body: map[string]any{"requestType": "NOPE", "email": "a@example.com"},
			msg:  "INVALID_REQ_TYPE : Unsupported request type.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.signUp(t, "a@example.com")

			tt.body["returnOobLink"] = true

			r := e.admin(t, "sendOobCode", tt.body)
			if tt.msg != "" {
				e.assertErr(t, r, http.StatusBadRequest, tt.msg)

				return
			}

			require.Equal(t, http.StatusOK, r.status, r.body)
			assert.Contains(t, r.str("oobLink"), tt.mode)
			assert.Contains(t, r.str("oobLink"), "oobCode="+r.str("oobCode"))
		})
	}
}

func TestVerifyAndChangeEmail(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	up := e.signUp(t, "a@example.com")

	r := e.admin(t, "sendOobCode", map[string]any{
		"requestType":   "VERIFY_AND_CHANGE_EMAIL",
		"email":         "a@example.com",
		"newEmail":      "B@example.com",
		"returnOobLink": true,
	})
	require.Equal(t, http.StatusOK, r.status, r.body)

	upd := e.client(t, "update", map[string]any{"oobCode": r.str("oobCode")})
	require.Equal(t, http.StatusOK, upd.status, upd.body)
	assert.Equal(t, "b@example.com", upd.str("email"))

	lk := e.admin(t, "lookup", map[string]any{"localId": []string{up.str("localId")}})
	u := lk.body["users"].([]any)[0].(map[string]any)
	assert.Equal(t, "b@example.com", u["email"])
	assert.Equal(t, true, u["emailVerified"])
}

func TestCreateAuthURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		identifier string
		msg        string
		methods    []any
		privacy    bool
		registered bool
	}{
		{name: "protection hides existence", privacy: true, identifier: "me@example.com"},
		{name: "registered", identifier: "me@example.com", registered: true, methods: []any{"password", "emailLink"}},
		{name: "unregistered", identifier: "nobody@example.com"},
		{name: "invalid identifier", identifier: "nope", msg: "INVALID_IDENTIFIER"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.signUp(t, "me@example.com")
			e.setConfig(
				t,
				map[string]any{"emailPrivacyConfig": map[string]any{"enableImprovedEmailPrivacy": tt.privacy}},
			)

			r := e.client(
				t,
				"createAuthUri",
				map[string]any{"identifier": tt.identifier, "continueUri": "http://localhost"},
			)
			if tt.msg != "" {
				e.assertErr(t, r, http.StatusBadRequest, tt.msg)

				return
			}

			require.Equal(t, http.StatusOK, r.status, r.body)

			if tt.privacy {
				assert.NotContains(t, r.body, "registered")
				assert.NotContains(t, r.body, "allProviders")

				return
			}

			assert.Equal(t, tt.registered, r.body["registered"])
			if tt.methods == nil {
				assert.NotContains(t, r.body, "signinMethods")

				return
			}

			assert.Equal(t, tt.methods, r.body["signinMethods"])
		})
	}
}
