package gcpidentitytoolkit_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUndeliverableMessagesAreLogged(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		verb string
		body map[string]any
		want string
		to   string
	}{
		{
			name: "email", verb: "sendOobCode", want: "would send email", to: "log@example.com",
			body: map[string]any{"requestType": "PASSWORD_RESET", "email": "log@example.com"},
		},
		{
			name: "sms", verb: "sendVerificationCode", want: "would send SMS", to: "+15555550123",
			body: map[string]any{"phoneNumber": "+15555550123", "recaptchaToken": "tok"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.signUp(t, "log@example.com")

			r := e.client(t, tt.verb, tt.body)
			require.Equal(t, 200, r.status, r.body)

			out := e.logs.String()
			assert.Contains(t, out, tt.want)
			assert.Contains(t, out, tt.to)
		})
	}
}

func TestAPIKeyRestrictionNoticeIsLoggedOnce(t *testing.T) {
	t.Parallel()

	e := newEnv(t)

	for range 3 {
		r := e.do(t, "POST", "/v1/projects/"+e.project+"/accounts:signUp?key=AIzaSecretKeyValue", "", map[string]any{})
		require.Equal(t, 200, r.status, r.body)
	}

	out := e.logs.String()
	assert.Equal(t, 1, strings.Count(out, "would check the API key restrictions"))
	assert.NotContains(t, out, "AIzaSecretKeyValue")
}

func TestAPIKeyRestrictionsAreLogged(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		key    string
		status int
		logged bool
	}{
		{name: "user key", key: "?key=AIzaUserKey", status: 200, logged: true},
		{name: "emulator key", key: "?key=fake-api-key", status: 200},
		{name: "missing key", key: "", status: 403},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			r := e.do(t, "POST", "/v1/projects/"+e.project+"/accounts:signUp"+tt.key, "", map[string]any{})

			assert.Equal(t, tt.status, r.status, r.body)
			assert.Equal(t, tt.logged, strings.Contains(e.logs.String(), "would check the API key restrictions"))
		})
	}
}
