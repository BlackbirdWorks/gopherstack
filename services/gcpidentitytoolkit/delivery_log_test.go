package gcpidentitytoolkit_test

import (
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
