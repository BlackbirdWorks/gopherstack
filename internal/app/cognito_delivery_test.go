package app

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cognitoidpbackend "github.com/blackbirdworks/gopherstack/services/cognitoidp"
	sesbackend "github.com/blackbirdworks/gopherstack/services/ses"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
)

func cognitoCall(t *testing.T, h *cognitoidpbackend.Handler, action, body string) string {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSCognitoIdentityProviderService."+action)

	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(echo.New().NewContext(req, rec)))
	require.Equal(t, 200, rec.Code, rec.Body.String())

	return rec.Body.String()
}

func TestWireCognitoMessageDelivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		attribute string
		pool      string
		wantEmail bool
	}{
		{
			name: "email via ses", attribute: `{"Name":"email","Value":"bob@corp.example"}`,
			pool: `"AutoVerifiedAttributes":["email"]`, wantEmail: true,
		},
		{
			name: "sms via sns", attribute: `{"Name":"phone_number","Value":"+15555550123"}`,
			pool: `"AutoVerifiedAttributes":["phone_number"]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ses, sns := sesbackend.NewInMemoryBackend(), snsbackend.NewInMemoryBackend()
			cognito := cognitoidpbackend.NewHandler(
				cognitoidpbackend.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost"), "us-east-1")

			wireCognitoMessageDelivery(cognito, sesbackend.NewHandler(ses), snsbackend.NewHandler(sns))

			pool := cognitoCall(t, cognito, "CreateUserPool", `{"PoolName":"p",`+tt.pool+`}`)
			poolID := between(pool, `"Id":"`, `"`)
			client := cognitoCall(t, cognito, "CreateUserPoolClient", `{"UserPoolId":"`+poolID+`","ClientName":"c"}`)
			clientID := between(client, `"ClientId":"`, `"`)

			signUp := `{"ClientId":"` + clientID + `","Username":"u1","Password":"Pass1234!",` +
				`"UserAttributes":[` + tt.attribute + `]}`
			out := cognitoCall(t, cognito, "SignUp", signUp)
			code := between(out, `"ConfirmationCode":"`, `"`)
			require.NotEmpty(t, code)

			if tt.wantEmail {
				emails := ses.ListEmails()
				require.Len(t, emails, 1)
				assert.Equal(t, []string{"bob@corp.example"}, emails[0].To)
				assert.Contains(t, emails[0].BodyText, code)

				return
			}

			sms := sns.DrainSMSDeliveries()
			require.Len(t, sms, 1)
			assert.Equal(t, "+15555550123", sms[0].PhoneNumber)
			assert.Contains(t, sms[0].Message, code)
		})
	}
}

func between(s, start, end string) string {
	_, rest, ok := strings.Cut(s, start)
	if !ok {
		return ""
	}

	val, _, _ := strings.Cut(rest, end)

	return val
}
