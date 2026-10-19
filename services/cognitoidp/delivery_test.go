package cognitoidp_test

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

var errSendFailed = errors.New("send failed")

type recordingSender struct {
	err    error
	emails []cognitoidp.EmailMessage
	sms    []cognitoidp.SMSMessage
	mu     sync.Mutex
}

func (r *recordingSender) SendEmail(_ context.Context, m cognitoidp.EmailMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.err != nil {
		return r.err
	}

	r.emails = append(r.emails, m)

	return nil
}

func (r *recordingSender) SendSMS(_ context.Context, m cognitoidp.SMSMessage) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.err != nil {
		return r.err
	}

	r.sms = append(r.sms, m)

	return nil
}

type deliveryEnv struct {
	h        *cognitoidp.Handler
	snd      *recordingSender
	poolID   string
	clientID string
}

func newDeliveryEnv(t *testing.T, poolExtra map[string]any) *deliveryEnv {
	t.Helper()

	h := newTestHandler(t)
	snd := &recordingSender{}
	h.Backend.SetMessageSender(snd)

	body := map[string]any{"PoolName": "delivery"}
	maps.Copy(body, poolExtra)

	rec := doCognitoRequest(t, h, "CreateUserPool", body)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var pool struct {
		UserPool struct {
			ID string `json:"Id"`
		} `json:"UserPool"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &pool))

	rec = doCognitoRequest(t, h, "CreateUserPoolClient", map[string]any{
		"UserPoolId": pool.UserPool.ID, "ClientName": "app",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var client struct {
		UserPoolClient struct {
			ClientID string `json:"ClientId"`
		} `json:"UserPoolClient"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &client))

	return &deliveryEnv{h: h, snd: snd, poolID: pool.UserPool.ID, clientID: client.UserPoolClient.ClientID}
}

func (e *deliveryEnv) call(t *testing.T, action string, body map[string]any) map[string]any {
	t.Helper()

	rec := doCognitoRequest(t, e.h, action, body)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out map[string]any
	if rec.Body.Len() > 0 {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	}

	return out
}

func (e *deliveryEnv) signUp(t *testing.T, username string, attrs map[string]string) map[string]any {
	t.Helper()

	list := make([]map[string]string, 0, len(attrs))
	for k, v := range attrs {
		list = append(list, map[string]string{"Name": k, "Value": v})
	}

	return e.call(t, "SignUp", map[string]any{
		"ClientId": e.clientID, "Username": username, "Password": "Pass1234!", "UserAttributes": list,
	})
}

func TestCodeDelivery(t *testing.T) {
	t.Parallel()

	developer := map[string]any{
		"EmailConfiguration": map[string]any{
			"EmailSendingAccount": "DEVELOPER", "From": "Ops <ops@corp.example>",
			"ReplyToEmailAddress": "help@corp.example",
			"SourceArn":           "arn:aws:ses:us-east-1:000000000000:identity/ops@corp.example",
		},
		"VerificationMessageTemplate": map[string]any{
			"EmailSubject": "Confirm your account", "EmailMessage": "Hello, code {####}.",
			"SmsMessage": "SMS code {####}",
		},
		"AutoVerifiedAttributes": []string{"email"},
	}

	tests := []struct {
		run  func(t *testing.T, e *deliveryEnv)
		pool map[string]any
		name string
	}{
		{
			name: "signup email uses developer sender and template",
			pool: developer,
			run: func(t *testing.T, e *deliveryEnv) {
				t.Helper()

				out := e.signUp(t, "bob", map[string]string{"email": "bob@corp.example"})
				d := out["CodeDeliveryDetails"].(map[string]any)
				code := d["ConfirmationCode"].(string)

				require.Len(t, e.snd.emails, 1)

				m := e.snd.emails[0]
				assert.Equal(t, "bob@corp.example", m.To)
				assert.Equal(t, "Ops <ops@corp.example>", m.From)
				assert.Equal(t, "help@corp.example", m.ReplyTo)
				assert.Equal(t, "Confirm your account", m.Subject)
				assert.Equal(t, "Hello, code "+code+".", m.Text)
				assert.Equal(t, "EMAIL", d["DeliveryMedium"])
				assert.NotEqual(t, "bob@corp.example", d["Destination"])
				assert.Equal(t, "email", d["AttributeName"])
			},
		},
		{
			name: "signup phone only sends sms",
			pool: map[string]any{"AutoVerifiedAttributes": []string{"phone_number"}},
			run: func(t *testing.T, e *deliveryEnv) {
				t.Helper()

				out := e.signUp(t, "sam", map[string]string{"phone_number": "+15555550123"})
				code := out["CodeDeliveryDetails"].(map[string]any)["ConfirmationCode"].(string)

				require.Len(t, e.snd.sms, 1)
				assert.Equal(t, "+15555550123", e.snd.sms[0].PhoneNumber)
				assert.Equal(t, "Your verification code is "+code, e.snd.sms[0].Message)
				assert.Empty(t, e.snd.emails)
			},
		},
		{
			name: "forgot password and resend",
			pool: developer,
			run: func(t *testing.T, e *deliveryEnv) {
				t.Helper()

				e.signUp(t, "ann", map[string]string{"email": "ann@corp.example"})
				e.call(t, "AdminConfirmSignUp", map[string]any{"UserPoolId": e.poolID, "Username": "ann"})
				out := e.call(t, "ForgotPassword", map[string]any{"ClientId": e.clientID, "Username": "ann"})
				code := out["CodeDeliveryDetails"].(map[string]any)["ConfirmationCode"].(string)

				require.Len(t, e.snd.emails, 2)
				assert.Equal(t, "Hello, code "+code+".", e.snd.emails[1].Text)
				assert.Equal(t, "ann@corp.example", e.snd.emails[1].To)
			},
		},
		{
			name: "admin create user invite honours template and suppress",
			pool: map[string]any{"AdminCreateUserConfig": map[string]any{"InviteMessageTemplate": map[string]any{
				"EmailSubject": "Welcome", "EmailMessage": "User {username} pass {####}",
			}}},
			run: func(t *testing.T, e *deliveryEnv) {
				t.Helper()

				attrs := []map[string]string{{"Name": "email", "Value": "kim@corp.example"}}
				e.call(t, "AdminCreateUser", map[string]any{
					"UserPoolId": e.poolID, "Username": "kim", "UserAttributes": attrs,
					"TemporaryPassword": "Tmp12345!x", "DesiredDeliveryMediums": []string{"EMAIL"},
				})
				e.call(t, "AdminCreateUser", map[string]any{
					"UserPoolId": e.poolID, "Username": "lee", "UserAttributes": attrs,
					"MessageAction": "SUPPRESS",
				})

				require.Len(t, e.snd.emails, 1)
				assert.Equal(t, "Welcome", e.snd.emails[0].Subject)
				assert.Equal(t, "User kim pass Tmp12345!x", e.snd.emails[0].Text)
			},
		},
		{
			name: "admin update attributes sends verification to new email",
			pool: developer,
			run: func(t *testing.T, e *deliveryEnv) {
				t.Helper()

				e.signUp(t, "zed", map[string]string{"email": "zed@corp.example"})
				e.call(t, "AdminConfirmSignUp", map[string]any{"UserPoolId": e.poolID, "Username": "zed"})
				e.call(t, "AdminUpdateUserAttributes", map[string]any{
					"UserPoolId": e.poolID, "Username": "zed",
					"UserAttributes": []map[string]string{{"Name": "email", "Value": "zed2@corp.example"}},
				})

				require.Len(t, e.snd.emails, 2)
				assert.Equal(t, "zed2@corp.example", e.snd.emails[1].To)
			},
		},
		{
			name: "sms mfa challenge sends the code",
			pool: map[string]any{"MfaConfiguration": "ON"},
			run: func(t *testing.T, e *deliveryEnv) {
				t.Helper()

				require.NoError(t, e.h.Backend.SetUserPoolMfaConfigFull(e.poolID, cognitoidp.UserPoolMfaFullConfig{
					MfaConfiguration: "ON",
					SmsMfaConfiguration: &cognitoidp.SmsMfaConfiguration{
						SmsAuthenticationMessage: "Login code {####}",
						SmsConfiguration:         &cognitoidp.SmsConfiguration{SnsRegion: "us-east-1"},
					},
				}))

				e.call(t, "AdminCreateUser", map[string]any{
					"UserPoolId": e.poolID, "Username": "mia", "MessageAction": "SUPPRESS",
					"UserAttributes": []map[string]string{{"Name": "phone_number", "Value": "+15555550199"}},
				})
				e.call(t, "AdminSetUserPassword", map[string]any{
					"UserPoolId": e.poolID, "Username": "mia", "Password": "Pass1234!", "Permanent": true,
				})
				require.NoError(t, e.h.Backend.AdminSetUserMFASetting(e.poolID, "mia", true, false, "SMS_MFA"))

				out := e.call(t, "InitiateAuth", map[string]any{
					"ClientId": e.clientID, "AuthFlow": "USER_PASSWORD_AUTH",
					"AuthParameters": map[string]string{"USERNAME": "mia", "PASSWORD": "Pass1234!"},
				})
				require.Equal(t, "SMS_MFA", out["ChallengeName"])
				require.Len(t, e.snd.sms, 1)
				assert.Equal(t, "+15555550199", e.snd.sms[0].PhoneNumber)
				assert.Regexp(t, `^Login code \d{6}$`, e.snd.sms[0].Message)
			},
		},
		{
			name: "developer email failure is a delivery failure",
			pool: developer,
			run: func(t *testing.T, e *deliveryEnv) {
				t.Helper()

				e.snd.err = errSendFailed
				rec := doCognitoRequest(t, e.h, "SignUp", map[string]any{
					"ClientId": e.clientID, "Username": "x", "Password": "Pass1234!",
					"UserAttributes": []map[string]string{{"Name": "email", "Value": "x@corp.example"}},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "CodeDeliveryFailureException")
			},
		},
		{
			name: "default sender failure keeps code retrievable",
			pool: map[string]any{"AutoVerifiedAttributes": []string{"email"}},
			run: func(t *testing.T, e *deliveryEnv) {
				t.Helper()

				e.snd.err = errSendFailed
				out := e.signUp(t, "y", map[string]string{"email": "y@corp.example"})
				assert.NotEmpty(t, out["CodeDeliveryDetails"].(map[string]any)["ConfirmationCode"])
			},
		},
		{
			name: "sms failure is a delivery failure",
			pool: map[string]any{"AutoVerifiedAttributes": []string{"phone_number"}},
			run: func(t *testing.T, e *deliveryEnv) {
				t.Helper()

				e.snd.err = errSendFailed
				rec := doCognitoRequest(t, e.h, "SignUp", map[string]any{
					"ClientId": e.clientID, "Username": "z", "Password": "Pass1234!",
					"UserAttributes": []map[string]string{{"Name": "phone_number", "Value": "+15555550123"}},
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), "CodeDeliveryFailureException")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newDeliveryEnv(t, tt.pool))
		})
	}
}
