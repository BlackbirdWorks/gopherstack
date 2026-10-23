package cognitoidp_test

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

var errRoleDenied = errors.New("role denied")

type fakeRoleAssumer struct {
	err   error
	roles []string
	exts  []string
	mu    sync.Mutex
}

func (f *fakeRoleAssumer) AssumeSMSRole(_ context.Context, roleARN, externalID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.roles = append(f.roles, roleARN)
	f.exts = append(f.exts, externalID)

	return f.err
}

const smsRoleARN = "arn:aws:iam::000000000000:role/cognito-sms"

func TestMFACustomMessageAuthentication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		respond     func(map[string]any) (map[string]any, error)
		name        string
		wantMessage string
		wantPattern string
	}{
		{
			name: "trigger message replaces default",
			respond: func(map[string]any) (map[string]any, error) {
				return map[string]any{"smsMessage": "Hi, your MFA code is ####"}, nil
			},
			wantPattern: `^Hi, your MFA code is \d{6}$`,
		},
		{
			name: "empty response keeps template",
			respond: func(map[string]any) (map[string]any, error) {
				return map[string]any{"smsMessage": ""}, nil
			},
			wantPattern: `^Login code \d{6}$`,
		},
		{
			name:        "trigger failure keeps template",
			respond:     func(map[string]any) (map[string]any, error) { return nil, errSendFailed },
			wantPattern: `^Login code \d{6}$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newDeliveryEnv(t, map[string]any{
				"MfaConfiguration": "ON",
				"LambdaConfig": map[string]any{
					"CustomMessage": "arn:aws:lambda:us-east-1:000000000000:function:cm",
				},
			})

			var source string

			inv := &fakeInvoker{respond: func(_ string, ev map[string]any) (map[string]any, error) {
				source, _ = ev["triggerSource"].(string)

				return tt.respond(ev)
			}}
			e.h.Backend.SetLambdaTriggerInvoker(inv)

			assumer := &fakeRoleAssumer{}
			e.h.Backend.SetSMSRoleAssumer(assumer)

			require.NoError(t, e.h.Backend.SetUserPoolMfaConfigFull(e.poolID, cognitoidp.UserPoolMfaFullConfig{
				MfaConfiguration: "ON",
				SmsMfaConfiguration: &cognitoidp.SmsMfaConfiguration{
					SmsAuthenticationMessage: "Login code {####}",
					SmsConfiguration: &cognitoidp.SmsConfiguration{
						SnsCallerArn: smsRoleARN,
						ExternalID:   "ext-1",
					},
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
			assert.Equal(t, "CustomMessage_Authentication", source)

			require.Len(t, e.snd.sms, 1)
			assert.Regexp(t, tt.wantPattern, e.snd.sms[0].Message)
			assert.Equal(t, []string{smsRoleARN}, assumer.roles)
			assert.Equal(t, []string{"ext-1"}, assumer.exts)
		})
	}
}

func TestSMSRoleAssumption(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err      error
		name     string
		wantCode int
		wantSMS  int
	}{
		{name: "assumed", wantCode: http.StatusOK, wantSMS: 1},
		{name: "cannot assume", err: errRoleDenied, wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newDeliveryEnv(t, map[string]any{
				"AutoVerifiedAttributes": []string{"phone_number"},
				"SmsConfiguration":       map[string]any{"SnsCallerArn": smsRoleARN, "ExternalId": "ext-2"},
			})
			assumer := &fakeRoleAssumer{err: tt.err}
			e.h.Backend.SetSMSRoleAssumer(assumer)

			rec := doCognitoRequest(t, e.h, "SignUp", map[string]any{
				"ClientId": e.clientID, "Username": "z", "Password": "Pass1234!",
				"UserAttributes": []map[string]string{{"Name": "phone_number", "Value": "+15555550123"}},
			})

			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
			assert.Len(t, e.snd.sms, tt.wantSMS)
			assert.Equal(t, []string{smsRoleARN}, assumer.roles)
			assert.Equal(t, []string{"ext-2"}, assumer.exts)

			if tt.err != nil {
				assert.Contains(t, rec.Body.String(), "InvalidSmsRoleTrustRelationshipException")
			}
		})
	}
}
