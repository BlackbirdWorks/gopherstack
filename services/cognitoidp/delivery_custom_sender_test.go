package cognitoidp_test

import (
	"context"
	"encoding/base64"
	"fmt"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

const testSenderARN = "arn:aws:lambda:us-east-1:000000000000:function:sender"

type prefixEncrypter struct{}

func (prefixEncrypter) EncryptCode(_ context.Context, keyID string, plaintext []byte) ([]byte, error) {
	return append([]byte(keyID+":"), plaintext...), nil
}

func TestCustomSenderDelivery(t *testing.T) {
	t.Parallel()

	const fnARN = testSenderARN

	tests := []struct {
		lambdaCfg  map[string]any
		attrs      map[string]string
		pool       map[string]any
		name       string
		wantSource string
		wantType   string
		wantErr    string
		encrypt    bool
	}{
		{
			name:       "email sender receives encrypted code",
			lambdaCfg:  senderCfg("CustomEmailSender", true),
			pool:       map[string]any{"AutoVerifiedAttributes": []string{"email"}},
			attrs:      map[string]string{"email": "bob@corp.example"},
			wantSource: "CustomEmailSender_SignUp", wantType: "customEmailSenderRequestV1", encrypt: true,
		},
		{
			name:       "sms sender receives encrypted code",
			lambdaCfg:  senderCfg("CustomSMSSender", true),
			pool:       map[string]any{"AutoVerifiedAttributes": []string{"phone_number"}},
			attrs:      map[string]string{"phone_number": "+15555550123"},
			wantSource: "CustomSMSSender_SignUp", wantType: "customSMSSenderRequestV1", encrypt: true,
		},
		{
			name:      "missing kms key fails delivery",
			lambdaCfg: senderCfg("CustomEmailSender", false),
			pool:      map[string]any{"AutoVerifiedAttributes": []string{"email"}},
			attrs:     map[string]string{"email": "bob@corp.example"},
			encrypt:   true, wantErr: "CodeDeliveryFailureException",
		},
		{
			name:      "no encrypter fails delivery",
			lambdaCfg: senderCfg("CustomEmailSender", true),
			pool:      map[string]any{"AutoVerifiedAttributes": []string{"email"}},
			attrs:     map[string]string{"email": "bob@corp.example"},
			wantErr:   "CodeDeliveryFailureException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pool := map[string]any{"LambdaConfig": tt.lambdaCfg}
			maps.Copy(pool, tt.pool)

			e := newDeliveryEnv(t, pool)
			inv := &fakeInvoker{}
			e.h.Backend.SetLambdaTriggerInvoker(inv)

			if tt.encrypt {
				e.h.Backend.SetCodeEncrypter(prefixEncrypter{})
			}

			if tt.wantErr != "" {
				rec := doCognitoRequest(t, e.h, "SignUp", signUpBody(e, tt.attrs))
				assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
				assert.Contains(t, rec.Body.String(), tt.wantErr)

				return
			}

			out := e.signUp(t, "bob", tt.attrs)
			code := out["CodeDeliveryDetails"].(map[string]any)["ConfirmationCode"].(string)

			assert.Empty(t, e.snd.emails)
			assert.Empty(t, e.snd.sms)
			require.Len(t, inv.calls, 1)

			ev := inv.calls[0].event
			assert.Equal(t, fnARN, inv.calls[0].functionARN)
			assert.Equal(t, tt.wantSource, ev["triggerSource"])

			req := ev["request"].(map[string]any)
			assert.Equal(t, tt.wantType, req["type"])

			blob, err := base64.StdEncoding.DecodeString(req["code"].(string))
			require.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("key-1:%s", code), string(blob))
		})
	}
}

func senderCfg(key string, withKMS bool) map[string]any {
	cfg := map[string]any{
		key: map[string]any{"LambdaArn": testSenderARN, "LambdaVersion": "V1_0"},
	}
	if withKMS {
		cfg["KMSKeyID"] = "key-1"
	}

	return cfg
}

func signUpBody(e *deliveryEnv, attrs map[string]string) map[string]any {
	list := make([]map[string]string, 0, len(attrs))
	for k, v := range attrs {
		list = append(list, map[string]string{"Name": k, "Value": v})
	}

	return map[string]any{
		"ClientId": e.clientID, "Username": "bob", "Password": "Pass1234!", "UserAttributes": list,
	}
}

func TestEmailRoleAccessPolicyError(t *testing.T) {
	t.Parallel()

	e := newDeliveryEnv(t, map[string]any{
		"AutoVerifiedAttributes": []string{"email"},
		"EmailConfiguration": map[string]any{
			"EmailSendingAccount": "DEVELOPER", "From": "ops@corp.example",
			"SourceArn": "arn:aws:ses:us-east-1:000000000000:identity/ops@corp.example",
		},
	})
	e.snd.err = fmt.Errorf("identity not verified: %w", cognitoidp.ErrInvalidEmailRoleAccessPolicy)

	rec := doCognitoRequest(t, e.h, "SignUp", signUpBody(e, map[string]string{"email": "bob@corp.example"}))
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "InvalidEmailRoleAccessPolicyException")
}
