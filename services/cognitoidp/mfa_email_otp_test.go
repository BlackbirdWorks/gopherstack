package cognitoidp_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An EMAIL_OTP challenge raised by pool MFA (not a USER_AUTH first factor) answers with EMAIL_OTP_CODE.
func TestRespondToAuthChallenge_EmailOTPAsMFA(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		code     func(valid string) string
		wantCode string
	}{
		{name: "right code", code: func(valid string) string { return valid }},
		{name: "wrong code", code: func(string) string { return "000000" }, wantCode: "CodeMismatchException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newPasskeyEnv(t, passkeyOpts{})

			_, err := env.client.SetUserPoolMfaConfig(t.Context(), &cognitoidpsdk.SetUserPoolMfaConfigInput{
				UserPoolId: aws.String(env.poolID), MfaConfiguration: types.UserPoolMfaTypeOn,
				EmailMfaConfiguration: &types.EmailMfaConfigType{},
			})
			require.NoError(t, err)

			require.NoError(t, env.h.Backend.AdminSetUserMFAPreference(
				env.poolID, passkeyUsername, false, false, "EMAIL_OTP", "EMAIL_OTP"))

			out, err := env.client.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
				ClientId: aws.String(env.clientID), AuthFlow: types.AuthFlowTypeUserPasswordAuth,
				AuthParameters: map[string]string{"USERNAME": passkeyUsername, "PASSWORD": passkeyPassword},
			})
			require.NoError(t, err)
			require.Equal(t, types.ChallengeNameTypeEmailOtp, out.ChallengeName)

			code := env.h.Backend.GetMFASessionCodeForTest(aws.ToString(out.Session))
			require.Len(t, code, 6)

			done, err := env.client.RespondToAuthChallenge(t.Context(), &cognitoidpsdk.RespondToAuthChallengeInput{
				ClientId: aws.String(
					env.clientID,
				), ChallengeName: types.ChallengeNameTypeEmailOtp, Session: out.Session,
				ChallengeResponses: map[string]string{"USERNAME": passkeyUsername, "EMAIL_OTP_CODE": tt.code(code)},
			})
			if tt.wantCode != "" {
				requireAPIErrorCode(t, err, tt.wantCode)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, done.AuthenticationResult)
			assert.NotEmpty(t, aws.ToString(done.AuthenticationResult.AccessToken))
		})
	}
}
