package cognitoidp_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMFAGateByPoolConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		mfaConfig     types.UserPoolMfaType
		wantChallenge types.ChallengeNameType
		enableTOTP    bool
	}{
		{name: "off", mfaConfig: types.UserPoolMfaTypeOff},
		{name: "optional without factor", mfaConfig: types.UserPoolMfaTypeOptional},
		{
			name: "optional with factor", mfaConfig: types.UserPoolMfaTypeOptional,
			enableTOTP: true, wantChallenge: types.ChallengeNameTypeSoftwareTokenMfa,
		},
		{name: "on without factor", mfaConfig: types.UserPoolMfaTypeOn, wantChallenge: types.ChallengeNameTypeMfaSetup},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			client := newTestCognitoIDPClient(t, h)

			pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
				PoolName:         aws.String("mfa-gate-pool"),
				MfaConfiguration: tt.mfaConfig,
			})
			require.NoError(t, err)

			poolID := aws.ToString(pool.UserPool.Id)
			cl, err := client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
				UserPoolId: aws.String(poolID),
				ClientName: aws.String("c"),
			})
			require.NoError(t, err)

			_, err = client.AdminCreateUser(t.Context(), &cognitoidpsdk.AdminCreateUserInput{
				UserPoolId: aws.String(poolID), Username: aws.String("u"),
				MessageAction: types.MessageActionTypeSuppress,
			})
			require.NoError(t, err)

			_, err = client.AdminSetUserPassword(t.Context(), &cognitoidpsdk.AdminSetUserPasswordInput{
				UserPoolId: aws.String(poolID), Username: aws.String("u"),
				Password: aws.String("Pass1234!"), Permanent: true,
			})
			require.NoError(t, err)

			if tt.enableTOTP {
				_, err = client.AdminSetUserMFAPreference(t.Context(), &cognitoidpsdk.AdminSetUserMFAPreferenceInput{
					UserPoolId: aws.String(poolID), Username: aws.String("u"),
					SoftwareTokenMfaSettings: &types.SoftwareTokenMfaSettingsType{Enabled: true, PreferredMfa: true},
				})
				require.NoError(t, err)
			}

			out, err := client.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
				ClientId: cl.UserPoolClient.ClientId,
				AuthFlow: types.AuthFlowTypeUserPasswordAuth,
				AuthParameters: map[string]string{
					"USERNAME": "u", "PASSWORD": "Pass1234!",
				},
			})
			require.NoError(t, err)

			if tt.wantChallenge == "" {
				require.NotNil(t, out.AuthenticationResult)
				assert.NotEmpty(t, aws.ToString(out.AuthenticationResult.AccessToken))

				return
			}

			assert.Equal(t, tt.wantChallenge, out.ChallengeName)
		})
	}
}
