package cognitoidp_test

// user_auth_test.go exercises USER_AUTH choice-based authentication (gopherstack-5f20,
// see user_auth.go's design comment) through the real aws-sdk-go-v2 client, driving the
// full SELECT_CHALLENGE negotiation and both live first factors (PASSWORD, EMAIL_OTP).

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	userAuthTestUsername = "user-auth-tester"
	userAuthTestPassword = "Passw0rd1!"
)

// newUserAuthTestPool creates a pool with SignInPolicy AllowedFirstAuthFactors
// [PASSWORD, EMAIL_OTP], a client with ALLOW_USER_AUTH, and a confirmed user with a
// permanent password and a verified email.
func newUserAuthTestPool(t *testing.T, client *cognitoidpsdk.Client) (string, string) {
	t.Helper()

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("user-auth-pool"),
		Policies: &types.UserPoolPolicyType{
			SignInPolicy: &types.SignInPolicyType{
				AllowedFirstAuthFactors: []types.AuthFactorType{
					types.AuthFactorTypePassword, types.AuthFactorTypeEmailOtp,
				},
			},
		},
	})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	cl, err := client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId:        aws.String(poolID),
		ClientName:        aws.String("user-auth-client"),
		ExplicitAuthFlows: []types.ExplicitAuthFlowsType{types.ExplicitAuthFlowsTypeAllowUserAuth},
	})
	require.NoError(t, err)
	clientID := aws.ToString(cl.UserPoolClient.ClientId)

	_, err = client.AdminCreateUser(t.Context(), &cognitoidpsdk.AdminCreateUserInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String(userAuthTestUsername),
		UserAttributes: []types.AttributeType{
			{Name: aws.String("email"), Value: aws.String("user-auth-tester@example.com")},
			{Name: aws.String("email_verified"), Value: aws.String("true")},
		},
	})
	require.NoError(t, err)

	_, err = client.AdminSetUserPassword(t.Context(), &cognitoidpsdk.AdminSetUserPasswordInput{
		UserPoolId: aws.String(poolID),
		Username:   aws.String(userAuthTestUsername),
		Password:   aws.String(userAuthTestPassword),
		Permanent:  true,
	})
	require.NoError(t, err)

	return poolID, clientID
}

func TestInitiateAuth_UserAuth_SelectChallengeOffersConfiguredFactors(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)
	_, clientID := newUserAuthTestPool(t, client)

	out, err := client.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
		ClientId:       aws.String(clientID),
		AuthFlow:       types.AuthFlowTypeUserAuth,
		AuthParameters: map[string]string{"USERNAME": userAuthTestUsername},
	})
	require.NoError(t, err)
	assert.Equal(t, types.ChallengeNameTypeSelectChallenge, out.ChallengeName)
	assert.NotEmpty(t, aws.ToString(out.Session))
	require.Len(t, out.AvailableChallenges, 2)

	got := make([]string, len(out.AvailableChallenges))
	for i, c := range out.AvailableChallenges {
		got[i] = string(c)
	}

	assert.Equal(t, []string{"PASSWORD", "EMAIL_OTP"}, got, "must preserve the pool's configured order")
}

func TestRespondToAuthChallenge_UserAuth_SelectChallengeToPasswordFlow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)
	_, clientID := newUserAuthTestPool(t, client)

	initOut, err := client.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
		ClientId:       aws.String(clientID),
		AuthFlow:       types.AuthFlowTypeUserAuth,
		AuthParameters: map[string]string{"USERNAME": userAuthTestUsername},
	})
	require.NoError(t, err)
	require.Equal(t, types.ChallengeNameTypeSelectChallenge, initOut.ChallengeName)

	selectOut, err := client.RespondToAuthChallenge(t.Context(), &cognitoidpsdk.RespondToAuthChallengeInput{
		ClientId:      aws.String(clientID),
		ChallengeName: types.ChallengeNameTypeSelectChallenge,
		Session:       initOut.Session,
		ChallengeResponses: map[string]string{
			"USERNAME": userAuthTestUsername,
			"ANSWER":   "PASSWORD",
		},
	})
	require.NoError(t, err)
	require.Equal(t, types.ChallengeNameTypePassword, selectOut.ChallengeName)
	require.NotNil(t, selectOut.Session)

	tokensOut, err := client.RespondToAuthChallenge(t.Context(), &cognitoidpsdk.RespondToAuthChallengeInput{
		ClientId:      aws.String(clientID),
		ChallengeName: types.ChallengeNameTypePassword,
		Session:       selectOut.Session,
		ChallengeResponses: map[string]string{
			"USERNAME": userAuthTestUsername,
			"PASSWORD": userAuthTestPassword,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, tokensOut.AuthenticationResult)
	assert.NotEmpty(t, aws.ToString(tokensOut.AuthenticationResult.AccessToken))
}

func TestRespondToAuthChallenge_UserAuth_SelectChallengeToEmailOTPFlow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)
	_, clientID := newUserAuthTestPool(t, client)

	initOut, err := client.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
		ClientId:       aws.String(clientID),
		AuthFlow:       types.AuthFlowTypeUserAuth,
		AuthParameters: map[string]string{"USERNAME": userAuthTestUsername},
	})
	require.NoError(t, err)

	selectOut, err := client.RespondToAuthChallenge(t.Context(), &cognitoidpsdk.RespondToAuthChallengeInput{
		ClientId:      aws.String(clientID),
		ChallengeName: types.ChallengeNameTypeSelectChallenge,
		Session:       initOut.Session,
		ChallengeResponses: map[string]string{
			"USERNAME": userAuthTestUsername,
			"ANSWER":   "EMAIL_OTP",
		},
	})
	require.NoError(t, err)
	require.Equal(t, types.ChallengeNameTypeEmailOtp, selectOut.ChallengeName)
	require.NotNil(t, selectOut.Session)

	code := h.Backend.GetMFASessionCodeForTest(aws.ToString(selectOut.Session))
	require.Len(t, code, 6, "sanity: newFirstFactorChallengeSession must generate a 6-digit code")

	tokensOut, err := client.RespondToAuthChallenge(t.Context(), &cognitoidpsdk.RespondToAuthChallengeInput{
		ClientId:      aws.String(clientID),
		ChallengeName: types.ChallengeNameTypeEmailOtp,
		Session:       selectOut.Session,
		ChallengeResponses: map[string]string{
			"USERNAME":       userAuthTestUsername,
			"EMAIL_OTP_CODE": code,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, tokensOut.AuthenticationResult)
	assert.NotEmpty(t, aws.ToString(tokensOut.AuthenticationResult.AccessToken))
}

func TestInitiateAuth_UserAuth_PreferredChallengeSkipsSelection(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)
	_, clientID := newUserAuthTestPool(t, client)

	initOut, err := client.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
		ClientId: aws.String(clientID),
		AuthFlow: types.AuthFlowTypeUserAuth,
		AuthParameters: map[string]string{
			"USERNAME":            userAuthTestUsername,
			"PREFERRED_CHALLENGE": "PASSWORD",
		},
	})
	require.NoError(t, err)
	require.Equal(t, types.ChallengeNameTypePassword, initOut.ChallengeName, "must skip SELECT_CHALLENGE entirely")
	require.NotNil(t, initOut.Session)

	tokensOut, err := client.RespondToAuthChallenge(t.Context(), &cognitoidpsdk.RespondToAuthChallengeInput{
		ClientId:      aws.String(clientID),
		ChallengeName: types.ChallengeNameTypePassword,
		Session:       initOut.Session,
		ChallengeResponses: map[string]string{
			"USERNAME": userAuthTestUsername,
			"PASSWORD": userAuthTestPassword,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, tokensOut.AuthenticationResult)
	assert.NotEmpty(t, aws.ToString(tokensOut.AuthenticationResult.AccessToken))
}

func TestInitiateAuth_UserAuth_RejectedConfigurations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		buildClientID func(t *testing.T, client *cognitoidpsdk.Client) string
		name          string
		wantCode      string
	}{
		{
			name: "client_without_allow_user_auth",
			buildClientID: func(t *testing.T, client *cognitoidpsdk.Client) string {
				t.Helper()

				poolID, _ := newUserAuthTestPool(t, client)

				cl, err := client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
					UserPoolId:        aws.String(poolID),
					ClientName:        aws.String("no-user-auth-client"),
					ExplicitAuthFlows: []types.ExplicitAuthFlowsType{types.ExplicitAuthFlowsTypeAllowUserPasswordAuth},
				})
				require.NoError(t, err)

				return aws.ToString(cl.UserPoolClient.ClientId)
			},
			wantCode: "InvalidUserPoolConfigurationException",
		},
		{
			name: "pool_without_sign_in_policy",
			buildClientID: func(t *testing.T, client *cognitoidpsdk.Client) string {
				t.Helper()

				pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
					PoolName: aws.String("no-sign-in-policy-pool"),
				})
				require.NoError(t, err)
				poolID := aws.ToString(pool.UserPool.Id)

				cl, err := client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
					UserPoolId:        aws.String(poolID),
					ClientName:        aws.String("no-policy-client"),
					ExplicitAuthFlows: []types.ExplicitAuthFlowsType{types.ExplicitAuthFlowsTypeAllowUserAuth},
				})
				require.NoError(t, err)

				_, err = client.AdminCreateUser(t.Context(), &cognitoidpsdk.AdminCreateUserInput{
					UserPoolId: aws.String(poolID),
					Username:   aws.String(userAuthTestUsername),
				})
				require.NoError(t, err)

				_, err = client.AdminSetUserPassword(t.Context(), &cognitoidpsdk.AdminSetUserPasswordInput{
					UserPoolId: aws.String(poolID),
					Username:   aws.String(userAuthTestUsername),
					Password:   aws.String(userAuthTestPassword),
					Permanent:  true,
				})
				require.NoError(t, err)

				return aws.ToString(cl.UserPoolClient.ClientId)
			},
			wantCode: "InvalidUserPoolConfigurationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			client := newTestCognitoIDPClient(t, h)
			clientID := tt.buildClientID(t, client)

			_, err := client.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
				ClientId:       aws.String(clientID),
				AuthFlow:       types.AuthFlowTypeUserAuth,
				AuthParameters: map[string]string{"USERNAME": userAuthTestUsername},
			})
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
		})
	}
}

func TestRespondToAuthChallenge_UserAuth_AnswerNotOffered(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)
	_, clientID := newUserAuthTestPool(t, client)

	initOut, err := client.InitiateAuth(t.Context(), &cognitoidpsdk.InitiateAuthInput{
		ClientId:       aws.String(clientID),
		AuthFlow:       types.AuthFlowTypeUserAuth,
		AuthParameters: map[string]string{"USERNAME": userAuthTestUsername},
	})
	require.NoError(t, err)

	_, err = client.RespondToAuthChallenge(t.Context(), &cognitoidpsdk.RespondToAuthChallengeInput{
		ClientId:      aws.String(clientID),
		ChallengeName: types.ChallengeNameTypeSelectChallenge,
		Session:       initOut.Session,
		ChallengeResponses: map[string]string{
			"USERNAME": userAuthTestUsername,
			"ANSWER":   "SMS_OTP",
		},
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InvalidParameterException", apiErr.ErrorCode())
}
