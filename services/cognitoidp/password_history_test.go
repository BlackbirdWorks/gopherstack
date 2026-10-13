package cognitoidp_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPasswordHistory_ChangePassword(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		propose       string
		size          int32
		wantViolation bool
	}{
		{name: "disabled allows old", size: 0, propose: "Pass1word!"},
		{name: "size1 rejects current", size: 1, propose: "Pass3word!", wantViolation: true},
		{name: "size1 allows previous", size: 1, propose: "Pass2word!"},
		{name: "size2 rejects previous", size: 2, propose: "Pass2word!", wantViolation: true},
		{name: "size2 allows older", size: 2, propose: "Pass1word!"},
		{name: "size3 rejects oldest", size: 3, propose: "Pass1word!", wantViolation: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestCognitoIDPClient(t, newTestHandler(t))
			ctx := t.Context()

			pool, err := c.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{
				PoolName: aws.String("hist"),
				Policies: &types.UserPoolPolicyType{PasswordPolicy: &types.PasswordPolicyType{
					MinimumLength: aws.Int32(8), PasswordHistorySize: aws.Int32(tc.size),
				}},
			})
			require.NoError(t, err)
			poolID := pool.UserPool.Id
			assert.Equal(t, tc.size, aws.ToInt32(pool.UserPool.Policies.PasswordPolicy.PasswordHistorySize))

			cl, err := c.CreateUserPoolClient(ctx, &cognitoidpsdk.CreateUserPoolClientInput{
				UserPoolId: poolID, ClientName: aws.String("c"),
				ExplicitAuthFlows: []types.ExplicitAuthFlowsType{types.ExplicitAuthFlowsTypeAllowUserPasswordAuth},
			})
			require.NoError(t, err)

			_, err = c.AdminCreateUser(ctx, &cognitoidpsdk.AdminCreateUserInput{
				UserPoolId: poolID, Username: aws.String("u"),
			})
			require.NoError(t, err)

			_, err = c.AdminSetUserPassword(ctx, &cognitoidpsdk.AdminSetUserPasswordInput{
				UserPoolId: poolID, Username: aws.String("u"), Password: aws.String("Pass1word!"), Permanent: true,
			})
			require.NoError(t, err)

			current := "Pass1word!"
			for _, next := range []string{"Pass2word!", "Pass3word!"} {
				auth, aerr := c.InitiateAuth(ctx, &cognitoidpsdk.InitiateAuthInput{
					ClientId: cl.UserPoolClient.ClientId, AuthFlow: types.AuthFlowTypeUserPasswordAuth,
					AuthParameters: map[string]string{"USERNAME": "u", "PASSWORD": current},
				})
				require.NoError(t, aerr)

				_, err = c.ChangePassword(ctx, &cognitoidpsdk.ChangePasswordInput{
					AccessToken:      auth.AuthenticationResult.AccessToken,
					PreviousPassword: aws.String(current), ProposedPassword: aws.String(next),
				})
				require.NoError(t, err)

				current = next
			}

			auth, err := c.InitiateAuth(ctx, &cognitoidpsdk.InitiateAuthInput{
				ClientId: cl.UserPoolClient.ClientId, AuthFlow: types.AuthFlowTypeUserPasswordAuth,
				AuthParameters: map[string]string{"USERNAME": "u", "PASSWORD": current},
			})
			require.NoError(t, err)

			_, err = c.ChangePassword(ctx, &cognitoidpsdk.ChangePasswordInput{
				AccessToken:      auth.AuthenticationResult.AccessToken,
				PreviousPassword: aws.String(current), ProposedPassword: aws.String(tc.propose),
			})

			if !tc.wantViolation {
				require.NoError(t, err)

				return
			}

			var violation *types.PasswordHistoryPolicyViolationException

			require.ErrorAs(t, err, &violation)
		})
	}
}
