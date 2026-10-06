package cognitoidp_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUserPoolClient_UpdateResetsOmittedToDefaults(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name              string
		update            func(poolID, clientID *string) *cognitoidpsdk.UpdateUserPoolClientInput
		wantCallbacks     []string
		wantRefresh       int32
		wantRevocation    bool
		wantAccessPresent bool
	}{
		{
			name: "omitted attributes reset",
			update: func(poolID, clientID *string) *cognitoidpsdk.UpdateUserPoolClientInput {
				return &cognitoidpsdk.UpdateUserPoolClientInput{UserPoolId: poolID, ClientId: clientID}
			},
			wantRefresh: 30, wantRevocation: true,
		},
		{
			name: "supplied attributes kept",
			update: func(poolID, clientID *string) *cognitoidpsdk.UpdateUserPoolClientInput {
				return &cognitoidpsdk.UpdateUserPoolClientInput{
					UserPoolId: poolID, ClientId: clientID,
					CallbackURLs: []string{"https://a"}, RefreshTokenValidity: 7,
					AccessTokenValidity: aws.Int32(5), EnableTokenRevocation: aws.Bool(false),
				}
			},
			wantCallbacks: []string{"https://a"}, wantRefresh: 7, wantAccessPresent: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestCognitoIDPClient(t, newTestHandler(t))
			ctx := t.Context()

			pool, err := c.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{PoolName: aws.String("p")})
			require.NoError(t, err)

			created, err := c.CreateUserPoolClient(ctx, &cognitoidpsdk.CreateUserPoolClientInput{
				UserPoolId: pool.UserPool.Id, ClientName: aws.String("c"),
				CallbackURLs:        []string{"https://old"},
				AccessTokenValidity: aws.Int32(3),
				ExplicitAuthFlows:   []types.ExplicitAuthFlowsType{types.ExplicitAuthFlowsTypeAllowUserPasswordAuth},
			})
			require.NoError(t, err)
			assert.Equal(t, int32(30), created.UserPoolClient.RefreshTokenValidity)
			assert.True(t, aws.ToBool(created.UserPoolClient.EnableTokenRevocation))

			_, err = c.UpdateUserPoolClient(ctx, tc.update(pool.UserPool.Id, created.UserPoolClient.ClientId))
			require.NoError(t, err)

			got, err := c.DescribeUserPoolClient(ctx, &cognitoidpsdk.DescribeUserPoolClientInput{
				UserPoolId: pool.UserPool.Id, ClientId: created.UserPoolClient.ClientId,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.wantRefresh, got.UserPoolClient.RefreshTokenValidity)
			assert.Equal(t, tc.wantRevocation, aws.ToBool(got.UserPoolClient.EnableTokenRevocation))
			assert.Equal(t, tc.wantAccessPresent, got.UserPoolClient.AccessTokenValidity != nil)
			assert.ElementsMatch(t, tc.wantCallbacks, got.UserPoolClient.CallbackURLs)
			assert.Empty(t, got.UserPoolClient.ExplicitAuthFlows)
		})
	}
}
