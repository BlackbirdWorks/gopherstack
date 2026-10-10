package cognitoidp_test

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func signedInClient(
	t *testing.T, rotation *types.RefreshTokenRotationType,
) (*cognitoidpsdk.Client, string, *types.AuthenticationResultType) {
	t.Helper()

	client := newTestCognitoIDPClient(t, newTestHandler(t))
	ctx := t.Context()

	pool, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{PoolName: aws.String("rot")})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	app, err := client.CreateUserPoolClient(ctx, &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId:           aws.String(poolID),
		ClientName:           aws.String("rot-client"),
		RefreshTokenRotation: rotation,
	})
	require.NoError(t, err)
	clientID := aws.ToString(app.UserPoolClient.ClientId)

	_, err = client.AdminCreateUser(ctx, &cognitoidpsdk.AdminCreateUserInput{
		UserPoolId: aws.String(poolID), Username: aws.String("rot-user"),
	})
	require.NoError(t, err)

	_, err = client.AdminSetUserPassword(ctx, &cognitoidpsdk.AdminSetUserPasswordInput{
		UserPoolId: aws.String(poolID), Username: aws.String("rot-user"),
		Password: aws.String("Pass1234!"), Permanent: true,
	})
	require.NoError(t, err)

	auth, err := client.InitiateAuth(ctx, &cognitoidpsdk.InitiateAuthInput{
		AuthFlow: types.AuthFlowTypeUserPasswordAuth,
		ClientId: aws.String(clientID),
		AuthParameters: map[string]string{
			"USERNAME": "rot-user", "PASSWORD": "Pass1234!",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, auth.AuthenticationResult)

	return client, clientID, auth.AuthenticationResult
}

func TestGetTokensFromRefreshToken_RetryGracePeriod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		grace     *int32
		name      string
		wantReuse bool
	}{
		{name: "grace disabled", grace: aws.Int32(0), wantReuse: true},
		{name: "unset", grace: nil, wantReuse: true},
		{name: "within grace", grace: aws.Int32(30)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, clientID, first := signedInClient(t, &types.RefreshTokenRotationType{
				Feature: types.FeatureTypeEnabled, RetryGracePeriodSeconds: tt.grace,
			})
			ctx := t.Context()

			rotated, err := client.GetTokensFromRefreshToken(ctx, &cognitoidpsdk.GetTokensFromRefreshTokenInput{
				ClientId: aws.String(clientID), RefreshToken: first.RefreshToken,
			})
			require.NoError(t, err)
			require.NotEmpty(t, aws.ToString(rotated.AuthenticationResult.RefreshToken))

			retry, err := client.GetTokensFromRefreshToken(ctx, &cognitoidpsdk.GetTokensFromRefreshTokenInput{
				ClientId: aws.String(clientID), RefreshToken: first.RefreshToken,
			})

			if tt.wantReuse {
				require.Error(t, err)

				if tt.grace != nil {
					var reuse *types.RefreshTokenReuseException
					require.ErrorAs(t, err, &reuse)
				}

				return
			}

			require.NoError(t, err, "a retry inside the grace period must succeed")
			assert.NotEmpty(t, aws.ToString(retry.AuthenticationResult.AccessToken))

			again, err := client.GetTokensFromRefreshToken(ctx, &cognitoidpsdk.GetTokensFromRefreshTokenInput{
				ClientId: aws.String(clientID), RefreshToken: rotated.AuthenticationResult.RefreshToken,
			})
			require.NoError(t, err)
			assert.NotEmpty(t, aws.ToString(again.AuthenticationResult.AccessToken))
		})
	}
}

func TestGetTokensFromRefreshToken_ReuseAfterGraceExpiry(t *testing.T) {
	t.Parallel()

	client, clientID, first := signedInClient(t, &types.RefreshTokenRotationType{
		Feature: types.FeatureTypeEnabled, RetryGracePeriodSeconds: aws.Int32(1),
	})
	ctx := t.Context()

	_, err := client.GetTokensFromRefreshToken(ctx, &cognitoidpsdk.GetTokensFromRefreshTokenInput{
		ClientId: aws.String(clientID), RefreshToken: first.RefreshToken,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, retryErr := client.GetTokensFromRefreshToken(ctx, &cognitoidpsdk.GetTokensFromRefreshTokenInput{
			ClientId: aws.String(clientID), RefreshToken: first.RefreshToken,
		})
		var reuse *types.RefreshTokenReuseException

		return retryErr != nil && asReuse(retryErr, &reuse)
	}, 5*time.Second, 100*time.Millisecond)
}

func asReuse(err error, target **types.RefreshTokenReuseException) bool {
	return errors.As(err, target)
}

func TestRevokeToken_InvalidatesAccessTokens(t *testing.T) {
	t.Parallel()

	client, clientID, auth := signedInClient(t, nil)
	ctx := t.Context()

	_, err := client.GetUser(ctx, &cognitoidpsdk.GetUserInput{AccessToken: auth.AccessToken})
	require.NoError(t, err)

	refreshed, err := client.InitiateAuth(ctx, &cognitoidpsdk.InitiateAuthInput{
		AuthFlow:       types.AuthFlowTypeRefreshTokenAuth,
		ClientId:       aws.String(clientID),
		AuthParameters: map[string]string{"REFRESH_TOKEN": aws.ToString(auth.RefreshToken)},
	})
	require.NoError(t, err)

	_, err = client.RevokeToken(ctx, &cognitoidpsdk.RevokeTokenInput{
		ClientId: aws.String(clientID), Token: refreshed.AuthenticationResult.RefreshToken,
	})
	require.NoError(t, err)

	for name, token := range map[string]*string{
		"original access token":  auth.AccessToken,
		"refreshed access token": refreshed.AuthenticationResult.AccessToken,
	} {
		_, getErr := client.GetUser(ctx, &cognitoidpsdk.GetUserInput{AccessToken: token})
		require.Error(t, getErr, name)

		var notAuth *types.NotAuthorizedException
		require.ErrorAs(t, getErr, &notAuth, name)
	}
}
