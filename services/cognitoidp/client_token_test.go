package cognitoidp_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type clientTokenEnv struct {
	client   *cognitoidpsdk.Client
	poolID   string
	clientID string
	secret   string
	plainID  string
}

func newClientTokenEnv(t *testing.T) *clientTokenEnv {
	t.Helper()

	client := newTestCognitoIDPClient(t, newTestHandler(t))

	pool, err := client.CreateUserPool(
		t.Context(),
		&cognitoidpsdk.CreateUserPoolInput{PoolName: aws.String("m2m-pool")},
	)
	require.NoError(t, err)

	poolID := aws.ToString(pool.UserPool.Id)

	_, err = client.CreateResourceServer(t.Context(), &cognitoidpsdk.CreateResourceServerInput{
		UserPoolId: aws.String(poolID), Identifier: aws.String("api"), Name: aws.String("api"),
		Scopes: []types.ResourceServerScopeType{
			{ScopeName: aws.String("read"), ScopeDescription: aws.String("read")},
			{ScopeName: aws.String("write"), ScopeDescription: aws.String("write")},
		},
	})
	require.NoError(t, err)

	m2m, err := client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID), ClientName: aws.String("m2m"), GenerateSecret: true,
		ExplicitAuthFlows:  []types.ExplicitAuthFlowsType{"ALLOW_CLIENT_TOKEN_AUTH"},
		AllowedOAuthScopes: []string{"api/read"},
	})
	require.NoError(t, err)

	plain, err := client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID), ClientName: aws.String("plain"), GenerateSecret: true,
	})
	require.NoError(t, err)

	return &clientTokenEnv{
		client: client, poolID: poolID,
		clientID: aws.ToString(m2m.UserPoolClient.ClientId),
		secret:   aws.ToString(m2m.UserPoolClient.ClientSecret),
		plainID:  aws.ToString(plain.UserPoolClient.ClientId),
	}
}

func TestGetClientToken_Issues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantScope string
		scopes    []string
	}{
		{name: "default scopes", wantScope: "api/read"},
		{name: "explicit scope", scopes: []string{"api/read"}, wantScope: "api/read"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newClientTokenEnv(t)

			out, err := env.client.GetClientToken(t.Context(), &cognitoidpsdk.GetClientTokenInput{
				ClientId: aws.String(env.clientID), Secret: aws.String(env.secret), Scopes: tc.scopes,
			})
			require.NoError(t, err)
			require.NotNil(t, out.ClientAuthenticationResult)

			res := out.ClientAuthenticationResult
			assert.Equal(t, "Bearer", aws.ToString(res.TokenType))
			assert.Positive(t, res.ExpiresIn)

			claims := jwtClaims(t, aws.ToString(res.AccessToken))
			assert.Equal(t, env.clientID, claims["client_id"])
			assert.Equal(t, "access", claims["token_use"])
			assert.Equal(t, tc.wantScope, claims["scope"])
		})
	}
}

func TestGetClientToken_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(env *clientTokenEnv, in *cognitoidpsdk.GetClientTokenInput)
		name   string
		code   string
	}{
		{
			name: "unknown client", code: "ResourceNotFoundException",
			mutate: func(_ *clientTokenEnv, in *cognitoidpsdk.GetClientTokenInput) { in.ClientId = aws.String("nope") },
		},
		{
			name: "wrong secret", code: "NotAuthorizedException",
			mutate: func(_ *clientTokenEnv, in *cognitoidpsdk.GetClientTokenInput) { in.Secret = aws.String("bad") },
		},
		{
			name: "flow not enabled", code: "NotAuthorizedException",
			mutate: func(env *clientTokenEnv, in *cognitoidpsdk.GetClientTokenInput) {
				in.ClientId = aws.String(env.plainID)
			},
		},
		{
			name: "unknown scope", code: "InvalidParameterException",
			mutate: func(_ *clientTokenEnv, in *cognitoidpsdk.GetClientTokenInput) {
				in.Scopes = []string{"api/missing"}
			},
		},
		{
			name: "scope not allowed", code: "InvalidParameterException",
			mutate: func(_ *clientTokenEnv, in *cognitoidpsdk.GetClientTokenInput) {
				in.Scopes = []string{"api/write"}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newClientTokenEnv(t)
			in := &cognitoidpsdk.GetClientTokenInput{ClientId: aws.String(env.clientID), Secret: aws.String(env.secret)}
			tc.mutate(env, in)

			_, err := env.client.GetClientToken(t.Context(), in)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.code)
		})
	}
}
