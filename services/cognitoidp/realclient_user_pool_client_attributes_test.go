package cognitoidp_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

// TestRealClient_UserPoolClientAttributeFields drives CreateUserPoolClient/
// UpdateUserPoolClient's DefaultRedirectURI/ReadAttributes/WriteAttributes,
// and CreateUserPool's Schema, through the real aws-sdk-go-v2 client
// (gopherstack-xhu2t) -- none of the four was decoded from the request at
// all before this pass.
func TestRealClient_UserPoolClientAttributeFields(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "create_user_pool_client_attribute_fields", run: func(t *testing.T) {
			t.Helper()

			backend := cognitoidp.NewInMemoryBackend(
				"123456789012",
				"us-east-1",
				"http://localhost:8000",
			)
			client := newTestCognitoIDPClient(t, cognitoidp.NewHandler(backend, "us-east-1"))
			ctx := t.Context()

			pool, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{
				PoolName: aws.String("attr-pool"),
			})
			require.NoError(t, err)
			poolID := aws.ToString(pool.UserPool.Id)

			created, err := client.CreateUserPoolClient(
				ctx,
				&cognitoidpsdk.CreateUserPoolClientInput{
					UserPoolId:         aws.String(poolID),
					ClientName:         aws.String("attr-client"),
					DefaultRedirectURI: aws.String("https://example.com/callback"),
					CallbackURLs:       []string{"https://example.com/callback"},
					ReadAttributes:     []string{"email", "custom:tier"},
					WriteAttributes:    []string{"email"},
					AllowedOAuthFlows:  []types.OAuthFlowType{types.OAuthFlowTypeCode},
					AllowedOAuthScopes: []string{"openid"},
				},
			)
			require.NoError(t, err)
			clientID := aws.ToString(created.UserPoolClient.ClientId)

			assert.Equal(
				t,
				"https://example.com/callback",
				aws.ToString(created.UserPoolClient.DefaultRedirectURI),
			)
			assert.ElementsMatch(
				t,
				[]string{"email", "custom:tier"},
				created.UserPoolClient.ReadAttributes,
			)
			assert.ElementsMatch(t, []string{"email"}, created.UserPoolClient.WriteAttributes)

			described, err := client.DescribeUserPoolClient(
				ctx,
				&cognitoidpsdk.DescribeUserPoolClientInput{
					UserPoolId: aws.String(poolID),
					ClientId:   aws.String(clientID),
				},
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				"https://example.com/callback",
				aws.ToString(described.UserPoolClient.DefaultRedirectURI),
			)
			assert.ElementsMatch(
				t,
				[]string{"email", "custom:tier"},
				described.UserPoolClient.ReadAttributes,
			)
			assert.ElementsMatch(t, []string{"email"}, described.UserPoolClient.WriteAttributes)
		}},
		{name: "update_user_pool_client_attribute_fields", run: func(t *testing.T) {
			t.Helper()

			backend := cognitoidp.NewInMemoryBackend(
				"123456789012",
				"us-east-1",
				"http://localhost:8000",
			)
			client := newTestCognitoIDPClient(t, cognitoidp.NewHandler(backend, "us-east-1"))
			ctx := t.Context()

			pool, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{
				PoolName: aws.String("attr-update-pool"),
			})
			require.NoError(t, err)
			poolID := aws.ToString(pool.UserPool.Id)

			created, err := client.CreateUserPoolClient(
				ctx,
				&cognitoidpsdk.CreateUserPoolClientInput{
					UserPoolId: aws.String(poolID),
					ClientName: aws.String("attr-update-client"),
				},
			)
			require.NoError(t, err)
			clientID := aws.ToString(created.UserPoolClient.ClientId)

			updated, err := client.UpdateUserPoolClient(
				ctx,
				&cognitoidpsdk.UpdateUserPoolClientInput{
					UserPoolId:         aws.String(poolID),
					ClientId:           aws.String(clientID),
					DefaultRedirectURI: aws.String("https://example.com/updated"),
					CallbackURLs:       []string{"https://example.com/updated"},
					ReadAttributes:     []string{"phone_number"},
					WriteAttributes:    []string{"phone_number", "custom:tier"},
				},
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				"https://example.com/updated",
				aws.ToString(updated.UserPoolClient.DefaultRedirectURI),
			)
			assert.ElementsMatch(t, []string{"phone_number"}, updated.UserPoolClient.ReadAttributes)
			assert.ElementsMatch(
				t,
				[]string{"phone_number", "custom:tier"},
				updated.UserPoolClient.WriteAttributes,
			)
		}},
		{name: "create_user_pool_schema_custom_attributes", run: func(t *testing.T) {
			t.Helper()

			backend := cognitoidp.NewInMemoryBackend(
				"123456789012",
				"us-east-1",
				"http://localhost:8000",
			)
			client := newTestCognitoIDPClient(t, cognitoidp.NewHandler(backend, "us-east-1"))
			ctx := t.Context()

			pool, err := client.CreateUserPool(ctx, &cognitoidpsdk.CreateUserPoolInput{
				PoolName: aws.String("schema-pool"),
				Schema: []types.SchemaAttributeType{
					{Name: aws.String("email"), AttributeDataType: types.AttributeDataTypeString},
					{
						Name:              aws.String("custom:tier"),
						AttributeDataType: types.AttributeDataTypeString,
						Mutable:           aws.Bool(true),
					},
				},
			})
			require.NoError(t, err)
			poolID := aws.ToString(pool.UserPool.Id)

			described, err := client.DescribeUserPool(ctx, &cognitoidpsdk.DescribeUserPoolInput{
				UserPoolId: aws.String(poolID),
			})
			require.NoError(t, err)

			var found bool
			for _, s := range described.UserPool.SchemaAttributes {
				if aws.ToString(s.Name) == "custom:tier" {
					found = true
				}
				assert.NotEqual(
					t,
					"email",
					aws.ToString(s.Name),
					"unprefixed standard attributes aren't modeled by this backend",
				)
			}
			assert.True(
				t,
				found,
				"custom:-prefixed schema entries from CreateUserPool must be stored",
			)
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
