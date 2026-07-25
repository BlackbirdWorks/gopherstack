package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidentitysdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_CognitoIdentity_PoolLifecycle exercises the full Cognito Identity Pool lifecycle:
// CreateIdentityPool → DescribeIdentityPool → ListIdentityPools → UpdateIdentityPool →
// SetIdentityPoolRoles → GetIdentityPoolRoles → GetId → GetCredentialsForIdentity →
// GetOpenIdToken → DeleteIdentityPool.
//
// SetIdentityPoolRoles must run before GetCredentialsForIdentity: real AWS returns
// InvalidIdentityPoolConfigurationException from GetCredentialsForIdentity when the pool
// has no authenticated/unauthenticated IAM role configured (see the doc comment on
// cognitoidentity/types.InvalidIdentityPoolConfigurationException), since that call assumes
// the pool's IAM role via STS.
func TestIntegration_CognitoIdentity_PoolLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCognitoIdentityClient(t)
	ctx := t.Context()

	// CreateIdentityPool
	createOut, err := client.CreateIdentityPool(ctx, &cognitoidentitysdk.CreateIdentityPoolInput{
		IdentityPoolName:               aws.String("test-pool"),
		AllowUnauthenticatedIdentities: true,
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.IdentityPoolId)
	assert.Contains(t, *createOut.IdentityPoolId, "us-east-1:")
	assert.Equal(t, "test-pool", *createOut.IdentityPoolName)

	poolID := *createOut.IdentityPoolId

	// DescribeIdentityPool
	descOut, err := client.DescribeIdentityPool(ctx, &cognitoidentitysdk.DescribeIdentityPoolInput{
		IdentityPoolId: aws.String(poolID),
	})
	require.NoError(t, err)
	assert.Equal(t, poolID, *descOut.IdentityPoolId)
	assert.Equal(t, "test-pool", *descOut.IdentityPoolName)

	// ListIdentityPools
	listOut, err := client.ListIdentityPools(ctx, &cognitoidentitysdk.ListIdentityPoolsInput{
		MaxResults: aws.Int32(10),
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(listOut.IdentityPools), 1)

	found := false
	for _, p := range listOut.IdentityPools {
		if *p.IdentityPoolId == poolID {
			found = true

			break
		}
	}
	assert.True(t, found, "created pool should appear in list")

	// UpdateIdentityPool (keep unauth enabled so subsequent GetId succeeds)
	updateOut, err := client.UpdateIdentityPool(ctx, &cognitoidentitysdk.UpdateIdentityPoolInput{
		IdentityPoolId:                 aws.String(poolID),
		IdentityPoolName:               aws.String("test-pool"),
		AllowUnauthenticatedIdentities: true,
	})
	require.NoError(t, err)
	assert.True(t, updateOut.AllowUnauthenticatedIdentities)

	// SetIdentityPoolRoles (must precede GetCredentialsForIdentity below: real AWS
	// requires a configured role before it will hand out credentials).
	authRoleARN := "arn:aws:iam::000000000000:role/CognitoAuthRole"
	unauthRoleARN := "arn:aws:iam::000000000000:role/CognitoUnauthRole"

	_, err = client.SetIdentityPoolRoles(ctx, &cognitoidentitysdk.SetIdentityPoolRolesInput{
		IdentityPoolId: aws.String(poolID),
		Roles: map[string]string{
			"authenticated":   authRoleARN,
			"unauthenticated": unauthRoleARN,
		},
	})
	require.NoError(t, err)

	// GetIdentityPoolRoles
	rolesOut, err := client.GetIdentityPoolRoles(ctx, &cognitoidentitysdk.GetIdentityPoolRolesInput{
		IdentityPoolId: aws.String(poolID),
	})
	require.NoError(t, err)
	assert.Equal(t, authRoleARN, rolesOut.Roles["authenticated"])
	assert.Equal(t, unauthRoleARN, rolesOut.Roles["unauthenticated"])

	// GetId (unauthenticated; pool allows unauth)
	getIDOut, err := client.GetId(ctx, &cognitoidentitysdk.GetIdInput{
		AccountId:      aws.String("000000000000"),
		IdentityPoolId: aws.String(poolID),
	})
	require.NoError(t, err)
	require.NotNil(t, getIDOut.IdentityId)
	assert.Contains(t, *getIDOut.IdentityId, "us-east-1:")

	identityID := *getIDOut.IdentityId

	// GetCredentialsForIdentity
	credsOut, err := client.GetCredentialsForIdentity(ctx, &cognitoidentitysdk.GetCredentialsForIdentityInput{
		IdentityId: aws.String(identityID),
	})
	require.NoError(t, err)
	assert.Equal(t, identityID, *credsOut.IdentityId)
	require.NotNil(t, credsOut.Credentials)
	assert.NotEmpty(t, *credsOut.Credentials.AccessKeyId)
	assert.NotEmpty(t, *credsOut.Credentials.SecretKey)
	assert.NotEmpty(t, *credsOut.Credentials.SessionToken)

	// GetOpenIdToken
	tokenOut, err := client.GetOpenIdToken(ctx, &cognitoidentitysdk.GetOpenIdTokenInput{
		IdentityId: aws.String(identityID),
	})
	require.NoError(t, err)
	assert.Equal(t, identityID, *tokenOut.IdentityId)
	assert.NotEmpty(t, *tokenOut.Token)

	// DeleteIdentityPool
	_, err = client.DeleteIdentityPool(ctx, &cognitoidentitysdk.DeleteIdentityPoolInput{
		IdentityPoolId: aws.String(poolID),
	})
	require.NoError(t, err)

	// Verify deleted
	_, err = client.DescribeIdentityPool(ctx, &cognitoidentitysdk.DescribeIdentityPoolInput{
		IdentityPoolId: aws.String(poolID),
	})
	require.Error(t, err, "pool should be gone after deletion")
}

// TestIntegration_CognitoIdentity_GetId_Idempotent verifies that GetId returns the same
// identity ID when called twice with the same logins.
func TestIntegration_CognitoIdentity_GetId_Idempotent(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCognitoIdentityClient(t)
	ctx := t.Context()

	createOut, err := client.CreateIdentityPool(ctx, &cognitoidentitysdk.CreateIdentityPoolInput{
		IdentityPoolName:               aws.String("idempotent-pool"),
		AllowUnauthenticatedIdentities: true,
	})
	require.NoError(t, err)

	poolID := *createOut.IdentityPoolId
	logins := map[string]string{"cognito-idp.us-east-1.amazonaws.com/us-east-1_xxx": "sometoken"}

	id1, err := client.GetId(ctx, &cognitoidentitysdk.GetIdInput{
		AccountId:      aws.String("000000000000"),
		IdentityPoolId: aws.String(poolID),
		Logins:         logins,
	})
	require.NoError(t, err)

	id2, err := client.GetId(ctx, &cognitoidentitysdk.GetIdInput{
		AccountId:      aws.String("000000000000"),
		IdentityPoolId: aws.String(poolID),
		Logins:         logins,
	})
	require.NoError(t, err)

	assert.Equal(t, *id1.IdentityId, *id2.IdentityId, "same logins should produce same identity ID")
}

// TestIntegration_CognitoIdentity_PoolNotFound verifies that operations on a non-existent pool return errors.
func TestIntegration_CognitoIdentity_PoolNotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCognitoIdentityClient(t)
	ctx := t.Context()

	_, err := client.DescribeIdentityPool(ctx, &cognitoidentitysdk.DescribeIdentityPoolInput{
		IdentityPoolId: aws.String("us-east-1:00000000-0000-0000-0000-000000000000"),
	})
	require.Error(t, err)
}

func TestIntegration_CognitoIdentity_UnlinkOperations(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		run  func(t *testing.T, client *cognitoidentitysdk.Client, poolID string)
		name string
	}{
		{
			name: "unlink_identity",
			run: func(t *testing.T, client *cognitoidentitysdk.Client, poolID string) {
				t.Helper()

				ctx := t.Context()
				getIDOut, err := client.GetId(ctx, &cognitoidentitysdk.GetIdInput{
					AccountId:      aws.String("000000000000"),
					IdentityPoolId: aws.String(poolID),
					Logins: map[string]string{
						"accounts.google.com": "google-token",
						"graph.facebook.com":  "facebook-token",
					},
				})
				require.NoError(t, err)
				require.NotNil(t, getIDOut.IdentityId)

				_, err = client.UnlinkIdentity(ctx, &cognitoidentitysdk.UnlinkIdentityInput{
					IdentityId: getIDOut.IdentityId,
					Logins: map[string]string{
						"accounts.google.com": "google-token",
					},
					LoginsToRemove: []string{"accounts.google.com"},
				})
				require.NoError(t, err)

				desc, err := client.DescribeIdentity(ctx, &cognitoidentitysdk.DescribeIdentityInput{
					IdentityId: getIDOut.IdentityId,
				})
				require.NoError(t, err)
				assert.Equal(t, []string{"graph.facebook.com"}, desc.Logins)
			},
		},
		{
			name: "unlink_developer_identity",
			run: func(t *testing.T, client *cognitoidentitysdk.Client, poolID string) {
				t.Helper()

				ctx := t.Context()
				tokenOut, err := client.GetOpenIdTokenForDeveloperIdentity(
					ctx,
					&cognitoidentitysdk.GetOpenIdTokenForDeveloperIdentityInput{
						IdentityPoolId: aws.String(poolID),
						Logins: map[string]string{
							"developer.example.com": "dev-user",
						},
					},
				)
				require.NoError(t, err)
				require.NotNil(t, tokenOut.IdentityId)

				_, err = client.UnlinkDeveloperIdentity(ctx, &cognitoidentitysdk.UnlinkDeveloperIdentityInput{
					IdentityId:              tokenOut.IdentityId,
					IdentityPoolId:          aws.String(poolID),
					DeveloperProviderName:   aws.String("developer.example.com"),
					DeveloperUserIdentifier: aws.String("dev-user"),
				})
				require.NoError(t, err)

				lookup, err := client.LookupDeveloperIdentity(ctx, &cognitoidentitysdk.LookupDeveloperIdentityInput{
					IdentityPoolId: aws.String(poolID),
					IdentityId:     tokenOut.IdentityId,
				})
				require.NoError(t, err)
				assert.Empty(t, lookup.DeveloperUserIdentifierList)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := createCognitoIdentityClient(t)
			ctx := t.Context()

			createOut, err := client.CreateIdentityPool(ctx, &cognitoidentitysdk.CreateIdentityPoolInput{
				IdentityPoolName:               aws.String("unlink-pool-" + tt.name),
				AllowUnauthenticatedIdentities: true,
			})
			require.NoError(t, err)
			require.NotNil(t, createOut.IdentityPoolId)

			tt.run(t, client, *createOut.IdentityPoolId)
		})
	}
}
