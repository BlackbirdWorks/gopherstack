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
// GetId → GetCredentialsForIdentity → GetOpenIdToken → SetIdentityPoolRoles →
// GetIdentityPoolRoles → DeleteIdentityPool.
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

	// UpdateIdentityPool
	updateOut, err := client.UpdateIdentityPool(ctx, &cognitoidentitysdk.UpdateIdentityPoolInput{
		IdentityPoolId:                 aws.String(poolID),
		IdentityPoolName:               aws.String("test-pool"),
		AllowUnauthenticatedIdentities: false,
	})
	require.NoError(t, err)
	assert.False(t, updateOut.AllowUnauthenticatedIdentities)

	// GetId
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

	// SetIdentityPoolRoles
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
