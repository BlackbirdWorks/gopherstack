package cognitoidp_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClientSecretRotation_RoundTrip drives Add/List/DeleteUserPoolClientSecret
// through a real SDK client and proves ClientSecretId is genuinely required
// and honored, instead of the pre-fix behavior where DeleteUserPoolClientSecret
// ignored it entirely -- the model held a single ClientSecret string rather
// than a keyed set, so rotation with two concurrent secrets could not be
// represented at all (gopherstack-h910).
func TestClientSecretRotation_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	pool, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName: aws.String("rotation-pool"),
	})
	require.NoError(t, err)
	poolID := aws.ToString(pool.UserPool.Id)

	appClient, err := client.CreateUserPoolClient(t.Context(), &cognitoidpsdk.CreateUserPoolClientInput{
		UserPoolId: aws.String(poolID),
		ClientName: aws.String("rotation-client"),
	})
	require.NoError(t, err)
	clientID := aws.ToString(appClient.UserPoolClient.ClientId)

	first, err := client.AddUserPoolClientSecret(t.Context(), &cognitoidpsdk.AddUserPoolClientSecretInput{
		UserPoolId: aws.String(poolID),
		ClientId:   aws.String(clientID),
	})
	require.NoError(t, err)
	firstID := aws.ToString(first.ClientSecretDescriptor.ClientSecretId)
	require.NotEmpty(t, firstID)
	require.NotEmpty(t, aws.ToString(first.ClientSecretDescriptor.ClientSecretValue))

	listed, err := client.ListUserPoolClientSecrets(t.Context(), &cognitoidpsdk.ListUserPoolClientSecretsInput{
		UserPoolId: aws.String(poolID),
		ClientId:   aws.String(clientID),
	})
	require.NoError(t, err)
	require.Len(t, listed.ClientSecrets, 1)
	assert.Equal(t, firstID, aws.ToString(listed.ClientSecrets[0].ClientSecretId))
	assert.Empty(
		t, aws.ToString(listed.ClientSecrets[0].ClientSecretValue),
		"ListUserPoolClientSecrets must never reveal the secret value",
	)

	// Deleting with the wrong ClientSecretId must not remove the real secret
	// (must happen before the correct-id delete below, so both steps run
	// sequentially against the same client rather than as parallel subtests).
	_, deleteWrongIDErr := client.DeleteUserPoolClientSecret(
		t.Context(),
		&cognitoidpsdk.DeleteUserPoolClientSecretInput{
			UserPoolId:     aws.String(poolID),
			ClientId:       aws.String(clientID),
			ClientSecretId: aws.String("not-the-real-id"),
		},
	)
	require.Error(t, deleteWrongIDErr)
	assert.Contains(t, deleteWrongIDErr.Error(), "ResourceNotFoundException")

	stillListed, err := client.ListUserPoolClientSecrets(t.Context(), &cognitoidpsdk.ListUserPoolClientSecretsInput{
		UserPoolId: aws.String(poolID),
		ClientId:   aws.String(clientID),
	})
	require.NoError(t, err)
	require.Len(t, stillListed.ClientSecrets, 1)

	_, deleteErr := client.DeleteUserPoolClientSecret(t.Context(), &cognitoidpsdk.DeleteUserPoolClientSecretInput{
		UserPoolId:     aws.String(poolID),
		ClientId:       aws.String(clientID),
		ClientSecretId: aws.String(firstID),
	})
	require.NoError(t, deleteErr)

	afterDelete, err := client.ListUserPoolClientSecrets(t.Context(), &cognitoidpsdk.ListUserPoolClientSecretsInput{
		UserPoolId: aws.String(poolID),
		ClientId:   aws.String(clientID),
	})
	require.NoError(t, err)
	assert.Empty(t, afterDelete.ClientSecrets)
}

// TestAddUserPoolClientSecret_ClientRequiresClientSecretId proves the real
// SDK client refuses to send DeleteUserPoolClientSecret without
// ClientSecretId, confirming it is genuinely a required member on the pinned
// SDK, not an assumption (gopherstack-h910).
func TestDeleteUserPoolClientSecret_ClientRequiresClientSecretId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestCognitoIDPClient(t, h)

	_, err := client.DeleteUserPoolClientSecret(t.Context(), &cognitoidpsdk.DeleteUserPoolClientSecretInput{
		UserPoolId: aws.String("pool-1"),
		ClientId:   aws.String("client-1"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ClientSecretId")
}
