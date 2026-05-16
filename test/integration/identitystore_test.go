package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	identitystoresdk "github.com/aws/aws-sdk-go-v2/service/identitystore"
	"github.com/aws/aws-sdk-go-v2/service/identitystore/document"
	identitystoretypes "github.com/aws/aws-sdk-go-v2/service/identitystore/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_IdentityStore_UserAndGroupLifecycle exercises the core
// Identity Store workflow via the AWS SDK v2: create a user, describe/list it,
// create a group, then delete. Primary integration coverage for the Identity
// Store AWSIdentityStore JSON-RPC handler.
func TestIntegration_IdentityStore_UserAndGroupLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createIdentityStoreClient(t)
	ctx := t.Context()

	const (
		identityStoreID = "d-1234567890"
		userName        = "it-idstore-user"
		groupName       = "it-idstore-group"
	)

	// CreateUser.
	createUserOut, err := client.CreateUser(ctx, &identitystoresdk.CreateUserInput{
		IdentityStoreId: aws.String(identityStoreID),
		UserName:        aws.String(userName),
		DisplayName:     aws.String("Integration Test User"),
	})
	require.NoError(t, err)
	userID := aws.ToString(createUserOut.UserId)
	require.NotEmpty(t, userID)

	t.Cleanup(func() {
		_, _ = client.DeleteUser(ctx, &identitystoresdk.DeleteUserInput{
			IdentityStoreId: aws.String(identityStoreID),
			UserId:          aws.String(userID),
		})
	})

	// DescribeUser.
	descUserOut, err := client.DescribeUser(ctx, &identitystoresdk.DescribeUserInput{
		IdentityStoreId: aws.String(identityStoreID),
		UserId:          aws.String(userID),
	})
	require.NoError(t, err)
	assert.Equal(t, userName, aws.ToString(descUserOut.UserName))

	// ListUsers should contain the new user.
	listUsersOut, err := client.ListUsers(ctx, &identitystoresdk.ListUsersInput{
		IdentityStoreId: aws.String(identityStoreID),
	})
	require.NoError(t, err)

	foundUser := false

	for _, u := range listUsersOut.Users {
		if aws.ToString(u.UserId) == userID {
			foundUser = true

			break
		}
	}

	assert.True(t, foundUser, "newly created user should be listed")

	// CreateGroup.
	createGroupOut, err := client.CreateGroup(ctx, &identitystoresdk.CreateGroupInput{
		IdentityStoreId: aws.String(identityStoreID),
		DisplayName:     aws.String(groupName),
		Description:     aws.String("integration test group"),
	})
	require.NoError(t, err)
	groupID := aws.ToString(createGroupOut.GroupId)
	require.NotEmpty(t, groupID)

	t.Cleanup(func() {
		_, _ = client.DeleteGroup(ctx, &identitystoresdk.DeleteGroupInput{
			IdentityStoreId: aws.String(identityStoreID),
			GroupId:         aws.String(groupID),
		})
	})

	// DescribeGroup.
	descGroupOut, err := client.DescribeGroup(ctx, &identitystoresdk.DescribeGroupInput{
		IdentityStoreId: aws.String(identityStoreID),
		GroupId:         aws.String(groupID),
	})
	require.NoError(t, err)
	assert.Equal(t, groupName, aws.ToString(descGroupOut.DisplayName))

	// GetUserId should resolve by UserName.
	getIDOut, err := client.GetUserId(ctx, &identitystoresdk.GetUserIdInput{
		IdentityStoreId: aws.String(identityStoreID),
		AlternateIdentifier: &identitystoretypes.AlternateIdentifierMemberUniqueAttribute{
			Value: identitystoretypes.UniqueAttribute{
				AttributePath:  aws.String("UserName"),
				AttributeValue: document.NewLazyDocument(userName),
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, userID, aws.ToString(getIDOut.UserId))
}
