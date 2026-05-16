package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	transfersdk "github.com/aws/aws-sdk-go-v2/service/transfer"
	transfertypes "github.com/aws/aws-sdk-go-v2/service/transfer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_Transfer_ServerAndUserLifecycle exercises the AWS Transfer
// Family core workflow via the AWS SDK v2: create an SFTP server, list/describe
// it, create a user under it, list/describe the user, then delete user and
// server. Primary integration coverage for the Transfer JSON-RPC handler.
func TestIntegration_Transfer_ServerAndUserLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createTransferClient(t)
	ctx := t.Context()

	const (
		userName = "it-transfer-user"
		homeDir  = "/it-bucket/home"
		roleArn  = "arn:aws:iam::000000000000:role/TransferUserRole"
	)

	// CreateServer (SFTP).
	createOut, err := client.CreateServer(ctx, &transfersdk.CreateServerInput{
		Protocols: []transfertypes.Protocol{transfertypes.ProtocolSftp},
	})
	require.NoError(t, err)
	serverID := aws.ToString(createOut.ServerId)
	require.NotEmpty(t, serverID)

	t.Cleanup(func() {
		_, _ = client.DeleteServer(ctx, &transfersdk.DeleteServerInput{ServerId: aws.String(serverID)})
	})

	// DescribeServer.
	descOut, err := client.DescribeServer(ctx, &transfersdk.DescribeServerInput{
		ServerId: aws.String(serverID),
	})
	require.NoError(t, err)
	require.NotNil(t, descOut.Server)
	assert.Equal(t, serverID, aws.ToString(descOut.Server.ServerId))
	assert.Contains(t, descOut.Server.Protocols, transfertypes.ProtocolSftp)

	// ListServers should include the new server.
	listOut, err := client.ListServers(ctx, &transfersdk.ListServersInput{})
	require.NoError(t, err)

	foundServer := false

	for _, s := range listOut.Servers {
		if aws.ToString(s.ServerId) == serverID {
			foundServer = true

			break
		}
	}

	assert.True(t, foundServer, "newly created server should be listed")

	// CreateUser.
	createUserOut, err := client.CreateUser(ctx, &transfersdk.CreateUserInput{
		ServerId:      aws.String(serverID),
		UserName:      aws.String(userName),
		HomeDirectory: aws.String(homeDir),
		Role:          aws.String(roleArn),
	})
	require.NoError(t, err)
	assert.Equal(t, userName, aws.ToString(createUserOut.UserName))
	assert.Equal(t, serverID, aws.ToString(createUserOut.ServerId))

	t.Cleanup(func() {
		_, _ = client.DeleteUser(ctx, &transfersdk.DeleteUserInput{
			ServerId: aws.String(serverID),
			UserName: aws.String(userName),
		})
	})

	// DescribeUser.
	descUserOut, err := client.DescribeUser(ctx, &transfersdk.DescribeUserInput{
		ServerId: aws.String(serverID),
		UserName: aws.String(userName),
	})
	require.NoError(t, err)
	require.NotNil(t, descUserOut.User)
	assert.Equal(t, userName, aws.ToString(descUserOut.User.UserName))
	assert.Equal(t, homeDir, aws.ToString(descUserOut.User.HomeDirectory))
	assert.Equal(t, roleArn, aws.ToString(descUserOut.User.Role))

	// ListUsers should contain the user.
	listUsersOut, err := client.ListUsers(ctx, &transfersdk.ListUsersInput{
		ServerId: aws.String(serverID),
	})
	require.NoError(t, err)

	foundUser := false

	for _, u := range listUsersOut.Users {
		if aws.ToString(u.UserName) == userName {
			foundUser = true

			break
		}
	}

	assert.True(t, foundUser, "newly created user should be listed")
}
