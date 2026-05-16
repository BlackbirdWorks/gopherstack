package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codestarconnectionssdk "github.com/aws/aws-sdk-go-v2/service/codestarconnections"
	codestarconnectionstypes "github.com/aws/aws-sdk-go-v2/service/codestarconnections/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_CodeStarConnections_ConnectionLifecycle exercises the
// legacy CodeStar Connections API (the predecessor name for CodeConnections):
// create, get, list, delete. Primary integration coverage for the
// CodeStarConnections JSON-RPC handler.
func TestIntegration_CodeStarConnections_ConnectionLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCodeStarConnectionsClient(t)
	ctx := t.Context()

	const connectionName = "it-codestarconnections-conn"

	// CreateConnection.
	createOut, err := client.CreateConnection(ctx, &codestarconnectionssdk.CreateConnectionInput{
		ConnectionName: aws.String(connectionName),
		ProviderType:   codestarconnectionstypes.ProviderTypeGithub,
	})
	require.NoError(t, err)
	arn := aws.ToString(createOut.ConnectionArn)
	require.NotEmpty(t, arn)

	t.Cleanup(func() {
		_, _ = client.DeleteConnection(ctx, &codestarconnectionssdk.DeleteConnectionInput{
			ConnectionArn: aws.String(arn),
		})
	})

	// GetConnection.
	getOut, err := client.GetConnection(ctx, &codestarconnectionssdk.GetConnectionInput{
		ConnectionArn: aws.String(arn),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Connection)
	assert.Equal(t, connectionName, aws.ToString(getOut.Connection.ConnectionName))

	// ListConnections should contain the new connection.
	listOut, err := client.ListConnections(ctx, &codestarconnectionssdk.ListConnectionsInput{})
	require.NoError(t, err)

	foundConn := false

	for _, c := range listOut.Connections {
		if aws.ToString(c.ConnectionArn) == arn {
			foundConn = true

			break
		}
	}

	assert.True(t, foundConn, "newly created connection should be listed")
}
