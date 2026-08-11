package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codeconnectionssdk "github.com/aws/aws-sdk-go-v2/service/codeconnections"
	codeconnectionstypes "github.com/aws/aws-sdk-go-v2/service/codeconnections/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_CodeConnections_ConnectionLifecycle exercises the core
// CodeConnections workflow via the AWS SDK v2: create a GitHub connection,
// get/list it, then delete. Primary integration coverage for the
// CodeConnections JSON-RPC handler.
func TestIntegration_CodeConnections_ConnectionLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCodeConnectionsClient(t)
	ctx := t.Context()

	const connectionName = "it-codeconnections-conn"

	// CreateConnection.
	createOut, err := client.CreateConnection(ctx, &codeconnectionssdk.CreateConnectionInput{
		ConnectionName: aws.String(connectionName),
		ProviderType:   codeconnectionstypes.ProviderTypeGithub,
	})
	require.NoError(t, err)
	arn := aws.ToString(createOut.ConnectionArn)
	require.NotEmpty(t, arn)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteConnection(cleanupCtx, &codeconnectionssdk.DeleteConnectionInput{
			ConnectionArn: aws.String(arn),
		})
	})

	// GetConnection.
	getOut, err := client.GetConnection(ctx, &codeconnectionssdk.GetConnectionInput{
		ConnectionArn: aws.String(arn),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Connection)
	assert.Equal(t, connectionName, aws.ToString(getOut.Connection.ConnectionName))
	assert.Equal(t, codeconnectionstypes.ProviderTypeGithub, getOut.Connection.ProviderType)

	// ListConnections should contain the new connection.
	listOut, err := client.ListConnections(ctx, &codeconnectionssdk.ListConnectionsInput{})
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
