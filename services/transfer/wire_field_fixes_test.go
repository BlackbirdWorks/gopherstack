package transfer_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	transfersdk "github.com/aws/aws-sdk-go-v2/service/transfer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transfer"
)

// TestListServers_LoggingRole_RealClient covers a layer-3 bug
// (gopherstack-g8k9): Server.LoggingRole is real, tracked state --
// CreateServer stores it and DescribeServer already emits it correctly (the
// second-op signal, handler_servers.go's toDescribedServer) -- but
// ListServers' serverListItem never carried it through, so a real client's
// ListServers().Servers[i].LoggingRole was always empty regardless of what
// the server was configured with. Real field confirmed against
// transfer@v1.75.4 deserializers.go's
// awsAwsjson11_deserializeDocumentListedServer, which has a "LoggingRole"
// case identical to the one on ListedUser/DescribedServer.
func TestListServers_LoggingRole_RealClient(t *testing.T) {
	t.Parallel()

	backend := transfer.NewInMemoryBackend(context.Background(), "123456789012", "us-east-1")
	client := newTestTransferClient(t, transfer.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateServer(ctx, &transfersdk.CreateServerInput{
		LoggingRole: aws.String("arn:aws:iam::123456789012:role/transfer-logging-role"),
	})
	require.NoError(t, err)

	listed, err := client.ListServers(ctx, &transfersdk.ListServersInput{})
	require.NoError(t, err)

	var found *string
	for _, s := range listed.Servers {
		if aws.ToString(s.ServerId) == aws.ToString(created.ServerId) {
			found = s.LoggingRole

			break
		}
	}

	require.NotNil(t, found,
		"ListServers: LoggingRole must round-trip; pre-fix it was always nil")
	assert.Equal(t, "arn:aws:iam::123456789012:role/transfer-logging-role", aws.ToString(found))
}
