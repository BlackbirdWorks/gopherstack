package directconnect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	directconnectsdk "github.com/aws/aws-sdk-go-v2/service/directconnect"
	"github.com/aws/aws-sdk-go-v2/service/directconnect/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/directconnect"
)

// TestPersistenceSnapshotRestore verifies that a Connection, a
// DirectConnectGateway (with its GLOBAL ARN), and their tags survive a
// Snapshot/Restore round-trip into a fresh backend.
func TestPersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	backend := directconnect.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	t.Cleanup(backend.Close)

	h := directconnect.NewHandler(backend)
	client := newRoundTripClient(t, h)
	ctx := t.Context()

	connOut, err := client.CreateConnection(ctx, &directconnectsdk.CreateConnectionInput{
		Bandwidth:      aws.String("1Gbps"),
		ConnectionName: aws.String("persisted-conn"),
		Location:       aws.String("EqDC2"),
		Tags:           []types.Tag{{Key: aws.String("team"), Value: aws.String("sre")}},
	})
	require.NoError(t, err)

	gwOut, err := client.CreateDirectConnectGateway(ctx, &directconnectsdk.CreateDirectConnectGatewayInput{
		DirectConnectGatewayName: aws.String("persisted-gw"),
		AmazonSideAsn:            aws.Int64(65001),
	})
	require.NoError(t, err)

	data := backend.Snapshot(ctx)
	require.NotEmpty(t, data)

	restored := directconnect.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	t.Cleanup(restored.Close)
	require.NoError(t, restored.Restore(ctx, data))

	restoredConns := restored.DescribeConnections(aws.ToString(connOut.ConnectionId))
	require.Len(t, restoredConns, 1)
	require.Equal(t, "persisted-conn", restoredConns[0].ConnectionName)

	tagVal, ok := restoredConns[0].Tags.Get("team")
	require.True(t, ok)
	require.Equal(t, "sre", tagVal)

	restoredGws := restored.DescribeDirectConnectGateways(
		aws.ToString(gwOut.DirectConnectGateway.DirectConnectGatewayId),
	)
	require.Len(t, restoredGws, 1)
	require.Equal(t, int64(65001), restoredGws[0].AmazonSideAsn)

	expectedARN := "arn:aws:directconnect::000000000000:dx-gateway/" +
		aws.ToString(gwOut.DirectConnectGateway.DirectConnectGatewayId)
	require.Equal(t, expectedARN, restored.GatewayARN(aws.ToString(gwOut.DirectConnectGateway.DirectConnectGatewayId)))
}
