package directconnect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	directconnectsdk "github.com/aws/aws-sdk-go-v2/service/directconnect"
	"github.com/aws/aws-sdk-go-v2/service/directconnect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// containsConnectionID reports whether conns includes one with the given
// ConnectionId -- CreateLag(NumberOfConnections>0) seeds a LAG with its own
// member connections sharing LagID, so a hosted-onto-LAG assertion cannot
// assume length 1.
func containsConnectionID(conns []types.Connection, id string) bool {
	for _, c := range conns {
		if aws.ToString(c.ConnectionId) == id {
			return true
		}
	}

	return false
}

// TestAllocateConnectionOnInterconnect_ListedInHostedConnections verifies
// gopherstack-41bv6: a connection allocated with AllocateConnectionOnInterconnect
// (which only sets InterconnectID, not ParentConnectionID/LagID) must appear
// in both DescribeHostedConnections(interconnectId) --
// api_op_DescribeHostedConnections.go:15-16, "Lists the hosted connections
// that have been provisioned on the specified interconnect or link
// aggregation group (LAG)" -- and its deprecated sibling
// DescribeConnectionsOnInterconnect.
func TestAllocateConnectionOnInterconnect_ListedInHostedConnections(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	ic, err := client.CreateInterconnect(ctx, &directconnectsdk.CreateInterconnectInput{
		Bandwidth:        aws.String("1Gbps"),
		InterconnectName: aws.String("ic-1"),
		Location:         aws.String("EqDC2"),
	})
	require.NoError(t, err)

	//nolint:staticcheck // SA1019: deliberately exercising the deprecated-but-real op under test (gopherstack-41bv6)
	allocated, err := client.AllocateConnectionOnInterconnect(
		ctx,
		&directconnectsdk.AllocateConnectionOnInterconnectInput{
			Bandwidth:      aws.String("500Mbps"),
			ConnectionName: aws.String("hosted-on-ic"),
			InterconnectId: ic.InterconnectId,
			OwnerAccount:   aws.String("999999999999"),
			Vlan:           101,
		},
	)
	require.NoError(t, err)

	hosted, err := client.DescribeHostedConnections(
		ctx,
		&directconnectsdk.DescribeHostedConnectionsInput{
			ConnectionId: ic.InterconnectId,
		},
	)
	require.NoError(t, err)
	require.Len(t, hosted.Connections, 1)
	assert.Equal(
		t,
		aws.ToString(allocated.ConnectionId),
		aws.ToString(hosted.Connections[0].ConnectionId),
	)

	//nolint:staticcheck // SA1019: deliberately exercising a deprecated-but-real op
	onIc, err := client.DescribeConnectionsOnInterconnect(
		ctx,
		&directconnectsdk.DescribeConnectionsOnInterconnectInput{InterconnectId: ic.InterconnectId},
	)
	require.NoError(t, err)
	require.Len(t, onIc.Connections, 1)
	assert.Equal(
		t,
		aws.ToString(allocated.ConnectionId),
		aws.ToString(onIc.Connections[0].ConnectionId),
	)
}

// TestAllocateHostedConnection_ParentKinds verifies AllocateHostedConnection's
// connectionId (api_op_AllocateHostedConnection.go:49: "The ID of the
// interconnect or LAG" -- this emulator additionally accepts a plain
// Connection) is recorded so the new hosted connection is listed under
// DescribeHostedConnections(<that same id>), regardless of which of the
// three kinds it names.
func TestAllocateHostedConnection_ParentKinds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		makeParent func(t *testing.T, client *directconnectsdk.Client) string
		name       string
	}{
		{
			name: "connection",
			makeParent: func(t *testing.T, client *directconnectsdk.Client) string {
				t.Helper()

				return aws.ToString(createTestConnection(t, client).ConnectionId)
			},
		},
		{
			name: "lag",
			makeParent: func(t *testing.T, client *directconnectsdk.Client) string {
				t.Helper()

				lag, err := client.CreateLag(t.Context(), &directconnectsdk.CreateLagInput{
					ConnectionsBandwidth: aws.String("1Gbps"),
					LagName:              aws.String("lag-for-hosted"),
					Location:             aws.String("EqDC2"),
					NumberOfConnections:  1,
				})
				require.NoError(t, err)

				return aws.ToString(lag.LagId)
			},
		},
		{
			name: "interconnect",
			makeParent: func(t *testing.T, client *directconnectsdk.Client) string {
				t.Helper()

				ic, err := client.CreateInterconnect(
					t.Context(),
					&directconnectsdk.CreateInterconnectInput{
						Bandwidth:        aws.String("1Gbps"),
						InterconnectName: aws.String("ic-for-hosted"),
						Location:         aws.String("EqDC2"),
					},
				)
				require.NoError(t, err)

				return aws.ToString(ic.InterconnectId)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)
			ctx := t.Context()

			parentID := tc.makeParent(t, client)

			hostedOut, err := client.AllocateHostedConnection(
				ctx,
				&directconnectsdk.AllocateHostedConnectionInput{
					Bandwidth:      aws.String("100Mbps"),
					ConnectionId:   aws.String(parentID),
					ConnectionName: aws.String("hosted-conn"),
					OwnerAccount:   aws.String("999999999999"),
					Vlan:           202,
				},
			)
			require.NoError(t, err)

			hosted, err := client.DescribeHostedConnections(
				ctx,
				&directconnectsdk.DescribeHostedConnectionsInput{
					ConnectionId: aws.String(parentID),
				},
			)
			require.NoError(t, err)
			assert.True(
				t, containsConnectionID(hosted.Connections, aws.ToString(hostedOut.ConnectionId)),
				"expected %s among %v", aws.ToString(hostedOut.ConnectionId), hosted.Connections,
			)
		})
	}
}

// TestAssociateHostedConnection_MovesParent verifies AssociateHostedConnection
// (api_op_AssociateHostedConnection.go:14-17: "Associates a hosted connection
// ... with a link aggregation group (LAG) or interconnect") reassigns a
// hosted connection between all three parent kinds this emulator supports,
// and that DescribeHostedConnections reflects the move: gone from the old
// parent's list, present under the new one.
func TestAssociateHostedConnection_MovesParent(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	origLagOut, err := client.CreateLag(ctx, &directconnectsdk.CreateLagInput{
		ConnectionsBandwidth: aws.String("1Gbps"),
		LagName:              aws.String("orig-lag"),
		Location:             aws.String("EqDC2"),
		NumberOfConnections:  1,
	})
	require.NoError(t, err)

	hostedOut, err := client.AllocateHostedConnection(
		ctx,
		&directconnectsdk.AllocateHostedConnectionInput{
			Bandwidth:      aws.String("100Mbps"),
			ConnectionId:   origLagOut.LagId,
			ConnectionName: aws.String("movable-hosted-conn"),
			OwnerAccount:   aws.String("999999999999"),
			Vlan:           303,
		},
	)
	require.NoError(t, err)

	ic, err := client.CreateInterconnect(ctx, &directconnectsdk.CreateInterconnectInput{
		Bandwidth:        aws.String("1Gbps"),
		InterconnectName: aws.String("dest-ic"),
		Location:         aws.String("EqDC2"),
	})
	require.NoError(t, err)

	_, err = client.AssociateHostedConnection(ctx, &directconnectsdk.AssociateHostedConnectionInput{
		ConnectionId:       hostedOut.ConnectionId,
		ParentConnectionId: ic.InterconnectId,
	})
	require.NoError(t, err)

	stillOnLag, err := client.DescribeHostedConnections(
		ctx,
		&directconnectsdk.DescribeHostedConnectionsInput{
			ConnectionId: origLagOut.LagId,
		},
	)
	require.NoError(t, err)
	assert.False(
		t, containsConnectionID(stillOnLag.Connections, aws.ToString(hostedOut.ConnectionId)),
		"moved connection must no longer be listed under its old LAG parent",
	)

	nowOnIc, err := client.DescribeHostedConnections(
		ctx,
		&directconnectsdk.DescribeHostedConnectionsInput{
			ConnectionId: ic.InterconnectId,
		},
	)
	require.NoError(t, err)
	require.Len(t, nowOnIc.Connections, 1)
	assert.Equal(
		t,
		aws.ToString(hostedOut.ConnectionId),
		aws.ToString(nowOnIc.Connections[0].ConnectionId),
	)
}

// TestAssociateHostedConnection_UnknownParent verifies associating onto a
// parent id that names no connection, LAG, or interconnect is rejected with
// a DirectConnectClientException, not silently accepted.
func TestAssociateHostedConnection_UnknownParent(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	conn := createTestConnection(t, client)

	lagOut, err := client.CreateLag(ctx, &directconnectsdk.CreateLagInput{
		ConnectionsBandwidth: aws.String("1Gbps"),
		LagName:              aws.String("host-lag"),
		Location:             aws.String("EqDC2"),
		NumberOfConnections:  1,
	})
	require.NoError(t, err)

	hostedOut, err := client.AllocateHostedConnection(
		ctx,
		&directconnectsdk.AllocateHostedConnectionInput{
			Bandwidth:      aws.String("100Mbps"),
			ConnectionId:   lagOut.LagId,
			ConnectionName: aws.String("hosted-conn"),
			OwnerAccount:   aws.String("999999999999"),
			Vlan:           404,
		},
	)
	require.NoError(t, err)

	_, err = client.AssociateHostedConnection(ctx, &directconnectsdk.AssociateHostedConnectionInput{
		ConnectionId:       hostedOut.ConnectionId,
		ParentConnectionId: aws.String("dxlag-doesnotexist"),
	})
	require.Error(t, err)

	var clientErr *types.DirectConnectClientException
	require.ErrorAs(t, err, &clientErr)

	_, err = client.AssociateHostedConnection(ctx, &directconnectsdk.AssociateHostedConnectionInput{
		ConnectionId:       aws.String("dxcon-doesnotexist"),
		ParentConnectionId: conn.ConnectionId,
	})
	require.Error(t, err)
	require.ErrorAs(t, err, &clientErr)
}
