package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestDeleteVpnConnection_Tombstone locks in gopherstack-54bv0: a just-deleted
// VPN connection stays describable by id in "deleted" state (matching real
// AWS and terraform-provider-aws's findVPNConnectionByID, which treats state
// "deleted" the same as NotFound), while an unfiltered DescribeVpnConnections
// never surfaces it.
func TestDeleteVpnConnection_Tombstone(t *testing.T) {
	t.Parallel()

	backend := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestEC2Client(t, ec2.NewHandler(backend))

	cgwOut, setupErr := client.CreateCustomerGateway(t.Context(), &ec2sdk.CreateCustomerGatewayInput{
		Type:     types.GatewayTypeIpsec1,
		BgpAsn:   aws.Int32(65000),
		PublicIp: aws.String("203.0.113.9"),
	})
	require.NoError(t, setupErr)

	vgwOut, setupErr := client.CreateVpnGateway(t.Context(), &ec2sdk.CreateVpnGatewayInput{
		Type: types.GatewayTypeIpsec1,
	})
	require.NoError(t, setupErr)

	connOut, setupErr := client.CreateVpnConnection(t.Context(), &ec2sdk.CreateVpnConnectionInput{
		Type:              aws.String("ipsec.1"),
		CustomerGatewayId: cgwOut.CustomerGateway.CustomerGatewayId,
		VpnGatewayId:      vgwOut.VpnGateway.VpnGatewayId,
	})
	require.NoError(t, setupErr)

	connID := aws.ToString(connOut.VpnConnection.VpnConnectionId)

	_, setupErr = client.DeleteVpnConnection(t.Context(), &ec2sdk.DeleteVpnConnectionInput{
		VpnConnectionId: aws.String(connID),
	})
	require.NoError(t, setupErr)

	t.Run("by id returns deleted tombstone", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeVpnConnections(t.Context(), &ec2sdk.DescribeVpnConnectionsInput{
			VpnConnectionIds: []string{connID},
		})
		require.NoError(t, err)
		require.Len(t, out.VpnConnections, 1)
		require.Equal(t, types.VpnStateDeleted, out.VpnConnections[0].State)
	})

	t.Run("unfiltered list omits it", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeVpnConnections(t.Context(), &ec2sdk.DescribeVpnConnectionsInput{})
		require.NoError(t, err)

		for _, conn := range out.VpnConnections {
			require.NotEqual(t, connID, aws.ToString(conn.VpnConnectionId))
		}
	})
}
