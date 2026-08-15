package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestDescribeVpnConnections_TunnelOptions_RealClient covers gopherstack-6flj:
// vpnConnectionOptionsItem.TunnelOptionsSet was emitted under "tunnelOptions",
// but the real DescribeVpnConnections deserializer (ec2@v1.319.1
// deserializers.go: awsEc2query_deserializeDocumentVpnConnectionOptions) reads
// "tunnelOptionSet". Every tunnel's negotiated config -- pre-shared key,
// inside CIDR, IKE versions, lifetimes -- was silently dropped for any real
// client regardless of what CreateVpnConnection actually generated.
func TestDescribeVpnConnections_TunnelOptions_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1")))
	ctx := t.Context()

	cgw, err := client.CreateCustomerGateway(ctx, &ec2sdk.CreateCustomerGatewayInput{
		Type:      types.GatewayTypeIpsec1,
		BgpAsn:    aws.Int32(65000),
		IpAddress: aws.String("203.0.113.1"),
	})
	require.NoError(t, err)

	vgw, err := client.CreateVpnGateway(ctx, &ec2sdk.CreateVpnGatewayInput{
		Type: types.GatewayTypeIpsec1,
	})
	require.NoError(t, err)

	connOut, err := client.CreateVpnConnection(ctx, &ec2sdk.CreateVpnConnectionInput{
		Type:              aws.String("ipsec.1"),
		CustomerGatewayId: cgw.CustomerGateway.CustomerGatewayId,
		VpnGatewayId:      vgw.VpnGateway.VpnGatewayId,
	})
	require.NoError(t, err)
	require.NotNil(t, connOut.VpnConnection)

	out, err := client.DescribeVpnConnections(ctx, &ec2sdk.DescribeVpnConnectionsInput{
		VpnConnectionIds: []string{aws.ToString(connOut.VpnConnection.VpnConnectionId)},
	})
	require.NoError(t, err)
	require.Len(t, out.VpnConnections, 1)

	opts := out.VpnConnections[0].Options
	require.NotNil(t, opts)
	require.Len(t, opts.TunnelOptions, 2,
		"TunnelOptions must round-trip; pre-fix the real deserializer's element name never "+
			"matched the emitted one, so this was always empty")
	tun := opts.TunnelOptions[0]
	assert.NotEmpty(t, aws.ToString(tun.OutsideIpAddress))
	assert.NotEmpty(t, aws.ToString(tun.PreSharedKey))
	require.Len(t, tun.IkeVersions, 2,
		"IkeVersions must round-trip; pre-fix it was emitted under \"ikeVersions\" instead of "+
			"the real \"ikeVersionSet\"")
	ikeVersions := make([]string, len(tun.IkeVersions))
	for i, v := range tun.IkeVersions {
		ikeVersions[i] = aws.ToString(v.Value)
	}
	assert.ElementsMatch(t, []string{"ikev1", "ikev2"}, ikeVersions)
}
