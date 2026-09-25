package terraform_test

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2svc "github.com/aws/aws-sdk-go-v2/service/ec2"
	nmsvc "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_TransitGatewayConnect provisions a Transit Gateway Connect
// attachment, a Connect peer with an explicit BGP ASN, and the Network
// Manager device association for that peer via Terraform, verifying each
// through the EC2 and Network Manager SDK clients.
func TestTerraform_TransitGatewayConnect(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "ec2-transit-gateway-connect",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyTransitGatewayConnectPeer(ctx, t)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

func verifyTransitGatewayConnectPeer(ctx context.Context, t *testing.T) {
	t.Helper()

	ec2Client := createEC2Client(t)

	peersOut, err := ec2Client.DescribeTransitGatewayConnectPeers(
		ctx,
		&ec2svc.DescribeTransitGatewayConnectPeersInput{},
	)
	require.NoError(t, err, "DescribeTransitGatewayConnectPeers should succeed")

	var peerID string

	for _, p := range peersOut.TransitGatewayConnectPeers {
		if p.ConnectPeerConfiguration == nil {
			continue
		}

		if aws.ToString(p.ConnectPeerConfiguration.PeerAddress) != "192.0.2.10" {
			continue
		}

		peerID = aws.ToString(p.TransitGatewayConnectPeerId)

		require.NotEmpty(t, p.ConnectPeerConfiguration.BgpConfigurations, "peer should report BGP configurations")
		assert.Equal(t, int64(65001), aws.ToInt64(p.ConnectPeerConfiguration.BgpConfigurations[0].PeerAsn))
		assert.Equal(t, "169.254.100.1", aws.ToString(p.ConnectPeerConfiguration.BgpConfigurations[0].PeerAddress))
	}

	require.NotEmpty(t, peerID, "connect peer with PeerAddress 192.0.2.10 should exist")

	cfg := megaConfig(t)

	nmClient := nmsvc.NewFromConfig(cfg, func(o *nmsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	gnOut, err := nmClient.DescribeGlobalNetworks(ctx, &nmsvc.DescribeGlobalNetworksInput{})
	require.NoError(t, err, "DescribeGlobalNetworks should succeed")

	var globalNetworkID string

	for _, gn := range gnOut.GlobalNetworks {
		if aws.ToString(gn.Description) == "tgcn global network" {
			globalNetworkID = aws.ToString(gn.GlobalNetworkId)

			break
		}
	}

	require.NotEmpty(t, globalNetworkID, "tgcn global network should exist")

	assocOut, err := nmClient.GetTransitGatewayConnectPeerAssociations(
		ctx,
		&nmsvc.GetTransitGatewayConnectPeerAssociationsInput{
			GlobalNetworkId: aws.String(globalNetworkID),
		},
	)
	require.NoError(t, err, "GetTransitGatewayConnectPeerAssociations should succeed")

	found := false

	for _, a := range assocOut.TransitGatewayConnectPeerAssociations {
		if strings.HasSuffix(aws.ToString(a.TransitGatewayConnectPeerArn), peerID) {
			found = true

			break
		}
	}

	assert.True(t, found, "association for the connect peer should be listed")
}
