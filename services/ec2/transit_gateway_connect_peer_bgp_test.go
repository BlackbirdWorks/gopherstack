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

// TestTransitGatewayConnectPeer_BgpConfigurations proves the fix for
// gopherstack-zfrof: terraform-provider-aws v5.100.0's
// findTransitGatewayConnectPeer (internal/service/ec2/find.go) filters out
// any peer whose ConnectPeerConfiguration.BgpConfigurations is empty via
// tfresource.AssertSingleValueResult, turning it into a NotFoundError. Our
// wire response never populated BgpConfigurations, so the create waiter
// timed out on every poll despite State=available. Each case drives the
// real SDK client end to end and re-derives the same non-empty check the
// provider's waiter performs.
func TestTransitGatewayConnectPeer_BgpConfigurations(t *testing.T) {
	t.Parallel()

	cases := []struct {
		bgpOptions         *types.TransitGatewayConnectRequestBgpOptions
		name               string
		insideCidrBlocks   []string
		wantPeerAsn        []int64
		wantPeerAddresses  []string
		wantTransitGwAddrs []string
		tgwAmazonSideAsn   int64
		wantTransitGwAsn   int64
	}{
		{
			name:               "ipv4_default_bgp_asn",
			tgwAmazonSideAsn:   65000,
			insideCidrBlocks:   []string{"169.254.100.0/29"},
			wantPeerAsn:        []int64{64512},
			wantTransitGwAsn:   65000,
			wantPeerAddresses:  []string{"169.254.100.1"},
			wantTransitGwAddrs: []string{"169.254.100.2"},
		},
		{
			name:               "explicit_bgp_asn",
			tgwAmazonSideAsn:   64512,
			insideCidrBlocks:   []string{"169.254.101.0/29"},
			bgpOptions:         &types.TransitGatewayConnectRequestBgpOptions{PeerAsn: aws.Int64(65001)},
			wantPeerAsn:        []int64{65001},
			wantTransitGwAsn:   64512,
			wantPeerAddresses:  []string{"169.254.101.1"},
			wantTransitGwAddrs: []string{"169.254.101.2"},
		},
		{
			name:               "dual_stack_two_bgp_sessions",
			tgwAmazonSideAsn:   64512,
			insideCidrBlocks:   []string{"169.254.102.0/29", "fd00:1234:5678::/125"},
			wantPeerAsn:        []int64{64512, 64512},
			wantTransitGwAsn:   64512,
			wantPeerAddresses:  []string{"169.254.102.1", "fd00:1234:5678::1"},
			wantTransitGwAddrs: []string{"169.254.102.2", "fd00:1234:5678::2"},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
			client := newTestEC2Client(t, h)

			tgw, err := client.CreateTransitGateway(t.Context(), &ec2sdk.CreateTransitGatewayInput{
				Options: &types.TransitGatewayRequestOptions{AmazonSideAsn: aws.Int64(tt.tgwAmazonSideAsn)},
			})
			require.NoError(t, err)

			vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.38.0.0/16")})
			require.NoError(t, err)
			subnet, err := client.CreateSubnet(t.Context(), &ec2sdk.CreateSubnetInput{
				VpcId:     vpc.Vpc.VpcId,
				CidrBlock: aws.String("10.38.1.0/24"),
			})
			require.NoError(t, err)

			vpcAtt, err := client.CreateTransitGatewayVpcAttachment(
				t.Context(), &ec2sdk.CreateTransitGatewayVpcAttachmentInput{
					TransitGatewayId: tgw.TransitGateway.TransitGatewayId,
					VpcId:            vpc.Vpc.VpcId,
					SubnetIds:        []string{aws.ToString(subnet.Subnet.SubnetId)},
				},
			)
			require.NoError(t, err)

			connect, err := client.CreateTransitGatewayConnect(t.Context(), &ec2sdk.CreateTransitGatewayConnectInput{
				TransportTransitGatewayAttachmentId: vpcAtt.TransitGatewayVpcAttachment.TransitGatewayAttachmentId,
				Options: &types.CreateTransitGatewayConnectRequestOptions{
					Protocol: types.ProtocolValueGre,
				},
			})
			require.NoError(t, err)

			created, err := client.CreateTransitGatewayConnectPeer(
				t.Context(), &ec2sdk.CreateTransitGatewayConnectPeerInput{
					TransitGatewayAttachmentId: connect.TransitGatewayConnect.TransitGatewayAttachmentId,
					PeerAddress:                aws.String("192.0.2.10"),
					InsideCidrBlocks:           tt.insideCidrBlocks,
					BgpOptions:                 tt.bgpOptions,
				},
			)
			require.NoError(t, err)
			peerID := created.TransitGatewayConnectPeer.TransitGatewayConnectPeerId

			// Mirrors terraform-provider-aws's findTransitGatewayConnectPeer:
			// a single Describe by ID, then the same non-empty-BgpConfigurations
			// check its AssertSingleValueResult predicate performs.
			described, err := client.DescribeTransitGatewayConnectPeers(
				t.Context(), &ec2sdk.DescribeTransitGatewayConnectPeersInput{
					TransitGatewayConnectPeerIds: []string{aws.ToString(peerID)},
				},
			)
			require.NoError(t, err)
			require.Len(t, described.TransitGatewayConnectPeers, 1)

			peer := described.TransitGatewayConnectPeers[0]
			require.NotNil(
				t,
				peer.ConnectPeerConfiguration,
				"provider treats a nil ConnectPeerConfiguration as not-found",
			)
			require.NotEmpty(
				t, peer.ConnectPeerConfiguration.BgpConfigurations,
				"provider's AssertSingleValueResult treats an empty BgpConfigurations as not-found (gopherstack-zfrof)",
			)

			gotPeerAsn := make([]int64, len(peer.ConnectPeerConfiguration.BgpConfigurations))
			gotPeerAddrs := make([]string, len(peer.ConnectPeerConfiguration.BgpConfigurations))
			gotTgwAddrs := make([]string, len(peer.ConnectPeerConfiguration.BgpConfigurations))
			for i, c := range peer.ConnectPeerConfiguration.BgpConfigurations {
				gotPeerAsn[i] = aws.ToInt64(c.PeerAsn)
				gotPeerAddrs[i] = aws.ToString(c.PeerAddress)
				gotTgwAddrs[i] = aws.ToString(c.TransitGatewayAddress)
				assert.Equal(t, tt.wantTransitGwAsn, aws.ToInt64(c.TransitGatewayAsn))
				assert.Equal(t, types.BgpStatusUp, c.BgpStatus)
			}
			assert.Equal(t, tt.wantPeerAsn, gotPeerAsn)
			assert.Equal(t, tt.wantPeerAddresses, gotPeerAddrs)
			assert.Equal(t, tt.wantTransitGwAddrs, gotTgwAddrs)
		})
	}
}
