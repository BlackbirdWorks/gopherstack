package main

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/directconnect"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/elasticsearchservice"
	estypes "github.com/aws/aws-sdk-go-v2/service/elasticsearchservice/types"
	"github.com/aws/aws-sdk-go-v2/service/networkmanager"
	nmtypes "github.com/aws/aws-sdk-go-v2/service/networkmanager/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestElasticsearchSubnetResolverWiring(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantZones []string
		azs       []string
	}{
		{name: "two_zones", azs: []string{"us-east-1b", "us-east-1a"}, wantZones: []string{"us-east-1a", "us-east-1b"}},
		{name: "one_zone", azs: []string{"us-east-1c"}, wantZones: []string{"us-east-1c"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			ec2c := ec2.NewFromConfig(fx.cfg)

			vpc, err := ec2c.CreateVpc(t.Context(), &ec2.CreateVpcInput{CidrBlock: aws.String("10.7.0.0/16")})
			require.NoError(t, err)

			var subnetIDs []string

			for i, az := range tt.azs {
				sn, serr := ec2c.CreateSubnet(t.Context(), &ec2.CreateSubnetInput{
					VpcId: vpc.Vpc.VpcId, AvailabilityZone: aws.String(az),
					CidrBlock: aws.String("10.7." + string(rune('1'+i)) + ".0/24"),
				})
				require.NoError(t, serr)

				subnetIDs = append(subnetIDs, aws.ToString(sn.Subnet.SubnetId))
			}

			esc := elasticsearchservice.NewFromConfig(fx.cfg)
			out, err := esc.CreateElasticsearchDomain(t.Context(), &elasticsearchservice.CreateElasticsearchDomainInput{
				DomainName: aws.String("vpc-dom"),
				VPCOptions: &estypes.VPCOptions{SubnetIds: subnetIDs},
			})
			require.NoError(t, err)
			require.NotNil(t, out.DomainStatus.VPCOptions)
			assert.Equal(t, aws.ToString(vpc.Vpc.VpcId), aws.ToString(out.DomainStatus.VPCOptions.VPCId))
			assert.Equal(t, tt.wantZones, out.DomainStatus.VPCOptions.AvailabilityZones)
		})
	}
}

func TestDirectConnectMacSecSecretWiring(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cak  string
		ckn  string
	}{
		{
			name: "raw_key_pair",
			cak:  "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
			ckn:  "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			dc := directconnect.NewFromConfig(fx.cfg)

			conn, err := dc.CreateConnection(t.Context(), &directconnect.CreateConnectionInput{
				Bandwidth: aws.String("10Gbps"), ConnectionName: aws.String("c"), Location: aws.String("EqDC2"),
			})
			require.NoError(t, err)

			out, err := dc.AssociateMacSecKey(t.Context(), &directconnect.AssociateMacSecKeyInput{
				ConnectionId: conn.ConnectionId, Cak: aws.String(tt.cak), Ckn: aws.String(tt.ckn),
			})
			require.NoError(t, err)
			require.Len(t, out.MacSecKeys, 1)

			val, err := secretsmanager.NewFromConfig(fx.cfg).GetSecretValue(t.Context(),
				&secretsmanager.GetSecretValueInput{SecretId: out.MacSecKeys[0].SecretARN})
			require.NoError(t, err)
			assert.JSONEq(t, `{"ckn":"`+tt.ckn+`","cak":"`+tt.cak+`"}`, aws.ToString(val.SecretString))
		})
	}
}

func TestNetworkManagerRouteAnalysisAcrossPeeringWiring(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantResult nmtypes.RouteAnalysisCompletionResultCode
		wantHops   int
		peerRoute  bool
	}{
		{
			name:       "crosses_peering",
			peerRoute:  true,
			wantResult: nmtypes.RouteAnalysisCompletionResultCodeConnected,
			wantHops:   2,
		},
		{name: "no_route_on_peer", wantResult: nmtypes.RouteAnalysisCompletionResultCodeNotConnected, wantHops: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			us := ec2.NewFromConfig(fx.cfg)
			eu := ec2.NewFromConfig(fx.cfg, func(o *ec2.Options) { o.Region = euRegion })

			type side struct {
				tgw, vpcAtt, rtb string
			}

			build := func(c *ec2.Client, cidr, subnetCIDR string) side {
				tgw, err := c.CreateTransitGateway(t.Context(), &ec2.CreateTransitGatewayInput{})
				require.NoError(t, err)

				vpc, err := c.CreateVpc(t.Context(), &ec2.CreateVpcInput{CidrBlock: aws.String(cidr)})
				require.NoError(t, err)

				sn, err := c.CreateSubnet(t.Context(), &ec2.CreateSubnetInput{
					VpcId: vpc.Vpc.VpcId, CidrBlock: aws.String(subnetCIDR),
				})
				require.NoError(t, err)

				att, err := c.CreateTransitGatewayVpcAttachment(
					t.Context(),
					&ec2.CreateTransitGatewayVpcAttachmentInput{
						TransitGatewayId: tgw.TransitGateway.TransitGatewayId, VpcId: vpc.Vpc.VpcId,
						SubnetIds: []string{aws.ToString(sn.Subnet.SubnetId)},
					},
				)
				require.NoError(t, err)

				rtb, err := c.CreateTransitGatewayRouteTable(t.Context(), &ec2.CreateTransitGatewayRouteTableInput{
					TransitGatewayId: tgw.TransitGateway.TransitGatewayId,
				})
				require.NoError(t, err)

				return side{
					tgw:    aws.ToString(tgw.TransitGateway.TransitGatewayId),
					vpcAtt: aws.ToString(att.TransitGatewayVpcAttachment.TransitGatewayAttachmentId),
					rtb:    aws.ToString(rtb.TransitGatewayRouteTable.TransitGatewayRouteTableId),
				}
			}

			a, b := build(us, "10.1.0.0/16", "10.1.1.0/24"), build(eu, "10.2.0.0/16", "10.2.1.0/24")

			peer, err := us.CreateTransitGatewayPeeringAttachment(
				t.Context(),
				&ec2.CreateTransitGatewayPeeringAttachmentInput{
					TransitGatewayId: aws.String(a.tgw), PeerTransitGatewayId: aws.String(b.tgw),
					PeerAccountId: aws.String("000000000000"), PeerRegion: aws.String(euRegion),
				},
			)
			require.NoError(t, err)

			peerID := aws.ToString(peer.TransitGatewayPeeringAttachment.TransitGatewayAttachmentId)
			_, err = eu.AcceptTransitGatewayPeeringAttachment(
				t.Context(),
				&ec2.AcceptTransitGatewayPeeringAttachmentInput{
					TransitGatewayAttachmentId: aws.String(peerID),
				},
			)
			require.NoError(t, err)

			assoc := func(c *ec2.Client, rtb, att string) {
				_, aerr := c.AssociateTransitGatewayRouteTable(t.Context(), &ec2.AssociateTransitGatewayRouteTableInput{
					TransitGatewayRouteTableId: aws.String(rtb), TransitGatewayAttachmentId: aws.String(att),
				})
				require.NoError(t, aerr)
			}
			route := func(c *ec2.Client, rtb, cidr, att string) {
				_, rerr := c.CreateTransitGatewayRoute(t.Context(), &ec2.CreateTransitGatewayRouteInput{
					TransitGatewayRouteTableId: aws.String(rtb), DestinationCidrBlock: aws.String(cidr),
					TransitGatewayAttachmentId: aws.String(att),
				})
				require.NoError(t, rerr)
			}

			assoc(us, a.rtb, a.vpcAtt)
			route(us, a.rtb, "10.2.0.0/16", peerID)
			assoc(eu, b.rtb, peerID)

			if tt.peerRoute {
				route(eu, b.rtb, "10.2.0.0/16", b.vpcAtt)
			}

			nm := networkmanager.NewFromConfig(fx.cfg)
			gn, err := nm.CreateGlobalNetwork(t.Context(), &networkmanager.CreateGlobalNetworkInput{})
			require.NoError(t, err)

			start, err := nm.StartRouteAnalysis(t.Context(), &networkmanager.StartRouteAnalysisInput{
				GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
				Source: &nmtypes.RouteAnalysisEndpointOptionsSpecification{
					TransitGatewayAttachmentArn: aws.String(
						"arn:aws:ec2:us-east-1:000000000000:transit-gateway-attachment/" + a.vpcAtt,
					),
					IpAddress: aws.String("10.1.0.5"),
				},
				Destination: &nmtypes.RouteAnalysisEndpointOptionsSpecification{IpAddress: aws.String("10.2.0.5")},
			})
			require.NoError(t, err)

			var got *networkmanager.GetRouteAnalysisOutput

			require.Eventually(t, func() bool {
				o, gerr := nm.GetRouteAnalysis(t.Context(), &networkmanager.GetRouteAnalysisInput{
					GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
					RouteAnalysisId: start.RouteAnalysis.RouteAnalysisId,
				})
				if gerr != nil || o.RouteAnalysis.Status != nmtypes.RouteAnalysisStatusCompleted {
					return false
				}

				got = o

				return true
			}, 10*time.Second, 50*time.Millisecond)

			fwd := got.RouteAnalysis.ForwardPath
			require.NotNil(t, fwd)
			assert.Equal(t, tt.wantResult, fwd.CompletionStatus.ResultCode)
			assert.Len(t, fwd.Path, tt.wantHops)
		})
	}
}
