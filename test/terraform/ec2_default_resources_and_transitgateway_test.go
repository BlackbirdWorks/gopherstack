package terraform_test

import (
	"context"
	"fmt"
	"hash/fnv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2svc "github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// twoVPCCIDRVars returns VPCCidr/PeerVPCCidr template vars for a fixture that
// provisions two independent (non-overlapping) VPCs, derived from a stable
// hash of the test's name -- see vpcCIDRVars for why this must be stable
// across runs but distinct across parallel subtests.
func twoVPCCIDRVars(t *testing.T) map[string]any {
	t.Helper()

	h := fnv.New32a()
	_, _ = h.Write([]byte(t.Name()))
	octetA := int(h.Sum32()%126) + 1

	h2 := fnv.New32a()
	_, _ = h2.Write([]byte(t.Name() + "-peer"))
	octetB := int(h2.Sum32()%126) + 128

	return map[string]any{
		"VPCCidr":     fmt.Sprintf("10.%d.0.0/16", octetA),
		"PeerVPCCidr": fmt.Sprintf("10.%d.0.0/16", octetB),
	}
}

// TestTerraform_MegaBatch12 provisions default-resource adoption (default VPC,
// subnet, route table, network ACL, security group, DHCP options), VPC
// peering (connection/accepter/options), customer/VPN gateway family, a
// carrier gateway, and a transit gateway with a VPC attachment and route
// table family, then verifies each through the EC2 SDK.
func TestTerraform_MegaBatch12(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "mega-batch-12",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return twoVPCCIDRVars(t)
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				client := createEC2Client(t)

				vpcsOut, err := client.DescribeVpcs(ctx, &ec2svc.DescribeVpcsInput{
					Filters: []ec2types.Filter{
						{Name: aws.String("isDefault"), Values: []string{"true"}},
					},
				})
				require.NoError(t, err, "DescribeVpcs should succeed")
				require.Len(t, vpcsOut.Vpcs, 1, "exactly one default VPC should exist")
				defaultVPC := vpcsOut.Vpcs[0]

				rtOut, err := client.DescribeRouteTables(ctx, &ec2svc.DescribeRouteTablesInput{
					Filters: []ec2types.Filter{
						{Name: aws.String("vpc-id"), Values: []string{aws.ToString(defaultVPC.VpcId)}},
						{Name: aws.String("association.main"), Values: []string{"true"}},
					},
				})
				require.NoError(t, err, "DescribeRouteTables should succeed")
				require.Len(t, rtOut.RouteTables, 1, "default VPC should have a main route table")

				tagsOut, err := client.DescribeTags(ctx, &ec2svc.DescribeTagsInput{
					Filters: []ec2types.Filter{
						{
							Name:   aws.String("resource-id"),
							Values: []string{aws.ToString(rtOut.RouteTables[0].RouteTableId)},
						},
						{Name: aws.String("key"), Values: []string{"Name"}},
					},
				})
				require.NoError(t, err, "DescribeTags should succeed")
				require.Len(t, tagsOut.Tags, 1)
				assert.Equal(t, "mega-batch-12-default-rt", aws.ToString(tagsOut.Tags[0].Value))

				naclOut, err := client.DescribeNetworkAcls(ctx, &ec2svc.DescribeNetworkAclsInput{
					Filters: []ec2types.Filter{
						{Name: aws.String("vpc-id"), Values: []string{aws.ToString(defaultVPC.VpcId)}},
						{Name: aws.String("default"), Values: []string{"true"}},
					},
				})
				require.NoError(t, err, "DescribeNetworkAcls should succeed")
				require.Len(t, naclOut.NetworkAcls, 1, "default VPC should have a default network ACL")

				sgOut, err := client.DescribeSecurityGroups(ctx, &ec2svc.DescribeSecurityGroupsInput{
					Filters: []ec2types.Filter{
						{Name: aws.String("vpc-id"), Values: []string{aws.ToString(defaultVPC.VpcId)}},
						{Name: aws.String("group-name"), Values: []string{"default"}},
					},
				})
				require.NoError(t, err, "DescribeSecurityGroups should succeed")
				require.Len(t, sgOut.SecurityGroups, 1, "default VPC should have a default security group")

				dhcpOut, err := client.DescribeDhcpOptions(ctx, &ec2svc.DescribeDhcpOptionsInput{
					DhcpOptionsIds: []string{aws.ToString(defaultVPC.DhcpOptionsId)},
				})
				require.NoError(t, err, "DescribeDhcpOptions should succeed")
				require.Len(
					t,
					dhcpOut.DhcpOptions,
					1,
					"default VPC's DHCP options set should be a real, describable resource",
				)

				subnetsOut, err := client.DescribeSubnets(ctx, &ec2svc.DescribeSubnetsInput{
					Filters: []ec2types.Filter{
						{Name: aws.String("vpc-id"), Values: []string{aws.ToString(defaultVPC.VpcId)}},
						{Name: aws.String("availability-zone"), Values: []string{"us-east-1a"}},
						{Name: aws.String("default-for-az"), Values: []string{"true"}},
					},
				})
				require.NoError(t, err, "DescribeSubnets should succeed")
				require.Len(t, subnetsOut.Subnets, 1, "default subnet in us-east-1a should exist")

				peeringOut, err := client.DescribeVpcPeeringConnections(ctx, &ec2svc.DescribeVpcPeeringConnectionsInput{
					Filters: []ec2types.Filter{
						{Name: aws.String("tag:Name"), Values: []string{"mega-batch-12-peering"}},
					},
				})
				require.NoError(t, err, "DescribeVpcPeeringConnections should succeed")
				require.Len(t, peeringOut.VpcPeeringConnections, 1)
				pcx := peeringOut.VpcPeeringConnections[0]
				assert.Equal(t, ec2types.VpcPeeringConnectionStateReasonCodeActive, pcx.Status.Code,
					"aws_vpc_peering_connection_accepter should have accepted the connection")
				require.NotNil(t, pcx.AccepterVpcInfo)
				require.NotNil(t, pcx.AccepterVpcInfo.PeeringOptions)
				assert.True(t, aws.ToBool(pcx.AccepterVpcInfo.PeeringOptions.AllowDnsResolutionFromRemoteVpc),
					"aws_vpc_peering_connection_options should have set the accepter DNS resolution option")

				cgwOut, err := client.DescribeCarrierGateways(ctx, &ec2svc.DescribeCarrierGatewaysInput{
					Filters: []ec2types.Filter{
						{Name: aws.String("tag:Name"), Values: []string{"mega-batch-12-carrier-gw"}},
					},
				})
				require.NoError(t, err, "DescribeCarrierGateways should succeed")
				require.Len(t, cgwOut.CarrierGateways, 1)

				cgwsOut, err := client.DescribeCustomerGateways(ctx, &ec2svc.DescribeCustomerGatewaysInput{
					Filters: []ec2types.Filter{
						{Name: aws.String("tag:Name"), Values: []string{"mega-batch-12-cgw"}},
					},
				})
				require.NoError(t, err, "DescribeCustomerGateways should succeed")
				require.Len(t, cgwsOut.CustomerGateways, 1)
				customerGatewayID := aws.ToString(cgwsOut.CustomerGateways[0].CustomerGatewayId)

				vgwOut, err := client.DescribeVpnGateways(ctx, &ec2svc.DescribeVpnGatewaysInput{
					Filters: []ec2types.Filter{
						{Name: aws.String("tag:Name"), Values: []string{"mega-batch-12-vgw"}},
					},
				})
				require.NoError(t, err, "DescribeVpnGateways should succeed")
				require.Len(t, vgwOut.VpnGateways, 1)
				vgw := vgwOut.VpnGateways[0]
				require.Len(t, vgw.VpcAttachments, 1, "aws_vpn_gateway_attachment should attach the VGW to the VPC")
				assert.Equal(t, ec2types.AttachmentStatusAttached, vgw.VpcAttachments[0].State)

				vpnOut, err := client.DescribeVpnConnections(ctx, &ec2svc.DescribeVpnConnectionsInput{
					Filters: []ec2types.Filter{
						{Name: aws.String("tag:Name"), Values: []string{"mega-batch-12-vpn"}},
					},
				})
				require.NoError(t, err, "DescribeVpnConnections should succeed")
				require.Len(t, vpnOut.VpnConnections, 1)
				vpnConn := vpnOut.VpnConnections[0]
				assert.Equal(t, customerGatewayID, aws.ToString(vpnConn.CustomerGatewayId))

				var foundRoute bool

				for _, r := range vpnConn.Routes {
					if aws.ToString(r.DestinationCidrBlock) == "172.20.0.0/24" {
						foundRoute = true
					}
				}

				assert.True(t, foundRoute, "aws_vpn_connection_route should add the static route")

				tgwOut, err := client.DescribeTransitGateways(ctx, &ec2svc.DescribeTransitGatewaysInput{
					Filters: []ec2types.Filter{
						{Name: aws.String("tag:Name"), Values: []string{"mega-batch-12-tgw"}},
					},
				})
				require.NoError(t, err, "DescribeTransitGateways should succeed")
				require.Len(t, tgwOut.TransitGateways, 1)
				tgwID := aws.ToString(tgwOut.TransitGateways[0].TransitGatewayId)

				require.Eventually(t, func() bool {
					attOut, pollErr := client.DescribeTransitGatewayVpcAttachments(
						ctx,
						&ec2svc.DescribeTransitGatewayVpcAttachmentsInput{
							Filters: []ec2types.Filter{
								{Name: aws.String("transit-gateway-id"), Values: []string{tgwID}},
							},
						},
					)
					if pollErr != nil || len(attOut.TransitGatewayVpcAttachments) != 1 {
						return false
					}

					return attOut.TransitGatewayVpcAttachments[0].State == ec2types.TransitGatewayAttachmentStateAvailable
				}, 10*time.Second, 100*time.Millisecond, "transit gateway VPC attachment should become available")

				attOut, err := client.DescribeTransitGatewayVpcAttachments(
					ctx,
					&ec2svc.DescribeTransitGatewayVpcAttachmentsInput{
						Filters: []ec2types.Filter{
							{Name: aws.String("transit-gateway-id"), Values: []string{tgwID}},
						},
					},
				)
				require.NoError(t, err, "DescribeTransitGatewayVpcAttachments should succeed")
				require.Len(t, attOut.TransitGatewayVpcAttachments, 1)
				tgwAttachmentID := aws.ToString(attOut.TransitGatewayVpcAttachments[0].TransitGatewayAttachmentId)

				rtsOut, err := client.DescribeTransitGatewayRouteTables(
					ctx,
					&ec2svc.DescribeTransitGatewayRouteTablesInput{
						Filters: []ec2types.Filter{
							{Name: aws.String("transit-gateway-id"), Values: []string{tgwID}},
						},
					},
				)
				require.NoError(t, err, "DescribeTransitGatewayRouteTables should succeed")
				require.Len(
					t,
					rtsOut.TransitGatewayRouteTables,
					1,
					"exactly one non-default TGW route table should exist (default association/propagation were disabled)",
				)
				tgwRouteTableID := aws.ToString(rtsOut.TransitGatewayRouteTables[0].TransitGatewayRouteTableId)
				assert.True(
					t,
					aws.ToBool(rtsOut.TransitGatewayRouteTables[0].DefaultAssociationRouteTable),
					"aws_ec2_transit_gateway_default_route_table_association should make this the default association RT",
				)
				assert.True(
					t,
					aws.ToBool(rtsOut.TransitGatewayRouteTables[0].DefaultPropagationRouteTable),
					"aws_ec2_transit_gateway_default_route_table_propagation should make this the default propagation RT",
				)

				assocOut, err := client.GetTransitGatewayRouteTableAssociations(
					ctx,
					&ec2svc.GetTransitGatewayRouteTableAssociationsInput{
						TransitGatewayRouteTableId: aws.String(tgwRouteTableID),
					},
				)
				require.NoError(t, err, "GetTransitGatewayRouteTableAssociations should succeed")

				var foundAssociation bool

				for _, a := range assocOut.Associations {
					if aws.ToString(a.TransitGatewayAttachmentId) == tgwAttachmentID {
						foundAssociation = true
					}
				}

				assert.True(
					t,
					foundAssociation,
					"aws_ec2_transit_gateway_route_table_association should associate the attachment",
				)

				propOut, err := client.GetTransitGatewayRouteTablePropagations(
					ctx,
					&ec2svc.GetTransitGatewayRouteTablePropagationsInput{
						TransitGatewayRouteTableId: aws.String(tgwRouteTableID),
					},
				)
				require.NoError(t, err, "GetTransitGatewayRouteTablePropagations should succeed")

				var foundPropagation bool

				for _, p := range propOut.TransitGatewayRouteTablePropagations {
					if aws.ToString(p.TransitGatewayAttachmentId) == tgwAttachmentID {
						foundPropagation = true
					}
				}

				assert.True(
					t,
					foundPropagation,
					"aws_ec2_transit_gateway_route_table_propagation should propagate the attachment",
				)

				searchOut, err := client.SearchTransitGatewayRoutes(ctx, &ec2svc.SearchTransitGatewayRoutesInput{
					TransitGatewayRouteTableId: aws.String(tgwRouteTableID),
					Filters: []ec2types.Filter{
						{Name: aws.String("type"), Values: []string{"static"}},
					},
				})
				require.NoError(t, err, "SearchTransitGatewayRoutes should succeed")

				var foundTGWRoute bool

				for _, r := range searchOut.Routes {
					if aws.ToString(r.DestinationCidrBlock) == "10.200.0.0/24" {
						foundTGWRoute = true
					}
				}

				assert.True(t, foundTGWRoute, "aws_ec2_transit_gateway_route should create the static route")
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
