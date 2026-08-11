package networkmanager_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	networkmanagersdk "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/aws/aws-sdk-go-v2/service/networkmanager/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// attachmentARN builds an attachment's own taggable ARN by the documented
// convention (arn:{partition}:networkmanager::{account}:attachment/{id}).
// The real SDK's Attachment.ResourceArn field is the ARN of the *underlying*
// resource being attached (e.g. the VPC), not the attachment itself
// (networkmanager@v1.44.4 types/types.go:Attachment) -- confirmed against
// gopherstack's own CreateVpcAttachment, which sets ResourceArn to the VpcArn
// argument, not a self-referencing ARN. No Create/Get/List/Summary response
// exposes the attachment's own ARN, so a real client can only address it by
// this convention -- the same "no ARN in the read path" gap already
// documented for dms/sesv2 and for this file's ConnectPeer case below.
func attachmentARN(id *string) *string {
	return aws.String("arn:aws:networkmanager::" + rtTestAccountID + ":attachment/" + aws.ToString(id))
}

// TestCreateOpsWithTags_RoundTrip drives every networkmanager Create* op
// whose real Input struct accepts Tags (networkmanager@v1.44.4:
// api_op_CreateConnectAttachment.go, api_op_CreateConnection.go,
// api_op_CreateConnectPeer.go, api_op_CreateCoreNetwork.go,
// api_op_CreateDevice.go, api_op_CreateDirectConnectGatewayAttachment.go,
// api_op_CreateGlobalNetwork.go, api_op_CreateLink.go, api_op_CreateSite.go,
// api_op_CreateSiteToSiteVpnAttachment.go,
// api_op_CreateTransitGatewayPeering.go,
// api_op_CreateTransitGatewayRouteTableAttachment.go,
// api_op_CreateVpcAttachment.go, all `Tags []types.Tag`) through the real
// SDK client and asserts ListTagsForResource sees what was supplied at
// creation (gopherstack-2mwl). CreateCoreNetworkPrefixListAssociation takes
// no Tags field in the real SDK and is excluded.
func TestCreateOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	tags := []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}}

	tests := []struct {
		setup func(t *testing.T, client *networkmanagersdk.Client) *string
		name  string
	}{
		{
			name: "global network",
			setup: func(t *testing.T, client *networkmanagersdk.Client) *string {
				t.Helper()
				out, err := client.CreateGlobalNetwork(t.Context(), &networkmanagersdk.CreateGlobalNetworkInput{
					Tags: tags,
				})
				require.NoError(t, err)

				return out.GlobalNetwork.GlobalNetworkArn
			},
		},
		{
			name: "site",
			setup: func(t *testing.T, client *networkmanagersdk.Client) *string {
				t.Helper()
				gn, err := client.CreateGlobalNetwork(t.Context(), &networkmanagersdk.CreateGlobalNetworkInput{})
				require.NoError(t, err)

				out, err := client.CreateSite(t.Context(), &networkmanagersdk.CreateSiteInput{
					GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
					Tags:            tags,
				})
				require.NoError(t, err)

				return out.Site.SiteArn
			},
		},
		{
			name: "device",
			setup: func(t *testing.T, client *networkmanagersdk.Client) *string {
				t.Helper()
				gn, err := client.CreateGlobalNetwork(t.Context(), &networkmanagersdk.CreateGlobalNetworkInput{})
				require.NoError(t, err)

				out, err := client.CreateDevice(t.Context(), &networkmanagersdk.CreateDeviceInput{
					GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
					Tags:            tags,
				})
				require.NoError(t, err)

				return out.Device.DeviceArn
			},
		},
		{
			name: "link",
			setup: func(t *testing.T, client *networkmanagersdk.Client) *string {
				t.Helper()
				gn, err := client.CreateGlobalNetwork(t.Context(), &networkmanagersdk.CreateGlobalNetworkInput{})
				require.NoError(t, err)

				site, err := client.CreateSite(t.Context(), &networkmanagersdk.CreateSiteInput{
					GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
				})
				require.NoError(t, err)

				out, err := client.CreateLink(t.Context(), &networkmanagersdk.CreateLinkInput{
					GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
					SiteId:          site.Site.SiteId,
					Bandwidth:       &types.Bandwidth{UploadSpeed: aws.Int32(10), DownloadSpeed: aws.Int32(10)},
					Tags:            tags,
				})
				require.NoError(t, err)

				return out.Link.LinkArn
			},
		},
		{
			name: "connection",
			setup: func(t *testing.T, client *networkmanagersdk.Client) *string {
				t.Helper()
				gn, err := client.CreateGlobalNetwork(t.Context(), &networkmanagersdk.CreateGlobalNetworkInput{})
				require.NoError(t, err)

				dev1, err := client.CreateDevice(t.Context(), &networkmanagersdk.CreateDeviceInput{
					GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
				})
				require.NoError(t, err)

				dev2, err := client.CreateDevice(t.Context(), &networkmanagersdk.CreateDeviceInput{
					GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
				})
				require.NoError(t, err)

				out, err := client.CreateConnection(t.Context(), &networkmanagersdk.CreateConnectionInput{
					GlobalNetworkId:   gn.GlobalNetwork.GlobalNetworkId,
					DeviceId:          dev1.Device.DeviceId,
					ConnectedDeviceId: dev2.Device.DeviceId,
					Tags:              tags,
				})
				require.NoError(t, err)

				return out.Connection.ConnectionArn
			},
		},
		{
			name: "core network",
			setup: func(t *testing.T, client *networkmanagersdk.Client) *string {
				t.Helper()
				gn, err := client.CreateGlobalNetwork(t.Context(), &networkmanagersdk.CreateGlobalNetworkInput{})
				require.NoError(t, err)

				out, err := client.CreateCoreNetwork(t.Context(), &networkmanagersdk.CreateCoreNetworkInput{
					GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
					Tags:            tags,
				})
				require.NoError(t, err)

				return out.CoreNetwork.CoreNetworkArn
			},
		},
		{
			name: "vpc attachment",
			setup: func(t *testing.T, client *networkmanagersdk.Client) *string {
				t.Helper()
				cn := createTestCoreNetwork(t, client)

				out, err := client.CreateVpcAttachment(t.Context(), &networkmanagersdk.CreateVpcAttachmentInput{
					CoreNetworkId: cn.CoreNetwork.CoreNetworkId,
					VpcArn:        aws.String("arn:aws:ec2:us-east-1:000000000000:vpc/vpc-tagtest"),
					SubnetArns:    []string{"arn:aws:ec2:us-east-1:000000000000:subnet/subnet-tagtest"},
					Tags:          tags,
				})
				require.NoError(t, err)

				return attachmentARN(out.VpcAttachment.Attachment.AttachmentId)
			},
		},
		{
			name: "connect attachment",
			setup: func(t *testing.T, client *networkmanagersdk.Client) *string {
				t.Helper()
				cn := createTestCoreNetwork(t, client)

				vpcAttachment, err := client.CreateVpcAttachment(
					t.Context(),
					&networkmanagersdk.CreateVpcAttachmentInput{
						CoreNetworkId: cn.CoreNetwork.CoreNetworkId,
						VpcArn:        aws.String("arn:aws:ec2:us-east-1:000000000000:vpc/vpc-tagtest2"),
						SubnetArns:    []string{"arn:aws:ec2:us-east-1:000000000000:subnet/subnet-tagtest2"},
					},
				)
				require.NoError(t, err)

				out, err := client.CreateConnectAttachment(t.Context(), &networkmanagersdk.CreateConnectAttachmentInput{
					CoreNetworkId:         cn.CoreNetwork.CoreNetworkId,
					EdgeLocation:          aws.String("us-east-1"),
					Options:               &types.ConnectAttachmentOptions{Protocol: types.TunnelProtocolGre},
					TransportAttachmentId: vpcAttachment.VpcAttachment.Attachment.AttachmentId,
					Tags:                  tags,
				})
				require.NoError(t, err)

				return attachmentARN(out.ConnectAttachment.Attachment.AttachmentId)
			},
		},
		{
			// ConnectPeer carries no ARN field anywhere in the real SDK's
			// Create/Get/List/Summary responses (networkmanager@v1.44.4
			// types/types.go:ConnectPeer, ConnectPeerSummary) -- a real
			// client can only address it by the documented ARN convention
			// (arn:{partition}:networkmanager::{account}:connect-peer/{id}),
			// same "no ARN in the read path" gap the campaign already
			// documented for dms/sesv2.
			name: "connect peer",
			setup: func(t *testing.T, client *networkmanagersdk.Client) *string {
				t.Helper()
				cn := createTestCoreNetwork(t, client)
				connectAttachmentID := createTestConnectAttachment(t, client, cn.CoreNetwork.CoreNetworkId)

				out, err := client.CreateConnectPeer(t.Context(), &networkmanagersdk.CreateConnectPeerInput{
					ConnectAttachmentId: connectAttachmentID,
					PeerAddress:         aws.String("10.0.0.1"),
					Tags:                tags,
				})
				require.NoError(t, err)

				return aws.String(
					"arn:aws:networkmanager::" + rtTestAccountID + ":connect-peer/" + aws.ToString(
						out.ConnectPeer.ConnectPeerId,
					),
				)
			},
		},
		{
			name: "site to site vpn attachment",
			setup: func(t *testing.T, client *networkmanagersdk.Client) *string {
				t.Helper()
				cn := createTestCoreNetwork(t, client)

				out, err := client.CreateSiteToSiteVpnAttachment(
					t.Context(),
					&networkmanagersdk.CreateSiteToSiteVpnAttachmentInput{
						CoreNetworkId:    cn.CoreNetwork.CoreNetworkId,
						VpnConnectionArn: aws.String("arn:aws:ec2:us-east-1:000000000000:vpn-connection/vpn-tagtest"),
						Tags:             tags,
					},
				)
				require.NoError(t, err)

				return attachmentARN(out.SiteToSiteVpnAttachment.Attachment.AttachmentId)
			},
		},
		{
			name: "direct connect gateway attachment",
			setup: func(t *testing.T, client *networkmanagersdk.Client) *string {
				t.Helper()
				cn := createTestCoreNetwork(t, client)

				out, err := client.CreateDirectConnectGatewayAttachment(
					t.Context(),
					&networkmanagersdk.CreateDirectConnectGatewayAttachmentInput{
						CoreNetworkId: cn.CoreNetwork.CoreNetworkId,
						DirectConnectGatewayArn: aws.String(
							"arn:aws:directconnect::000000000000:dx-gateway/dxgw-tagtest",
						),
						EdgeLocations: []string{"us-east-1"},
						Tags:          tags,
					},
				)
				require.NoError(t, err)

				return attachmentARN(out.DirectConnectGatewayAttachment.Attachment.AttachmentId)
			},
		},
		{
			name: "transit gateway peering",
			setup: func(t *testing.T, client *networkmanagersdk.Client) *string {
				t.Helper()
				cn := createTestCoreNetwork(t, client)

				out, err := client.CreateTransitGatewayPeering(
					t.Context(),
					&networkmanagersdk.CreateTransitGatewayPeeringInput{
						CoreNetworkId:     cn.CoreNetwork.CoreNetworkId,
						TransitGatewayArn: aws.String("arn:aws:ec2:us-east-1:000000000000:transit-gateway/tgw-tagtest"),
						Tags:              tags,
					},
				)
				require.NoError(t, err)

				return out.TransitGatewayPeering.Peering.ResourceArn
			},
		},
		{
			name: "transit gateway route table attachment",
			setup: func(t *testing.T, client *networkmanagersdk.Client) *string {
				t.Helper()
				cn := createTestCoreNetwork(t, client)

				peering, err := client.CreateTransitGatewayPeering(
					t.Context(),
					&networkmanagersdk.CreateTransitGatewayPeeringInput{
						CoreNetworkId: cn.CoreNetwork.CoreNetworkId,
						TransitGatewayArn: aws.String(
							"arn:aws:ec2:us-east-1:000000000000:transit-gateway/tgw-tagtest2",
						),
					},
				)
				require.NoError(t, err)

				out, err := client.CreateTransitGatewayRouteTableAttachment(
					t.Context(),
					&networkmanagersdk.CreateTransitGatewayRouteTableAttachmentInput{
						PeeringId: peering.TransitGatewayPeering.Peering.PeeringId,
						TransitGatewayRouteTableArn: aws.String(
							"arn:aws:ec2:us-east-1:000000000000:transit-gateway-route-table/tgw-rtb-tagtest",
						),
						Tags: tags,
					},
				)
				require.NoError(t, err)

				return attachmentARN(out.TransitGatewayRouteTableAttachment.Attachment.AttachmentId)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestHandlerAndClient(t)

			arn := tc.setup(t, client)
			require.NotNil(t, arn)
			require.NotEmpty(t, aws.ToString(arn))

			got, err := client.ListTagsForResource(t.Context(), &networkmanagersdk.ListTagsForResourceInput{
				ResourceArn: arn,
			})
			require.NoError(t, err)
			require.Len(t, got.TagList, 1)
			assert.Equal(t, "env", aws.ToString(got.TagList[0].Key))
			assert.Equal(t, "prod", aws.ToString(got.TagList[0].Value))
		})
	}
}
