package networkmanager_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	networkmanagersdk "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/aws/aws-sdk-go-v2/service/networkmanager/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_DeviceConnect drives networkmanager's typed-coverage-blind
// ops (gopherstack-n3zi) through the real aws-sdk-go-v2 client.
func TestRealClient_DeviceConnect(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "device site link lifecycle",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newTestHandlerAndClient(t)
				ctx := t.Context()

				gn, err := client.CreateGlobalNetwork(ctx, &networkmanagersdk.CreateGlobalNetworkInput{})
				require.NoError(t, err)
				gnID := gn.GlobalNetwork.GlobalNetworkId

				site, err := client.CreateSite(ctx, &networkmanagersdk.CreateSiteInput{
					GlobalNetworkId: gnID,
					Location:        &types.Location{Address: aws.String("1 Main St")},
				})
				require.NoError(t, err)
				siteID := site.Site.SiteId

				device, err := client.CreateDevice(ctx, &networkmanagersdk.CreateDeviceInput{
					GlobalNetworkId: gnID, SiteId: siteID, Vendor: aws.String("Acme"),
				})
				require.NoError(t, err)
				deviceID := device.Device.DeviceId

				link, err := client.CreateLink(ctx, &networkmanagersdk.CreateLinkInput{
					GlobalNetworkId: gnID, SiteId: siteID,
					Bandwidth: &types.Bandwidth{UploadSpeed: aws.Int32(10), DownloadSpeed: aws.Int32(20)},
				})
				require.NoError(t, err)
				linkID := link.Link.LinkId

				updDevice, err := client.UpdateDevice(ctx, &networkmanagersdk.UpdateDeviceInput{
					GlobalNetworkId: gnID, DeviceId: deviceID, Description: aws.String("s15 device"),
				})
				require.NoError(t, err)
				assert.Equal(t, "s15 device", aws.ToString(updDevice.Device.Description))

				getDevicesOut, err := client.GetDevices(ctx, &networkmanagersdk.GetDevicesInput{
					GlobalNetworkId: gnID, DeviceIds: []string{aws.ToString(deviceID)},
				})
				require.NoError(t, err)
				require.Len(t, getDevicesOut.Devices, 1)
				assert.Equal(t, aws.ToString(deviceID), aws.ToString(getDevicesOut.Devices[0].DeviceId))
				assert.Equal(t, "s15 device", aws.ToString(getDevicesOut.Devices[0].Description))

				updLink, err := client.UpdateLink(ctx, &networkmanagersdk.UpdateLinkInput{
					GlobalNetworkId: gnID, LinkId: linkID, Description: aws.String("s15 link"),
				})
				require.NoError(t, err)
				assert.Equal(t, "s15 link", aws.ToString(updLink.Link.Description))

				getLinksOut, err := client.GetLinks(ctx, &networkmanagersdk.GetLinksInput{
					GlobalNetworkId: gnID, SiteId: siteID,
				})
				require.NoError(t, err)
				require.Len(t, getLinksOut.Links, 1)
				assert.Equal(t, aws.ToString(linkID), aws.ToString(getLinksOut.Links[0].LinkId))
				assert.Equal(t, "s15 link", aws.ToString(getLinksOut.Links[0].Description))

				updSite, err := client.UpdateSite(ctx, &networkmanagersdk.UpdateSiteInput{
					GlobalNetworkId: gnID, SiteId: siteID, Description: aws.String("s15 site"),
				})
				require.NoError(t, err)
				assert.Equal(t, "s15 site", aws.ToString(updSite.Site.Description))
			},
		},
		{
			name: "connect attachment and transit gateway connect peer associations",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newTestHandlerAndClient(t)
				ctx := t.Context()

				cn := createTestCoreNetwork(t, client)
				connectAttachmentID := createTestConnectAttachment(t, client, cn.CoreNetwork.CoreNetworkId)

				getAttOut, err := client.GetConnectAttachment(ctx, &networkmanagersdk.GetConnectAttachmentInput{
					AttachmentId: connectAttachmentID,
				})
				require.NoError(t, err)
				require.NotNil(t, getAttOut.ConnectAttachment)
				assert.Equal(
					t,
					aws.ToString(connectAttachmentID),
					aws.ToString(getAttOut.ConnectAttachment.Attachment.AttachmentId),
				)

				gn, err := client.CreateGlobalNetwork(ctx, &networkmanagersdk.CreateGlobalNetworkInput{})
				require.NoError(t, err)

				device, err := client.CreateDevice(
					ctx,
					&networkmanagersdk.CreateDeviceInput{GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId},
				)
				require.NoError(t, err)

				tgwCpArn := "arn:aws:ec2:us-east-1:000000000000:transit-gateway-connect-peer/tgw-connect-peer-s15"
				assoc, err := client.AssociateTransitGatewayConnectPeer(
					ctx,
					&networkmanagersdk.AssociateTransitGatewayConnectPeerInput{
						GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId, DeviceId: device.Device.DeviceId,
						TransitGatewayConnectPeerArn: aws.String(tgwCpArn),
					},
				)
				require.NoError(t, err)
				require.NotNil(t, assoc.TransitGatewayConnectPeerAssociation)

				getAssocOut, err := client.GetTransitGatewayConnectPeerAssociations(
					ctx, &networkmanagersdk.GetTransitGatewayConnectPeerAssociationsInput{
						GlobalNetworkId:               gn.GlobalNetwork.GlobalNetworkId,
						TransitGatewayConnectPeerArns: []string{tgwCpArn},
					},
				)
				require.NoError(t, err)
				require.Len(t, getAssocOut.TransitGatewayConnectPeerAssociations, 1)
				assert.Equal(t, tgwCpArn,
					aws.ToString(getAssocOut.TransitGatewayConnectPeerAssociations[0].TransitGatewayConnectPeerArn))
				assert.Equal(t, aws.ToString(device.Device.DeviceId),
					aws.ToString(getAssocOut.TransitGatewayConnectPeerAssociations[0].DeviceId))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
