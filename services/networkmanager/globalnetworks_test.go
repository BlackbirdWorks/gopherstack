package networkmanager_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	networkmanagersdk "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/aws/aws-sdk-go-v2/service/networkmanager/types"
	"github.com/stretchr/testify/require"
)

// TestRoundTrip_GlobalNetworkLifecycle drives Create/Describe/Update/Delete
// for GlobalNetwork -- the root container every other family in this
// service scopes under.
func TestRoundTrip_GlobalNetworkLifecycle(t *testing.T) {
	t.Parallel()

	h, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	require.Equal(t, "NetworkManager", h.Name())
	require.Len(t, h.GetSupportedOperations(), 95)

	created, err := client.CreateGlobalNetwork(ctx, &networkmanagersdk.CreateGlobalNetworkInput{
		Description: aws.String("my network"),
		Tags:        []types.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	require.NoError(t, err)
	require.NotNil(t, created.GlobalNetwork)
	require.NotEmpty(t, aws.ToString(created.GlobalNetwork.GlobalNetworkId))
	require.NotEmpty(t, aws.ToString(created.GlobalNetwork.GlobalNetworkArn))
	require.Equal(t, "my network", aws.ToString(created.GlobalNetwork.Description))
	require.Equal(t, types.GlobalNetworkStatePending, created.GlobalNetwork.State)

	id := created.GlobalNetwork.GlobalNetworkId

	require.Eventually(t, func() bool {
		d, describeErr := client.DescribeGlobalNetworks(ctx, &networkmanagersdk.DescribeGlobalNetworksInput{
			GlobalNetworkIds: []string{aws.ToString(id)},
		})

		return describeErr == nil && len(d.GlobalNetworks) == 1 &&
			d.GlobalNetworks[0].State == types.GlobalNetworkStateAvailable
	}, defaultAsyncWait, defaultAsyncPoll)

	updated, err := client.UpdateGlobalNetwork(ctx, &networkmanagersdk.UpdateGlobalNetworkInput{
		GlobalNetworkId: id, Description: aws.String("renamed"),
	})
	require.NoError(t, err)
	require.Equal(t, "renamed", aws.ToString(updated.GlobalNetwork.Description))

	listed, err := client.DescribeGlobalNetworks(ctx, &networkmanagersdk.DescribeGlobalNetworksInput{})
	require.NoError(t, err)
	require.Len(t, listed.GlobalNetworks, 1)

	_, err = client.DeleteGlobalNetwork(ctx, &networkmanagersdk.DeleteGlobalNetworkInput{GlobalNetworkId: id})
	require.NoError(t, err)

	_, err = client.UpdateGlobalNetwork(ctx, &networkmanagersdk.UpdateGlobalNetworkInput{
		GlobalNetworkId: aws.String("nonexistent"), Description: aws.String("x"),
	})
	require.Error(t, err)

	var nf *types.ResourceNotFoundException
	require.ErrorAs(t, err, &nf)
}

// TestRoundTrip_SiteDeviceLinkAssociation drives Site/Device/Link creation
// plus AssociateLink/DisassociateLink/GetLinkAssociations -- family E, the
// pure Device<->Link binding.
func TestRoundTrip_SiteDeviceLinkAssociation(t *testing.T) {
	t.Parallel()

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
	require.Equal(t, "1 Main St", aws.ToString(site.Site.Location.Address))

	device, err := client.CreateDevice(ctx, &networkmanagersdk.CreateDeviceInput{
		GlobalNetworkId: gnID, SiteId: site.Site.SiteId, Vendor: aws.String("Acme"),
	})
	require.NoError(t, err)
	require.Equal(t, "Acme", aws.ToString(device.Device.Vendor))

	link, err := client.CreateLink(ctx, &networkmanagersdk.CreateLinkInput{
		GlobalNetworkId: gnID, SiteId: site.Site.SiteId,
		Bandwidth: &types.Bandwidth{DownloadSpeed: aws.Int32(100), UploadSpeed: aws.Int32(50)},
	})
	require.NoError(t, err)
	require.Equal(t, int32(100), aws.ToInt32(link.Link.Bandwidth.DownloadSpeed))

	assoc, err := client.AssociateLink(ctx, &networkmanagersdk.AssociateLinkInput{
		GlobalNetworkId: gnID, DeviceId: device.Device.DeviceId, LinkId: link.Link.LinkId,
	})
	require.NoError(t, err)
	require.Equal(t, types.LinkAssociationStatePending, assoc.LinkAssociation.LinkAssociationState)

	require.Eventually(t, func() bool {
		l, getErr := client.GetLinkAssociations(ctx, &networkmanagersdk.GetLinkAssociationsInput{GlobalNetworkId: gnID})

		return getErr == nil && len(l.LinkAssociations) == 1 &&
			l.LinkAssociations[0].LinkAssociationState == types.LinkAssociationStateAvailable
	}, defaultAsyncWait, defaultAsyncPoll)

	_, err = client.DisassociateLink(ctx, &networkmanagersdk.DisassociateLinkInput{
		GlobalNetworkId: gnID, DeviceId: device.Device.DeviceId, LinkId: link.Link.LinkId,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		l, getErr := client.GetLinkAssociations(ctx, &networkmanagersdk.GetLinkAssociationsInput{GlobalNetworkId: gnID})

		return getErr == nil && len(l.LinkAssociations) == 0
	}, defaultAsyncWait, defaultAsyncPoll)
}

// TestRoundTrip_Connection drives CreateConnection/UpdateConnection/
// GetConnections/DeleteConnection -- family F, the on-prem device-to-device
// logical link distinct from a Cloud WAN Connect attachment.
func TestRoundTrip_Connection(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	gn, err := client.CreateGlobalNetwork(ctx, &networkmanagersdk.CreateGlobalNetworkInput{})
	require.NoError(t, err)

	gnID := gn.GlobalNetwork.GlobalNetworkId

	d1, err := client.CreateDevice(ctx, &networkmanagersdk.CreateDeviceInput{GlobalNetworkId: gnID})
	require.NoError(t, err)

	d2, err := client.CreateDevice(ctx, &networkmanagersdk.CreateDeviceInput{GlobalNetworkId: gnID})
	require.NoError(t, err)

	conn, err := client.CreateConnection(ctx, &networkmanagersdk.CreateConnectionInput{
		GlobalNetworkId: gnID, DeviceId: d1.Device.DeviceId, ConnectedDeviceId: d2.Device.DeviceId,
		Description: aws.String("link between devices"),
	})
	require.NoError(t, err)
	require.Equal(t, "link between devices", aws.ToString(conn.Connection.Description))

	updated, err := client.UpdateConnection(ctx, &networkmanagersdk.UpdateConnectionInput{
		GlobalNetworkId: gnID, ConnectionId: conn.Connection.ConnectionId, Description: aws.String("renamed"),
	})
	require.NoError(t, err)
	require.Equal(t, "renamed", aws.ToString(updated.Connection.Description))

	listed, err := client.GetConnections(ctx, &networkmanagersdk.GetConnectionsInput{GlobalNetworkId: gnID})
	require.NoError(t, err)
	require.Len(t, listed.Connections, 1)

	_, err = client.DeleteConnection(ctx, &networkmanagersdk.DeleteConnectionInput{
		GlobalNetworkId: gnID, ConnectionId: conn.Connection.ConnectionId,
	})
	require.NoError(t, err)
}
