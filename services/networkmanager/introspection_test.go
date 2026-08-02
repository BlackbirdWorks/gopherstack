package networkmanager_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	networkmanagersdk "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/aws/aws-sdk-go-v2/service/networkmanager/types"
	"github.com/stretchr/testify/require"
)

// TestRoundTrip_NetworkIntrospection drives family T: GetNetworkResources/
// GetNetworkResourceCounts/GetNetworkResourceRelationships derive honestly
// from already-modeled state; GetNetworkRoutes returns an honest empty
// route list (no route-propagation engine exists).
func TestRoundTrip_NetworkIntrospection(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	gn, err := client.CreateGlobalNetwork(ctx, &networkmanagersdk.CreateGlobalNetworkInput{})
	require.NoError(t, err)

	site, err := client.CreateSite(
		ctx,
		&networkmanagersdk.CreateSiteInput{GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId},
	)
	require.NoError(t, err)

	device, err := client.CreateDevice(ctx, &networkmanagersdk.CreateDeviceInput{
		GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId, SiteId: site.Site.SiteId,
	})
	require.NoError(t, err)

	resources, err := client.GetNetworkResources(ctx, &networkmanagersdk.GetNetworkResourcesInput{
		GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
	})
	require.NoError(t, err)
	require.Len(t, resources.NetworkResources, 2) // site + device

	for _, r := range resources.NetworkResources {
		require.NotEmpty(t, aws.ToString(r.Definition))
		require.NotEmpty(t, aws.ToString(r.ResourceArn))
	}

	counts, err := client.GetNetworkResourceCounts(ctx, &networkmanagersdk.GetNetworkResourceCountsInput{
		GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
	})
	require.NoError(t, err)

	total := int32(0)
	for _, c := range counts.NetworkResourceCounts {
		total += aws.ToInt32(c.Count)
	}

	require.Equal(t, int32(2), total)

	rels, err := client.GetNetworkResourceRelationships(ctx, &networkmanagersdk.GetNetworkResourceRelationshipsInput{
		GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
	})
	require.NoError(t, err)
	require.Len(t, rels.Relationships, 1)
	require.Equal(t, aws.ToString(device.Device.DeviceArn), aws.ToString(rels.Relationships[0].From))
	require.Equal(t, aws.ToString(site.Site.SiteArn), aws.ToString(rels.Relationships[0].To))

	routes, err := client.GetNetworkRoutes(ctx, &networkmanagersdk.GetNetworkRoutesInput{
		GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
		RouteTableIdentifier: &types.RouteTableIdentifier{
			TransitGatewayRouteTableArn: aws.String(
				"arn:aws:ec2:us-east-1:000000000000:transit-gateway-route-table/tgw-rtb-1",
			),
		},
	})
	require.NoError(t, err)
	require.Empty(t, routes.NetworkRoutes)
	require.Equal(
		t,
		"arn:aws:ec2:us-east-1:000000000000:transit-gateway-route-table/tgw-rtb-1",
		aws.ToString(routes.RouteTableArn),
	)

	telemetry, err := client.GetNetworkTelemetry(ctx, &networkmanagersdk.GetNetworkTelemetryInput{
		GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
	})
	require.NoError(t, err)
	require.Empty(t, telemetry.NetworkTelemetry) // no Connection/ConnectPeer created yet

	metadata, err := client.UpdateNetworkResourceMetadata(ctx, &networkmanagersdk.UpdateNetworkResourceMetadataInput{
		GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId, ResourceArn: device.Device.DeviceArn,
		Metadata: map[string]string{"owner": "team-a"},
	})
	require.NoError(t, err)
	require.Equal(t, "team-a", metadata.Metadata["owner"])
}
