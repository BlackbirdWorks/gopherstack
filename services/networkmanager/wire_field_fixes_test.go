package networkmanager_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	networkmanagersdk "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/aws/aws-sdk-go-v2/service/networkmanager/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOwnerAccountID_Attachment proves every Attachment subtype's
// OwnerAccountId echoes the account that created it, rather than the always-
// blank string every Create*Attachment path emitted before -- the value was
// one field access away (InMemoryBackend.accountID, already wired into
// NetworkResource.AccountID in introspection.go) but newAttachmentLocked
// never read it (gopherstack-6flj).
func TestOwnerAccountID_Attachment(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	cn := createTestCoreNetwork(t, client)

	created, err := client.CreateSiteToSiteVpnAttachment(ctx, &networkmanagersdk.CreateSiteToSiteVpnAttachmentInput{
		CoreNetworkId:    cn.CoreNetwork.CoreNetworkId,
		VpnConnectionArn: aws.String("arn:aws:ec2:us-east-1:000000000000:vpn-connection/vpn-0123456789abcdef0"),
	})
	require.NoError(t, err)
	assert.Equal(t, rtTestAccountID, aws.ToString(created.SiteToSiteVpnAttachment.Attachment.OwnerAccountId))

	fetched, err := client.GetSiteToSiteVpnAttachment(ctx, &networkmanagersdk.GetSiteToSiteVpnAttachmentInput{
		AttachmentId: created.SiteToSiteVpnAttachment.Attachment.AttachmentId,
	})
	require.NoError(t, err)
	assert.Equal(t, rtTestAccountID, aws.ToString(fetched.SiteToSiteVpnAttachment.Attachment.OwnerAccountId))

	listed, err := client.ListAttachments(ctx, &networkmanagersdk.ListAttachmentsInput{
		CoreNetworkId: cn.CoreNetwork.CoreNetworkId,
	})
	require.NoError(t, err)
	require.Len(t, listed.Attachments, 1)
	assert.Equal(t, rtTestAccountID, aws.ToString(listed.Attachments[0].OwnerAccountId))
}

// TestOwnerAccountID_PeeringAndCoreNetworkSummary proves TransitGatewayPeering
// and ListCoreNetworks' CoreNetworkSummary items echo OwnerAccountId the same
// way -- both were also always blank (Peering never set it at all;
// CoreNetworkSummary's converter hardcoded the empty string) despite the same
// InMemoryBackend.accountID being available (gopherstack-6flj).
func TestOwnerAccountID_PeeringAndCoreNetworkSummary(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	cn := createTestCoreNetwork(t, client)

	peering, err := client.CreateTransitGatewayPeering(ctx, &networkmanagersdk.CreateTransitGatewayPeeringInput{
		CoreNetworkId:     cn.CoreNetwork.CoreNetworkId,
		TransitGatewayArn: aws.String("arn:aws:ec2:us-east-1:000000000000:transit-gateway/tgw-0123456789abcdef0"),
	})
	require.NoError(t, err)
	assert.Equal(t, rtTestAccountID, aws.ToString(peering.TransitGatewayPeering.Peering.OwnerAccountId))

	listedCN, err := client.ListCoreNetworks(ctx, &networkmanagersdk.ListCoreNetworksInput{})
	require.NoError(t, err)
	require.Len(t, listedCN.CoreNetworks, 1)
	assert.Equal(t, rtTestAccountID, aws.ToString(listedCN.CoreNetworks[0].OwnerAccountId))
}

// TestGetNetworkResources_ResourceIDAndTags proves every gathered
// networkResourceItem (site/device/link/connection/core-network/attachment/
// connect-peer/peering) carries the real NetworkResource.ResourceId/Tags
// members -- both were one field access away on every source struct
// (SiteID/DeviceID/.../Tags) but every one of the 7 gatherers in
// introspection.go dropped them entirely (gopherstack-6flj).
func TestGetNetworkResources_ResourceIDAndTags(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	gn, err := client.CreateGlobalNetwork(ctx, &networkmanagersdk.CreateGlobalNetworkInput{})
	require.NoError(t, err)

	site, err := client.CreateSite(ctx, &networkmanagersdk.CreateSiteInput{
		GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
		Tags:            []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	resources, err := client.GetNetworkResources(ctx, &networkmanagersdk.GetNetworkResourcesInput{
		GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
	})
	require.NoError(t, err)
	require.Len(t, resources.NetworkResources, 1)

	r := resources.NetworkResources[0]
	assert.Equal(t, aws.ToString(site.Site.SiteId), aws.ToString(r.ResourceId))
	require.Len(t, r.Tags, 1)
	assert.Equal(t, "env", aws.ToString(r.Tags[0].Key))
	assert.Equal(t, "prod", aws.ToString(r.Tags[0].Value))
}

// TestListCoreNetworks_Tags proves CoreNetworkSummary echoes Tags -- the
// real type carries them (types.CoreNetworkSummary.Tags) but
// toCoreNetworkSummaryWire never populated the field at all
// (gopherstack-6flj).
func TestListCoreNetworks_Tags(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	gn, err := client.CreateGlobalNetwork(ctx, &networkmanagersdk.CreateGlobalNetworkInput{})
	require.NoError(t, err)

	_, err = client.CreateCoreNetwork(ctx, &networkmanagersdk.CreateCoreNetworkInput{
		GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
		Tags:            []types.Tag{{Key: aws.String("team"), Value: aws.String("platform")}},
	})
	require.NoError(t, err)

	listed, err := client.ListCoreNetworks(ctx, &networkmanagersdk.ListCoreNetworksInput{})
	require.NoError(t, err)
	require.Len(t, listed.CoreNetworks, 1)
	require.Len(t, listed.CoreNetworks[0].Tags, 1)
	assert.Equal(t, "team", aws.ToString(listed.CoreNetworks[0].Tags[0].Key))
	assert.Equal(t, "platform", aws.ToString(listed.CoreNetworks[0].Tags[0].Value))
}

// TestRouteAnalysis_OwnerAccountIDStartTimestampUseMiddleboxes proves three
// real GetRouteAnalysisOutput/RouteAnalysis members StartRouteAnalysis's
// caller could previously never observe: OwnerAccountId (never set),
// StartTimestamp (no field existed to set), and UseMiddleboxes (read off the
// request into a parameter the backend method signature explicitly discarded
// with `_`, so a real client's UseMiddleboxes: true request had zero effect
// and was never echoed back) (gopherstack-6flj).
func TestRouteAnalysis_OwnerAccountIDStartTimestampUseMiddleboxes(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	gn, err := client.CreateGlobalNetwork(ctx, &networkmanagersdk.CreateGlobalNetworkInput{})
	require.NoError(t, err)

	started, err := client.StartRouteAnalysis(ctx, &networkmanagersdk.StartRouteAnalysisInput{
		GlobalNetworkId:   gn.GlobalNetwork.GlobalNetworkId,
		Source:            &types.RouteAnalysisEndpointOptionsSpecification{IpAddress: aws.String("10.0.0.1")},
		Destination:       &types.RouteAnalysisEndpointOptionsSpecification{},
		UseMiddleboxes:    true,
		IncludeReturnPath: false,
	})
	require.NoError(t, err)
	assert.Equal(t, rtTestAccountID, aws.ToString(started.RouteAnalysis.OwnerAccountId))
	require.NotNil(t, started.RouteAnalysis.StartTimestamp)
	assert.True(t, started.RouteAnalysis.UseMiddleboxes)

	final, err := client.GetRouteAnalysis(ctx, &networkmanagersdk.GetRouteAnalysisInput{
		GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId, RouteAnalysisId: started.RouteAnalysis.RouteAnalysisId,
	})
	require.NoError(t, err)
	assert.Equal(t, rtTestAccountID, aws.ToString(final.RouteAnalysis.OwnerAccountId))
	require.NotNil(t, final.RouteAnalysis.StartTimestamp)
	assert.Equal(t, *started.RouteAnalysis.StartTimestamp, *final.RouteAnalysis.StartTimestamp)
	assert.True(t, final.RouteAnalysis.UseMiddleboxes)
}
