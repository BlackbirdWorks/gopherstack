package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	nmsvc "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	nmtypes "github.com/aws/aws-sdk-go-v2/service/networkmanager/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_MegaBatch34 provisions Network Manager core network, policy
// attachment, VPC/Connect/site-to-site-VPN/Direct-Connect-gateway/transit-
// gateway-route-table attachments, an attachment accepter for each, a
// Connect peer, transit gateway peering/registration, and customer gateway
// and link associations via Terraform, verifying each through the Network
// Manager SDK client.
func TestTerraform_MegaBatch34(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "mega-batch-34",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyMegaBatch34NetworkManager(ctx, t)
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

func verifyMegaBatch34NetworkManager(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := nmsvc.NewFromConfig(cfg, func(o *nmsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	gnOut, err := client.DescribeGlobalNetworks(ctx, &nmsvc.DescribeGlobalNetworksInput{})
	require.NoError(t, err, "DescribeGlobalNetworks should succeed")
	require.NotEmpty(t, gnOut.GlobalNetworks)

	var globalNetworkID string

	for _, gn := range gnOut.GlobalNetworks {
		if aws.ToString(gn.Description) == "mega-batch-34 global network" {
			globalNetworkID = aws.ToString(gn.GlobalNetworkId)
		}
	}

	require.NotEmpty(t, globalNetworkID, "global network should be listed")

	cnOut, err := client.ListCoreNetworks(ctx, &nmsvc.ListCoreNetworksInput{})
	require.NoError(t, err, "ListCoreNetworks should succeed")
	require.NotEmpty(t, cnOut.CoreNetworks)

	coreNetworkID := aws.ToString(cnOut.CoreNetworks[0].CoreNetworkId)

	policyOut, err := client.GetCoreNetworkPolicy(ctx, &nmsvc.GetCoreNetworkPolicyInput{
		CoreNetworkId: aws.String(coreNetworkID),
	})
	require.NoError(t, err, "GetCoreNetworkPolicy should succeed")
	require.NotNil(t, policyOut.CoreNetworkPolicy)
	assert.Equal(t, nmtypes.ChangeSetStateExecutionSucceeded, policyOut.CoreNetworkPolicy.ChangeSetState)

	attachOut, err := client.ListAttachments(ctx, &nmsvc.ListAttachmentsInput{
		CoreNetworkId: aws.String(coreNetworkID),
	})
	require.NoError(t, err, "ListAttachments should succeed")

	byType := map[nmtypes.AttachmentType][]nmtypes.Attachment{}
	for _, a := range attachOut.Attachments {
		byType[a.AttachmentType] = append(byType[a.AttachmentType], a)
	}

	for _, at := range []nmtypes.AttachmentType{
		nmtypes.AttachmentTypeVpc,
		nmtypes.AttachmentTypeConnect,
		nmtypes.AttachmentTypeSiteToSiteVpn,
		nmtypes.AttachmentTypeDirectConnectGateway,
		nmtypes.AttachmentTypeTransitGatewayRouteTable,
	} {
		require.NotEmptyf(t, byType[at], "attachment type %s should be listed", at)
		assert.Equalf(t, nmtypes.AttachmentStateAvailable, byType[at][0].State,
			"attachment type %s should be AVAILABLE after acceptance", at)
	}

	connectAttachmentID := aws.ToString(byType[nmtypes.AttachmentTypeConnect][0].AttachmentId)

	peersOut, err := client.ListConnectPeers(ctx, &nmsvc.ListConnectPeersInput{
		ConnectAttachmentId: aws.String(connectAttachmentID),
	})
	require.NoError(t, err, "ListConnectPeers should succeed")
	require.NotEmpty(t, peersOut.ConnectPeers)

	peeringsOut, err := client.ListPeerings(ctx, &nmsvc.ListPeeringsInput{
		CoreNetworkId: aws.String(coreNetworkID),
	})
	require.NoError(t, err, "ListPeerings should succeed")
	require.NotEmpty(t, peeringsOut.Peerings)
	assert.Equal(t, nmtypes.PeeringTypeTransitGateway, peeringsOut.Peerings[0].PeeringType)

	tgwRegOut, err := client.GetTransitGatewayRegistrations(ctx, &nmsvc.GetTransitGatewayRegistrationsInput{
		GlobalNetworkId: aws.String(globalNetworkID),
	})
	require.NoError(t, err, "GetTransitGatewayRegistrations should succeed")
	require.NotEmpty(t, tgwRegOut.TransitGatewayRegistrations)

	sitesOut, err := client.GetSites(ctx, &nmsvc.GetSitesInput{
		GlobalNetworkId: aws.String(globalNetworkID),
	})
	require.NoError(t, err, "GetSites should succeed")
	require.NotEmpty(t, sitesOut.Sites)

	devicesOut, err := client.GetDevices(ctx, &nmsvc.GetDevicesInput{
		GlobalNetworkId: aws.String(globalNetworkID),
	})
	require.NoError(t, err, "GetDevices should succeed")
	require.NotEmpty(t, devicesOut.Devices)

	deviceID := aws.ToString(devicesOut.Devices[0].DeviceId)

	cgwAssocOut, err := client.GetCustomerGatewayAssociations(ctx, &nmsvc.GetCustomerGatewayAssociationsInput{
		GlobalNetworkId: aws.String(globalNetworkID),
	})
	require.NoError(t, err, "GetCustomerGatewayAssociations should succeed")
	require.NotEmpty(t, cgwAssocOut.CustomerGatewayAssociations)

	linkAssocOut, err := client.GetLinkAssociations(ctx, &nmsvc.GetLinkAssociationsInput{
		GlobalNetworkId: aws.String(globalNetworkID),
		DeviceId:        aws.String(deviceID),
	})
	require.NoError(t, err, "GetLinkAssociations should succeed")
	require.NotEmpty(t, linkAssocOut.LinkAssociations)
}
