package networkmanager_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	networkmanagersdk "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/aws/aws-sdk-go-v2/service/networkmanager/types"
	"github.com/stretchr/testify/require"
)

// TestRoundTrip_VpcAttachmentLifecycle drives family Q + Q1: Create ->
// Accept -> Get -> Update -> List -> Delete, exercising the generic
// Accept/Reject/Delete/List lifecycle every attachment subtype shares.
func TestRoundTrip_VpcAttachmentLifecycle(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	cn := createTestCoreNetwork(t, client)

	created, err := client.CreateVpcAttachment(ctx, &networkmanagersdk.CreateVpcAttachmentInput{
		CoreNetworkId: cn.CoreNetwork.CoreNetworkId,
		VpcArn:        aws.String("arn:aws:ec2:us-east-1:000000000000:vpc/vpc-0123456789abcdef0"),
		SubnetArns:    []string{"arn:aws:ec2:us-east-1:000000000000:subnet/subnet-aaa"},
	})
	require.NoError(t, err)
	require.Equal(t, types.AttachmentStatePendingAttachmentAcceptance, created.VpcAttachment.Attachment.State)
	require.True(t, aws.ToBool(created.VpcAttachment.Options.SecurityGroupReferencingSupport))

	attachmentID := created.VpcAttachment.Attachment.AttachmentId

	accepted, err := client.AcceptAttachment(ctx, &networkmanagersdk.AcceptAttachmentInput{AttachmentId: attachmentID})
	require.NoError(t, err)
	require.Equal(t, types.AttachmentStateCreating, accepted.Attachment.State)

	require.Eventually(t, func() bool {
		g, getErr := client.GetVpcAttachment(ctx, &networkmanagersdk.GetVpcAttachmentInput{AttachmentId: attachmentID})

		return getErr == nil && g.VpcAttachment.Attachment.State == types.AttachmentStateAvailable
	}, defaultAsyncWait, defaultAsyncPoll)

	updated, err := client.UpdateVpcAttachment(ctx, &networkmanagersdk.UpdateVpcAttachmentInput{
		AttachmentId:  attachmentID,
		AddSubnetArns: []string{"arn:aws:ec2:us-east-1:000000000000:subnet/subnet-bbb"},
	})
	require.NoError(t, err)
	require.Len(t, updated.VpcAttachment.SubnetArns, 2)

	listed, err := client.ListAttachments(ctx, &networkmanagersdk.ListAttachmentsInput{
		CoreNetworkId: cn.CoreNetwork.CoreNetworkId,
	})
	require.NoError(t, err)
	require.Len(t, listed.Attachments, 1)

	_, err = client.DeleteAttachment(ctx, &networkmanagersdk.DeleteAttachmentInput{AttachmentId: attachmentID})
	require.NoError(t, err)
}

// TestRoundTrip_RejectAttachment confirms RejectAttachment resolves a
// pending attachment to the terminal REJECTED state.
func TestRoundTrip_RejectAttachment(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	cn := createTestCoreNetwork(t, client)

	created, err := client.CreateSiteToSiteVpnAttachment(ctx, &networkmanagersdk.CreateSiteToSiteVpnAttachmentInput{
		CoreNetworkId:    cn.CoreNetwork.CoreNetworkId,
		VpnConnectionArn: aws.String("arn:aws:ec2:us-east-1:000000000000:vpn-connection/vpn-0123456789abcdef0"),
	})
	require.NoError(t, err)

	rejected, err := client.RejectAttachment(ctx, &networkmanagersdk.RejectAttachmentInput{
		AttachmentId: created.SiteToSiteVpnAttachment.Attachment.AttachmentId,
	})
	require.NoError(t, err)
	require.Equal(t, types.AttachmentStateRejected, rejected.Attachment.State)

	// Rejecting again fails -- no longer PENDING_ATTACHMENT_ACCEPTANCE.
	_, err = client.RejectAttachment(ctx, &networkmanagersdk.RejectAttachmentInput{
		AttachmentId: created.SiteToSiteVpnAttachment.Attachment.AttachmentId,
	})
	require.Error(t, err)
}

// TestRoundTrip_DirectConnectGatewayAttachment drives family Q4.
// DirectConnectGatewayArn is accepted unvalidated, since services/directconnect
// has no reachable backend reference from this package -- see attachments.go.
func TestRoundTrip_DirectConnectGatewayAttachment(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	cn := createTestCoreNetwork(t, client)

	created, err := client.CreateDirectConnectGatewayAttachment(
		ctx, &networkmanagersdk.CreateDirectConnectGatewayAttachmentInput{
			CoreNetworkId:           cn.CoreNetwork.CoreNetworkId,
			DirectConnectGatewayArn: aws.String("arn:aws:directconnect::000000000000:dx-gateway/abcd1234"),
			EdgeLocations:           []string{"us-east-1"},
		},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"us-east-1"}, created.DirectConnectGatewayAttachment.Attachment.EdgeLocations)

	updated, err := client.UpdateDirectConnectGatewayAttachment(
		ctx, &networkmanagersdk.UpdateDirectConnectGatewayAttachmentInput{
			AttachmentId:  created.DirectConnectGatewayAttachment.Attachment.AttachmentId,
			EdgeLocations: []string{"us-east-1", "us-west-2"},
		},
	)
	require.NoError(t, err)
	require.Len(t, updated.DirectConnectGatewayAttachment.Attachment.EdgeLocations, 2)

	got, err := client.GetDirectConnectGatewayAttachment(ctx, &networkmanagersdk.GetDirectConnectGatewayAttachmentInput{
		AttachmentId: created.DirectConnectGatewayAttachment.Attachment.AttachmentId,
	})
	require.NoError(t, err)
	require.Len(t, got.DirectConnectGatewayAttachment.Attachment.EdgeLocations, 2)
}

// TestRoundTrip_TransitGatewayRouteTableAttachment drives family Q5, which
// requires an existing Peering (not a bare TransitGatewayArn).
func TestRoundTrip_TransitGatewayRouteTableAttachment(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	cn := createTestCoreNetwork(t, client)

	peering, err := client.CreateTransitGatewayPeering(ctx, &networkmanagersdk.CreateTransitGatewayPeeringInput{
		CoreNetworkId:     cn.CoreNetwork.CoreNetworkId,
		TransitGatewayArn: aws.String("arn:aws:ec2:us-east-1:000000000000:transit-gateway/tgw-0123456789abcdef0"),
	})
	require.NoError(t, err)

	created, err := client.CreateTransitGatewayRouteTableAttachment(
		ctx, &networkmanagersdk.CreateTransitGatewayRouteTableAttachmentInput{
			PeeringId: peering.TransitGatewayPeering.Peering.PeeringId,
			TransitGatewayRouteTableArn: aws.String(
				"arn:aws:ec2:us-east-1:000000000000:transit-gateway-route-table/tgw-rtb-0123456789abcdef0",
			),
		},
	)
	require.NoError(t, err)
	require.Equal(
		t, aws.ToString(peering.TransitGatewayPeering.Peering.PeeringId),
		aws.ToString(created.TransitGatewayRouteTableAttachment.PeeringId),
	)

	got, err := client.GetTransitGatewayRouteTableAttachment(
		ctx, &networkmanagersdk.GetTransitGatewayRouteTableAttachmentInput{
			AttachmentId: created.TransitGatewayRouteTableAttachment.Attachment.AttachmentId,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, got.TransitGatewayRouteTableAttachment)

	// A nonexistent PeeringId fails.
	_, err = client.CreateTransitGatewayRouteTableAttachment(
		ctx, &networkmanagersdk.CreateTransitGatewayRouteTableAttachmentInput{
			PeeringId: aws.String("nonexistent"),
			TransitGatewayRouteTableArn: aws.String(
				"arn:aws:ec2:us-east-1:000000000000:transit-gateway-route-table/tgw-rtb-x",
			),
		},
	)
	require.Error(t, err)
}
