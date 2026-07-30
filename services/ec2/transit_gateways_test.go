package ec2_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ec2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransitGatewayRoutePropagation(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	tgw, err := b.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test tgw"})
	require.NoError(t, err)
	rt, err := b.CreateTransitGatewayRouteTable(tgw.ID)
	require.NoError(t, err)
	att, err := b.CreateTransitGatewayVpcAttachment(tgw.ID, "vpc-default", nil)
	require.NoError(t, err)

	// Before enabling, both getters return empty.
	props, err := b.GetTransitGatewayRouteTablePropagations(rt.RouteTableID)
	require.NoError(t, err)
	assert.Empty(t, props)

	attProps, err := b.GetTransitGatewayAttachmentPropagations(att.TransitGatewayAttachmentID)
	require.NoError(t, err)
	assert.Empty(t, attProps)

	prop, err := b.EnableTransitGatewayRouteTablePropagation(rt.RouteTableID, att.TransitGatewayAttachmentID)
	require.NoError(t, err)
	assert.Equal(t, "enabled", prop.State)
	assert.Equal(t, "vpc", prop.ResourceType)
	assert.Equal(t, "vpc-default", prop.ResourceID)

	props, err = b.GetTransitGatewayRouteTablePropagations(rt.RouteTableID)
	require.NoError(t, err)
	require.Len(t, props, 1)
	assert.Equal(t, att.TransitGatewayAttachmentID, props[0].TransitGatewayAttachmentID)

	attProps, err = b.GetTransitGatewayAttachmentPropagations(att.TransitGatewayAttachmentID)
	require.NoError(t, err)
	require.Len(t, attProps, 1)
	assert.Equal(t, rt.RouteTableID, attProps[0].TransitGatewayRouteTableID)
	assert.Equal(t, "enabled", attProps[0].State)

	disabled, err := b.DisableTransitGatewayRouteTablePropagation(rt.RouteTableID, att.TransitGatewayAttachmentID)
	require.NoError(t, err)
	assert.Equal(t, "disabled", disabled.State)

	props, err = b.GetTransitGatewayRouteTablePropagations(rt.RouteTableID)
	require.NoError(t, err)
	assert.Empty(t, props)

	_, err = b.DisableTransitGatewayRouteTablePropagation(rt.RouteTableID, att.TransitGatewayAttachmentID)
	require.ErrorIs(t, err, ec2.ErrTGWPropagationNotFound)

	_, err = b.EnableTransitGatewayRouteTablePropagation("tgw-rtb-missing", att.TransitGatewayAttachmentID)
	require.ErrorIs(t, err, ec2.ErrTGWRouteTableNotFound)

	_, err = b.EnableTransitGatewayRouteTablePropagation(rt.RouteTableID, "tgw-attach-missing")
	require.ErrorIs(t, err, ec2.ErrTGWAttachmentNotFound)
}

// TestTransitGatewayRouteTableOps_ClientVpnAttachment proves the
// transitGatewayAttachmentExistsLocked existence check (shared by
// EnableTransitGatewayRouteTablePropagation, GetTransitGatewayAttachmentPropagations,
// and now AssociateTransitGatewayRouteTable/DisassociateTransitGatewayRouteTable)
// recognizes a real Client VPN TGW attachment. Before this pass, that helper
// only checked the VPC/peering/Connect attachment maps -- added when TGW
// Client VPN attachments were introduced, it was never wired in, so a real,
// existing Client VPN attachment ID was wrongly reported as
// ErrTGWAttachmentNotFound (gopherstack-8pce).
func TestTransitGatewayRouteTableOps_ClientVpnAttachment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "client-vpn attachment recognized by propagation and association ops"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			tgw, err := b.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test tgw"})
			require.NoError(t, err)

			rt, err := b.CreateTransitGatewayRouteTable(tgw.ID)
			require.NoError(t, err)

			_, err = b.CreateClientVpnEndpointWithOptions(
				"10.10.0.0/22", "cvpn-tgw-test", nil,
				ec2.ClientVpnEndpointOptions{TransitGatewayID: tgw.ID},
			)
			require.NoError(t, err)

			// Locate the implicitly-created Client VPN attachment via the
			// unified attachment view.
			var attachmentID string

			for _, att := range b.DescribeTransitGatewayAttachments(nil) {
				if att.ResourceType == "client-vpn" {
					attachmentID = att.TransitGatewayAttachmentID
				}
			}

			require.NotEmpty(t, attachmentID, "expected an implicitly-created client-vpn attachment")

			prop, err := b.EnableTransitGatewayRouteTablePropagation(rt.RouteTableID, attachmentID)
			require.NoError(t, err)
			assert.Equal(t, "client-vpn", prop.ResourceType)

			attProps, err := b.GetTransitGatewayAttachmentPropagations(attachmentID)
			require.NoError(t, err)
			assert.Len(t, attProps, 1)

			assoc, err := b.AssociateTransitGatewayRouteTable(rt.RouteTableID, attachmentID)
			require.NoError(t, err)
			assert.Equal(t, "client-vpn", assoc.ResourceType)
		})
	}
}

func TestDescribeTransitGatewayAttachments(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	tgw, err := b.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "test tgw"})
	require.NoError(t, err)
	att, err := b.CreateTransitGatewayVpcAttachment(tgw.ID, "vpc-default", nil)
	require.NoError(t, err)

	all := b.DescribeTransitGatewayAttachments(nil)
	require.NotEmpty(t, all)

	found := b.DescribeTransitGatewayAttachments([]string{att.TransitGatewayAttachmentID})
	require.Len(t, found, 1)
	assert.Equal(t, "vpc", found[0].ResourceType)
	assert.Equal(t, "vpc-default", found[0].ResourceID)
	assert.Equal(t, tgw.ID, found[0].TransitGatewayID)
}

// ---- Capacity Reservation extras ----

// TestTransitGateway tests transit gateway CRUD.
func TestTransitGateway(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		description string
		wantErr     bool
	}{
		{name: "create_tgw", description: "my transit gateway", wantErr: false},
		{name: "empty_description", description: "", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

			tgw, err := b.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: tt.description})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, tgw.ID)
			assert.Equal(t, "available", tgw.State)
			// ARN, CreationTime, and Options (with real AWS documented
			// defaults) were previously entirely absent from the backend
			// model.
			assert.Contains(t, tgw.Arn, tgw.ID)
			assert.False(t, tgw.CreationTime.IsZero())
			assert.EqualValues(t, 64512, tgw.Options.AmazonSideAsn)
			assert.Equal(t, "disable", tgw.Options.AutoAcceptSharedAttachments)
			assert.Equal(t, "enable", tgw.Options.DefaultRouteTableAssociation)
			assert.Equal(t, "enable", tgw.Options.DefaultRouteTablePropagation)
			assert.Equal(t, "enable", tgw.Options.DNSSupport)
			assert.Equal(t, "enable", tgw.Options.VpnEcmpSupport)

			// Creating a second transit gateway in the same account must not
			// collide: the ID used to be deterministically derived from the
			// account ID alone ("tgw-" + accountID[:8]), so a second create
			// silently overwrote the first in the backend's table.
			second, err := b.CreateTransitGateway(ec2.CreateTransitGatewayParams{Description: "second"})
			require.NoError(t, err)
			assert.NotEqual(t, tgw.ID, second.ID)
			assert.Len(t, b.DescribeTransitGateways(nil), 2)

			// List returns the new gateway.
			tgws := b.DescribeTransitGateways(nil)
			assert.NotEmpty(t, tgws)

			// Delete.
			deleted, deleteErr := b.DeleteTransitGateway(tgw.ID)
			require.NoError(t, deleteErr)
			assert.Equal(t, "deleting", deleted.State)

			tgws = b.DescribeTransitGateways([]string{tgw.ID})
			assert.Empty(t, tgws)
		})
	}
}

// TestHandlerReplaceNetworkACLEntry verifies the HTTP handler.

// TestHandlerCreateTransitGateway verifies TGW creation handler.
func TestHandlerCreateTransitGateway(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, "Action=CreateTransitGateway&Version=2016-11-15&Description=test-tgw")
	assert.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "CreateTransitGatewayResponse")
	assert.Contains(t, rec.Body.String(), "available")
}
