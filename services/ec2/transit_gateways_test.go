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

	tgw, err := b.CreateTransitGateway("test tgw")
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

func TestDescribeTransitGatewayAttachments(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	tgw, err := b.CreateTransitGateway("test tgw")
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

			tgw, err := b.CreateTransitGateway(tt.description)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, tgw.ID)
			assert.Equal(t, "available", tgw.State)

			// List returns the new gateway.
			tgws := b.DescribeTransitGateways(nil)
			assert.NotEmpty(t, tgws)

			// Delete.
			require.NoError(t, b.DeleteTransitGateway(tgw.ID))

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
