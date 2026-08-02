package networkmanager_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	networkmanagersdk "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/aws/aws-sdk-go-v2/service/networkmanager/types"
	"github.com/stretchr/testify/require"
)

// TestRoundTrip_CustomerGatewayAssociation drives family G.
func TestRoundTrip_CustomerGatewayAssociation(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	gn, err := client.CreateGlobalNetwork(ctx, &networkmanagersdk.CreateGlobalNetworkInput{})
	require.NoError(t, err)

	device, err := client.CreateDevice(
		ctx,
		&networkmanagersdk.CreateDeviceInput{GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId},
	)
	require.NoError(t, err)

	cgwArn := "arn:aws:ec2:us-east-1:000000000000:customer-gateway/cgw-0123456789abcdef0"

	assoc, err := client.AssociateCustomerGateway(ctx, &networkmanagersdk.AssociateCustomerGatewayInput{
		GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId, CustomerGatewayArn: aws.String(cgwArn),
		DeviceId: device.Device.DeviceId,
	})
	require.NoError(t, err)
	require.Equal(t, cgwArn, aws.ToString(assoc.CustomerGatewayAssociation.CustomerGatewayArn))

	require.Eventually(t, func() bool {
		l, listErr := client.GetCustomerGatewayAssociations(ctx, &networkmanagersdk.GetCustomerGatewayAssociationsInput{
			GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
		})

		return listErr == nil && len(l.CustomerGatewayAssociations) == 1 &&
			l.CustomerGatewayAssociations[0].State == types.CustomerGatewayAssociationStateAvailable
	}, defaultAsyncWait, defaultAsyncPoll)

	_, err = client.DisassociateCustomerGateway(ctx, &networkmanagersdk.DisassociateCustomerGatewayInput{
		GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId, CustomerGatewayArn: aws.String(cgwArn),
	})
	require.NoError(t, err)
}

// TestRoundTrip_TransitGatewayRegistration drives family H.
func TestRoundTrip_TransitGatewayRegistration(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	gn, err := client.CreateGlobalNetwork(ctx, &networkmanagersdk.CreateGlobalNetworkInput{})
	require.NoError(t, err)

	tgwArn := "arn:aws:ec2:us-east-1:000000000000:transit-gateway/tgw-0123456789abcdef0"

	reg, err := client.RegisterTransitGateway(ctx, &networkmanagersdk.RegisterTransitGatewayInput{
		GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId, TransitGatewayArn: aws.String(tgwArn),
	})
	require.NoError(t, err)
	require.Equal(t, types.TransitGatewayRegistrationStatePending, reg.TransitGatewayRegistration.State.Code)

	require.Eventually(t, func() bool {
		l, listErr := client.GetTransitGatewayRegistrations(ctx, &networkmanagersdk.GetTransitGatewayRegistrationsInput{
			GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
		})

		return listErr == nil && len(l.TransitGatewayRegistrations) == 1 &&
			l.TransitGatewayRegistrations[0].State.Code == types.TransitGatewayRegistrationStateAvailable
	}, defaultAsyncWait, defaultAsyncPoll)

	_, err = client.DeregisterTransitGateway(ctx, &networkmanagersdk.DeregisterTransitGatewayInput{
		GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId, TransitGatewayArn: aws.String(tgwArn),
	})
	require.NoError(t, err)
}

// TestRoundTrip_TransitGatewayConnectPeerAssociation drives family I.
func TestRoundTrip_TransitGatewayConnectPeerAssociation(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	gn, err := client.CreateGlobalNetwork(ctx, &networkmanagersdk.CreateGlobalNetworkInput{})
	require.NoError(t, err)

	device, err := client.CreateDevice(
		ctx,
		&networkmanagersdk.CreateDeviceInput{GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId},
	)
	require.NoError(t, err)

	tgwCpArn := "arn:aws:ec2:us-east-1:000000000000:transit-gateway-connect-peer/tgw-connect-peer-0123456789abcdef0"

	assoc, err := client.AssociateTransitGatewayConnectPeer(
		ctx,
		&networkmanagersdk.AssociateTransitGatewayConnectPeerInput{
			GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId, DeviceId: device.Device.DeviceId,
			TransitGatewayConnectPeerArn: aws.String(tgwCpArn),
		},
	)
	require.NoError(t, err)
	require.Equal(t, tgwCpArn, aws.ToString(assoc.TransitGatewayConnectPeerAssociation.TransitGatewayConnectPeerArn))

	_, err = client.DisassociateTransitGatewayConnectPeer(
		ctx, &networkmanagersdk.DisassociateTransitGatewayConnectPeerInput{
			GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId, TransitGatewayConnectPeerArn: aws.String(tgwCpArn),
		},
	)
	require.NoError(t, err)
}

// TestRoundTrip_ConnectPeerAssociation drives family J -- the concrete
// bridge between Global Networks and Cloud WAN: a Cloud WAN ConnectPeer
// (created via a Connect attachment) bound to a Global-Networks Device.
func TestRoundTrip_ConnectPeerAssociation(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	gn, err := client.CreateGlobalNetwork(ctx, &networkmanagersdk.CreateGlobalNetworkInput{})
	require.NoError(t, err)

	device, err := client.CreateDevice(
		ctx,
		&networkmanagersdk.CreateDeviceInput{GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId},
	)
	require.NoError(t, err)

	cn, err := client.CreateCoreNetwork(ctx, &networkmanagersdk.CreateCoreNetworkInput{
		GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
	})
	require.NoError(t, err)

	vpcAttachment, err := client.CreateVpcAttachment(ctx, &networkmanagersdk.CreateVpcAttachmentInput{
		CoreNetworkId: cn.CoreNetwork.CoreNetworkId,
		VpcArn:        aws.String("arn:aws:ec2:us-east-1:000000000000:vpc/vpc-0123456789abcdef0"),
		SubnetArns:    []string{"arn:aws:ec2:us-east-1:000000000000:subnet/subnet-0123456789abcdef0"},
	})
	require.NoError(t, err)

	connectAttachment, err := client.CreateConnectAttachment(ctx, &networkmanagersdk.CreateConnectAttachmentInput{
		CoreNetworkId: cn.CoreNetwork.CoreNetworkId, EdgeLocation: aws.String("us-east-1"),
		Options:               &types.ConnectAttachmentOptions{Protocol: types.TunnelProtocolNoEncap},
		TransportAttachmentId: vpcAttachment.VpcAttachment.Attachment.AttachmentId,
	})
	require.NoError(t, err)

	connectPeer, err := client.CreateConnectPeer(ctx, &networkmanagersdk.CreateConnectPeerInput{
		ConnectAttachmentId: connectAttachment.ConnectAttachment.Attachment.AttachmentId,
		PeerAddress:         aws.String("10.0.0.1"),
	})
	require.NoError(t, err)

	assoc, err := client.AssociateConnectPeer(ctx, &networkmanagersdk.AssociateConnectPeerInput{
		GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId, DeviceId: device.Device.DeviceId,
		ConnectPeerId: connectPeer.ConnectPeer.ConnectPeerId,
	})
	require.NoError(t, err)
	require.Equal(
		t,
		aws.ToString(connectPeer.ConnectPeer.ConnectPeerId),
		aws.ToString(assoc.ConnectPeerAssociation.ConnectPeerId),
	)

	require.Eventually(t, func() bool {
		l, listErr := client.GetConnectPeerAssociations(ctx, &networkmanagersdk.GetConnectPeerAssociationsInput{
			GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId,
		})

		return listErr == nil && len(l.ConnectPeerAssociations) == 1 &&
			l.ConnectPeerAssociations[0].State == types.ConnectPeerAssociationStateAvailable
	}, defaultAsyncWait, defaultAsyncPoll)

	_, err = client.DisassociateConnectPeer(ctx, &networkmanagersdk.DisassociateConnectPeerInput{
		GlobalNetworkId: gn.GlobalNetwork.GlobalNetworkId, ConnectPeerId: connectPeer.ConnectPeer.ConnectPeerId,
	})
	require.NoError(t, err)
}
