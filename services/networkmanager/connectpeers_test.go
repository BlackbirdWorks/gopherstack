package networkmanager_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	networkmanagersdk "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/aws/aws-sdk-go-v2/service/networkmanager/types"
	"github.com/stretchr/testify/require"
)

func createTestConnectAttachment(t *testing.T, client *networkmanagersdk.Client, coreNetworkID *string) *string {
	t.Helper()

	ctx := t.Context()

	vpcAttachment, err := client.CreateVpcAttachment(ctx, &networkmanagersdk.CreateVpcAttachmentInput{
		CoreNetworkId: coreNetworkID,
		VpcArn:        aws.String("arn:aws:ec2:us-east-1:000000000000:vpc/vpc-0123456789abcdef0"),
		SubnetArns:    []string{"arn:aws:ec2:us-east-1:000000000000:subnet/subnet-0123456789abcdef0"},
	})
	require.NoError(t, err)

	connectAttachment, err := client.CreateConnectAttachment(ctx, &networkmanagersdk.CreateConnectAttachmentInput{
		CoreNetworkId: coreNetworkID, EdgeLocation: aws.String("us-east-1"),
		Options:               &types.ConnectAttachmentOptions{Protocol: types.TunnelProtocolGre},
		TransportAttachmentId: vpcAttachment.VpcAttachment.Attachment.AttachmentId,
	})
	require.NoError(t, err)

	return connectAttachment.ConnectAttachment.Attachment.AttachmentId
}

// TestRoundTrip_ConnectPeerLifecycle drives family K: Create -> Get ->
// List -> Delete, including the GRE BgpOptions path.
func TestRoundTrip_ConnectPeerLifecycle(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	cn := createTestCoreNetwork(t, client)
	connectAttachmentID := createTestConnectAttachment(t, client, cn.CoreNetwork.CoreNetworkId)

	created, err := client.CreateConnectPeer(ctx, &networkmanagersdk.CreateConnectPeerInput{
		ConnectAttachmentId: connectAttachmentID,
		PeerAddress:         aws.String("10.0.0.1"),
		BgpOptions:          &types.BgpOptions{PeerAsn: aws.Int64(65000)},
		InsideCidrBlocks:    []string{"169.254.0.0/29"},
	})
	require.NoError(t, err)
	require.Equal(t, types.TunnelProtocolGre, created.ConnectPeer.Configuration.Protocol)
	require.Len(t, created.ConnectPeer.Configuration.BgpConfigurations, 1)
	require.Equal(t, int64(65000), aws.ToInt64(created.ConnectPeer.Configuration.BgpConfigurations[0].PeerAsn))

	connectPeerID := created.ConnectPeer.ConnectPeerId

	require.Eventually(t, func() bool {
		g, getErr := client.GetConnectPeer(ctx, &networkmanagersdk.GetConnectPeerInput{ConnectPeerId: connectPeerID})

		return getErr == nil && g.ConnectPeer.State == types.ConnectPeerStateAvailable
	}, defaultAsyncWait, defaultAsyncPoll)

	listed, err := client.ListConnectPeers(ctx, &networkmanagersdk.ListConnectPeersInput{
		ConnectAttachmentId: connectAttachmentID,
	})
	require.NoError(t, err)
	require.Len(t, listed.ConnectPeers, 1)

	deleted, err := client.DeleteConnectPeer(
		ctx,
		&networkmanagersdk.DeleteConnectPeerInput{ConnectPeerId: connectPeerID},
	)
	require.NoError(t, err)
	require.Equal(t, types.ConnectPeerStateDeleting, deleted.ConnectPeer.State)

	// CreateConnectPeer requires a real CONNECT attachment -- a VPC
	// attachment ID is rejected.
	vpcAttachment, err := client.CreateVpcAttachment(ctx, &networkmanagersdk.CreateVpcAttachmentInput{
		CoreNetworkId: cn.CoreNetwork.CoreNetworkId,
		VpcArn:        aws.String("arn:aws:ec2:us-east-1:000000000000:vpc/vpc-1"),
		SubnetArns:    []string{"arn:aws:ec2:us-east-1:000000000000:subnet/subnet-1"},
	})
	require.NoError(t, err)

	_, err = client.CreateConnectPeer(ctx, &networkmanagersdk.CreateConnectPeerInput{
		ConnectAttachmentId: vpcAttachment.VpcAttachment.Attachment.AttachmentId,
		PeerAddress:         aws.String("10.0.0.2"),
	})
	require.Error(t, err)
}
