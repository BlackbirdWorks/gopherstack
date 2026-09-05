package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestDescribeTransitGatewayAttachments_IdFilter_RealClient covers
// handleDescribeTransitGatewayAttachments. TransitGatewayAttachmentIds is
// serialized as the flat key "TransitGatewayAttachmentIds.N" (ec2@v1.319.1
// serializers.go:81579, awsEc2query_serializeOpDocumentDescribeTransitGatewayAttachmentsInput),
// not "TransitGatewayAttachmentId.N". The handler read the singular key, a
// key a real client never sends, so the ID filter was always silently
// ignored and every call returned every attachment.
func TestDescribeTransitGatewayAttachments_IdFilter_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1")))
	ctx := t.Context()

	tgw, err := client.CreateTransitGateway(ctx, &ec2sdk.CreateTransitGatewayInput{})
	require.NoError(t, err)

	attA, err := client.CreateTransitGatewayVpcAttachment(ctx, &ec2sdk.CreateTransitGatewayVpcAttachmentInput{
		TransitGatewayId: tgw.TransitGateway.TransitGatewayId,
		VpcId:            aws.String("vpc-aaaaaaaa"),
		SubnetIds:        []string{"subnet-aaaaaaaa"},
	})
	require.NoError(t, err)

	_, err = client.CreateTransitGatewayVpcAttachment(ctx, &ec2sdk.CreateTransitGatewayVpcAttachmentInput{
		TransitGatewayId: tgw.TransitGateway.TransitGatewayId,
		VpcId:            aws.String("vpc-bbbbbbbb"),
		SubnetIds:        []string{"subnet-bbbbbbbb"},
	})
	require.NoError(t, err)

	out, err := client.DescribeTransitGatewayAttachments(ctx, &ec2sdk.DescribeTransitGatewayAttachmentsInput{
		TransitGatewayAttachmentIds: []string{
			aws.ToString(attA.TransitGatewayVpcAttachment.TransitGatewayAttachmentId),
		},
	})
	require.NoError(t, err)
	require.Len(t, out.TransitGatewayAttachments, 1,
		"TransitGatewayAttachmentIds filter ignored - returned every attachment")
	assert.Equal(t,
		aws.ToString(attA.TransitGatewayVpcAttachment.TransitGatewayAttachmentId),
		aws.ToString(out.TransitGatewayAttachments[0].TransitGatewayAttachmentId))
}

// TestDescribeTransitGatewayConnects_IdFilter_RealClient covers
// handleDescribeTransitGatewayConnects. TransitGatewayAttachmentIds is
// serialized as the flat key "TransitGatewayAttachmentIds.N" (ec2@v1.319.1
// serializers.go:81579), not "TransitGatewayAttachmentId.N". The handler
// read the singular key, so the ID filter was always silently ignored.
func TestDescribeTransitGatewayConnects_IdFilter_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1")))
	ctx := t.Context()

	tgw, err := client.CreateTransitGateway(ctx, &ec2sdk.CreateTransitGatewayInput{})
	require.NoError(t, err)

	transportA, err := client.CreateTransitGatewayVpcAttachment(ctx, &ec2sdk.CreateTransitGatewayVpcAttachmentInput{
		TransitGatewayId: tgw.TransitGateway.TransitGatewayId,
		VpcId:            aws.String("vpc-aaaaaaaa"),
		SubnetIds:        []string{"subnet-aaaaaaaa"},
	})
	require.NoError(t, err)

	transportB, err := client.CreateTransitGatewayVpcAttachment(ctx, &ec2sdk.CreateTransitGatewayVpcAttachmentInput{
		TransitGatewayId: tgw.TransitGateway.TransitGatewayId,
		VpcId:            aws.String("vpc-bbbbbbbb"),
		SubnetIds:        []string{"subnet-bbbbbbbb"},
	})
	require.NoError(t, err)

	connA, err := client.CreateTransitGatewayConnect(ctx, &ec2sdk.CreateTransitGatewayConnectInput{
		TransportTransitGatewayAttachmentId: transportA.TransitGatewayVpcAttachment.TransitGatewayAttachmentId,
		Options: &types.CreateTransitGatewayConnectRequestOptions{
			Protocol: types.ProtocolValueGre,
		},
	})
	require.NoError(t, err)

	_, err = client.CreateTransitGatewayConnect(ctx, &ec2sdk.CreateTransitGatewayConnectInput{
		TransportTransitGatewayAttachmentId: transportB.TransitGatewayVpcAttachment.TransitGatewayAttachmentId,
		Options: &types.CreateTransitGatewayConnectRequestOptions{
			Protocol: types.ProtocolValueGre,
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeTransitGatewayConnects(ctx, &ec2sdk.DescribeTransitGatewayConnectsInput{
		TransitGatewayAttachmentIds: []string{aws.ToString(connA.TransitGatewayConnect.TransitGatewayAttachmentId)},
	})
	require.NoError(t, err)
	require.Len(t, out.TransitGatewayConnects, 1,
		"TransitGatewayAttachmentIds filter ignored - returned every Connect attachment")
	assert.Equal(t,
		aws.ToString(connA.TransitGatewayConnect.TransitGatewayAttachmentId),
		aws.ToString(out.TransitGatewayConnects[0].TransitGatewayAttachmentId))
}

// TestDescribeTransitGatewayConnectPeers_IdFilter_RealClient covers
// handleDescribeTransitGatewayConnectPeers. TransitGatewayConnectPeerIds is
// serialized as the flat key "TransitGatewayConnectPeerIds.N" (ec2@v1.319.1
// serializers.go:81541), not "TransitGatewayConnectPeerId.N". The handler
// read the singular key, so the ID filter was always silently ignored.
func TestDescribeTransitGatewayConnectPeers_IdFilter_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1")))
	ctx := t.Context()

	tgw, err := client.CreateTransitGateway(ctx, &ec2sdk.CreateTransitGatewayInput{})
	require.NoError(t, err)

	transport, err := client.CreateTransitGatewayVpcAttachment(ctx, &ec2sdk.CreateTransitGatewayVpcAttachmentInput{
		TransitGatewayId: tgw.TransitGateway.TransitGatewayId,
		VpcId:            aws.String("vpc-aaaaaaaa"),
		SubnetIds:        []string{"subnet-aaaaaaaa"},
	})
	require.NoError(t, err)

	conn, err := client.CreateTransitGatewayConnect(ctx, &ec2sdk.CreateTransitGatewayConnectInput{
		TransportTransitGatewayAttachmentId: transport.TransitGatewayVpcAttachment.TransitGatewayAttachmentId,
		Options: &types.CreateTransitGatewayConnectRequestOptions{
			Protocol: types.ProtocolValueGre,
		},
	})
	require.NoError(t, err)

	peerA, err := client.CreateTransitGatewayConnectPeer(ctx, &ec2sdk.CreateTransitGatewayConnectPeerInput{
		TransitGatewayAttachmentId: conn.TransitGatewayConnect.TransitGatewayAttachmentId,
		PeerAddress:                aws.String("169.254.6.1"),
		InsideCidrBlocks:           []string{"169.254.6.0/29"},
	})
	require.NoError(t, err)

	_, err = client.CreateTransitGatewayConnectPeer(ctx, &ec2sdk.CreateTransitGatewayConnectPeerInput{
		TransitGatewayAttachmentId: conn.TransitGatewayConnect.TransitGatewayAttachmentId,
		PeerAddress:                aws.String("169.254.7.1"),
		InsideCidrBlocks:           []string{"169.254.7.0/29"},
	})
	require.NoError(t, err)

	out, err := client.DescribeTransitGatewayConnectPeers(ctx, &ec2sdk.DescribeTransitGatewayConnectPeersInput{
		TransitGatewayConnectPeerIds: []string{
			aws.ToString(peerA.TransitGatewayConnectPeer.TransitGatewayConnectPeerId),
		},
	})
	require.NoError(t, err)
	require.Len(t, out.TransitGatewayConnectPeers, 1,
		"TransitGatewayConnectPeerIds filter ignored - returned every Connect peer")
	assert.Equal(t,
		aws.ToString(peerA.TransitGatewayConnectPeer.TransitGatewayConnectPeerId),
		aws.ToString(out.TransitGatewayConnectPeers[0].TransitGatewayConnectPeerId))
}

// TestDescribeTransitGatewayPeeringAttachments_IdFilter_RealClient covers
// handleDescribeTransitGatewayPeeringAttachments. TransitGatewayAttachmentIds
// is serialized as the flat key "TransitGatewayAttachmentIds.N" (ec2@v1.319.1
// serializers.go:81579), not "TransitGatewayAttachmentId.N". The handler
// read the singular key, so the ID filter was always silently ignored.
func TestDescribeTransitGatewayPeeringAttachments_IdFilter_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1")))
	ctx := t.Context()

	tgw, err := client.CreateTransitGateway(ctx, &ec2sdk.CreateTransitGatewayInput{})
	require.NoError(t, err)

	peerA, err := client.CreateTransitGatewayPeeringAttachment(ctx, &ec2sdk.CreateTransitGatewayPeeringAttachmentInput{
		TransitGatewayId:     tgw.TransitGateway.TransitGatewayId,
		PeerTransitGatewayId: aws.String("tgw-aaaaaaaa"),
		PeerAccountId:        aws.String("111111111111"),
		PeerRegion:           aws.String("us-west-2"),
	})
	require.NoError(t, err)

	_, err = client.CreateTransitGatewayPeeringAttachment(ctx, &ec2sdk.CreateTransitGatewayPeeringAttachmentInput{
		TransitGatewayId:     tgw.TransitGateway.TransitGatewayId,
		PeerTransitGatewayId: aws.String("tgw-bbbbbbbb"),
		PeerAccountId:        aws.String("222222222222"),
		PeerRegion:           aws.String("us-west-2"),
	})
	require.NoError(t, err)

	out, err := client.DescribeTransitGatewayPeeringAttachments(
		ctx, &ec2sdk.DescribeTransitGatewayPeeringAttachmentsInput{
			TransitGatewayAttachmentIds: []string{
				aws.ToString(peerA.TransitGatewayPeeringAttachment.TransitGatewayAttachmentId),
			},
		})
	require.NoError(t, err)
	require.Len(t, out.TransitGatewayPeeringAttachments, 1,
		"TransitGatewayAttachmentIds filter ignored - returned every peering attachment")
	assert.Equal(t,
		aws.ToString(peerA.TransitGatewayPeeringAttachment.TransitGatewayAttachmentId),
		aws.ToString(out.TransitGatewayPeeringAttachments[0].TransitGatewayAttachmentId))
}

// TestDescribeTransitGatewayRouteTables_IdFilter_RealClient covers
// handleDescribeTransitGatewayRouteTables. TransitGatewayRouteTableIds is
// serialized as the flat key "TransitGatewayRouteTableIds.N" (ec2@v1.319.1
// serializers.go:81796), not "TransitGatewayRouteTableId.N". The handler
// read the singular key, so the ID filter was always silently ignored.
func TestDescribeTransitGatewayRouteTables_IdFilter_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1")))
	ctx := t.Context()

	tgw, err := client.CreateTransitGateway(ctx, &ec2sdk.CreateTransitGatewayInput{})
	require.NoError(t, err)

	rtA, err := client.CreateTransitGatewayRouteTable(ctx, &ec2sdk.CreateTransitGatewayRouteTableInput{
		TransitGatewayId: tgw.TransitGateway.TransitGatewayId,
	})
	require.NoError(t, err)

	_, err = client.CreateTransitGatewayRouteTable(ctx, &ec2sdk.CreateTransitGatewayRouteTableInput{
		TransitGatewayId: tgw.TransitGateway.TransitGatewayId,
	})
	require.NoError(t, err)

	out, err := client.DescribeTransitGatewayRouteTables(ctx, &ec2sdk.DescribeTransitGatewayRouteTablesInput{
		TransitGatewayRouteTableIds: []string{aws.ToString(rtA.TransitGatewayRouteTable.TransitGatewayRouteTableId)},
	})
	require.NoError(t, err)
	require.Len(t, out.TransitGatewayRouteTables, 1,
		"TransitGatewayRouteTableIds filter ignored - returned every route table")
	assert.Equal(t,
		aws.ToString(rtA.TransitGatewayRouteTable.TransitGatewayRouteTableId),
		aws.ToString(out.TransitGatewayRouteTables[0].TransitGatewayRouteTableId))
}
