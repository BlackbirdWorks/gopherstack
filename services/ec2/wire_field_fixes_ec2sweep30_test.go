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

// TestDescribeCapacityReservationTopology_State_RealClient covers
// gopherstack-6flj/21my: capacityReservationTopologyItem never carried the
// reservation's state (ec2@v1.319.1 deserializers.go:
// awsEc2query_deserializeDocumentCapacityReservationTopology has a "state" case
// reading types.CapacityReservationTopology.State), even though the backend
// tracks CapacityReservation.State for the exact same reservation. A real
// client's CapacityReservations[].State was always empty regardless of the
// reservation's actual lifecycle state.
func TestDescribeCapacityReservationTopology_State_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("111122223333", "us-east-1")))
	ctx := t.Context()

	createOut, err := client.CreateCapacityReservation(ctx, &ec2sdk.CreateCapacityReservationInput{
		InstanceType:     aws.String("m5.large"),
		InstancePlatform: types.CapacityReservationInstancePlatformLinuxUnix,
		AvailabilityZone: aws.String("us-east-1a"),
		InstanceCount:    aws.Int32(1),
	})
	require.NoError(t, err)
	crID := aws.ToString(createOut.CapacityReservation.CapacityReservationId)
	require.NotEmpty(t, crID)
	require.Equal(t, types.CapacityReservationStateActive, createOut.CapacityReservation.State)

	out, err := client.DescribeCapacityReservationTopology(ctx, &ec2sdk.DescribeCapacityReservationTopologyInput{
		CapacityReservationIds: []string{crID},
	})
	require.NoError(t, err)
	require.Len(t, out.CapacityReservations, 1)
	assert.Equal(t, string(types.CapacityReservationStateActive), aws.ToString(out.CapacityReservations[0].State),
		"State decoded empty - pre-fix capacityReservationTopologyItem had no state field at all, "+
			"despite the same reservation's State being readily available on the backend")
	assert.Equal(t, crID, aws.ToString(out.CapacityReservations[0].CapacityReservationId))
}

// TestRouteServerEndpointAndPeer_FailureReason_RealClient covers
// gopherstack-6flj/21my: routeServerEndpointItem and routeServerPeerItem rendered
// FailureReason as a nested <failureReason><code>/<message></failureReason>
// element, but both awsEc2query_deserializeDocumentRouteServerEndpoint and
// awsEc2query_deserializeDocumentRouteServerPeer (ec2@v1.319.1 deserializers.go)
// read "failureReason" as a flat scalar via decoder.Value() -- the same hard
// decode error class ("expected value for X element, got xml.StartElement")
// confirmed elsewhere in this campaign. This backend never actually populates a
// failure reason (no failure path is modeled for route server endpoints/peers),
// so the field stays empty either way; this test instead pins that the rest of
// the shared item shape -- state, IDs, peer address, BGP options -- still
// decodes correctly through a real client after narrowing FailureReason from a
// struct to a string, guarding against a regression the next time this shape
// is touched.
func TestRouteServerEndpointAndPeer_FailureReason_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("111122223333", "us-east-1")))
	ctx := t.Context()

	rsOut, err := client.CreateRouteServer(ctx, &ec2sdk.CreateRouteServerInput{
		AmazonSideAsn: aws.Int64(65000),
	})
	require.NoError(t, err)
	rsID := aws.ToString(rsOut.RouteServer.RouteServerId)
	require.NotEmpty(t, rsID)

	epOut, err := client.CreateRouteServerEndpoint(ctx, &ec2sdk.CreateRouteServerEndpointInput{
		RouteServerId: aws.String(rsID),
		SubnetId:      aws.String("subnet-default"),
	})
	require.NoError(t, err)
	epID := aws.ToString(epOut.RouteServerEndpoint.RouteServerEndpointId)
	require.NotEmpty(t, epID)
	assert.Empty(t, aws.ToString(epOut.RouteServerEndpoint.FailureReason))

	describeEps, err := client.DescribeRouteServerEndpoints(ctx, &ec2sdk.DescribeRouteServerEndpointsInput{
		RouteServerEndpointIds: []string{epID},
	})
	require.NoError(t, err)
	require.Len(t, describeEps.RouteServerEndpoints, 1)
	assert.Equal(t, epID, aws.ToString(describeEps.RouteServerEndpoints[0].RouteServerEndpointId))
	assert.NotEmpty(t, string(describeEps.RouteServerEndpoints[0].State))

	peerOut, err := client.CreateRouteServerPeer(ctx, &ec2sdk.CreateRouteServerPeerInput{
		RouteServerEndpointId: aws.String(epID),
		PeerAddress:           aws.String("10.0.0.5"),
		BgpOptions:            &types.RouteServerBgpOptionsRequest{PeerAsn: aws.Int64(65001)},
	})
	require.NoError(t, err)
	peerID := aws.ToString(peerOut.RouteServerPeer.RouteServerPeerId)
	require.NotEmpty(t, peerID)
	assert.Empty(t, aws.ToString(peerOut.RouteServerPeer.FailureReason))

	describePeers, err := client.DescribeRouteServerPeers(ctx, &ec2sdk.DescribeRouteServerPeersInput{
		RouteServerPeerIds: []string{peerID},
	})
	require.NoError(t, err)
	require.Len(t, describePeers.RouteServerPeers, 1)
	assert.Equal(t, "10.0.0.5", aws.ToString(describePeers.RouteServerPeers[0].PeerAddress),
		"decode of the shared routeServerPeerItem shape broke after narrowing FailureReason to a scalar")
	assert.Equal(t, int64(65001), aws.ToInt64(describePeers.RouteServerPeers[0].BgpOptions.PeerAsn))
}
