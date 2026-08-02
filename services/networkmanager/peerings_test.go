package networkmanager_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	networkmanagersdk "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/aws/aws-sdk-go-v2/service/networkmanager/types"
	"github.com/stretchr/testify/require"
)

// TestRoundTrip_TransitGatewayPeeringLifecycle drives family R.
func TestRoundTrip_TransitGatewayPeeringLifecycle(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	cn := createTestCoreNetwork(t, client)

	created, err := client.CreateTransitGatewayPeering(ctx, &networkmanagersdk.CreateTransitGatewayPeeringInput{
		CoreNetworkId:     cn.CoreNetwork.CoreNetworkId,
		TransitGatewayArn: aws.String("arn:aws:ec2:us-east-1:000000000000:transit-gateway/tgw-0123456789abcdef0"),
	})
	require.NoError(t, err)
	require.Equal(t, types.PeeringStateCreating, created.TransitGatewayPeering.Peering.State)
	require.Equal(t, types.PeeringTypeTransitGateway, created.TransitGatewayPeering.Peering.PeeringType)

	peeringID := created.TransitGatewayPeering.Peering.PeeringId

	require.Eventually(t, func() bool {
		g, getErr := client.GetTransitGatewayPeering(
			ctx,
			&networkmanagersdk.GetTransitGatewayPeeringInput{PeeringId: peeringID},
		)

		return getErr == nil && g.TransitGatewayPeering.Peering.State == types.PeeringStateAvailable
	}, defaultAsyncWait, defaultAsyncPoll)

	listed, err := client.ListPeerings(
		ctx,
		&networkmanagersdk.ListPeeringsInput{CoreNetworkId: cn.CoreNetwork.CoreNetworkId},
	)
	require.NoError(t, err)
	require.Len(t, listed.Peerings, 1)

	deleted, err := client.DeletePeering(ctx, &networkmanagersdk.DeletePeeringInput{PeeringId: peeringID})
	require.NoError(t, err)
	require.Equal(t, types.PeeringStateDeleting, deleted.Peering.State)
}
