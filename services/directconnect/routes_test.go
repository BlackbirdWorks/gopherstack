package directconnect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	directconnectsdk "github.com/aws/aws-sdk-go-v2/service/directconnect"
	"github.com/aws/aws-sdk-go-v2/service/directconnect/types"
	"github.com/stretchr/testify/require"
)

// TestRoundTripListVirtualInterfaceRoutes drives ListVirtualInterfaceRoutes
// through the real aws-sdk-go-v2 client (see newRoundTripClient's doc
// comment), proving the wire shape is client-compatible. gopherstack has no
// real BGP route exchange modeled (see routes.go's doc comment), so an
// existing virtual interface always reports zero routes -- honestly, not a
// fabricated route list -- while a nonexistent one still returns the real
// DirectConnectClientException error shape.
func TestRoundTripListVirtualInterfaceRoutes(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	conn := createTestConnection(t, client)

	vif, err := client.CreatePrivateVirtualInterface(ctx, &directconnectsdk.CreatePrivateVirtualInterfaceInput{
		ConnectionId: conn.ConnectionId,
		NewPrivateVirtualInterface: &types.NewPrivateVirtualInterface{
			VirtualInterfaceName: aws.String("test-routes-vif"),
			Vlan:                 200,
		},
	})
	require.NoError(t, err)

	out, err := client.ListVirtualInterfaceRoutes(ctx, &directconnectsdk.ListVirtualInterfaceRoutesInput{
		VirtualInterfaceId: vif.VirtualInterfaceId,
		Filters:            &types.RouteFilters{RouteDirection: types.RouteDirectionAccepted},
	})
	require.NoError(t, err)
	require.Equal(t, aws.ToString(vif.VirtualInterfaceId), aws.ToString(out.VirtualInterfaceId))
	require.Empty(t, out.Routes)

	_, err = client.ListVirtualInterfaceRoutes(ctx, &directconnectsdk.ListVirtualInterfaceRoutesInput{
		VirtualInterfaceId: aws.String("dxvif-doesnotexist"),
	})
	require.Error(t, err)

	var clientErr *types.DirectConnectClientException
	require.ErrorAs(t, err, &clientErr)
}
