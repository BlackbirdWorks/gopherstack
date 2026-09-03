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

// SearchLocalGatewayRoutesInput.Filters documents "state" and
// "route-search.exact-match" as two distinct, unrelated filter names
// (api_op_SearchLocalGatewayRoutes.go: "state - The state of the route." vs
// "route-search.exact-match - The exact match of the specified filter.").
// searchLocalGatewayRouteStates (handler_local_gateway.go) collected BOTH
// names' values into the same []string and matched them against the
// route's State field, so a route-search.exact-match filter -- whatever it
// is meant to match -- was silently compared against State and excluded
// every real route (none of whose State is a CIDR/prefix string). It also
// read only Filter.N.Value.1 per filter entry, dropping any additional
// value on a single multi-value "state" filter.
func TestSearchLocalGatewayRoutes_StateFilter_RealClient(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("000000000000", "us-east-1")
	h := ec2.NewHandler(b)
	client := newTestEC2Client(t, h)

	lg, err := b.SeedLocalGateway(ec2.LocalGateway{OutpostArn: "arn:aws:outposts:us-east-1:000000000000:outpost/op-1"})
	require.NoError(t, err)

	rtOut, err := client.CreateLocalGatewayRouteTable(t.Context(), &ec2sdk.CreateLocalGatewayRouteTableInput{
		LocalGatewayId: aws.String(lg.LocalGatewayID),
	})
	require.NoError(t, err)
	routeTableID := rtOut.LocalGatewayRouteTable.LocalGatewayRouteTableId

	_, err = client.CreateLocalGatewayRoute(t.Context(), &ec2sdk.CreateLocalGatewayRouteInput{
		LocalGatewayRouteTableId: routeTableID,
		DestinationCidrBlock:     aws.String("10.100.0.0/24"),
	})
	require.NoError(t, err)

	// A "state" filter with multiple values must OR across them -- the
	// route (state "active") must be found when "active" is one of several
	// listed values, not just when it is the first.
	out, err := client.SearchLocalGatewayRoutes(t.Context(), &ec2sdk.SearchLocalGatewayRoutesInput{
		LocalGatewayRouteTableId: routeTableID,
		Filters: []types.Filter{
			{Name: aws.String("state"), Values: []string{"blackhole", "active"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Routes, 1,
		"state filter must OR across all listed values, not just Filter.N.Value.1")
	assert.Equal(t, "10.100.0.0/24", aws.ToString(out.Routes[0].DestinationCidrBlock))

	// A route-search.exact-match filter is a distinct, separately-documented
	// filter name from "state" and must not be matched against State --
	// whatever it filters on, it must not silently exclude every route by
	// comparing an unrelated value against the route's state.
	out2, err := client.SearchLocalGatewayRoutes(t.Context(), &ec2sdk.SearchLocalGatewayRoutesInput{
		LocalGatewayRouteTableId: routeTableID,
		Filters: []types.Filter{
			{Name: aws.String("route-search.exact-match"), Values: []string{"10.100.0.0/24"}},
		},
	})
	require.NoError(t, err)
	assert.Len(t, out2.Routes, 1,
		"route-search.exact-match must not be matched against route State")
}
