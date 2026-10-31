package gcpcompute_test

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
)

func (e *env) routeList(t *testing.T) map[string]*computepb.Route {
	t.Helper()

	it := e.routes(t).List(t.Context(), &computepb.ListRoutesRequest{Project: project})
	out := map[string]*computepb.Route{}

	for {
		r, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out
		}

		require.NoError(t, err)
		out[r.GetDestRange()] = r
	}
}

func TestSubnetRoutesFollowSubnetworkLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.network(t, "vpc", false)
	e.subnet(t, "sub", "10.10.0.0/24")

	r := e.routeList(t)["10.10.0.0/24"]
	require.NotNil(t, r)
	assert.True(t, strings.HasPrefix(r.GetName(), "default-route-"))
	assert.Len(t, r.GetName(), len("default-route-")+16)
	assert.Equal(t, "SUBNET", r.GetRouteType())
	assert.Equal(t, "ACTIVE", r.GetRouteStatus())
	assert.Equal(t, "Default local route to the subnetwork 10.10.0.0/24.", r.GetDescription())
	assert.Contains(t, r.GetNextHopNetwork(), "/global/networks/vpc")
	assert.Contains(t, r.GetNetwork(), "/global/networks/vpc")
	assert.Zero(t, r.GetPriority())
	assert.Empty(t, r.GetNextHopGateway())

	name := r.GetName()

	op, err := e.subnets(t).ExpandIpCidrRange(t.Context(), &computepb.ExpandIpCidrRangeSubnetworkRequest{
		Project: project, Region: region, Subnetwork: "sub",
		SubnetworksExpandIpCidrRangeRequestResource: &computepb.SubnetworksExpandIpCidrRangeRequest{IpCidrRange: s("10.10.0.0/23")},
	})
	e.done(t, op, err)

	routes := e.routeList(t)
	assert.NotContains(t, routes, "10.10.0.0/24")
	require.Contains(t, routes, "10.10.0.0/23")
	assert.Equal(t, name, routes["10.10.0.0/23"].GetName(), "the route keeps its name when the range grows")

	op, err = e.subnets(t).Patch(t.Context(), &computepb.PatchSubnetworkRequest{
		Project: project, Region: region, Subnetwork: "sub", SubnetworkResource: &computepb.Subnetwork{
			SecondaryIpRanges: []*computepb.SubnetworkSecondaryRange{{RangeName: s("pods"), IpCidrRange: s("192.168.0.0/24")}},
		},
	})
	e.done(t, op, err)

	routes = e.routeList(t)
	require.Contains(t, routes, "192.168.0.0/24")
	assert.Equal(t, "SUBNET", routes["192.168.0.0/24"].GetRouteType())

	_, err = e.routes(t).Delete(t.Context(), &computepb.DeleteRouteRequest{Project: project, Route: name})
	assertAPIError(t, err, http.StatusBadRequest, "invalid")

	op, err = e.subnets(t).Delete(t.Context(), &computepb.DeleteSubnetworkRequest{Project: project, Region: region, Subnetwork: "sub"})
	e.done(t, op, err)

	routes = e.routeList(t)
	assert.NotContains(t, routes, "10.10.0.0/23")
	assert.NotContains(t, routes, "192.168.0.0/24")
	assert.Contains(t, routes, "0.0.0.0/0", "the internet route stays")

	op, err = e.networks(t).Delete(t.Context(), &computepb.DeleteNetworkRequest{Project: project, Network: "vpc"})
	e.done(t, op, err)
	assert.Empty(t, e.routeList(t))
}

func TestAutoNetworkHasSubnetRoutePerRegion(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.network(t, "auto", true)

	n, err := e.networks(t).Get(t.Context(), &computepb.GetNetworkRequest{Project: project, Network: "auto"})
	require.NoError(t, err)

	subnetRoutes := 0

	for _, r := range e.routeList(t) {
		if r.GetRouteType() == "SUBNET" {
			subnetRoutes++
		}
	}

	assert.Equal(t, len(n.GetSubnetworks()), subnetRoutes)
	assert.Positive(t, subnetRoutes)
}
