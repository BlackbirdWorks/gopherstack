package gcpcompute_test

import (
	"errors"
	"net/http"
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
)

func netRef(name string) string { return "projects/" + project + "/global/networks/" + name }

// peerNet creates a custom-mode network with one subnetwork in the given range.
func (e *env) peerNet(t *testing.T, name, cidr string) {
	t.Helper()
	e.network(t, name, false)

	op, err := e.subnets(t).Insert(t.Context(), &computepb.InsertSubnetworkRequest{
		Project: project, Region: region, SubnetworkResource: &computepb.Subnetwork{
			Name: s(name + "-sub"), Network: s(netRef(name)), IpCidrRange: s(cidr),
		},
	})
	e.done(t, op, err)
}

func (e *env) addPeering(t *testing.T, net, name, peer string, mod func(*computepb.NetworkPeering)) error {
	t.Helper()

	p := &computepb.NetworkPeering{Name: s(name), Network: s(netRef(peer)), ExchangeSubnetRoutes: new(true)}
	if mod != nil {
		mod(p)
	}

	op, err := e.networks(t).AddPeering(t.Context(), &computepb.AddPeeringNetworkRequest{
		Project: project, Network: net,
		NetworksAddPeeringRequestResource: &computepb.NetworksAddPeeringRequest{NetworkPeering: p},
	})

	return e.finish(t, op, err)
}

func (e *env) peerings(t *testing.T, net string) map[string]*computepb.NetworkPeering {
	t.Helper()

	n, err := e.networks(t).Get(t.Context(), &computepb.GetNetworkRequest{Project: project, Network: net})
	require.NoError(t, err)

	out := map[string]*computepb.NetworkPeering{}
	for _, p := range n.GetPeerings() {
		out[p.GetName()] = p
	}

	return out
}

func (e *env) peeringRoutes(t *testing.T, net, peering, direction string) map[string]*computepb.ExchangedPeeringRoute {
	t.Helper()

	it := e.networks(t).ListPeeringRoutes(t.Context(), &computepb.ListPeeringRoutesNetworksRequest{
		Project: project, Network: net, PeeringName: &peering, Direction: &direction,
	})
	out := map[string]*computepb.ExchangedPeeringRoute{}

	for {
		r, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out
		}

		require.NoError(t, err)
		out[r.GetDestRange()] = r
	}
}

func TestPeeringActivation(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.peerNet(t, "a", "10.1.0.0/24")
	e.peerNet(t, "b", "10.2.0.0/24")

	require.NoError(t, e.addPeering(t, "a", "a-to-b", "b", nil))

	p := e.peerings(t, "a")["a-to-b"]
	require.NotNil(t, p)
	assert.Equal(t, "INACTIVE", p.GetState())
	assert.NotEmpty(t, p.GetStateDetails())
	assert.True(t, p.GetExchangeSubnetRoutes())
	assert.True(t, p.GetExportSubnetRoutesWithPublicIp())
	assert.False(t, p.GetImportCustomRoutes())
	assert.Equal(t, "IPV4_ONLY", p.GetStackType())
	assert.Equal(t, "INDEPENDENT", p.GetUpdateStrategy())
	assert.Equal(t, int32(1460), p.GetPeerMtu())
	assert.Contains(t, p.GetNetwork(), "/global/networks/b")
	assert.Empty(t, e.peerings(t, "b"))

	require.NoError(t, e.addPeering(t, "b", "b-to-a", "a", nil))

	assert.Equal(t, "ACTIVE", e.peerings(t, "a")["a-to-b"].GetState())
	assert.Equal(t, "ACTIVE", e.peerings(t, "b")["b-to-a"].GetState())
	assert.Equal(t, "Connected.", e.peerings(t, "b")["b-to-a"].GetStateDetails())

	op, err := e.networks(t).RemovePeering(t.Context(), &computepb.RemovePeeringNetworkRequest{
		Project: project, Network: "b",
		NetworksRemovePeeringRequestResource: &computepb.NetworksRemovePeeringRequest{Name: s("b-to-a")},
	})
	require.NoError(t, e.finish(t, op, err))
	assert.Empty(t, e.peerings(t, "b"))
	assert.Equal(t, "INACTIVE", e.peerings(t, "a")["a-to-b"].GetState())
}

func TestPeeringAddErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		net    string
		pname  string
		peer   string
		status int
		reason string
	}{
		{name: "overlap", net: "a", pname: "p", peer: "c", status: http.StatusBadRequest, reason: "invalid"},
		{name: "self", net: "a", pname: "p", peer: "a", status: http.StatusBadRequest, reason: "invalid"},
		{name: "duplicate name", net: "a", pname: "first", peer: "d", status: http.StatusBadRequest, reason: "invalid"},
		{name: "duplicate peer", net: "a", pname: "again", peer: "b", status: http.StatusBadRequest, reason: "invalid"},
		{name: "missing peer", net: "a", pname: "p", peer: "ghost", status: http.StatusNotFound, reason: "notFound"},
		{name: "bad name", net: "a", pname: "Bad_Name", peer: "d", status: http.StatusBadRequest, reason: "invalid"},
		{name: "missing network", net: "ghost", pname: "p", peer: "d", status: http.StatusNotFound, reason: "notFound"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.peerNet(t, "a", "10.1.0.0/24")
			e.peerNet(t, "b", "10.2.0.0/24")
			e.peerNet(t, "c", "10.1.0.0/24")
			e.peerNet(t, "d", "10.4.0.0/24")
			require.NoError(t, e.addPeering(t, "a", "first", "b", nil))

			assertAPIError(t, e.addPeering(t, tt.net, tt.pname, tt.peer, nil), tt.status, tt.reason)
		})
	}
}

func TestPeeringAutoModeNetworksOverlap(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.network(t, "auto1", true)
	e.network(t, "auto2", true)

	err := e.addPeering(t, "auto1", "p", "auto2", nil)
	assertAPIError(t, err, http.StatusBadRequest, "invalid")
	assert.Contains(t, err.Error(), "overlaps with an IP range in the local network")
}

func TestPeeringRouteExchange(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.peerNet(t, "a", "10.1.0.0/24")
	e.peerNet(t, "b", "10.2.0.0/24")

	op, err := e.routes(t).Insert(t.Context(), &computepb.InsertRouteRequest{Project: project, RouteResource: &computepb.Route{
		Name: s("b-custom"), Network: s(netRef("b")), DestRange: s("192.168.50.0/24"), NextHopIp: s("10.2.0.5"),
		Priority: new(uint32(500)),
	}})
	e.done(t, op, err)

	require.NoError(t, e.addPeering(t, "a", "a-to-b", "b", nil))
	assert.Empty(t, e.peeringRoutes(t, "a", "a-to-b", "INCOMING"), "inactive peering exchanges nothing")

	require.NoError(t, e.addPeering(t, "b", "b-to-a", "a", nil))

	in := e.peeringRoutes(t, "a", "a-to-b", "INCOMING")
	require.Contains(t, in, "10.2.0.0/24")
	assert.Equal(t, "SUBNET_PEERING_ROUTE", in["10.2.0.0/24"].GetType())
	assert.True(t, in["10.2.0.0/24"].GetImported())
	assert.NotContains(t, in, "192.168.50.0/24", "custom routes need exportCustomRoutes")
	assert.NotContains(t, in, "0.0.0.0/0")

	out := e.peeringRoutes(t, "a", "a-to-b", "OUTGOING")
	assert.Contains(t, out, "10.1.0.0/24")
	assert.NotContains(t, out, "10.2.0.0/24")

	update := func(net, peering string, export, imp *bool) {
		t.Helper()

		op, uerr := e.networks(t).UpdatePeering(t.Context(), &computepb.UpdatePeeringNetworkRequest{
			Project: project, Network: net,
			NetworksUpdatePeeringRequestResource: &computepb.NetworksUpdatePeeringRequest{
				NetworkPeering: &computepb.NetworkPeering{Name: s(peering), ExportCustomRoutes: export, ImportCustomRoutes: imp},
			},
		})
		require.NoError(t, e.finish(t, op, uerr))
	}

	update("b", "b-to-a", new(true), nil)

	in = e.peeringRoutes(t, "a", "a-to-b", "INCOMING")
	require.Contains(t, in, "192.168.50.0/24")
	assert.Equal(t, "STATIC_PEERING_ROUTE", in["192.168.50.0/24"].GetType())
	assert.Equal(t, uint32(500), in["192.168.50.0/24"].GetPriority())
	assert.False(t, in["192.168.50.0/24"].GetImported(), "importCustomRoutes is off locally")

	update("a", "a-to-b", nil, new(true))

	in = e.peeringRoutes(t, "a", "a-to-b", "INCOMING")
	assert.True(t, in["192.168.50.0/24"].GetImported())
	assert.True(t, e.peerings(t, "a")["a-to-b"].GetImportCustomRoutes())
	assert.True(t, e.peerings(t, "b")["b-to-a"].GetExportCustomRoutes())

	update("b", "b-to-a", new(false), nil)
	assert.NotContains(t, e.peeringRoutes(t, "a", "a-to-b", "INCOMING"), "192.168.50.0/24")
}

func TestPeeringUpdateAndRemoveErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.peerNet(t, "a", "10.1.0.0/24")
	e.peerNet(t, "b", "10.2.0.0/24")
	require.NoError(t, e.addPeering(t, "a", "a-to-b", "b", nil))

	updates := []struct {
		name string
		p    *computepb.NetworkPeering
	}{
		{name: "stack type", p: &computepb.NetworkPeering{Name: s("a-to-b"), StackType: s("IPV4_IPV6")}},
		{name: "exchange routes", p: &computepb.NetworkPeering{Name: s("a-to-b"), ExchangeSubnetRoutes: new(false)}},
		{name: "unknown peering", p: &computepb.NetworkPeering{Name: s("nope"), ExportCustomRoutes: new(true)}},
	}

	for _, tt := range updates {
		t.Run("update "+tt.name, func(t *testing.T) {
			t.Parallel()

			op, err := e.networks(t).UpdatePeering(t.Context(), &computepb.UpdatePeeringNetworkRequest{
				Project: project, Network: "a",
				NetworksUpdatePeeringRequestResource: &computepb.NetworksUpdatePeeringRequest{NetworkPeering: tt.p},
			})
			assertAPIError(t, e.finish(t, op, err), http.StatusBadRequest, "invalid")
		})
	}

	t.Run("remove unknown", func(t *testing.T) {
		t.Parallel()

		op, err := e.networks(t).RemovePeering(t.Context(), &computepb.RemovePeeringNetworkRequest{
			Project: project, Network: "a",
			NetworksRemovePeeringRequestResource: &computepb.NetworksRemovePeeringRequest{Name: s("nope")},
		})
		assertAPIError(t, e.finish(t, op, err), http.StatusBadRequest, "invalid")
	})

	t.Run("list unknown peering", func(t *testing.T) {
		t.Parallel()

		_, err := e.networks(t).ListPeeringRoutes(t.Context(), &computepb.ListPeeringRoutesNetworksRequest{
			Project: project, Network: "a", PeeringName: s("nope"),
		}).Next()
		assertAPIError(t, err, http.StatusBadRequest, "invalid")
	})
}

func TestPeeringBlocksNetworkDeleteAndOverlappingSubnets(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.peerNet(t, "a", "10.1.0.0/24")
	e.peerNet(t, "b", "10.2.0.0/24")
	require.NoError(t, e.addPeering(t, "a", "a-to-b", "b", nil))
	require.NoError(t, e.addPeering(t, "b", "b-to-a", "a", nil))

	insertSub := func(name, cidr string) error {
		op, err := e.subnets(t).Insert(t.Context(), &computepb.InsertSubnetworkRequest{
			Project: project, Region: region, SubnetworkResource: &computepb.Subnetwork{
				Name: s(name), Network: s(netRef("a")), IpCidrRange: s(cidr),
			},
		})

		return e.finish(t, op, err)
	}

	assertAPIError(t, insertSub("clash", "10.2.0.0/25"), http.StatusBadRequest, "invalid")
	require.NoError(t, insertSub("fine", "10.3.0.0/24"))

	_, err := e.networks(t).Delete(t.Context(), &computepb.DeleteNetworkRequest{Project: project, Network: "a"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	op, err := e.networks(t).RemovePeering(t.Context(), &computepb.RemovePeeringNetworkRequest{
		Project: project, Network: "a",
		NetworksRemovePeeringRequestResource: &computepb.NetworksRemovePeeringRequest{Name: s("a-to-b")},
	})
	require.NoError(t, e.finish(t, op, err))
	require.NoError(t, insertSub("now-fine", "10.2.0.128/25"))
}
