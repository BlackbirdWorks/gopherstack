package gcpcompute_test

import (
	"net/http"
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/proto"
)

func allow(proto_ string, ports ...string) []*computepb.Allowed {
	return []*computepb.Allowed{{IPProtocol: s(proto_), Ports: ports}}
}

func TestFirewallInsertValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		fw     *computepb.Firewall
		name   string
		reason string
		status int
	}{
		{&computepb.Firewall{Name: s("ssh"), Network: s("global/networks/vpc"), Allowed: allow("tcp", "22"), SourceRanges: []string{"0.0.0.0/0"}}, "valid ingress", "", 200},
		{&computepb.Firewall{Name: s("egress"), Network: s("global/networks/vpc"), Direction: s("EGRESS"), Denied: []*computepb.Denied{{IPProtocol: s("all")}}, DestinationRanges: []string{"10.0.0.0/8"}}, "valid egress deny", "", 200},
		{&computepb.Firewall{Name: s("ports"), Network: s("global/networks/vpc"), Allowed: allow("tcp", "8000-9000", "443"), SourceTags: []string{"web"}, TargetTags: []string{"app"}}, "port ranges and tags", "", 200},
		{&computepb.Firewall{Name: s("prio0"), Network: s("global/networks/vpc"), Allowed: allow("icmp"), Priority: proto.Int32(0), SourceRanges: []string{"10.0.0.0/8"}}, "priority zero", "", 200},
		{&computepb.Firewall{Name: s("both"), Network: s("global/networks/vpc"), Allowed: allow("tcp"), Denied: []*computepb.Denied{{IPProtocol: s("udp")}}}, "allowed and denied", "invalid", 400},
		{&computepb.Firewall{Name: s("none"), Network: s("global/networks/vpc")}, "neither", "invalid", 400},
		{&computepb.Firewall{Name: s("proto"), Network: s("global/networks/vpc"), Allowed: allow("gre")}, "bad protocol", "invalid", 400},
		{&computepb.Firewall{Name: s("icmpports"), Network: s("global/networks/vpc"), Allowed: allow("icmp", "80")}, "ports on icmp", "invalid", 400},
		{&computepb.Firewall{Name: s("badport"), Network: s("global/networks/vpc"), Allowed: allow("tcp", "99999")}, "port range", "invalid", 400},
		{&computepb.Firewall{Name: s("backwards"), Network: s("global/networks/vpc"), Allowed: allow("tcp", "90-80")}, "backwards range", "invalid", 400},
		{&computepb.Firewall{Name: s("prio"), Network: s("global/networks/vpc"), Allowed: allow("tcp"), Priority: proto.Int32(70000)}, "priority", "invalid", 400},
		{&computepb.Firewall{Name: s("cidr"), Network: s("global/networks/vpc"), Allowed: allow("tcp"), SourceRanges: []string{"10.0.0.1/8"}}, "cidr", "invalid", 400},
		{&computepb.Firewall{Name: s("dir"), Network: s("global/networks/vpc"), Allowed: allow("tcp"), Direction: s("SIDEWAYS")}, "direction", "invalid", 400},
		{&computepb.Firewall{Name: s("egsrc"), Network: s("global/networks/vpc"), Direction: s("EGRESS"), Allowed: allow("tcp"), SourceRanges: []string{"10.0.0.0/8"}}, "egress with source", "invalid", 400},
		{&computepb.Firewall{Name: s("nonet"), Network: s("global/networks/missing"), Allowed: allow("tcp")}, "missing network", "notFound", 404},
		{&computepb.Firewall{Name: s("existing"), Network: s("global/networks/vpc"), Allowed: allow("tcp")}, "duplicate", "alreadyExists", 409},
		{&computepb.Firewall{Network: s("global/networks/vpc"), Allowed: allow("tcp")}, "no name", "required", 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.network(t, "vpc", false)

			fws := e.firewalls(t)
			op, err := fws.Insert(t.Context(), &computepb.InsertFirewallRequest{
				Project: project, FirewallResource: &computepb.Firewall{
					Name: s("existing"), Network: s("global/networks/vpc"), Allowed: allow("tcp", "1"),
				},
			})
			e.done(t, op, err)

			op, err = fws.Insert(t.Context(), &computepb.InsertFirewallRequest{Project: project, FirewallResource: tt.fw})
			if tt.status == http.StatusOK {
				e.done(t, op, err)

				got, gerr := fws.Get(t.Context(), &computepb.GetFirewallRequest{Project: project, Firewall: tt.fw.GetName()})
				require.NoError(t, gerr)
				assert.Equal(t, tt.fw.GetName(), got.GetName())
				assert.Contains(t, got.GetNetwork(), "/global/networks/vpc")
				assert.NotZero(t, got.GetId())

				return
			}

			assertAPIError(t, err, tt.status, tt.reason)
		})
	}
}

func TestFirewallDefaultsPatchUpdateDelete(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.network(t, "vpc", false)

	fws := e.firewalls(t)
	op, err := fws.Insert(t.Context(), &computepb.InsertFirewallRequest{
		Project: project, FirewallResource: &computepb.Firewall{
			Name: s("web"), Network: s("global/networks/vpc"), Allowed: allow("tcp", "80"), SourceRanges: []string{"0.0.0.0/0"},
		},
	})
	e.done(t, op, err)

	got, err := fws.Get(t.Context(), &computepb.GetFirewallRequest{Project: project, Firewall: "web"})
	require.NoError(t, err)
	assert.Equal(t, "INGRESS", got.GetDirection())
	assert.Equal(t, int32(1000), got.GetPriority())
	assert.False(t, got.GetDisabled())

	op, err = fws.Patch(t.Context(), &computepb.PatchFirewallRequest{
		Project: project, Firewall: "web", FirewallResource: &computepb.Firewall{
			Disabled: proto.Bool(true), Priority: proto.Int32(0), Allowed: allow("tcp", "80", "443"),
		},
	})
	e.done(t, op, err)

	got, err = fws.Get(t.Context(), &computepb.GetFirewallRequest{Project: project, Firewall: "web"})
	require.NoError(t, err)
	assert.True(t, got.GetDisabled())
	assert.Equal(t, int32(0), got.GetPriority())
	assert.Equal(t, []string{"80", "443"}, got.GetAllowed()[0].GetPorts())
	assert.Equal(t, []string{"0.0.0.0/0"}, got.GetSourceRanges())

	op, err = fws.Update(t.Context(), &computepb.UpdateFirewallRequest{
		Project: project, Firewall: "web", FirewallResource: &computepb.Firewall{
			Name: s("web"), Network: s("global/networks/vpc"), Allowed: allow("udp", "53"), SourceRanges: []string{"10.0.0.0/8"},
		},
	})
	e.done(t, op, err)

	got, err = fws.Get(t.Context(), &computepb.GetFirewallRequest{Project: project, Firewall: "web"})
	require.NoError(t, err)
	assert.Equal(t, "udp", got.GetAllowed()[0].GetIPProtocol())
	assert.Equal(t, int32(1000), got.GetPriority())

	_, err = fws.Update(t.Context(), &computepb.UpdateFirewallRequest{
		Project: project, Firewall: "web", FirewallResource: &computepb.Firewall{Name: s("renamed"), Allowed: allow("tcp")},
	})
	assertAPIError(t, err, http.StatusBadRequest, "invalid")

	_, err = e.networks(t).Delete(t.Context(), &computepb.DeleteNetworkRequest{Project: project, Network: "vpc"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	op, err = fws.Delete(t.Context(), &computepb.DeleteFirewallRequest{Project: project, Firewall: "web"})
	e.done(t, op, err)

	it := fws.List(t.Context(), &computepb.ListFirewallsRequest{Project: project})
	_, err = it.Next()
	assert.ErrorIs(t, err, iterator.Done)
}

func TestRoutes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		route  *computepb.Route
		name   string
		reason string
		status int
	}{
		{&computepb.Route{Name: s("gw"), DestRange: s("192.168.0.0/16"), NextHopGateway: s("global/gateways/default-internet-gateway")}, "gateway", "", 200},
		{&computepb.Route{Name: s("ip"), DestRange: s("10.50.0.0/16"), NextHopIp: s("10.0.0.5"), Priority: proto.Uint32(500), Tags: []string{"egress"}}, "next hop ip", "", 200},
		{&computepb.Route{Name: s("none"), DestRange: s("10.60.0.0/16")}, "no next hop", "invalid", 400},
		{&computepb.Route{Name: s("two"), DestRange: s("10.60.0.0/16"), NextHopIp: s("10.0.0.5"), NextHopGateway: s("global/gateways/default-internet-gateway")}, "two next hops", "invalid", 400},
		{&computepb.Route{Name: s("badip"), DestRange: s("10.60.0.0/16"), NextHopIp: s("nope")}, "bad ip", "invalid", 400},
		{&computepb.Route{Name: s("baddest"), DestRange: s("10.60.0.0/8"), NextHopIp: s("10.0.0.5")}, "dest host bits", "invalid", 400},
		{&computepb.Route{Name: s("nodest"), NextHopIp: s("10.0.0.5")}, "no dest", "required", 400},
		{&computepb.Route{Name: s("inst"), DestRange: s("10.70.0.0/16"), NextHopInstance: s("zones/" + zone + "/instances/ghost")}, "missing instance", "notFound", 404},
		{&computepb.Route{Name: s("gw2"), DestRange: s("10.80.0.0/16"), NextHopGateway: s("global/gateways/other")}, "other gateway", "invalid", 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.network(t, "vpc", false)

			tt.route.Network = s("global/networks/vpc")
			routes := e.routes(t)

			op, err := routes.Insert(t.Context(), &computepb.InsertRouteRequest{Project: project, RouteResource: tt.route})
			if tt.status != http.StatusOK {
				assertAPIError(t, err, tt.status, tt.reason)

				return
			}

			e.done(t, op, err)

			got, err := routes.Get(t.Context(), &computepb.GetRouteRequest{Project: project, Route: tt.route.GetName()})
			require.NoError(t, err)
			assert.Equal(t, tt.route.GetDestRange(), got.GetDestRange())
			assert.NotZero(t, got.GetPriority())

			op, err = routes.Delete(t.Context(), &computepb.DeleteRouteRequest{Project: project, Route: tt.route.GetName()})
			e.done(t, op, err)
		})
	}
}

func TestUserRouteBlocksNetworkDelete(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.network(t, "vpc", false)

	op, err := e.routes(t).Insert(t.Context(), &computepb.InsertRouteRequest{
		Project: project, RouteResource: &computepb.Route{
			Name: s("r"), Network: s("global/networks/vpc"), DestRange: s("10.9.0.0/16"), NextHopIp: s("10.0.0.2"),
		},
	})
	e.done(t, op, err)

	_, err = e.networks(t).Delete(t.Context(), &computepb.DeleteNetworkRequest{Project: project, Network: "vpc"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")
}
