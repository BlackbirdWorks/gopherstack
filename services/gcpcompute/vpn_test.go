package gcpcompute_test

import (
	"net/http"
	"testing"
	"time"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	vpcRef  = "projects/" + project + "/global/networks/vpc"
	gwA     = regBase + "vpnGateways/gw-a"
	gwB     = regBase + "vpnGateways/gw-b"
	extRef  = gl + "externalVpnGateways/ext"
	tunARef = regBase + "vpnTunnels/ta"
)

func (e *env) vpnGws(t *testing.T) *compute.VpnGatewaysClient {
	t.Helper()

	return client(t, e, compute.NewVpnGatewaysRESTClient)
}

func (e *env) vpnTunnels(t *testing.T) *compute.VpnTunnelsClient {
	t.Helper()

	return client(t, e, compute.NewVpnTunnelsRESTClient)
}

func (e *env) haGateway(t *testing.T, name string) {
	t.Helper()

	op, err := e.vpnGws(t).Insert(t.Context(), &computepb.InsertVpnGatewayRequest{
		Project: project, Region: region, VpnGatewayResource: &computepb.VpnGateway{Name: s(name), Network: s(vpcRef)},
	})
	e.done(t, op, err)
}

func (e *env) externalGateway(t *testing.T) {
	t.Helper()

	c := client(t, e, compute.NewExternalVpnGatewaysRESTClient)
	op, err := c.Insert(t.Context(), &computepb.InsertExternalVpnGatewayRequest{
		Project: project, ExternalVpnGatewayResource: &computepb.ExternalVpnGateway{
			Name: s("ext"), RedundancyType: s("TWO_IPS_REDUNDANCY"),
			Interfaces: []*computepb.ExternalVpnGatewayInterface{
				{Id: new(uint32(0)), IpAddress: s("203.0.113.1")}, {Id: new(uint32(1)), IpAddress: s("203.0.113.2")},
			},
		},
	})
	e.done(t, op, err)
}

func (e *env) insertTunnel(t *testing.T, tun *computepb.VpnTunnel) error {
	t.Helper()

	op, err := e.vpnTunnels(t).Insert(t.Context(), &computepb.InsertVpnTunnelRequest{
		Project: project, Region: region, VpnTunnelResource: tun,
	})

	return e.finish(t, op, err)
}

func (e *env) tunnel(t *testing.T, name string) *computepb.VpnTunnel {
	t.Helper()

	got, err := e.vpnTunnels(t).Get(t.Context(), &computepb.GetVpnTunnelRequest{
		Project: project, Region: region, VpnTunnel: name,
	})
	require.NoError(t, err)

	return got
}

func haTunnel(name, gw, peerGw string, iface int32) *computepb.VpnTunnel {
	return &computepb.VpnTunnel{
		Name: s(name), VpnGateway: s(gw), VpnGatewayInterface: new(iface), PeerGcpGateway: s(peerGw),
		SharedSecret: s("s3cret"),
	}
}

func TestVPNGatewayInsert(t *testing.T) {
	t.Parallel()

	tests := []struct {
		gw     *computepb.VpnGateway
		check  func(t *testing.T, g *computepb.VpnGateway)
		name   string
		status int
	}{
		{
			name: "ipv4", gw: &computepb.VpnGateway{Name: s("gw"), Network: s(vpcRef)},
			check: func(t *testing.T, g *computepb.VpnGateway) {
				t.Helper()
				require.Len(t, g.GetVpnInterfaces(), 2)
				assert.NotEmpty(t, g.GetVpnInterfaces()[0].GetIpAddress())
				assert.NotEqual(t, g.GetVpnInterfaces()[0].GetIpAddress(), g.GetVpnInterfaces()[1].GetIpAddress())
				assert.Equal(t, int32(1), int32(g.GetVpnInterfaces()[1].GetId()))
				assert.Equal(t, "IPV4_ONLY", g.GetStackType())
				assert.Equal(t, "IPV4", g.GetGatewayIpVersion())
				assert.Contains(t, g.GetNetwork(), "/global/networks/vpc")
				assert.Contains(t, g.GetRegion(), "regions/"+region)
			},
		},
		{
			name: "ipv6", gw: &computepb.VpnGateway{Name: s("gw"), Network: s(vpcRef), GatewayIpVersion: s("IPV6")},
			check: func(t *testing.T, g *computepb.VpnGateway) {
				t.Helper()
				assert.NotEmpty(t, g.GetVpnInterfaces()[0].GetIpv6Address())
				assert.NotEqual(t, g.GetVpnInterfaces()[0].GetIpv6Address(), g.GetVpnInterfaces()[1].GetIpv6Address())
				assert.Equal(t, "IPV4_IPV6", g.GetStackType())
			},
		},
		{name: "no network", gw: &computepb.VpnGateway{Name: s("gw")}, status: http.StatusBadRequest},
		{
			name: "missing network", status: http.StatusNotFound,
			gw: &computepb.VpnGateway{Name: s("gw"), Network: s("global/networks/none")},
		},
		{
			name: "bad ip version", status: http.StatusBadRequest,
			gw: &computepb.VpnGateway{Name: s("gw"), Network: s(vpcRef), GatewayIpVersion: s("IPV5")},
		},
		{
			name: "interconnect", status: http.StatusBadRequest,
			gw: &computepb.VpnGateway{
				Name: s("gw"), Network: s(vpcRef),
				VpnInterfaces: []*computepb.VpnGatewayVpnGatewayInterface{{InterconnectAttachment: s("x")}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)

			op, err := e.vpnGws(t).Insert(t.Context(), &computepb.InsertVpnGatewayRequest{
				Project: project, Region: region, VpnGatewayResource: tt.gw,
			})
			if tt.status != 0 {
				assertAPIError(t, e.finish(t, op, err), tt.status, "")

				return
			}

			e.done(t, op, err)

			got, gerr := e.vpnGws(t).Get(t.Context(), &computepb.GetVpnGatewayRequest{
				Project: project, Region: region, VpnGateway: "gw",
			})
			require.NoError(t, gerr)
			tt.check(t, got)
		})
	}
}

func TestVPNGatewayAddressesAreUnique(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	e.haGateway(t, "gw-a")
	e.haGateway(t, "gw-b")

	seen := map[string]bool{}

	for _, n := range []string{"gw-a", "gw-b"} {
		g, err := e.vpnGws(t).
			Get(t.Context(), &computepb.GetVpnGatewayRequest{Project: project, Region: region, VpnGateway: n})
		require.NoError(t, err)

		for _, vi := range g.GetVpnInterfaces() {
			assert.False(t, seen[vi.GetIpAddress()], "duplicate interface IP")
			seen[vi.GetIpAddress()] = true
		}
	}

	addr, err := e.addresses(t).Insert(t.Context(), &computepb.InsertAddressRequest{
		Project: project, Region: region, AddressResource: &computepb.Address{Name: s("a")},
	})
	e.done(t, addr, err)

	got, gerr := e.addresses(t).
		Get(t.Context(), &computepb.GetAddressRequest{Project: project, Region: region, Address: "a"})
	require.NoError(t, gerr)
	assert.False(t, seen[got.GetAddress()], "reserved addresses skip VPN gateway IPs")
}

func TestVPNGatewayLabelsAndDelete(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	e.haGateway(t, "gw-a")
	e.haGateway(t, "gw-b")

	g, err := e.vpnGws(t).
		Get(t.Context(), &computepb.GetVpnGatewayRequest{Project: project, Region: region, VpnGateway: "gw-a"})
	require.NoError(t, err)

	op, err := e.vpnGws(t).SetLabels(t.Context(), &computepb.SetLabelsVpnGatewayRequest{
		Project: project, Region: region, Resource: "gw-a", RegionSetLabelsRequestResource: &computepb.RegionSetLabelsRequest{
			Labels: map[string]string{"env": "x"}, LabelFingerprint: g.LabelFingerprint,
		},
	})
	e.done(t, op, err)

	_, err = e.vpnGws(t).SetLabels(t.Context(), &computepb.SetLabelsVpnGatewayRequest{
		Project: project, Region: region, Resource: "gw-a", RegionSetLabelsRequestResource: &computepb.RegionSetLabelsRequest{
			Labels: map[string]string{"env": "y"}, LabelFingerprint: s("stale"),
		},
	})
	assertAPIError(t, err, http.StatusPreconditionFailed, "")

	g, err = e.vpnGws(t).
		Get(t.Context(), &computepb.GetVpnGatewayRequest{Project: project, Region: region, VpnGateway: "gw-a"})
	require.NoError(t, err)
	assert.Equal(t, "x", g.GetLabels()["env"])

	require.NoError(t, e.insertTunnel(t, haTunnel("ta", gwA, gwB, 0)))

	_, err = e.vpnGws(t).
		Delete(t.Context(), &computepb.DeleteVpnGatewayRequest{Project: project, Region: region, VpnGateway: "gw-a"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	_, err = e.networks(t).Delete(t.Context(), &computepb.DeleteNetworkRequest{Project: project, Network: "vpc"})
	assertAPIError(t, err, http.StatusBadRequest, "")

	op, err = e.vpnTunnels(t).
		Delete(t.Context(), &computepb.DeleteVpnTunnelRequest{Project: project, Region: region, VpnTunnel: "ta"})
	e.done(t, op, err)

	op, err = e.vpnGws(t).
		Delete(t.Context(), &computepb.DeleteVpnGatewayRequest{Project: project, Region: region, VpnGateway: "gw-a"})
	e.done(t, op, err)
}

func TestExternalVPNGateway(t *testing.T) {
	t.Parallel()

	tests := []struct {
		gw     *computepb.ExternalVpnGateway
		name   string
		status int
	}{
		{
			name: "four ips", gw: &computepb.ExternalVpnGateway{
				Name: s(
					"ext",
				), RedundancyType: s("FOUR_IPS_REDUNDANCY"), Interfaces: ifaces("1.1.1.1", "1.1.1.2", "1.1.1.3", "1.1.1.4"),
			},
		},
		{
			name: "single",
			gw: &computepb.ExternalVpnGateway{
				Name:           s("ext"),
				RedundancyType: s("SINGLE_IP_INTERNALLY_REDUNDANT"),
				Interfaces:     ifaces("1.1.1.1"),
			},
		},
		{
			name: "count mismatch", status: http.StatusBadRequest,
			gw: &computepb.ExternalVpnGateway{
				Name:           s("ext"),
				RedundancyType: s("TWO_IPS_REDUNDANCY"),
				Interfaces:     ifaces("1.1.1.1"),
			},
		},
		{
			name:   "bad type",
			status: http.StatusBadRequest,
			gw:     &computepb.ExternalVpnGateway{Name: s("ext"), RedundancyType: s("X")},
		},
		{
			name: "bad ip", status: http.StatusBadRequest,
			gw: &computepb.ExternalVpnGateway{
				Name:           s("ext"),
				RedundancyType: s("SINGLE_IP_INTERNALLY_REDUNDANT"),
				Interfaces:     ifaces("nope"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			c := client(t, e, compute.NewExternalVpnGatewaysRESTClient)

			op, err := c.Insert(
				t.Context(),
				&computepb.InsertExternalVpnGatewayRequest{Project: project, ExternalVpnGatewayResource: tt.gw},
			)
			if tt.status != 0 {
				assertAPIError(t, e.finish(t, op, err), tt.status, "")

				return
			}

			e.done(t, op, err)

			got, gerr := c.Get(
				t.Context(),
				&computepb.GetExternalVpnGatewayRequest{Project: project, ExternalVpnGateway: "ext"},
			)
			require.NoError(t, gerr)
			assert.Len(t, got.GetInterfaces(), len(tt.gw.GetInterfaces()))
			assert.Equal(t, "compute#externalVpnGateway", got.GetKind())
		})
	}
}

func ifaces(ips ...string) []*computepb.ExternalVpnGatewayInterface {
	out := make([]*computepb.ExternalVpnGatewayInterface, len(ips))
	for i, ip := range ips {
		out[i] = &computepb.ExternalVpnGatewayInterface{Id: new(uint32(i)), IpAddress: s(ip)}
	}

	return out
}

func TestHATunnelValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mut    func(tun *computepb.VpnTunnel)
		name   string
		status int
	}{
		{name: "ok", mut: func(*computepb.VpnTunnel) {}},
		{
			name:   "no secret",
			status: http.StatusBadRequest,
			mut:    func(tun *computepb.VpnTunnel) { tun.SharedSecret = nil },
		},
		{
			name:   "bad ike",
			status: http.StatusBadRequest,
			mut:    func(tun *computepb.VpnTunnel) { tun.IkeVersion = new(int32(3)) },
		},
		{
			name:   "bad interface",
			status: http.StatusBadRequest,
			mut:    func(tun *computepb.VpnTunnel) { tun.VpnGatewayInterface = new(int32(2)) },
		},
		{
			name:   "peer ip",
			status: http.StatusBadRequest,
			mut:    func(tun *computepb.VpnTunnel) { tun.PeerIp = s("203.0.113.5") },
		},
		{
			name:   "no peer",
			status: http.StatusBadRequest,
			mut:    func(tun *computepb.VpnTunnel) { tun.PeerGcpGateway = nil },
		},
		{
			name: "both peers", status: http.StatusBadRequest,
			mut: func(tun *computepb.VpnTunnel) { tun.PeerExternalGateway = s(extRef) },
		},
		{
			name:   "missing gateway",
			status: http.StatusNotFound,
			mut:    func(tun *computepb.VpnTunnel) { tun.VpnGateway = s(regBase + "vpnGateways/none") },
		},
		{
			name:   "missing peer",
			status: http.StatusNotFound,
			mut:    func(tun *computepb.VpnTunnel) { tun.PeerGcpGateway = s(regBase + "vpnGateways/none") },
		},
		{
			name: "classic and ha", status: http.StatusBadRequest,
			mut: func(tun *computepb.VpnTunnel) { tun.TargetVpnGateway = s(regBase + "targetVpnGateways/t") },
		},
		{
			name:   "missing router",
			status: http.StatusNotFound,
			mut:    func(tun *computepb.VpnTunnel) { tun.Router = s(regBase + "routers/none") },
		},
		{
			name: "external interface", status: http.StatusBadRequest,
			mut: func(tun *computepb.VpnTunnel) {
				tun.PeerGcpGateway, tun.PeerExternalGateway, tun.PeerExternalGatewayInterface = nil, s(
					extRef,
				), new(
					int32(3),
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)
			e.haGateway(t, "gw-a")
			e.haGateway(t, "gw-b")
			e.externalGateway(t)

			tun := haTunnel("ta", gwA, gwB, 0)
			tt.mut(tun)

			err := e.insertTunnel(t, tun)
			if tt.status != 0 {
				assertAPIError(t, err, tt.status, "")

				return
			}

			require.NoError(t, err)

			got := e.tunnel(t, "ta")
			assert.EqualValues(t, 2, got.GetIkeVersion())
			assert.Empty(t, got.GetSharedSecret(), "the shared secret is not returned")
			assert.NotEmpty(t, got.GetSharedSecretHash())
			assert.Equal(t, []string{"0.0.0.0/0"}, got.GetLocalTrafficSelector())
			assert.Contains(t, got.GetVpnGateway(), "/vpnGateways/gw-a")
		})
	}
}

func TestHATunnelStatus(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	e.haGateway(t, "gw-a")
	e.haGateway(t, "gw-b")
	e.externalGateway(t)

	for _, tun := range []*computepb.VpnTunnel{haTunnel("ta", gwA, gwB, 0), haTunnel("tb", gwB, gwA, 0)} {
		_, err := e.vpnTunnels(t).Insert(t.Context(), &computepb.InsertVpnTunnelRequest{
			Project: project, Region: region, VpnTunnelResource: tun,
		})
		require.NoError(t, err)
	}

	assert.Equal(t, "PROVISIONING", e.tunnel(t, "ta").GetStatus())
	e.clock.Advance(200 * time.Millisecond)
	assert.Equal(t, "ALLOCATING_RESOURCES", e.tunnel(t, "ta").GetStatus())
	e.settle()
	assert.Equal(t, "ESTABLISHED", e.tunnel(t, "ta").GetStatus())
	assert.Equal(t, "ESTABLISHED", e.tunnel(t, "tb").GetStatus())

	mismatch := haTunnel("tc", gwA, gwB, 1)
	mismatch.SharedSecret = s("other")
	require.NoError(t, e.insertTunnel(t, mismatch))

	back := haTunnel("td", gwB, gwA, 1)
	require.NoError(t, e.insertTunnel(t, back))
	assert.Equal(t, "AUTHORIZATION_ERROR", e.tunnel(t, "tc").GetStatus())
	assert.Equal(t, "AUTHORIZATION_ERROR", e.tunnel(t, "td").GetStatus())

	lone := &computepb.VpnTunnel{
		Name: s("te"), VpnGateway: s(gwA), VpnGatewayInterface: new(int32(0)), PeerExternalGateway: s(extRef),
		PeerExternalGatewayInterface: new(int32(1)), SharedSecret: s("x"),
	}
	require.NoError(t, e.insertTunnel(t, lone))
	assert.Equal(t, "NO_INCOMING_PACKETS", e.tunnel(t, "te").GetStatus(), "no peer outside the project ever answers")

	st, err := e.vpnGws(t).GetStatus(t.Context(), &computepb.GetStatusVpnGatewayRequest{
		Project: project, Region: region, VpnGateway: "gw-a",
	})
	require.NoError(t, err)

	conns := st.GetResult().GetVpnConnections()
	require.Len(t, conns, 2)

	for _, c := range conns {
		switch {
		case c.GetPeerGcpGateway() != "":
			assert.Equal(t, "CONNECTION_REDUNDANCY_MET", c.GetState().GetState())
			assert.Len(t, c.GetTunnels(), 2)
		default:
			assert.Equal(t, "CONNECTION_REDUNDANCY_NOT_MET", c.GetState().GetState())
			assert.Equal(t, "INCOMPLETE_TUNNELS_COVERAGE", c.GetState().GetUnsatisfiedReason())
			assert.EqualValues(t, 1, c.GetTunnels()[0].GetPeerGatewayInterface())
		}
	}
}

func TestClassicVPN(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)

	tg := client(t, e, compute.NewTargetVpnGatewaysRESTClient)
	op, err := tg.Insert(t.Context(), &computepb.InsertTargetVpnGatewayRequest{
		Project: project, Region: region, TargetVpnGatewayResource: &computepb.TargetVpnGateway{
			Name: s("classic"), Network: s(vpcRef),
		},
	})
	e.done(t, op, err)

	got, err := tg.Get(
		t.Context(),
		&computepb.GetTargetVpnGatewayRequest{Project: project, Region: region, TargetVpnGateway: "classic"},
	)
	require.NoError(t, err)
	assert.Equal(t, "READY", got.GetStatus())

	op, err = e.addresses(t).Insert(t.Context(), &computepb.InsertAddressRequest{
		Project: project, Region: region, AddressResource: &computepb.Address{Name: s("vpn-ip")},
	})
	e.done(t, op, err)

	tunnel := &computepb.VpnTunnel{
		Name: s("ta"), TargetVpnGateway: s(regBase + "targetVpnGateways/classic"), PeerIp: s("198.51.100.7"),
		SharedSecret: s(
			"x",
		), LocalTrafficSelector: []string{"10.10.0.0/24"}, RemoteTrafficSelector: []string{"192.168.0.0/16"},
		IkeVersion: new(int32(1)),
	}
	require.NoError(t, e.insertTunnel(t, tunnel))
	assert.Equal(t, "WAITING_FOR_FULL_CONFIG", e.tunnel(t, "ta").GetStatus())
	assert.EqualValues(t, 1, e.tunnel(t, "ta").GetIkeVersion())

	rules := client(t, e, compute.NewForwardingRulesRESTClient)
	rule := func(name, proto, ports string) {
		fr := &computepb.ForwardingRule{
			Name: s(name), IPProtocol: s(proto), IPAddress: s(regBase + "addresses/vpn-ip"),
			Target: s(regBase + "targetVpnGateways/classic"),
		}
		if ports != "" {
			fr.PortRange = s(ports)
		}

		op, err := rules.Insert(t.Context(), &computepb.InsertForwardingRuleRequest{
			Project: project, Region: region, ForwardingRuleResource: fr,
		})
		e.done(t, op, err)
	}

	rule("esp", "ESP", "")
	rule("udp500", "UDP", "500")
	rule("udp4500", "UDP", "4500")
	assert.Equal(t, "WAITING_FOR_FULL_CONFIG", e.tunnel(t, "ta").GetStatus(), "a route or router is still missing")

	op, err = e.routes(t).Insert(t.Context(), &computepb.InsertRouteRequest{
		Project: project, RouteResource: &computepb.Route{
			Name: s("to-remote"), Network: s(vpcRef), DestRange: s("192.168.0.0/16"), NextHopVpnTunnel: s(tunARef),
		},
	})
	e.done(t, op, err)
	assert.Equal(t, "NO_INCOMING_PACKETS", e.tunnel(t, "ta").GetStatus())

	route, err := e.routes(t).Get(t.Context(), &computepb.GetRouteRequest{Project: project, Route: "to-remote"})
	require.NoError(t, err)
	assert.Contains(t, route.GetNextHopVpnTunnel(), "/vpnTunnels/ta")

	got, err = tg.Get(
		t.Context(),
		&computepb.GetTargetVpnGatewayRequest{Project: project, Region: region, TargetVpnGateway: "classic"},
	)
	require.NoError(t, err)
	assert.Len(t, got.GetForwardingRules(), 3)
	assert.Len(t, got.GetTunnels(), 1)

	_, err = e.vpnTunnels(t).
		Delete(t.Context(), &computepb.DeleteVpnTunnelRequest{Project: project, Region: region, VpnTunnel: "ta"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	_, err = tg.Delete(
		t.Context(),
		&computepb.DeleteTargetVpnGatewayRequest{Project: project, Region: region, TargetVpnGateway: "classic"},
	)
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")
}

func TestClassicTunnelValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mut    func(tun *computepb.VpnTunnel)
		name   string
		status int
	}{
		{name: "ok", mut: func(*computepb.VpnTunnel) {}},
		{name: "no peer ip", status: http.StatusBadRequest, mut: func(tun *computepb.VpnTunnel) { tun.PeerIp = nil }},
		{
			name:   "ipv6 peer",
			status: http.StatusBadRequest,
			mut:    func(tun *computepb.VpnTunnel) { tun.PeerIp = s("2001:db8::1") },
		},
		{
			name: "bad selector", status: http.StatusBadRequest,
			mut: func(tun *computepb.VpnTunnel) { tun.LocalTrafficSelector = []string{"nope"} },
		},
		{
			name: "peer gateway", status: http.StatusBadRequest,
			mut: func(tun *computepb.VpnTunnel) { tun.PeerGcpGateway = s(gwB) },
		},
		{
			name: "missing target", status: http.StatusNotFound,
			mut: func(tun *computepb.VpnTunnel) { tun.TargetVpnGateway = s(regBase + "targetVpnGateways/none") },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)
			e.haGateway(t, "gw-b")

			op, err := client(
				t,
				e,
				compute.NewTargetVpnGatewaysRESTClient,
			).Insert(t.Context(), &computepb.InsertTargetVpnGatewayRequest{
				Project: project, Region: region, TargetVpnGatewayResource: &computepb.TargetVpnGateway{Name: s("classic"), Network: s(vpcRef)},
			})
			e.done(t, op, err)

			tun := &computepb.VpnTunnel{
				Name: s(
					"ta",
				), TargetVpnGateway: s(regBase + "targetVpnGateways/classic"), PeerIp: s("198.51.100.7"), SharedSecret: s("x"),
			}
			tt.mut(tun)

			err = e.insertTunnel(t, tun)
			if tt.status != 0 {
				assertAPIError(t, err, tt.status, "")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, []string{"0.0.0.0/0"}, e.tunnel(t, "ta").GetRemoteTrafficSelector())
		})
	}
}

func TestRouterBGPPeersOverTunnels(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)
	e.haGateway(t, "gw-a")
	e.haGateway(t, "gw-b")

	routerOf := func(name string, ifc *computepb.RouterInterface, peer *computepb.RouterBgpPeer) *computepb.Router {
		r := &computepb.Router{Name: s(name), Network: s(vpcRef), Bgp: &computepb.RouterBgp{Asn: new(uint32(65001))}}
		if ifc != nil {
			r.Interfaces = []*computepb.RouterInterface{ifc}
		}

		if peer != nil {
			r.BgpPeers = []*computepb.RouterBgpPeer{peer}
		}

		return r
	}

	insertRouter := func(r *computepb.Router) error {
		op, err := e.routers(t).
			Insert(t.Context(), &computepb.InsertRouterRequest{Project: project, Region: region, RouterResource: r})

		return e.finish(t, op, err)
	}

	for _, tun := range []*computepb.VpnTunnel{haTunnel("ta", gwA, gwB, 0), haTunnel("tb", gwB, gwA, 0)} {
		tun.Router = s(regBase + "routers/r1")
	}

	require.NoError(t, insertRouter(routerOf("r1", nil, nil)))

	for _, tun := range []*computepb.VpnTunnel{haTunnel("ta", gwA, gwB, 0), haTunnel("tb", gwB, gwA, 0)} {
		tun.Router = s(regBase + "routers/r1")
		require.NoError(t, e.insertTunnel(t, tun))
	}

	ifc := &computepb.RouterInterface{Name: s("if0"), LinkedVpnTunnel: s(tunARef), IpRange: s("169.254.0.1/30")}
	peer := &computepb.RouterBgpPeer{
		Name: s(
			"p0",
		), InterfaceName: s("if0"), IpAddress: s("169.254.0.1"), PeerIpAddress: s("169.254.0.2"), PeerAsn: new(uint32(65002)),
	}

	op, err := e.routers(t).Patch(t.Context(), &computepb.PatchRouterRequest{
		Project: project, Region: region, Router: "r1", RouterResource: routerOf("r1", ifc, peer),
	})
	e.done(t, op, err)

	got, err := e.routers(t).
		Get(t.Context(), &computepb.GetRouterRequest{Project: project, Region: region, Router: "r1"})
	require.NoError(t, err)
	require.Len(t, got.GetBgpPeers(), 1)
	assert.Equal(t, "TRUE", got.GetBgpPeers()[0].GetEnable())
	assert.Equal(t, "DEFAULT", got.GetBgpPeers()[0].GetAdvertiseMode())
	assert.Contains(t, got.GetInterfaces()[0].GetLinkedVpnTunnel(), "/vpnTunnels/ta")

	status, err := e.routers(t).GetRouterStatus(t.Context(), &computepb.GetRouterStatusRouterRequest{
		Project: project, Region: region, Router: "r1",
	})
	require.NoError(t, err)

	ps := status.GetResult().GetBgpPeerStatus()
	require.Len(t, ps, 1)
	assert.Equal(t, "UP", ps[0].GetStatus())
	assert.Equal(t, "Established", ps[0].GetState())
	assert.Equal(t, "169.254.0.2", ps[0].GetPeerIpAddress())

	_, err = e.routers(t).
		Delete(t.Context(), &computepb.DeleteRouterRequest{Project: project, Region: region, Router: "r1"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")
	_, err = e.vpnTunnels(t).
		Delete(t.Context(), &computepb.DeleteVpnTunnelRequest{Project: project, Region: region, VpnTunnel: "ta"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	op, err = e.vpnTunnels(t).
		Delete(t.Context(), &computepb.DeleteVpnTunnelRequest{Project: project, Region: region, VpnTunnel: "tb"})
	e.done(t, op, err)

	status, err = e.routers(t).GetRouterStatus(t.Context(), &computepb.GetRouterStatusRouterRequest{
		Project: project, Region: region, Router: "r1",
	})
	require.NoError(t, err)
	assert.Equal(t, "DOWN", status.GetResult().GetBgpPeerStatus()[0].GetStatus(), "the other end is gone")
	assert.Equal(t, "Idle", status.GetResult().GetBgpPeerStatus()[0].GetState())
}

func TestRouterVPNValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ifc    *computepb.RouterInterface
		peer   *computepb.RouterBgpPeer
		name   string
		status int
	}{
		{
			name:   "public range",
			status: http.StatusBadRequest,
			ifc:    &computepb.RouterInterface{Name: s("if0"), IpRange: s("10.0.0.1/30")},
		},
		{
			name:   "bad range",
			status: http.StatusBadRequest,
			ifc:    &computepb.RouterInterface{Name: s("if0"), IpRange: s("nope")},
		},
		{name: "ula range", ifc: &computepb.RouterInterface{Name: s("if0"), IpRange: s("fdff:1::1/126")}},
		{
			name:   "missing tunnel",
			status: http.StatusNotFound,
			ifc:    &computepb.RouterInterface{Name: s("if0"), LinkedVpnTunnel: s(regBase + "vpnTunnels/none")},
		},
		{
			name: "interconnect", status: http.StatusBadRequest,
			ifc: &computepb.RouterInterface{Name: s("if0"), LinkedInterconnectAttachment: s("x")},
		},
		{
			name: "peer without interface", status: http.StatusBadRequest,
			peer: &computepb.RouterBgpPeer{Name: s("p0"), InterfaceName: s("nope"), PeerAsn: new(uint32(65002))},
		},
		{
			name: "peer asn", status: http.StatusBadRequest,
			ifc:  &computepb.RouterInterface{Name: s("if0")},
			peer: &computepb.RouterBgpPeer{Name: s("p0"), InterfaceName: s("if0")},
		},
		{
			name: "peer ip", status: http.StatusBadRequest,
			ifc: &computepb.RouterInterface{Name: s("if0")},
			peer: &computepb.RouterBgpPeer{
				Name:          s("p0"),
				InterfaceName: s("if0"),
				PeerAsn:       new(uint32(1)),
				PeerIpAddress: s("x"),
			},
		},
		{
			name: "peer ok",
			ifc:  &computepb.RouterInterface{Name: s("if0")},
			peer: &computepb.RouterBgpPeer{Name: s("p0"), InterfaceName: s("if0"), PeerAsn: new(uint32(4200000000))},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)

			r := &computepb.Router{Name: s("r"), Network: s(vpcRef)}
			if tt.ifc != nil {
				r.Interfaces = []*computepb.RouterInterface{tt.ifc}
			}

			if tt.peer != nil {
				r.BgpPeers = []*computepb.RouterBgpPeer{tt.peer}
			}

			op, err := e.routers(t).
				Insert(t.Context(), &computepb.InsertRouterRequest{Project: project, Region: region, RouterResource: r})
			if tt.status != 0 {
				assertAPIError(t, e.finish(t, op, err), tt.status, "")

				return
			}

			e.done(t, op, err)
		})
	}
}

func TestRouteNextHops(t *testing.T) {
	t.Parallel()

	tests := []struct {
		route  *computepb.Route
		name   string
		status int
	}{
		{
			name:  "tunnel",
			route: &computepb.Route{Name: s("r"), DestRange: s("192.168.0.0/16"), NextHopVpnTunnel: s(tunARef)},
		},
		{
			name: "missing tunnel", status: http.StatusNotFound,
			route: &computepb.Route{
				Name:             s("r"),
				DestRange:        s("192.168.0.0/16"),
				NextHopVpnTunnel: s(regBase + "vpnTunnels/none"),
			},
		},
		{
			name: "tunnel and ip", status: http.StatusBadRequest,
			route: &computepb.Route{
				Name:             s("r"),
				DestRange:        s("192.168.0.0/16"),
				NextHopVpnTunnel: s(tunARef),
				NextHopIp:        s("10.10.0.5"),
			},
		},
		{
			name: "ilb by url",
			route: &computepb.Route{
				Name:       s("r"),
				DestRange:  s("192.168.0.0/16"),
				NextHopIlb: s(regBase + "forwardingRules/ilb"),
			},
		},
		{
			name:  "ilb by ip",
			route: &computepb.Route{Name: s("r"), DestRange: s("192.168.0.0/16"), NextHopIlb: s("10.10.0.2")},
		},
		{
			name: "ilb unknown ip", status: http.StatusBadRequest,
			route: &computepb.Route{Name: s("r"), DestRange: s("192.168.0.0/16"), NextHopIlb: s("10.10.0.99")},
		},
		{
			name: "ilb missing rule", status: http.StatusNotFound,
			route: &computepb.Route{
				Name:       s("r"),
				DestRange:  s("192.168.0.0/16"),
				NextHopIlb: s(regBase + "forwardingRules/none"),
			},
		},
		{
			name: "network is output only", status: http.StatusBadRequest,
			route: &computepb.Route{Name: s("r"), DestRange: s("192.168.0.0/16"), NextHopNetwork: s(vpcRef)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)
			e.haGateway(t, "gw-a")
			e.haGateway(t, "gw-b")
			require.NoError(t, e.insertTunnel(t, haTunnel("ta", gwA, gwB, 0)))
			e.internalRule(t)

			tt.route.Network = s(vpcRef)
			op, err := e.routes(t).
				Insert(t.Context(), &computepb.InsertRouteRequest{Project: project, RouteResource: tt.route})

			if tt.status != 0 {
				assertAPIError(t, e.finish(t, op, err), tt.status, "")

				return
			}

			e.done(t, op, err)

			got, gerr := e.routes(t).Get(t.Context(), &computepb.GetRouteRequest{Project: project, Route: "r"})
			require.NoError(t, gerr)
			assert.NotEmpty(t, got.GetNextHopVpnTunnel()+got.GetNextHopIlb())
		})
	}
}

func (e *env) internalRule(t *testing.T) {
	t.Helper()

	op, err := client(
		t,
		e,
		compute.NewRegionHealthChecksRESTClient,
	).Insert(t.Context(), &computepb.InsertRegionHealthCheckRequest{
		Project: project, Region: region, HealthCheckResource: &computepb.HealthCheck{Name: s("rhc"), TcpHealthCheck: &computepb.TCPHealthCheck{}},
	})
	e.done(t, op, err)

	op, err = client(
		t,
		e,
		compute.NewRegionBackendServicesRESTClient,
	).Insert(t.Context(), &computepb.InsertRegionBackendServiceRequest{
		Project: project, Region: region, BackendServiceResource: &computepb.BackendService{
			Name: s(
				"rbs",
			), Protocol: s("TCP"), LoadBalancingScheme: s("INTERNAL"), HealthChecks: []string{regBase + "healthChecks/rhc"},
		},
	})
	e.done(t, op, err)

	op, err = client(
		t,
		e,
		compute.NewForwardingRulesRESTClient,
	).Insert(t.Context(), &computepb.InsertForwardingRuleRequest{
		Project: project, Region: region, ForwardingRuleResource: &computepb.ForwardingRule{
			Name: s("ilb"), LoadBalancingScheme: s("INTERNAL"), Subnetwork: s(subnetRef), Ports: []string{"80"},
			BackendService: s(regBase + "backendServices/rbs"),
		},
	})
	e.done(t, op, err)
}
