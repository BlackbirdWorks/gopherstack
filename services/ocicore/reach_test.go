package ocicore_test

import (
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ocicore"
)

func egressTo(dest, proto string, stateless bool, tcp *core.TcpOptions) core.EgressSecurityRule {
	return core.EgressSecurityRule{Protocol: new(proto), Destination: new(dest), IsStateless: new(stateless),
		TcpOptions: tcp}
}

func ingressFrom(src, proto string, stateless bool, tcp *core.TcpOptions) core.IngressSecurityRule {
	return core.IngressSecurityRule{Protocol: new(proto), Source: new(src), IsStateless: new(stateless),
		TcpOptions: tcp}
}

func tcpPort(lo, hi int) *core.TcpOptions {
	return &core.TcpOptions{DestinationPortRange: &core.PortRange{Min: new(lo), Max: new(hi)}}
}

func tcpFromPort(lo, hi int) *core.TcpOptions {
	return &core.TcpOptions{SourcePortRange: &core.PortRange{Min: new(lo), Max: new(hi)}}
}

type reachSide struct {
	ingress []core.IngressSecurityRule
	egress  []core.EgressSecurityRule
	nsgs    []string
}

type reachSetup struct {
	subnet [2]core.Subnet
	vcn    core.Vcn
	e      *env
	nicA   oci.ServiceNic
	nicB   oci.ServiceNic
}

func (e *env) mkSLSubnet(t *testing.T, v core.Vcn, cidr string, side reachSide) core.Subnet {
	t.Helper()

	sl, err := e.client.CreateSecurityList(t.Context(), core.CreateSecurityListRequest{CompartmentId: new(testComp),
		VcnId: v.Id, IngressSecurityRules: append([]core.IngressSecurityRule{}, side.ingress...),
		EgressSecurityRules: append([]core.EgressSecurityRule{}, side.egress...)})
	require.NoError(t, err)

	r, err := e.client.CreateSubnet(t.Context(), core.CreateSubnetRequest{CompartmentId: new(testComp), VcnId: v.Id,
		CidrBlock: new(cidr), SecurityListIds: []string{*sl.Id}})
	require.NoError(t, err)

	return r.Subnet
}

func (e *env) reachPair(t *testing.T, a, b reachSide) reachSetup {
	t.Helper()

	v := e.mkVcn(t, "10.0.0.0/16", "reach")
	sa, sb := e.mkSLSubnet(t, v, "10.0.1.0/24", a), e.mkSLSubnet(t, v, "10.0.2.0/24", b)
	e.settle()

	mk := func(s core.Subnet, side reachSide) oci.ServiceNic {
		n, err := e.backend.CreateServiceNic(oci.ServiceNicIn{CompartmentID: testComp, AD: testAD, SubnetID: *s.Id,
			NsgIDs: side.nsgs})
		require.NoError(t, err)

		return n
	}

	return reachSetup{e: e, vcn: v, nicA: mk(sa, a), nicB: mk(sb, b), subnet: [2]core.Subnet{sa, sb}}
}

func (r reachSetup) check(t *testing.T, f oci.Flow) oci.Verdict {
	t.Helper()

	v, err := r.e.backend.CanReach(oci.Endpoint{VnicID: r.nicA.VnicID}, oci.Endpoint{VnicID: r.nicB.VnicID}, f)
	require.NoError(t, err)

	return v
}

func TestCanReachSecurityLists(t *testing.T) {
	t.Parallel()

	openOut := []core.EgressSecurityRule{egressTo("0.0.0.0/0", "all", false, nil)}
	web := oci.Flow{Protocol: "tcp", Port: 443}

	tests := []struct {
		name        string
		a, b        reachSide
		flow        oci.Flow
		wantAllowed bool
		wantForward bool
		wantReply   bool
	}{
		{
			name: "stateful both ways",
			a:    reachSide{egress: openOut},
			b: reachSide{
				ingress: []core.IngressSecurityRule{ingressFrom("10.0.1.0/24", "6", false, tcpPort(443, 443))},
			},
			flow:        web,
			wantAllowed: true,
			wantForward: true,
			wantReply:   true,
		},
		{
			name: "port outside the rule",
			a:    reachSide{egress: openOut},
			b: reachSide{
				ingress: []core.IngressSecurityRule{ingressFrom("10.0.1.0/24", "6", false, tcpPort(80, 80))},
			},
			flow: web,
		},
		{
			name: "source cidr not matching", a: reachSide{egress: openOut},
			b:    reachSide{ingress: []core.IngressSecurityRule{ingressFrom("192.168.0.0/16", "6", false, nil)}},
			flow: web,
		},
		{
			name: "no egress rule", a: reachSide{},
			b:    reachSide{ingress: []core.IngressSecurityRule{ingressFrom("0.0.0.0/0", "all", false, nil)}},
			flow: web,
		},
		{
			name: "egress limited to another port", a: reachSide{egress: []core.EgressSecurityRule{
				egressTo("10.0.2.0/24", "6", false, tcpPort(22, 22))}},
			b:    reachSide{ingress: []core.IngressSecurityRule{ingressFrom("0.0.0.0/0", "all", false, nil)}},
			flow: web,
		},
		{
			name: "stateless ingress without return rule",
			a:    reachSide{egress: openOut},
			b: reachSide{
				ingress: []core.IngressSecurityRule{ingressFrom("10.0.1.0/24", "6", true, tcpPort(443, 443))},
			},
			flow:        web,
			wantForward: true,
		},
		{
			name: "stateless ingress with return rule", a: reachSide{egress: openOut},
			b: reachSide{
				ingress: []core.IngressSecurityRule{ingressFrom("10.0.1.0/24", "6", true, tcpPort(443, 443))},
				egress:  []core.EgressSecurityRule{egressTo("10.0.1.0/24", "6", true, tcpFromPort(443, 443))},
			},
			flow: web, wantAllowed: true, wantForward: true, wantReply: true,
		},
		{
			name: "return rule for the wrong source port", a: reachSide{egress: openOut},
			b: reachSide{
				ingress: []core.IngressSecurityRule{ingressFrom("10.0.1.0/24", "6", true, tcpPort(443, 443))},
				egress:  []core.EgressSecurityRule{egressTo("10.0.1.0/24", "6", true, tcpFromPort(80, 80))},
			},
			flow: web, wantForward: true,
		},
		{
			name: "stateless egress needs a stateless ingress at the source",
			a:    reachSide{egress: []core.EgressSecurityRule{egressTo("10.0.2.0/24", "6", true, tcpPort(443, 443))}},
			b:    reachSide{ingress: []core.IngressSecurityRule{ingressFrom("10.0.1.0/24", "6", false, nil)}},
			flow: web, wantForward: true,
		},
		{
			name: "stateless egress with return ingress",
			a: reachSide{
				egress:  []core.EgressSecurityRule{egressTo("10.0.2.0/24", "6", true, tcpPort(443, 443))},
				ingress: []core.IngressSecurityRule{ingressFrom("10.0.2.0/24", "6", true, tcpFromPort(443, 443))},
			},
			b:    reachSide{ingress: []core.IngressSecurityRule{ingressFrom("10.0.1.0/24", "6", false, nil)}},
			flow: web, wantAllowed: true, wantForward: true, wantReply: true,
		},
		{
			name: "a stateful rule wins over a stateless one", a: reachSide{egress: openOut},
			b: reachSide{ingress: []core.IngressSecurityRule{
				ingressFrom("10.0.1.0/24", "6", true, nil), ingressFrom("10.0.1.0/24", "all", false, nil)}},
			flow: web, wantAllowed: true, wantForward: true, wantReply: true,
		},
		{
			name: "udp does not match a tcp rule", a: reachSide{egress: openOut},
			b:    reachSide{ingress: []core.IngressSecurityRule{ingressFrom("0.0.0.0/0", "6", false, nil)}},
			flow: oci.Flow{Protocol: "udp", Port: 53},
		},
		{
			name: "protocol numbers are accepted", a: reachSide{egress: openOut},
			b:    reachSide{ingress: []core.IngressSecurityRule{ingressFrom("0.0.0.0/0", "17", false, nil)}},
			flow: oci.Flow{Protocol: "17", Port: 53}, wantAllowed: true, wantForward: true, wantReply: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := newEnv(t, false).reachPair(t, tt.a, tt.b)
			v := r.check(t, tt.flow)

			assert.Equal(t, tt.wantAllowed, v.Allowed, v.String())
			assert.Equal(t, tt.wantForward, v.Forward)
			assert.Equal(t, tt.wantReply, v.Reply)

			if !tt.wantAllowed {
				assert.NotEmpty(t, v.Reason)
			}
		})
	}
}

func TestCanReachIcmp(t *testing.T) {
	t.Parallel()

	echo := core.IcmpOptions{Type: new(8)}
	reply := core.IcmpOptions{Type: new(0)}
	openOut := []core.EgressSecurityRule{egressTo("0.0.0.0/0", "all", false, nil)}

	tests := []struct {
		name    string
		ingress core.IngressSecurityRule
		egress  []core.EgressSecurityRule
		want    bool
	}{
		{"all icmp", core.IngressSecurityRule{Protocol: new("1"), Source: new("0.0.0.0/0")}, nil, true},
		{
			"echo only",
			core.IngressSecurityRule{Protocol: new("1"), Source: new("0.0.0.0/0"), IcmpOptions: &echo},
			nil,
			true,
		},
		{"other type", core.IngressSecurityRule{Protocol: new("1"), Source: new("0.0.0.0/0"),
			IcmpOptions: &core.IcmpOptions{Type: new(3), Code: new(4)}}, nil, false},
		{"stateless echo without reply", core.IngressSecurityRule{Protocol: new("1"), Source: new("0.0.0.0/0"),
			IsStateless: new(true), IcmpOptions: &echo}, nil, false},
		{"stateless echo with reply", core.IngressSecurityRule{Protocol: new("1"), Source: new("0.0.0.0/0"),
			IsStateless: new(true), IcmpOptions: &echo}, []core.EgressSecurityRule{
			{Protocol: new("1"), Destination: new("0.0.0.0/0"), IsStateless: new(true), IcmpOptions: &reply}}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := newEnv(t, false).reachPair(t, reachSide{egress: openOut},
				reachSide{ingress: []core.IngressSecurityRule{tt.ingress}, egress: tt.egress})

			v := r.check(t, oci.Flow{Protocol: "icmp", IcmpType: 8})
			assert.Equal(t, tt.want, v.Allowed, v.String())
		})
	}
}

func TestCanReachNetworkSecurityGroups(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	v := e.mkVcn(t, "10.0.0.0/16", "nsg")
	nsgs := make([]string, 0, 3)

	for range 3 {
		n, err := e.client.CreateNetworkSecurityGroup(ctx, core.CreateNetworkSecurityGroupRequest{
			CompartmentId: new(testComp), VcnId: v.Id})
		require.NoError(t, err)

		nsgs = append(nsgs, *n.Id)
	}

	clients, servers, other := nsgs[0], nsgs[1], nsgs[2]

	add := func(nsg string, d core.AddSecurityRuleDetails) {
		_, err := e.client.AddNetworkSecurityGroupSecurityRules(ctx, core.AddNetworkSecurityGroupSecurityRulesRequest{
			NetworkSecurityGroupId: &nsg, SecurityRules: []core.AddSecurityRuleDetails{d}})
		require.NoError(t, err)
	}

	add(clients, core.AddSecurityRuleDetails{Direction: core.AddSecurityRuleDetailsDirectionEgress, Protocol: new("6"),
		Destination: new(servers), DestinationType: core.AddSecurityRuleDetailsDestinationTypeNetworkSecurityGroup,
		TcpOptions: tcpPort(5432, 5432)})
	add(servers, core.AddSecurityRuleDetails{Direction: core.AddSecurityRuleDetailsDirectionIngress, Protocol: new("6"),
		Source: new(clients), SourceType: core.AddSecurityRuleDetailsSourceTypeNetworkSecurityGroup,
		TcpOptions: tcpPort(5432, 5432)})

	sub := e.mkSLSubnet(t, v, "10.0.1.0/24", reachSide{})
	e.settle()

	mk := func(ids ...string) oci.ServiceNic {
		n, err := e.backend.CreateServiceNic(oci.ServiceNicIn{CompartmentID: testComp, AD: testAD, SubnetID: *sub.Id,
			NsgIDs: ids})
		require.NoError(t, err)

		return n
	}

	client, server, stranger := mk(clients), mk(servers), mk(other)

	tests := []struct {
		name     string
		src, dst oci.ServiceNic
		port     int
		want     bool
	}{
		{"client to server", client, server, 5432, true},
		{"wrong port", client, server, 3306, false},
		{"stranger to server", stranger, server, 5432, false},
		{"client to stranger", client, stranger, 5432, false},
		{"server to client", server, client, 5432, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			v, err := e.backend.CanReach(oci.Endpoint{VnicID: tt.src.VnicID}, oci.Endpoint{VnicID: tt.dst.VnicID},
				oci.Flow{Protocol: "tcp", Port: tt.port})
			require.NoError(t, err)
			assert.Equal(t, tt.want, v.Allowed, v.String())
		})
	}
}

func TestCanReachEndpoints(t *testing.T) {
	t.Parallel()

	open := reachSide{
		ingress: []core.IngressSecurityRule{ingressFrom("0.0.0.0/0", "all", false, nil)},
		egress:  []core.EgressSecurityRule{egressTo("0.0.0.0/0", "all", false, nil)},
	}
	r := newEnv(t, false).reachPair(t, open, open)

	byIP, err := r.e.backend.CanReach(oci.Endpoint{IP: r.nicA.IPAddress}, oci.Endpoint{IP: r.nicB.IPAddress},
		oci.Flow{Protocol: "tcp", Port: 80})
	require.NoError(t, err)
	assert.True(t, byIP.Allowed)

	_, err = r.e.backend.CanReach(oci.Endpoint{IP: "10.0.9.9"}, oci.Endpoint{VnicID: r.nicB.VnicID}, oci.Flow{})
	require.ErrorIs(t, err, oci.ErrEndpoint)

	_, err = r.e.backend.CanReach(oci.Endpoint{}, oci.Endpoint{VnicID: r.nicB.VnicID}, oci.Flow{})
	require.ErrorIs(t, err, oci.ErrEndpoint)

	_, err = r.e.backend.CanReach(
		oci.Endpoint{VnicID: r.nicA.VnicID},
		oci.Endpoint{VnicID: "ocid1.vnic.oc1.iad.nope"},
		oci.Flow{},
	)
	require.ErrorIs(t, err, oci.ErrEndpoint)

	other := r.e.mkVcn(t, "192.168.0.0/16", "reachother")
	sub := r.e.mkSLSubnet(t, other, "192.168.1.0/24", open)
	r.e.settle()

	far, err := r.e.backend.CreateServiceNic(oci.ServiceNicIn{CompartmentID: testComp, AD: testAD, SubnetID: *sub.Id})
	require.NoError(t, err)

	cross, err := r.e.backend.CanReach(
		oci.Endpoint{VnicID: r.nicA.VnicID},
		oci.Endpoint{VnicID: far.VnicID},
		oci.Flow{},
	)
	require.NoError(t, err)
	assert.False(t, cross.Allowed)
	assert.Contains(t, cross.Reason, "different VCNs")

	assert.Equal(t, "allowed", byIP.String())
}

func TestClassifyPeer(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	openOut := []core.EgressSecurityRule{egressTo("0.0.0.0/0", "all", false, nil)}
	rs := e.reachPair(t,
		reachSide{egress: openOut},
		reachSide{ingress: []core.IngressSecurityRule{ingressFrom("10.0.1.0/24", "6", false, tcpPort(443, 443))}},
	)

	other := e.mkVcn(t, "10.1.0.0/16", "other")
	otherSub := e.mkSLSubnet(t, other, "10.1.1.0/24", reachSide{egress: openOut})
	e.settle()

	foreign, err := e.backend.CreateServiceNic(
		oci.ServiceNicIn{CompartmentID: testComp, AD: testAD, SubnetID: *otherSub.Id},
	)
	require.NoError(t, err)

	tests := []struct {
		name string
		src  string
		port int
		want oci.PeerClass
	}{
		{"permitted", rs.nicA.IPAddress, 443, oci.PeerAllowed},
		{"port not permitted", rs.nicA.IPAddress, 80, oci.PeerDenied},
		{"address owned by no vnic", "192.0.2.1", 443, oci.PeerUnknown},
		{"vnic in another vcn", foreign.IPAddress, 443, oci.PeerOtherVCN},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := e.backend.ClassifyPeer(tt.src, rs.nicB.VnicID, oci.Flow{Protocol: "tcp", Port: tt.port})
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSetServiceNicNsgs(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	v := e.mkVcn(t, "10.0.0.0/16", "setnsg")
	sub := e.mkSLSubnet(t, v, "10.0.1.0/24", reachSide{})
	e.settle()

	nsg, err := e.client.CreateNetworkSecurityGroup(t.Context(), core.CreateNetworkSecurityGroupRequest{
		CompartmentId: new(testComp), VcnId: v.Id})
	require.NoError(t, err)

	nic, err := e.backend.CreateServiceNic(oci.ServiceNicIn{CompartmentID: testComp, AD: testAD, SubnetID: *sub.Id})
	require.NoError(t, err)

	require.Error(t, e.backend.SetServiceNicNsgs(nic.VnicID, []string{"ocid1.networksecuritygroup.oc1..none"}))
	require.NoError(t, e.backend.SetServiceNicNsgs(nic.VnicID, []string{*nsg.Id}))

	members, err := e.client.ListNetworkSecurityGroupVnics(t.Context(), core.ListNetworkSecurityGroupVnicsRequest{
		NetworkSecurityGroupId: nsg.Id})
	require.NoError(t, err)
	require.Len(t, members.Items, 1)
	assert.Equal(t, nic.VnicID, *members.Items[0].VnicId)

	require.NoError(t, e.backend.SetServiceNicNsgs(nic.VnicID, nil))

	members, err = e.client.ListNetworkSecurityGroupVnics(t.Context(), core.ListNetworkSecurityGroupVnicsRequest{
		NetworkSecurityGroupId: nsg.Id})
	require.NoError(t, err)
	assert.Empty(t, members.Items)
}
