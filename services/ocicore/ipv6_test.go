package ocicore_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type v6Fix struct {
	vcn    core.Vcn
	subnet core.Subnet
	plain  core.Subnet
}

func (e *env) v6Fixture(t *testing.T) v6Fix {
	t.Helper()

	ctx := t.Context()
	v, err := e.client.CreateVcn(ctx, core.CreateVcnRequest{CompartmentId: new(testComp),
		CidrBlocks: []string{"10.0.0.0/16"}, IsIpv6Enabled: new(true), DnsLabel: new("v6fix")})
	require.NoError(t, err)

	s, err := e.client.CreateSubnet(ctx, core.CreateSubnetRequest{CompartmentId: new(testComp), VcnId: v.Id,
		CidrBlock: new("10.0.1.0/24"), Ipv6CidrBlock: new(hexSubnet(v.Ipv6CidrBlocks[0])), DnsLabel: new("six")})
	require.NoError(t, err)

	plain := e.mkSubnet(t, v.Vcn, "10.0.2.0/24")
	e.settle()

	return v6Fix{vcn: v.Vcn, subnet: s.Subnet, plain: plain}
}

func (e *env) getIpv6(t *testing.T, id *string) core.Ipv6 {
	t.Helper()

	r, err := e.client.GetIpv6(t.Context(), core.GetIpv6Request{Ipv6Id: id})
	require.NoError(t, err)

	return r.Ipv6
}

func TestIpv6Create(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.v6Fixture(t)
	nic, err := e.nic(t, f.subnet, "")
	require.NoError(t, err)

	plainNic, err := e.nic(t, f.plain, "")
	require.NoError(t, err)

	prefix := hexSubnet(f.vcn.Ipv6CidrBlocks[0])
	base := prefix[:len(prefix)-len("::/64")]
	rt, err := e.client.CreateRouteTable(
		ctx,
		core.CreateRouteTableRequest{CompartmentId: new(testComp), VcnId: f.vcn.Id,
			RouteRules: []core.RouteRule{}},
	)
	require.NoError(t, err)

	tests := []struct {
		name     string
		det      core.CreateIpv6Details
		wantCode string
		wantAddr string
		state    core.Ipv6IpStateEnum
	}{
		{name: "auto address", det: core.CreateIpv6Details{VnicId: new(nic.VnicID)}, wantAddr: base + "::2",
			state: core.Ipv6IpStateAssigned},
		{name: "explicit address", det: core.CreateIpv6Details{VnicId: new(nic.VnicID), IpAddress: new(base + "::99"),
			DisplayName: new("named"), Hostname: new("host1"), RouteTableId: rt.Id},
			wantAddr: base + "::99", state: core.Ipv6IpStateAssigned},
		{name: "reserved without vnic", det: core.CreateIpv6Details{
			SubnetId: f.subnet.Id,
			Lifetime: core.CreateIpv6DetailsLifetimeReserved,
		}, wantAddr: base + "::3", state: core.Ipv6IpStateAvailable},
		{
			name:     "ephemeral without vnic",
			det:      core.CreateIpv6Details{SubnetId: f.subnet.Id},
			wantCode: "InvalidParameter",
		},
		{
			name:     "subnet has no prefix",
			det:      core.CreateIpv6Details{VnicId: new(plainNic.VnicID)},
			wantCode: "InvalidParameter",
		},
		{name: "missing target", det: core.CreateIpv6Details{}, wantCode: "MissingParameter"},
		{
			name:     "address outside prefix",
			det:      core.CreateIpv6Details{VnicId: new(nic.VnicID), IpAddress: new("2001:db8::5")},
			wantCode: "InvalidParameter",
		},
		{name: "router address", det: core.CreateIpv6Details{VnicId: new(nic.VnicID), IpAddress: new(base + "::1")},
			wantCode: "InvalidParameter"},
		{
			name:     "prefix mismatch",
			det:      core.CreateIpv6Details{VnicId: new(nic.VnicID), Ipv6SubnetCidr: new("2603:c020:4000:fff::/64")},
			wantCode: "InvalidParameter",
		},
		{name: "bad prefix length", det: core.CreateIpv6Details{VnicId: new(nic.VnicID), CidrPrefixLength: new(64)},
			wantCode: "InvalidParameter"},
		{name: "bad hostname", det: core.CreateIpv6Details{VnicId: new(nic.VnicID), Hostname: new("-bad-")},
			wantCode: "InvalidParameter"},
		{name: "unknown vnic", det: core.CreateIpv6Details{VnicId: new("ocid1.vnic.oc1.iad.nope")},
			wantCode: "NotAuthorizedOrNotFound"},
		{name: "unknown route table", det: core.CreateIpv6Details{VnicId: new(nic.VnicID),
			RouteTableId: new("ocid1.routetable.oc1.iad.nope")}, wantCode: "NotAuthorizedOrNotFound"},
	}

	for _, tt := range tests {
		// Sequential: automatic addresses depend on what already exists.
		r, cerr := e.client.CreateIpv6(ctx, core.CreateIpv6Request{CreateIpv6Details: tt.det})
		if tt.wantCode != "" {
			assert.Equal(t, tt.wantCode, svcErr(t, cerr).GetCode(), tt.name)

			continue
		}

		require.NoError(t, cerr, tt.name)
		assert.Equal(t, tt.wantAddr, *r.IpAddress, tt.name)
		assert.Equal(t, tt.state, r.IpState, tt.name)
		assert.Equal(t, core.Ipv6LifecycleStateAvailable, r.LifecycleState, tt.name)
		assert.Equal(t, *f.subnet.Id, *r.SubnetId)
		assert.NotEmpty(t, *r.Etag)
	}

	dup := core.CreateIpv6Details{VnicId: new(nic.VnicID), IpAddress: new(base + "::99")}
	_, err = e.client.CreateIpv6(ctx, core.CreateIpv6Request{CreateIpv6Details: dup})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "address already in use")

	vn, err := e.client.GetVnic(ctx, core.GetVnicRequest{VnicId: new(nic.VnicID)})
	require.NoError(t, err)
	assert.Equal(t, []string{base + "::2", base + "::99"}, vn.Ipv6Addresses)
}

func TestIpv6Lifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.v6Fixture(t)
	nicA, err := e.nic(t, f.subnet, "")
	require.NoError(t, err)

	nicB, err := e.nic(t, f.subnet, "")
	require.NoError(t, err)

	plainNic, err := e.nic(t, f.plain, "")
	require.NoError(t, err)

	r, err := e.client.CreateIpv6(ctx, core.CreateIpv6Request{
		VnicId: new(nicA.VnicID), Lifetime: core.CreateIpv6DetailsLifetimeReserved})
	require.NoError(t, err)

	ip := r.Ipv6

	eph, err := e.client.CreateIpv6(ctx, core.CreateIpv6Request{
		VnicId: new(nicA.VnicID)})
	require.NoError(t, err)

	_, err = e.client.UpdateIpv6(ctx, core.UpdateIpv6Request{Ipv6Id: eph.Id, VnicId: new(nicB.VnicID)})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode(), "ephemeral IPv6 stays on its VNIC")

	_, err = e.client.Ipv6VnicDetach(ctx, core.Ipv6VnicDetachRequest{Ipv6Id: eph.Id})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode(), "ephemeral IPv6 cannot be detached")

	up, err := e.client.UpdateIpv6(ctx, core.UpdateIpv6Request{Ipv6Id: ip.Id, VnicId: new(nicB.VnicID),
		DisplayName: new("moved"), Hostname: new("h2")})
	require.NoError(t, err)
	assert.Equal(t, nicB.VnicID, *up.VnicId)
	assert.Equal(t, "moved", *up.DisplayName)
	assert.Equal(t, "h2", *up.Hostname)

	_, err = e.client.UpdateIpv6(ctx, core.UpdateIpv6Request{Ipv6Id: ip.Id, VnicId: new(plainNic.VnicID)})
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode(), "another subnet")

	_, err = e.client.UpdateIpv6(ctx, core.UpdateIpv6Request{Ipv6Id: eph.Id, Hostname: new("h2")})
	assert.Equal(t, "Conflict", svcErr(t, err).GetCode(), "duplicate hostname")

	_, err = e.client.UpdateIpv6(ctx, core.UpdateIpv6Request{Ipv6Id: ip.Id, IfMatch: new("stale"),
		DisplayName: new("x")})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	vnA, err := e.client.GetVnic(ctx, core.GetVnicRequest{VnicId: new(nicA.VnicID)})
	require.NoError(t, err)
	assert.Equal(t, []string{*eph.IpAddress}, vnA.Ipv6Addresses)

	det, err := e.client.Ipv6VnicDetach(ctx, core.Ipv6VnicDetachRequest{Ipv6Id: ip.Id})
	require.NoError(t, err)
	assert.Nil(t, det.VnicId)
	assert.Equal(t, core.Ipv6IpStateAvailable, det.IpState)

	_, err = e.client.Ipv6VnicDetach(ctx, core.Ipv6VnicDetachRequest{Ipv6Id: ip.Id})
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode())

	_, err = e.client.UpdateIpv6(
		ctx,
		core.UpdateIpv6Request{Ipv6Id: ip.Id, Lifetime: core.UpdateIpv6DetailsLifetimeEphemeral},
	)
	assert.Equal(t, "InvalidParameter", svcErr(t, err).GetCode(), "an unassigned IPv6 is reserved")

	back, err := e.client.UpdateIpv6(ctx, core.UpdateIpv6Request{Ipv6Id: ip.Id, VnicId: new(nicA.VnicID)})
	require.NoError(t, err)
	assert.Equal(t, core.Ipv6IpStateAssigned, back.IpState)

	toReserved, err := e.client.UpdateIpv6(ctx, core.UpdateIpv6Request{Ipv6Id: eph.Id,
		Lifetime: core.UpdateIpv6DetailsLifetimeReserved})
	require.NoError(t, err)
	assert.Equal(t, core.Ipv6LifetimeReserved, toReserved.Lifetime)

	l, err := e.client.ListIpv6s(ctx, core.ListIpv6sRequest{VnicId: new(nicA.VnicID)})
	require.NoError(t, err)
	assert.Len(t, l.Items, 2)

	l, err = e.client.ListIpv6s(ctx, core.ListIpv6sRequest{SubnetId: f.subnet.Id, IpAddress: eph.IpAddress})
	require.NoError(t, err)
	require.Len(t, l.Items, 1)
	assert.Equal(t, *eph.Id, *l.Items[0].Id)

	l, err = e.client.ListIpv6s(ctx, core.ListIpv6sRequest{SubnetId: f.subnet.Id, Lifetime: new("EPHEMERAL")})
	require.NoError(t, err)
	assert.Empty(t, l.Items)

	l, err = e.client.ListIpv6s(ctx, core.ListIpv6sRequest{SubnetId: f.subnet.Id, IpState: new("AVAILABLE")})
	require.NoError(t, err)
	assert.Empty(t, l.Items)

	_, err = e.client.ListIpv6s(ctx, core.ListIpv6sRequest{})
	assert.Equal(t, "MissingParameter", svcErr(t, err).GetCode())

	e.backend.DeleteServiceNic(nicA.VnicID)
	e.settle()

	kept := e.getIpv6(t, ip.Id)
	assert.Equal(t, core.Ipv6IpStateAvailable, kept.IpState, "a reserved IPv6 survives its VNIC")
	assert.Nil(t, kept.VnicId)

	_, err = e.client.DeleteIpv6(ctx, core.DeleteIpv6Request{Ipv6Id: ip.Id})
	require.NoError(t, err)
	e.settle()

	assert.Equal(t, core.Ipv6LifecycleStateTerminated, e.getIpv6(t, ip.Id).LifecycleState)

	_, err = e.client.DeleteIpv6(ctx, core.DeleteIpv6Request{Ipv6Id: new("ocid1.ipv6.oc1.iad.nope")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestIpv6EphemeralEndsWithVnic(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	f := e.v6Fixture(t)
	nic, err := e.nic(t, f.subnet, "")
	require.NoError(t, err)

	r, err := e.client.CreateIpv6(ctx, core.CreateIpv6Request{
		VnicId: new(nic.VnicID)})
	require.NoError(t, err)

	e.backend.DeleteServiceNic(nic.VnicID)
	e.settle()

	assert.Equal(t, core.Ipv6LifecycleStateTerminated, e.getIpv6(t, r.Id).LifecycleState)
}

func TestLaunchWithIpv6(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	f := e.v6Fixture(t)

	tests := []struct {
		name     string
		mod      func(*core.CreateVnicDetails)
		subnet   core.Subnet
		wantCode string
		want     int
	}{
		{"assign", func(d *core.CreateVnicDetails) { d.AssignIpv6Ip = new(true) }, f.subnet, "", 1},
		{"pairs", func(d *core.CreateVnicDetails) {
			d.Ipv6AddressIpv6SubnetCidrPairDetails = []core.Ipv6AddressIpv6SubnetCidrPairDetails{
				{Ipv6SubnetCidr: new(hexSubnet(f.vcn.Ipv6CidrBlocks[0]))}, {}}
		}, f.subnet, "", 2},
		{
			"no ipv6 subnet",
			func(d *core.CreateVnicDetails) { d.AssignIpv6Ip = new(true) },
			f.plain,
			"InvalidParameter",
			0,
		},
		{"subnet cidr", func(d *core.CreateVnicDetails) { d.SubnetCidr = new("10.0.1.0/24") }, f.subnet, "", 0},
		{"bad subnet cidr", func(d *core.CreateVnicDetails) { d.SubnetCidr = new("10.9.0.0/24") }, f.subnet,
			"InvalidParameter", 0},
		{"duplicate pair address", func(d *core.CreateVnicDetails) {
			p := hexSubnet(f.vcn.Ipv6CidrBlocks[0])
			a := p[:len(p)-len("::/64")] + "::77"
			d.Ipv6AddressIpv6SubnetCidrPairDetails = []core.Ipv6AddressIpv6SubnetCidrPairDetails{
				{Ipv6Address: new(a)}, {Ipv6Address: new(a)}}
		}, f.subnet, "InvalidParameter", 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			inst, err := e.launch(t, tt.subnet, func(d *core.LaunchInstanceDetails) {
				d.CreateVnicDetails.SubnetId = tt.subnet.Id
				tt.mod(d.CreateVnicDetails)
			})
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, svcErr(t, err).GetCode())

				return
			}

			require.NoError(t, err)
			assert.Len(t, e.vnicOf(t, inst.Id).Ipv6Addresses, tt.want)
		})
	}
}
