package gcpcompute_test

import (
	"net/http"
	"net/netip"
	"testing"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *env) v6Network(t *testing.T) {
	t.Helper()

	op, err := e.networks(t).Insert(t.Context(), &computepb.InsertNetworkRequest{
		Project: project, NetworkResource: &computepb.Network{
			Name: s("vpc"), AutoCreateSubnetworks: new(false), EnableUlaInternalIpv6: new(true),
		},
	})
	e.done(t, op, err)
}

func (e *env) v6Subnet(t *testing.T, name, cidr, stack, access string) error {
	t.Helper()

	sub := &computepb.Subnetwork{
		Name: s(name), Network: s(vpcRef), IpCidrRange: s(cidr), StackType: s(stack),
	}
	if access != "" {
		sub.Ipv6AccessType = s(access)
	}

	op, err := e.subnets(t).Insert(t.Context(), &computepb.InsertSubnetworkRequest{
		Project: project, Region: region, SubnetworkResource: sub,
	})

	return e.finish(t, op, err)
}

func (e *env) getSubnet(t *testing.T, name string) *computepb.Subnetwork {
	t.Helper()

	sub, err := e.subnets(t).Get(t.Context(), &computepb.GetSubnetworkRequest{
		Project: project, Region: region, Subnetwork: name,
	})
	require.NoError(t, err)

	return sub
}

func subRef(name string) string { return "regions/" + region + "/subnetworks/" + name }

func (e *env) vmOn(t *testing.T, name, sub string, nic *computepb.NetworkInterface) error {
	t.Helper()

	spec := vmSpec(name)
	nic.Subnetwork = s(subRef(sub))
	spec.NetworkInterfaces = []*computepb.NetworkInterface{nic}

	op, err := e.instances(t).Insert(t.Context(), &computepb.InsertInstanceRequest{
		Project: project, Zone: zone, InstanceResource: spec,
	})

	return e.finish(t, op, err)
}

func TestNetworkULARange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		net    *computepb.Network
		check  func(t *testing.T, n *computepb.Network)
		name   string
		status int
	}{
		{
			name: "assigned", net: &computepb.Network{EnableUlaInternalIpv6: new(true)},
			check: func(t *testing.T, n *computepb.Network) {
				t.Helper()
				p := netip.MustParsePrefix(n.GetInternalIpv6Range())
				assert.Equal(t, 48, p.Bits())
				assert.True(t, netip.MustParsePrefix("fd20::/20").Contains(p.Addr()))
			},
		},
		{
			name: "requested", net: &computepb.Network{EnableUlaInternalIpv6: new(true), InternalIpv6Range: s("fd20:123:4::/48")},
			check: func(t *testing.T, n *computepb.Network) {
				t.Helper()
				assert.Equal(t, "fd20:123:4::/48", n.GetInternalIpv6Range())
			},
		},
		{
			name: "outside pool", status: http.StatusBadRequest,
			net: &computepb.Network{EnableUlaInternalIpv6: new(true), InternalIpv6Range: s("fd30:1:2::/48")},
		},
		{
			name: "wrong size", status: http.StatusBadRequest,
			net: &computepb.Network{EnableUlaInternalIpv6: new(true), InternalIpv6Range: s("fd20:1::/32")},
		},
		{name: "range without ula", status: http.StatusBadRequest, net: &computepb.Network{InternalIpv6Range: s("fd20:1:2::/48")}},
		{
			name: "default off",
			net:  &computepb.Network{},
			check: func(t *testing.T, n *computepb.Network) {
				t.Helper()
				assert.Empty(t, n.GetInternalIpv6Range())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			tt.net.Name, tt.net.AutoCreateSubnetworks = s("n"), new(false)

			op, err := e.networks(t).Insert(t.Context(), &computepb.InsertNetworkRequest{Project: project, NetworkResource: tt.net})
			if tt.status != 0 {
				assertAPIError(t, e.finish(t, op, err), tt.status, "")

				return
			}

			e.done(t, op, err)

			got, gerr := e.networks(t).Get(t.Context(), &computepb.GetNetworkRequest{Project: project, Network: "n"})
			require.NoError(t, gerr)
			tt.check(t, got)
		})
	}
}

func TestDualStackSubnets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		stack  string
		access string
		status int
	}{
		{name: "internal", stack: "IPV4_IPV6", access: "INTERNAL"},
		{name: "external", stack: "IPV4_IPV6", access: "EXTERNAL"},
		{name: "ipv6 only", stack: "IPV6_ONLY", access: "INTERNAL"},
		{name: "access required", stack: "IPV4_IPV6", status: http.StatusBadRequest},
		{name: "access on ipv4 only", stack: "IPV4_ONLY", access: "INTERNAL", status: http.StatusBadRequest},
		{name: "bad stack", stack: "IPV5", status: http.StatusBadRequest},
		{name: "bad access", stack: "IPV4_IPV6", access: "WORLD", status: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.v6Network(t)

			err := e.v6Subnet(t, "s6", "10.30.0.0/24", tt.stack, tt.access)
			if tt.status != 0 {
				assertAPIError(t, err, tt.status, "")

				return
			}

			require.NoError(t, err)

			got := e.getSubnet(t, "s6")
			assert.Equal(t, tt.stack, got.GetStackType())
			assert.Equal(t, tt.access, got.GetIpv6AccessType())

			p := netip.MustParsePrefix(got.GetIpv6CidrRange())
			assert.Equal(t, 64, p.Bits())

			if tt.access == "EXTERNAL" {
				assert.Equal(t, got.GetIpv6CidrRange(), got.GetExternalIpv6Prefix())
				assert.Equal(t, "VM_AND_FR", got.GetIpv6GceEndpoint())
			} else {
				assert.Equal(t, got.GetIpv6CidrRange(), got.GetInternalIpv6Prefix())
				assert.True(t, netip.MustParsePrefix(e.netULA(t)).Contains(p.Addr()))
			}
		})
	}
}

func (e *env) netULA(t *testing.T) string {
	t.Helper()

	n, err := e.networks(t).Get(t.Context(), &computepb.GetNetworkRequest{Project: project, Network: "vpc"})
	require.NoError(t, err)

	return n.GetInternalIpv6Range()
}

func TestSubnetPrefixesAreDistinctAndPatchable(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.v6Network(t)
	require.NoError(t, e.v6Subnet(t, "a", "10.30.0.0/24", "IPV4_IPV6", "INTERNAL"))
	require.NoError(t, e.v6Subnet(t, "b", "10.31.0.0/24", "IPV4_IPV6", "INTERNAL"))
	assert.NotEqual(t, e.getSubnet(t, "a").GetIpv6CidrRange(), e.getSubnet(t, "b").GetIpv6CidrRange())

	require.NoError(t, e.v6Subnet(t, "plain", "10.32.0.0/24", "IPV4_ONLY", ""))

	op, err := e.subnets(t).Patch(t.Context(), &computepb.PatchSubnetworkRequest{
		Project: project, Region: region, Subnetwork: "plain", SubnetworkResource: &computepb.Subnetwork{
			StackType: s("IPV4_IPV6"), Ipv6AccessType: s("EXTERNAL"), Fingerprint: e.getSubnet(t, "plain").Fingerprint,
		},
	})
	e.done(t, op, err)
	assert.Equal(t, "EXTERNAL", e.getSubnet(t, "plain").GetIpv6AccessType())
	assert.Contains(t, e.getSubnet(t, "plain").GetExternalIpv6Prefix(), "::/64")
}

func (e *env) getVMNic(t *testing.T, name string) *computepb.NetworkInterface {
	t.Helper()

	return e.getVM(t, name).GetNetworkInterfaces()[0]
}

func TestInstanceInternalIPv6(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.v6Network(t)
	require.NoError(t, e.v6Subnet(t, "s6", "10.30.0.0/24", "IPV4_IPV6", "INTERNAL"))
	require.NoError(t, e.v6Subnet(t, "v4", "10.31.0.0/24", "IPV4_ONLY", ""))

	require.NoError(t, e.vmOn(t, "vm1", "s6", &computepb.NetworkInterface{StackType: s("IPV4_IPV6")}))
	require.NoError(t, e.vmOn(t, "vm2", "s6", &computepb.NetworkInterface{StackType: s("IPV4_IPV6")}))

	prefix := netip.MustParsePrefix(e.getSubnet(t, "s6").GetIpv6CidrRange())
	n1, n2 := e.getVMNic(t, "vm1"), e.getVMNic(t, "vm2")

	for _, n := range []*computepb.NetworkInterface{n1, n2} {
		assert.Equal(t, "IPV4_IPV6", n.GetStackType())
		assert.Equal(t, "INTERNAL", n.GetIpv6AccessType())
		assert.EqualValues(t, 96, n.GetInternalIpv6PrefixLength())
		assert.True(t, prefix.Contains(netip.MustParseAddr(n.GetIpv6Address())))
		assert.NotEmpty(t, n.GetNetworkIP())
	}

	assert.NotEqual(t, n1.GetIpv6Address(), n2.GetIpv6Address())

	require.NoError(t, e.vmOn(t, "vm3", "s6", &computepb.NetworkInterface{}))
	assert.Equal(t, "IPV4_ONLY", e.getVMNic(t, "vm3").GetStackType())
	assert.Empty(t, e.getVMNic(t, "vm3").GetIpv6Address())

	assertAPIError(t, e.vmOn(t, "bad1", "v4", &computepb.NetworkInterface{StackType: s("IPV4_IPV6")}), http.StatusBadRequest, "")
	assertAPIError(t, e.vmOn(t, "bad2", "s6", &computepb.NetworkInterface{Ipv6Address: s(n1.GetIpv6Address())}), http.StatusBadRequest, "")
	assertAPIError(t, e.vmOn(t, "bad3", "s6", &computepb.NetworkInterface{
		StackType: s("IPV4_IPV6"), Ipv6Address: s(n1.GetIpv6Address()),
	}), http.StatusBadRequest, "")
	assertAPIError(t, e.vmOn(t, "bad4", "s6", &computepb.NetworkInterface{
		StackType: s("IPV4_IPV6"), Ipv6Address: s("2001:db8::"),
	}), http.StatusBadRequest, "")
	assertAPIError(t, e.vmOn(t, "bad5", "s6", &computepb.NetworkInterface{StackType: s("DUAL")}), http.StatusBadRequest, "")

	_, err := e.subnets(t).Patch(t.Context(), &computepb.PatchSubnetworkRequest{
		Project: project, Region: region, Subnetwork: "s6", SubnetworkResource: &computepb.Subnetwork{StackType: s("IPV4_ONLY")},
	})
	assert.Error(t, err, "instances still use IPv6")
}

func TestInstanceExternalIPv6AndIPv6Only(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.v6Network(t)
	require.NoError(t, e.v6Subnet(t, "ext", "10.30.0.0/24", "IPV4_IPV6", "EXTERNAL"))
	require.NoError(t, e.v6Subnet(t, "only", "10.31.0.0/24", "IPV6_ONLY", "INTERNAL"))

	require.NoError(t, e.vmOn(t, "vm1", "ext", &computepb.NetworkInterface{
		StackType:         s("IPV4_IPV6"),
		Ipv6AccessConfigs: []*computepb.AccessConfig{{Name: s("External IPv6"), Type: s("DIRECT_IPV6")}},
	}))

	nic := e.getVMNic(t, "vm1")
	require.Len(t, nic.GetIpv6AccessConfigs(), 1)
	ac := nic.GetIpv6AccessConfigs()[0]
	assert.Equal(t, "DIRECT_IPV6", ac.GetType())
	assert.EqualValues(t, 96, ac.GetExternalIpv6PrefixLength())
	assert.True(t, netip.MustParsePrefix(e.getSubnet(t, "ext").GetExternalIpv6Prefix()).Contains(netip.MustParseAddr(ac.GetExternalIpv6())))
	assert.Empty(t, nic.GetIpv6Address(), "external access subnets assign through ipv6AccessConfigs")

	assertAPIError(t, e.vmOn(t, "bad", "ext", &computepb.NetworkInterface{
		StackType: s("IPV4_IPV6"), Ipv6AccessConfigs: []*computepb.AccessConfig{{Type: s("ONE_TO_ONE_NAT")}},
	}), http.StatusBadRequest, "")

	require.NoError(t, e.vmOn(t, "six", "only", &computepb.NetworkInterface{}))

	six := e.getVMNic(t, "six")
	assert.Equal(t, "IPV6_ONLY", six.GetStackType())
	assert.Empty(t, six.GetNetworkIP())
	assert.NotEmpty(t, six.GetIpv6Address())

	assertAPIError(t, e.vmOn(t, "six2", "only", &computepb.NetworkInterface{NetworkIP: s("10.31.0.5")}), http.StatusBadRequest, "")
}

func TestUpdateNICStack(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.v6Network(t)
	require.NoError(t, e.v6Subnet(t, "s6", "10.30.0.0/24", "IPV4_IPV6", "INTERNAL"))
	require.NoError(t, e.vmOn(t, "vm1", "s6", &computepb.NetworkInterface{}))

	nic := e.getVMNic(t, "vm1")
	op, err := e.instances(t).UpdateNetworkInterface(t.Context(), &computepb.UpdateNetworkInterfaceInstanceRequest{
		Project: project, Zone: zone, Instance: "vm1", NetworkInterface: "nic0",
		NetworkInterfaceResource: &computepb.NetworkInterface{StackType: s("IPV4_IPV6"), Fingerprint: nic.Fingerprint},
	})
	e.done(t, op, err)
	assert.NotEmpty(t, e.getVMNic(t, "vm1").GetIpv6Address())

	nic = e.getVMNic(t, "vm1")
	op, err = e.instances(t).UpdateNetworkInterface(t.Context(), &computepb.UpdateNetworkInterfaceInstanceRequest{
		Project: project, Zone: zone, Instance: "vm1", NetworkInterface: "nic0",
		NetworkInterfaceResource: &computepb.NetworkInterface{StackType: s("IPV4_ONLY"), Fingerprint: nic.Fingerprint},
	})
	e.done(t, op, err)
	assert.Empty(t, e.getVMNic(t, "vm1").GetIpv6Address())
	assert.Equal(t, "IPV4_ONLY", e.getVMNic(t, "vm1").GetStackType())
}

func TestRegionalIPv6Addresses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		addr   *computepb.Address
		check  func(t *testing.T, a *computepb.Address)
		name   string
		status int
	}{
		{
			name: "external vm", addr: &computepb.Address{Subnetwork: s(subRef("ext")), Ipv6EndpointType: s("VM")},
			check: func(t *testing.T, a *computepb.Address) {
				t.Helper()
				assert.Equal(t, "EXTERNAL", a.GetAddressType())
				assert.Equal(t, "VM", a.GetIpv6EndpointType())
				assert.EqualValues(t, 96, a.GetPrefixLength())
				assert.Equal(t, "RESERVED", a.GetStatus())
			},
		},
		{
			name: "external default endpoint", addr: &computepb.Address{Subnetwork: s(subRef("ext"))},
			check: func(t *testing.T, a *computepb.Address) {
				t.Helper()
				assert.Equal(t, "VM", a.GetIpv6EndpointType())
			},
		},
		{
			name: "internal", addr: &computepb.Address{AddressType: s("INTERNAL"), Subnetwork: s(subRef("int"))},
			check: func(t *testing.T, a *computepb.Address) {
				t.Helper()
				assert.Equal(t, "GCE_ENDPOINT", a.GetPurpose())
				assert.True(t, netip.MustParsePrefix("fd20::/20").Contains(netip.MustParseAddr(a.GetAddress())))
			},
		},
		{name: "no subnetwork", addr: &computepb.Address{}, status: http.StatusBadRequest},
		{name: "wrong access type", addr: &computepb.Address{Subnetwork: s(subRef("int"))}, status: http.StatusBadRequest},
		{name: "ipv4 only subnet", addr: &computepb.Address{Subnetwork: s(subRef("v4"))}, status: http.StatusBadRequest},
		{
			name: "bad endpoint", addr: &computepb.Address{Subnetwork: s(subRef("ext")), Ipv6EndpointType: s("FR")},
			status: http.StatusBadRequest,
		},
		{
			name: "bad prefix", addr: &computepb.Address{Subnetwork: s(subRef("ext")), PrefixLength: new(int32(64))},
			status: http.StatusBadRequest,
		},
		{
			name: "address outside range", addr: &computepb.Address{Subnetwork: s(subRef("ext")), Address: s("2001:db8::")},
			status: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.v6Network(t)
			require.NoError(t, e.v6Subnet(t, "ext", "10.30.0.0/24", "IPV4_IPV6", "EXTERNAL"))
			require.NoError(t, e.v6Subnet(t, "int", "10.31.0.0/24", "IPV4_IPV6", "INTERNAL"))
			require.NoError(t, e.v6Subnet(t, "v4", "10.32.0.0/24", "IPV4_ONLY", ""))

			tt.addr.Name, tt.addr.IpVersion = s("a6"), s("IPV6")

			op, err := e.addresses(t).Insert(t.Context(), &computepb.InsertAddressRequest{
				Project: project, Region: region, AddressResource: tt.addr,
			})
			if tt.status != 0 {
				assertAPIError(t, e.finish(t, op, err), tt.status, "")

				return
			}

			e.done(t, op, err)

			got, gerr := e.addresses(t).Get(t.Context(), &computepb.GetAddressRequest{Project: project, Region: region, Address: "a6"})
			require.NoError(t, gerr)
			assert.Equal(t, "IPV6", got.GetIpVersion())
			tt.check(t, got)
		})
	}
}

func TestReservedIPv6UsedByInstance(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.v6Network(t)
	require.NoError(t, e.v6Subnet(t, "int", "10.31.0.0/24", "IPV4_IPV6", "INTERNAL"))

	op, err := e.addresses(t).Insert(t.Context(), &computepb.InsertAddressRequest{
		Project: project, Region: region, AddressResource: &computepb.Address{
			Name: s("a6"), IpVersion: s("IPV6"), AddressType: s("INTERNAL"), Subnetwork: s(subRef("int")),
		},
	})
	e.done(t, op, err)

	addr, err := e.addresses(t).Get(t.Context(), &computepb.GetAddressRequest{Project: project, Region: region, Address: "a6"})
	require.NoError(t, err)

	require.NoError(t, e.vmOn(t, "vm1", "int", &computepb.NetworkInterface{StackType: s("IPV4_IPV6"), Ipv6Address: s(addr.GetAddress())}))
	assert.Equal(t, addr.GetAddress(), e.getVMNic(t, "vm1").GetIpv6Address())

	addr, err = e.addresses(t).Get(t.Context(), &computepb.GetAddressRequest{Project: project, Region: region, Address: "a6"})
	require.NoError(t, err)
	assert.Equal(t, "IN_USE", addr.GetStatus())
	assert.Contains(t, addr.GetUsers()[0], "/instances/vm1")

	assertAPIError(t, e.vmOn(t, "vm2", "int", &computepb.NetworkInterface{
		StackType: s("IPV4_IPV6"), Ipv6Address: s(addr.GetAddress()),
	}), http.StatusBadRequest, "")

	_, err = e.addresses(t).Delete(t.Context(), &computepb.DeleteAddressRequest{Project: project, Region: region, Address: "a6"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")

	_, err = e.subnets(t).Delete(t.Context(), &computepb.DeleteSubnetworkRequest{Project: project, Region: region, Subnetwork: "int"})
	assertAPIError(t, err, http.StatusBadRequest, "resourceInUseByAnotherResource")
}

func TestPrivateServiceConnectAddresses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		addr   *computepb.Address
		name   string
		status int
	}{
		{name: "ok", addr: &computepb.Address{Address: s("10.50.0.5")}},
		{name: "no network", status: http.StatusBadRequest, addr: &computepb.Address{Address: s("10.50.0.5"), Network: nil}},
		{name: "no address", status: http.StatusBadRequest, addr: &computepb.Address{}},
		{name: "ipv6 literal", status: http.StatusBadRequest, addr: &computepb.Address{Address: s("fd00::1")}},
		{name: "prefix", status: http.StatusBadRequest, addr: &computepb.Address{Address: s("10.50.0.5"), PrefixLength: new(int32(24))}},
		{name: "other purpose", status: http.StatusBadRequest, addr: &computepb.Address{Address: s("10.50.0.5"), Purpose: s("SHARED_LOADBALANCER_VIP")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.network(t, "vpc", false)

			tt.addr.Name, tt.addr.AddressType = s("psc"), s("INTERNAL")

			if tt.addr.Purpose == nil {
				tt.addr.Purpose = s("PRIVATE_SERVICE_CONNECT")
			}

			if tt.name != "no network" {
				tt.addr.Network = s(vpcRef)
			}

			op, err := e.globalAddresses(t).Insert(t.Context(), &computepb.InsertGlobalAddressRequest{
				Project: project, AddressResource: tt.addr,
			})
			if tt.status != 0 {
				assertAPIError(t, e.finish(t, op, err), tt.status, "")

				return
			}

			e.done(t, op, err)

			got, gerr := e.globalAddresses(t).Get(t.Context(), &computepb.GetGlobalAddressRequest{Project: project, Address: "psc"})
			require.NoError(t, gerr)
			assert.Equal(t, "10.50.0.5", got.GetAddress())
			assert.Equal(t, "PRIVATE_SERVICE_CONNECT", got.GetPurpose())
			assert.Contains(t, got.GetNetwork(), "/networks/vpc")

			dup := &computepb.Address{
				Name: s("psc2"), AddressType: s("INTERNAL"), Purpose: s("PRIVATE_SERVICE_CONNECT"), Address: s("10.50.0.5"),
				Network: s(vpcRef),
			}
			_, err = e.globalAddresses(t).Insert(t.Context(), &computepb.InsertGlobalAddressRequest{Project: project, AddressResource: dup})
			assertAPIError(t, err, http.StatusBadRequest, "")
		})
	}
}

func (e *env) aliasSubnet(t *testing.T) {
	t.Helper()
	e.network(t, "vpc", false)

	op, err := e.subnets(t).Insert(t.Context(), &computepb.InsertSubnetworkRequest{
		Project: project, Region: region, SubnetworkResource: &computepb.Subnetwork{
			Name: s("sub"), Network: s(vpcRef), IpCidrRange: s("10.10.0.0/24"),
			SecondaryIpRanges: []*computepb.SubnetworkSecondaryRange{{RangeName: s("pods"), IpCidrRange: s("192.168.0.0/22")}},
		},
	})
	e.done(t, op, err)
}

func TestAliasIPRanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		aliases []*computepb.AliasIpRange
		want    []string
		status  int
	}{
		{
			name: "explicit cidr", want: []string{"192.168.1.0/24"},
			aliases: []*computepb.AliasIpRange{{IpCidrRange: s("192.168.1.0/24"), SubnetworkRangeName: s("pods")}},
		},
		{
			name: "netmask allocates first block", want: []string{"192.168.0.0/24"},
			aliases: []*computepb.AliasIpRange{{IpCidrRange: s("/24"), SubnetworkRangeName: s("pods")}},
		},
		{
			name: "two netmasks", want: []string{"192.168.0.0/24", "192.168.1.0/24"},
			aliases: []*computepb.AliasIpRange{
				{IpCidrRange: s("/24"), SubnetworkRangeName: s("pods")}, {IpCidrRange: s("/24"), SubnetworkRangeName: s("pods")},
			},
		},
		{
			name: "single ip", want: []string{"192.168.2.7/32"},
			aliases: []*computepb.AliasIpRange{{IpCidrRange: s("192.168.2.7"), SubnetworkRangeName: s("pods")}},
		},
		{
			name: "primary range", want: []string{"10.10.0.64/28"},
			aliases: []*computepb.AliasIpRange{{IpCidrRange: s("10.10.0.64/28")}},
		},
		{
			name: "unknown range", status: http.StatusBadRequest,
			aliases: []*computepb.AliasIpRange{{IpCidrRange: s("/24"), SubnetworkRangeName: s("nope")}},
		},
		{
			name: "outside range", status: http.StatusBadRequest,
			aliases: []*computepb.AliasIpRange{{IpCidrRange: s("172.16.0.0/24"), SubnetworkRangeName: s("pods")}},
		},
		{
			name: "overlap in request", status: http.StatusBadRequest,
			aliases: []*computepb.AliasIpRange{
				{IpCidrRange: s("192.168.1.0/24"), SubnetworkRangeName: s("pods")},
				{IpCidrRange: s("192.168.1.128/25"), SubnetworkRangeName: s("pods")},
			},
		},
		{
			name: "unaligned", status: http.StatusBadRequest,
			aliases: []*computepb.AliasIpRange{{IpCidrRange: s("192.168.1.1/24"), SubnetworkRangeName: s("pods")}},
		},
		{
			name: "too large", status: http.StatusBadRequest,
			aliases: []*computepb.AliasIpRange{{IpCidrRange: s("/16"), SubnetworkRangeName: s("pods")}},
		},
		{
			name: "garbage", status: http.StatusBadRequest,
			aliases: []*computepb.AliasIpRange{{IpCidrRange: s("pods"), SubnetworkRangeName: s("pods")}},
		},
		{
			name: "reserved primary address", status: http.StatusBadRequest,
			aliases: []*computepb.AliasIpRange{{IpCidrRange: s("10.10.0.1")}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.aliasSubnet(t)

			err := e.vmOn(t, "vm1", "sub", &computepb.NetworkInterface{AliasIpRanges: tt.aliases})
			if tt.status != 0 {
				assertAPIError(t, err, tt.status, "")

				return
			}

			require.NoError(t, err)

			var got []string
			for _, a := range e.getVMNic(t, "vm1").GetAliasIpRanges() {
				got = append(got, a.GetIpCidrRange())
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestAliasRangesAcrossInstances(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.aliasSubnet(t)

	alias := func(cidr string) *computepb.NetworkInterface {
		return &computepb.NetworkInterface{AliasIpRanges: []*computepb.AliasIpRange{{IpCidrRange: s(cidr), SubnetworkRangeName: s("pods")}}}
	}

	require.NoError(t, e.vmOn(t, "vm1", "sub", alias("/24")))
	require.NoError(t, e.vmOn(t, "vm2", "sub", alias("/24")))
	assert.Equal(t, "192.168.1.0/24", e.getVMNic(t, "vm2").GetAliasIpRanges()[0].GetIpCidrRange(), "the second /24 skips the first")

	assertAPIError(t, e.vmOn(t, "vm3", "sub", alias("192.168.1.64/26")), http.StatusBadRequest, "")

	nic := e.getVMNic(t, "vm2")
	op, err := e.instances(t).UpdateNetworkInterface(t.Context(), &computepb.UpdateNetworkInterfaceInstanceRequest{
		Project: project, Zone: zone, Instance: "vm2", NetworkInterface: "nic0",
		NetworkInterfaceResource: &computepb.NetworkInterface{
			Fingerprint:   nic.Fingerprint,
			AliasIpRanges: []*computepb.AliasIpRange{{IpCidrRange: s("192.168.1.0/25"), SubnetworkRangeName: s("pods")}},
		},
	})
	e.done(t, op, err)
	assert.Equal(t, "192.168.1.0/25", e.getVMNic(t, "vm2").GetAliasIpRanges()[0].GetIpCidrRange(), "a nic may reshape its own range")

	nic = e.getVMNic(t, "vm2")
	_, err = e.instances(t).UpdateNetworkInterface(t.Context(), &computepb.UpdateNetworkInterfaceInstanceRequest{
		Project: project, Zone: zone, Instance: "vm2", NetworkInterface: "nic0",
		NetworkInterfaceResource: &computepb.NetworkInterface{
			Fingerprint:   nic.Fingerprint,
			AliasIpRanges: []*computepb.AliasIpRange{{IpCidrRange: s("192.168.0.128/25"), SubnetworkRangeName: s("pods")}},
		},
	})
	assertAPIError(t, err, http.StatusBadRequest, "")

	// primary-range aliases keep other nics off their addresses
	require.NoError(t, e.vmOn(t, "vm4", "sub", &computepb.NetworkInterface{AliasIpRanges: []*computepb.AliasIpRange{{IpCidrRange: s("10.10.0.5")}}}))
	require.NoError(t, e.vmOn(t, "vm5", "sub", &computepb.NetworkInterface{}))
	assert.NotEqual(t, "10.10.0.5", e.getVMNic(t, "vm5").GetNetworkIP())
}

func TestLocalSSD(t *testing.T) {
	t.Parallel()

	scratch := func(n int, iface string) []*computepb.AttachedDisk {
		out := make([]*computepb.AttachedDisk, n)
		for i := range out {
			out[i] = &computepb.AttachedDisk{Type: s("SCRATCH"), Interface: s(iface), AutoDelete: new(true)}
		}

		return out
	}

	tests := []struct {
		name    string
		machine string
		disks   []*computepb.AttachedDisk
		mut     func(d []*computepb.AttachedDisk)
		status  int
	}{
		{name: "n1 one", machine: "n1-standard-4", disks: scratch(1, "NVME")},
		{name: "n1 eight scsi", machine: "n1-standard-8", disks: scratch(8, "SCSI")},
		{name: "n1 nine", machine: "n1-standard-8", disks: scratch(9, "SCSI"), status: http.StatusBadRequest},
		{name: "n1 sixteen", machine: "n1-standard-32", disks: scratch(16, "NVME")},
		{name: "n2 four on 4 cpus", machine: "n2-standard-4", disks: scratch(4, "NVME")},
		{name: "n2 three on 4 cpus", machine: "n2-standard-4", disks: scratch(3, "NVME"), status: http.StatusBadRequest},
		{name: "n2 one on 16 cpus", machine: "n2-standard-16", disks: scratch(1, "NVME"), status: http.StatusBadRequest},
		{name: "n2 two on 16 cpus", machine: "n2-standard-16", disks: scratch(2, "NVME")},
		{name: "n2 eight on 80 cpus", machine: "n2-standard-80", disks: scratch(8, "NVME")},
		{name: "e2 unsupported", machine: "e2-standard-4", disks: scratch(1, "NVME"), status: http.StatusBadRequest},
		{name: "bad interface", machine: "n1-standard-4", disks: scratch(1, "IDE"), status: http.StatusBadRequest},
		{
			name: "with source", machine: "n1-standard-4", disks: scratch(1, "NVME"), status: http.StatusBadRequest,
			mut: func(d []*computepb.AttachedDisk) { d[0].Source = s("zones/" + zone + "/disks/x") },
		},
		{
			name: "wrong size", machine: "n1-standard-4", disks: scratch(1, "NVME"), status: http.StatusBadRequest,
			mut: func(d []*computepb.AttachedDisk) {
				d[0].InitializeParams = &computepb.AttachedDiskInitializeParams{DiskSizeGb: new(int64(100))}
			},
		},
		{
			name: "read only", machine: "n1-standard-4", disks: scratch(1, "NVME"), status: http.StatusBadRequest,
			mut: func(d []*computepb.AttachedDisk) { d[0].Mode = s("READ_ONLY") },
		},
		{
			name: "custom n1", machine: "custom-4-8192", disks: scratch(2, "SCSI"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.baseNet(t)

			if tt.mut != nil {
				tt.mut(tt.disks)
			}

			spec := vmSpec("vm1")
			spec.MachineType = s("zones/" + zone + "/machineTypes/" + tt.machine)
			spec.Disks = append(spec.Disks, tt.disks...)

			op, err := e.instances(t).Insert(t.Context(), &computepb.InsertInstanceRequest{
				Project: project, Zone: zone, InstanceResource: spec,
			})
			if tt.status != 0 {
				assertAPIError(t, e.finish(t, op, err), tt.status, "")

				return
			}

			e.done(t, op, err)

			got := e.getVM(t, "vm1")
			require.Len(t, got.GetDisks(), 1+len(tt.disks))

			for i, d := range got.GetDisks()[1:] {
				assert.Equal(t, "SCRATCH", d.GetType())
				assert.EqualValues(t, 375, d.GetDiskSizeGb())
				assert.Equal(t, "READ_WRITE", d.GetMode())
				assert.Empty(t, d.GetSource())
				assert.Equal(t, tt.disks[i].GetInterface(), d.GetInterface())
				assert.Regexp(t, `^local-ssd-\d+$`, d.GetDeviceName())
			}
		})
	}
}

func TestLocalSSDDoesNotAffectPersistentDisks(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.baseNet(t)

	spec := vmSpec("vm1")
	spec.MachineType = s("zones/" + zone + "/machineTypes/n1-standard-4")
	spec.Disks = append(spec.Disks, &computepb.AttachedDisk{Type: s("SCRATCH")})
	e.vm(t, spec)

	op, err := e.instances(t).Delete(t.Context(), &computepb.DeleteInstanceRequest{Project: project, Zone: zone, Instance: "vm1"})
	e.done(t, op, err)

	it := e.disks(t).List(t.Context(), &computepb.ListDisksRequest{Project: project, Zone: zone})
	_, nerr := it.Next()
	require.Error(t, nerr, "the boot disk is auto-deleted and the scratch disk never was a disk resource")

}
