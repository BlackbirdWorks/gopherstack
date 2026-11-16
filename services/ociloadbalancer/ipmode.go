package ociloadbalancer

import (
	"encoding/binary"
	"net/netip"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	ipModeV4   = "IPV4"
	ipModeV6   = "IPV6"
	v6HostBits = 20
	v6Bits     = 128
	v6HostMax  = 1 << v6HostBits
)

// parseIPMode accepts the enum case-insensitively (GetMappingCreateLoadBalancerDetailsIpModeEnum in the SDK).
func parseIPMode(v string) (string, *APIError) {
	switch {
	case v == "":
		return "", nil
	case strs.Equal(v, ipModeV4):
		return ipModeV4, nil
	case strs.Equal(v, ipModeV6):
		return ipModeV6, nil
	}

	return "", errParam("ipMode must be IPV4 or IPV6")
}

func parseV6Prefix(v string) (netip.Prefix, *APIError) {
	p, err := netip.ParsePrefix(v)
	if err != nil || !p.Addr().Is6() || p.Addr().Is4In6() || p.Bits() > 112 {
		return netip.Prefix{}, errParam("ipv6SubnetCidr " + v + " is not an IPv6 prefix of /112 or shorter")
	}

	return p.Masked(), nil
}

func checkIPModeInputs(mode, v6 string) *APIError {
	m, aerr := parseIPMode(mode)
	if aerr != nil {
		return aerr
	}

	switch {
	case m == ipModeV6 && v6 == "":
		return errMissing("ipv6SubnetCidr is required when ipMode is IPV6")
	case m != ipModeV6 && v6 != "":
		return errParam("ipv6SubnetCidr applies only when ipMode is IPV6")
	case v6 != "":
		_, aerr = parseV6Prefix(v6)
	}

	return aerr
}

// allocIPv6 assigns the next host address of the prefix, wrapping inside min(2^hostBits, v6HostMax) hosts.
func (b *InMemoryBackend) allocIPv6(prefix netip.Prefix) string {
	b.ipSeq++

	hosts := int64(v6HostMax)
	if h := int64(1) << (v6Bits - prefix.Bits()); prefix.Bits() > v6Bits-v6HostBits && h < hosts {
		hosts = h
	}

	off := uint32(reservedHosts + (b.ipSeq-1)%(hosts-reservedHosts-1)) //nolint:gosec // bounded by v6HostMax

	raw := prefix.Addr().As16()
	binary.BigEndian.PutUint32(raw[12:], binary.BigEndian.Uint32(raw[12:])+off)

	return netip.AddrFrom16(raw).String()
}

// assignAddress allocates the load balancer address for an IP mode.
func (b *InMemoryBackend) assignAddress(lb *LoadBalancer, v6, subnetCIDR string) {
	var addr string

	if lb.IPMode == ipModeV6 {
		p, _ := parseV6Prefix(v6)
		addr = b.allocIPv6(p)
	} else {
		addr = b.allocIP(cidrFor(lb, subnetCIDR))
	}

	lb.IPAddresses = []IPAddress{{IPAddress: addr, IsPublic: !lb.IsPrivate}}
}

func (b *InMemoryBackend) updateIPMode(lb *LoadBalancer, in updateLBIn) *APIError {
	mode, aerr := parseIPMode(in.IPMode)
	if aerr != nil {
		return aerr
	}

	v6 := ""
	if in.Ipv6SubnetCidr != nil {
		v6 = *in.Ipv6SubnetCidr
	}

	if mode == "" && v6 == "" {
		return nil
	}

	if mode == "" {
		mode = lb.IPMode
	}

	if mode == lb.IPMode && v6 == "" {
		return nil
	}

	if aerr = checkIPModeInputs(mode, v6); aerr != nil {
		return aerr
	}

	subnetCIDR, aerr := b.checkSubnets(lb.SubnetIDs, lb.IsPrivate)
	if aerr != nil {
		return aerr
	}

	lb.IPMode = mode
	b.assignAddress(lb, v6, subnetCIDR)

	return nil
}
