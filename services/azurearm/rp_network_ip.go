package azurearm

import (
	"encoding/binary"
	"net/http"
	"net/netip"
	"slices"
)

// netParsePrefix parses a canonical CIDR prefix.
func netParsePrefix(s string) (netip.Prefix, error) {
	pf, err := netip.ParsePrefix(s)
	if err != nil || pf.Masked() != pf {
		return netip.Prefix{}, netErr(http.StatusBadRequest, netCodeCIDR,
			"The address prefix %s has invalid CIDR notation. For example, 10.0.0.0/8 is a valid CIDR.", s)
	}

	return pf, nil
}

func netParsePrefixes(list []string) ([]netip.Prefix, error) {
	out := make([]netip.Prefix, 0, len(list))

	for _, s := range list {
		pf, err := netParsePrefix(s)
		if err != nil {
			return nil, err
		}

		out = append(out, pf)
	}

	return out, nil
}

func prefixInside(inner netip.Prefix, outer []netip.Prefix) bool {
	for _, o := range outer {
		if o.Addr().Is4() == inner.Addr().Is4() && o.Bits() <= inner.Bits() && o.Contains(inner.Addr()) {
			return true
		}
	}

	return false
}

func prefixesOverlap(a, b []netip.Prefix) bool {
	for _, x := range a {
		if slices.ContainsFunc(b, x.Overlaps) {
			return true
		}
	}

	return false
}

const (
	netReservedLow = 4
	ipv4Bits       = 32
)

func v4u(a netip.Addr) uint32 {
	b := a.As4()

	return binary.BigEndian.Uint32(b[:])
}

func u4(v uint32) netip.Addr {
	var b [4]byte

	binary.BigEndian.PutUint32(b[:], v)

	return netip.AddrFrom4(b)
}

// subnetRange returns the first and last allocatable IPv4 addresses of pf: Azure reserves the first four addresses
// and the last one of every subnet.
func subnetRange(pf netip.Prefix) (uint32, uint32, bool) {
	if !pf.Addr().Is4() {
		return 0, 0, false
	}

	base := v4u(pf.Masked().Addr())
	size := uint32(1) << uint(ipv4Bits-pf.Bits())

	if size <= netReservedLow+1 {
		return 0, 0, false
	}

	return base + netReservedLow, base + size - 2, true //nolint:mnd // last address is reserved too
}

func subnetReserved(pf netip.Prefix, addr netip.Addr) bool {
	if !pf.Contains(addr) || !addr.Is4() {
		return false
	}

	lo, hi, ok := subnetRange(pf)
	v := v4u(addr)

	return !ok || v < lo || v > hi
}
