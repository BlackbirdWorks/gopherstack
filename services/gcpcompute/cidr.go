package gcpcompute

import (
	"encoding/binary"
	"net/netip"
)

const (
	minSubnetBits = 8
	maxSubnetBits = 29
	reservedTail  = 2
)

var privatePrefixes = []netip.Prefix{
	netip.MustParsePrefix("10.0.0.0/8"),
	netip.MustParsePrefix("172.16.0.0/12"),
	netip.MustParsePrefix("192.168.0.0/16"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
}

func parseCIDR(field, raw string) (netip.Prefix, *APIError) {
	p, err := netip.ParsePrefix(raw)
	if err != nil || !p.Addr().Is4() || p != p.Masked() {
		return p, errField(field, raw, "Invalid IPv4 range.")
	}

	return p, nil
}

func parseSubnetCIDR(field, raw string) (netip.Prefix, *APIError) {
	p, err := parseCIDR(field, raw)
	if err != nil {
		return p, err
	}

	if p.Bits() < minSubnetBits || p.Bits() > maxSubnetBits {
		return p, errField(field, raw, "Subnet prefix length must be between /8 and /29.")
	}

	for _, pp := range privatePrefixes {
		if pp.Contains(p.Addr()) && p.Bits() >= pp.Bits() {
			return p, nil
		}
	}

	return p, errField(field, raw, "Must be a valid IPv4 range in a private address space.")
}

func gateway(p netip.Prefix) netip.Addr { return p.Addr().Next() }

func lastAddr(p netip.Prefix) netip.Addr {
	a := p.Addr().As4()
	v := binary.BigEndian.Uint32(a[:]) | (1<<(32-p.Bits()) - 1)

	var out [4]byte

	binary.BigEndian.PutUint32(out[:], v)

	return netip.AddrFrom4(out)
}

// usableRange is the allocatable span: the first two and last two addresses of a subnet are reserved.
func usableRange(p netip.Prefix) (first, last netip.Addr) {
	first = p.Addr().Next().Next()
	last = lastAddr(p)

	for range reservedTail {
		last = last.Prev()
	}

	return first, last
}

func nextFree(p netip.Prefix, used map[netip.Addr]bool) (netip.Addr, bool) {
	first, last := usableRange(p)

	for a := first; a.Compare(last) <= 0; a = a.Next() {
		if !used[a] {
			return a, true
		}
	}

	return netip.Addr{}, false
}

func overlaps(a netip.Prefix, others ...netip.Prefix) bool {
	for _, o := range others {
		if a.Overlaps(o) {
			return true
		}
	}

	return false
}
