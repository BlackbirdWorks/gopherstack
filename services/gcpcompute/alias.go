package gcpcompute

import (
	"net/netip"
	"strconv"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const maxAliasBits = 32

// aliasBase returns the subnetwork range an alias draws from: a named secondary range or the primary range.
func aliasBase(s *computeapi.Subnetwork, rangeName string) (netip.Prefix, *APIError) {
	if rangeName == "" {
		return netip.MustParsePrefix(s.IpCidrRange), nil
	}

	for _, sec := range s.SecondaryIpRanges {
		if sec.RangeName == rangeName {
			return netip.MustParsePrefix(sec.IpCidrRange), nil
		}
	}

	return netip.Prefix{}, errField("aliasIpRanges.subnetworkRangeName", rangeName, "No such secondary range.")
}

// aliasInUseLocked collects the alias ranges other interfaces hold in the subnetwork's range, skipping skip.
func (b *InMemoryBackend) aliasInUseLocked(subnet, rangeName string, skip *computeapi.NetworkInterface) []netip.Prefix {
	var out []netip.Prefix

	for _, r := range b.instances {
		for _, nic := range r.Inst.NetworkInterfaces {
			if nic == skip || nic.Subnetwork != subnet {
				continue
			}

			for _, a := range nic.AliasIpRanges {
				if p, err := netip.ParsePrefix(a.IpCidrRange); err == nil && a.SubnetworkRangeName == rangeName {
					out = append(out, p)
				}
			}
		}
	}

	return out
}

type aliasSpec struct {
	prefix   netip.Prefix
	sizeOnly bool
}

// parseAliasSpec reads an alias ipCidrRange: a single IP, a CIDR range, or a bare netmask such as /24.
func parseAliasSpec(raw string) (aliasSpec, *APIError) {
	if rest, ok := strings.CutPrefix(raw, "/"); ok {
		n, perr := strconv.Atoi(rest)
		if perr != nil || n < 0 || n > maxAliasBits {
			return aliasSpec{}, errField("aliasIpRanges.ipCidrRange", raw, "Not a valid netmask.")
		}

		return aliasSpec{prefix: netip.PrefixFrom(netip.IPv4Unspecified(), n), sizeOnly: true}, nil
	}

	if p, perr := netip.ParsePrefix(raw); perr == nil && p.Addr().Is4() {
		if p != p.Masked() {
			return aliasSpec{}, errField(
				"aliasIpRanges.ipCidrRange",
				raw,
				"The range is not aligned to its prefix length.",
			)
		}

		return aliasSpec{prefix: p}, nil
	}

	if a, perr := netip.ParseAddr(raw); perr == nil && a.Is4() {
		return aliasSpec{prefix: netip.PrefixFrom(a, maxAliasBits)}, nil
	}

	return aliasSpec{}, errField("aliasIpRanges.ipCidrRange", raw, "Must be a CIDR range, an IP address or a netmask.")
}

// resolveAliasRangesLocked validates the alias ranges of a nic and allocates those given as a netmask only.
func (b *InMemoryBackend) resolveAliasRangesLocked(
	s *computeapi.Subnetwork, nic *computeapi.NetworkInterface, ranges, v6 []*computeapi.AliasIpRange,
) ([]*computeapi.AliasIpRange, *APIError) {
	if len(v6) > 0 {
		return nil, errField("aliasIpv6Ranges", "", "Alias IPv6 ranges are not supported by this emulator.")
	}

	if len(ranges) == 0 {
		return nil, nil
	}

	primaryUsed := b.usedInternalLocked(s.SelfLink)
	taken := map[string][]netip.Prefix{}
	out := make([]*computeapi.AliasIpRange, 0, len(ranges))

	for _, a := range ranges {
		base, err := aliasBase(s, a.SubnetworkRangeName)
		if err != nil {
			return nil, err
		}

		if _, seen := taken[a.SubnetworkRangeName]; !seen {
			taken[a.SubnetworkRangeName] = b.aliasInUseLocked(s.SelfLink, a.SubnetworkRangeName, nic)
		}

		got, err := pickAlias(
			a,
			base,
			taken[a.SubnetworkRangeName],
			a.SubnetworkRangeName == "",
			primaryUsed,
			nic.NetworkIP,
		)
		if err != nil {
			return nil, err
		}

		taken[a.SubnetworkRangeName] = append(taken[a.SubnetworkRangeName], got)
		out = append(
			out,
			&computeapi.AliasIpRange{IpCidrRange: got.String(), SubnetworkRangeName: a.SubnetworkRangeName},
		)
	}

	return out, nil
}

func pickAlias(
	a *computeapi.AliasIpRange,
	base netip.Prefix,
	taken []netip.Prefix,
	primary bool,
	used map[netip.Addr]bool,
	own string,
) (netip.Prefix, *APIError) {
	spec, err := parseAliasSpec(a.IpCidrRange)
	if err != nil {
		return netip.Prefix{}, err
	}

	want, sizeOnly := spec.prefix, spec.sizeOnly

	free := func(c netip.Prefix) bool {
		if overlaps(c, taken...) || primary && !withinUsable(base, c) {
			return false
		}

		return !primary || !primaryHolds(c, used, own)
	}

	if !sizeOnly {
		if !base.Contains(want.Addr()) || want.Bits() < base.Bits() {
			return netip.Prefix{}, errField("aliasIpRanges.ipCidrRange", a.IpCidrRange,
				"The range is not inside the subnetwork range.")
		}

		if !free(want) {
			return netip.Prefix{}, errField("aliasIpRanges.ipCidrRange", a.IpCidrRange,
				"The range overlaps a reserved or used address or another alias range.")
		}

		return want, nil
	}

	if want.Bits() < base.Bits() {
		return netip.Prefix{}, errField(
			"aliasIpRanges.ipCidrRange",
			a.IpCidrRange,
			"The netmask is larger than the subnetwork range.",
		)
	}

	for c := netip.PrefixFrom(base.Addr(), want.Bits()); base.Contains(c.Addr()); c = nextBlock(c) {
		if free(c) {
			return c, nil
		}

		if !c.IsValid() {
			break
		}
	}

	return netip.Prefix{}, errInvalid("No free /%d alias range is available in the subnetwork range.", want.Bits())
}

func withinUsable(base, c netip.Prefix) bool {
	first, last := usableRange(base)

	return c.Addr().Compare(first) >= 0 && lastAddr(c).Compare(last) <= 0
}

func primaryHolds(c netip.Prefix, used map[netip.Addr]bool, own string) bool {
	if a, err := netip.ParseAddr(own); err == nil && c.Contains(a) {
		return true
	}

	if c.Bits() < maxAliasBits-16 {
		return false
	}

	for a := range used {
		if c.Contains(a) {
			return true
		}
	}

	return false
}

// markPrimaryAliases reserves the addresses of alias ranges drawn from the primary range so no nic takes them.
func markPrimaryAliases(used map[netip.Addr]bool, nic *computeapi.NetworkInterface) {
	for _, a := range nic.AliasIpRanges {
		p, err := netip.ParsePrefix(a.IpCidrRange)
		if a.SubnetworkRangeName != "" || err != nil || p.Bits() < maxAliasBits-16 {
			continue
		}

		for ip := p.Addr(); p.Contains(ip); ip = ip.Next() {
			used[ip] = true
		}
	}
}
