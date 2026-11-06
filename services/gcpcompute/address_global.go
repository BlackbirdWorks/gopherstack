package gcpcompute

import (
	"fmt"
	"net/netip"
	"strconv"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	purposePeering   = "VPC_PEERING"
	ipv4             = "IPV4"
	ipv6             = "IPV6"
	minPeeringPrefix = 8
	maxPeeringPrefix = 29
	ipv6Prefix       = 96
)

func peeringPools() []string {
	return []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"}
}

// peeringRangesLocked returns the VPC_PEERING ranges reserved in the network.
func (b *InMemoryBackend) peeringRangesLocked(network string) []netip.Prefix {
	var out []netip.Prefix

	for _, a := range b.addresses {
		if a.Region != "" || a.Purpose != purposePeering || a.Network != network {
			continue
		}

		if p, err := netip.ParsePrefix(fmt.Sprintf("%s/%d", a.Address, a.PrefixLength)); err == nil {
			out = append(out, p)
		}
	}

	return out
}

func (b *InMemoryBackend) buildGlobalInternalLocked(project string, in, a *computeapi.Address) *APIError {
	if in.Purpose == purposePSC {
		return b.buildPSCLocked(project, in, a)
	}

	if in.Purpose != purposePeering {
		return errField("resource.purpose", in.Purpose,
			"Global INTERNAL addresses require purpose VPC_PEERING or PRIVATE_SERVICE_CONNECT.")
	}

	if in.Network == "" {
		return errRequired("network")
	}

	if in.Subnetwork != "" {
		return errInvalid(
			"Invalid value for field 'resource.subnetwork': global addresses cannot specify a subnetwork.",
		)
	}

	if in.PrefixLength < minPeeringPrefix || in.PrefixLength > maxPeeringPrefix {
		return errField("resource.prefixLength", strconv.FormatInt(in.PrefixLength, 10), "Must be between 8 and 29.")
	}

	n, err := b.resolveNetworkLocked(project, in.Network)
	if err != nil {
		return err
	}

	taken := append(b.networkRangesLocked(n.SelfLink, ""), b.peeringRangesLocked(n.SelfLink)...)

	p, perr := pickPeeringRange(in, taken)
	if perr != nil {
		return perr
	}

	a.Address, a.PrefixLength, a.Network, a.Purpose = p.Addr().String(), int64(p.Bits()), n.SelfLink, purposePeering

	return nil
}

func pickPeeringRange(in *computeapi.Address, taken []netip.Prefix) (netip.Prefix, *APIError) {
	bits := int(in.PrefixLength)

	if in.Address != "" {
		ip, err := netip.ParseAddr(in.Address)
		if err != nil || !ip.Is4() {
			return netip.Prefix{}, errField("resource.address", in.Address, "Must be a valid IPv4 address.")
		}

		p := netip.PrefixFrom(ip, bits)
		if p.Masked() != p || !privateRange(p) {
			return netip.Prefix{}, errField("resource.address", in.Address,
				"Must be the first address of a private range aligned to the prefix length.")
		}

		if overlaps(p, taken...) {
			return netip.Prefix{}, errField("resource.address", in.Address,
				"The range overlaps an existing subnetwork or allocated range in the network.")
		}

		return p, nil
	}

	for _, pool := range peeringPools() {
		pp := netip.MustParsePrefix(pool)
		if bits < pp.Bits() {
			continue
		}

		for cand := netip.PrefixFrom(pp.Addr(), bits); pp.Contains(cand.Addr()); cand = nextBlock(cand) {
			if !overlaps(cand, taken...) {
				return cand, nil
			}
		}
	}

	return netip.Prefix{}, errInvalid("No free IP range of /%d is available in the network.", bits)
}

func privateRange(p netip.Prefix) bool {
	for _, pool := range peeringPools() {
		if pp := netip.MustParsePrefix(pool); pp.Contains(p.Addr()) && p.Bits() >= pp.Bits() {
			return true
		}
	}

	return false
}

// nextBlock returns the prefix of the same size right after p, or the zero prefix at the end of the address space.
func nextBlock(p netip.Prefix) netip.Prefix {
	last := lastAddr(p)

	next := last.Next()
	if !next.IsValid() {
		return netip.Prefix{}
	}

	return netip.PrefixFrom(next, p.Bits())
}

// buildIPv6Locked reserves a global external IPv6 /96 from the emulator pool, or a regional one from a subnetwork.
func (b *InMemoryBackend) buildIPv6Locked(project, region string, in, a *computeapi.Address) *APIError {
	if region != "" {
		return b.buildRegionalIPv6Locked(project, region, in, a)
	}

	if a.AddressType != addrExternal {
		return errField("resource.ipVersion", in.IpVersion, "IPV6 is only supported for global EXTERNAL addresses.")
	}

	if in.PrefixLength != 0 && in.PrefixLength != ipv6Prefix {
		return errField(
			"resource.prefixLength",
			strconv.FormatInt(in.PrefixLength, 10),
			"IPV6 addresses are /96 ranges.",
		)
	}

	if in.NetworkTier != "" && in.NetworkTier != tierPremium {
		return errField("resource.networkTier", in.NetworkTier, "Global addresses are always PREMIUM.")
	}

	if in.Subnetwork != "" || in.Network != "" {
		return errInvalid("Invalid value for field 'resource.network': EXTERNAL addresses cannot specify a network.")
	}

	used := b.usedExternalLocked()

	if in.Address != "" {
		ip, err := netip.ParseAddr(in.Address)
		if err != nil || !ip.Is6() {
			return errField("resource.address", in.Address, "Must be a valid IPv6 address.")
		}

		if used[ip.String()] {
			return errField("resource.address", in.Address, "The IP address is already in use.")
		}

		a.Address = ip.String()
	} else {
		for n := 0; ; n++ {
			cand := fmt.Sprintf("2600:1901:0:%x::", n+1)
			if !used[cand] {
				a.Address = cand

				break
			}
		}
	}

	a.PrefixLength, a.NetworkTier, a.IpVersion = ipv6Prefix, tierPremium, ipv6

	return nil
}
