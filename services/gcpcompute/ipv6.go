package gcpcompute

import (
	"encoding/binary"
	"fmt"
	"net/netip"
	"slices"
	"strconv"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	stackIPv6Only   = "IPV6_ONLY"
	accessExternal  = "EXTERNAL"
	accessInternal  = "INTERNAL"
	ipv6Block       = 96
	endpointVM      = "VM"
	endpointNetLB   = "NETLB"
	directIPv6      = "DIRECT_IPV6"
	externalIPv6Fmt = "2600:1900:4000:%x::/64"
	ulaBits         = 48
	gceEndpointFull = "VM_AND_FR"
	ipv6BlockOffset = 10
	ulaSubnetOffset = 6
	ulaIDMask       = 0xffff
	ulaCandidates   = 1 << 12
	subnetV6Bits    = 64
)

var ulaPool = netip.MustParsePrefix("fd20::/20") //nolint:gochecknoglobals // constant range

// ipv6BlockAddr returns the n-th /96 block of a /64 prefix.
func ipv6BlockAddr(p netip.Prefix, n uint16) netip.Addr {
	raw := p.Addr().As16()
	binary.BigEndian.PutUint16(raw[ipv6BlockOffset:], n)

	return netip.AddrFrom16(raw)
}

func (b *InMemoryBackend) assignULALocked(n *computeapi.Network, given string) *APIError {
	if given != "" {
		p, err := netip.ParsePrefix(given)
		if err != nil || p.Bits() != ulaBits || !ulaPool.Contains(p.Addr()) || p != p.Masked() {
			return errField("resource.internalIpv6Range", given, "Must be a /48 range inside fd20::/20.")
		}

		n.InternalIpv6Range = p.String()

		return nil
	}

	for i := range ulaCandidates {
		cand := fmt.Sprintf("fd20:%x:%x::/48", i, n.Id&ulaIDMask)
		if !b.ulaTakenLocked(cand) {
			n.InternalIpv6Range = netip.MustParsePrefix(cand).String()

			return nil
		}
	}

	return errInvalid("No internal IPv6 range is available.")
}

func (b *InMemoryBackend) ulaTakenLocked(cand string) bool {
	want := netip.MustParsePrefix(cand).String()

	for _, o := range b.networks {
		if o.InternalIpv6Range == want {
			return true
		}
	}

	return false
}

// networkULALocked returns the /48 internal IPv6 range subnets of n draw from.
func (b *InMemoryBackend) networkULALocked(n *computeapi.Network) netip.Prefix {
	if p, err := netip.ParsePrefix(n.InternalIpv6Range); err == nil {
		return p
	}

	return netip.MustParsePrefix(fmt.Sprintf("fd20:0:%x::/48", n.Id&ulaIDMask))
}

func (b *InMemoryBackend) subnetPrefixTakenLocked(want string) bool {
	for _, s := range b.subnets {
		if s.Ipv6CidrRange == want {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) allocSubnetV6Locked(n *computeapi.Network, access string) string {
	if access == accessExternal {
		for i := 1; ; i++ {
			cand := netip.MustParsePrefix(fmt.Sprintf(externalIPv6Fmt, i)).String()
			if !b.subnetPrefixTakenLocked(cand) {
				return cand
			}
		}
	}

	base := b.networkULALocked(n).Addr().As16()

	for i := uint16(1); i != 0; i++ {
		raw := base
		binary.BigEndian.PutUint16(raw[ulaSubnetOffset:], i)

		cand := netip.PrefixFrom(netip.AddrFrom16(raw), subnetV6Bits).String()
		if !b.subnetPrefixTakenLocked(cand) {
			return cand
		}
	}

	return ""
}

// applySubnetStackLocked sets a subnetwork's stack type and, for a dual-stack or IPv6-only one, its IPv6 range.
func (b *InMemoryBackend) applySubnetStackLocked(
	n *computeapi.Network, s *computeapi.Subnetwork, in *computeapi.Subnetwork, creating bool,
) *APIError {
	if in.ExternalIpv6Prefix != "" || in.InternalIpv6Prefix != "" {
		return errField("resource.ipv6Prefix", cmpOr(in.ExternalIpv6Prefix, in.InternalIpv6Prefix),
			"Choosing the IPv6 range (BYOIP) is not supported by this emulator.")
	}

	stack := cmpOr(in.StackType, s.StackType)
	if !slices.Contains([]string{stackIPv4Only, stackIPv4IPv6, stackIPv6Only}, stack) {
		return errField("resource.stackType", stack, "Must be IPV4_ONLY, IPV4_IPV6 or IPV6_ONLY.")
	}

	if stack == stackIPv4Only {
		return b.applyIPv4OnlyStackLocked(s, in, creating)
	}

	access, err := subnetAccess(s, in)
	if err != nil {
		return err
	}

	s.StackType, s.Ipv6AccessType = stack, access

	if s.Ipv6CidrRange == "" {
		s.Ipv6CidrRange = b.allocSubnetV6Locked(n, access)
		if access == accessExternal {
			s.ExternalIpv6Prefix, s.Ipv6GceEndpoint = s.Ipv6CidrRange, gceEndpointFull
		} else {
			s.InternalIpv6Prefix = s.Ipv6CidrRange
		}
	}

	return nil
}

func (b *InMemoryBackend) applyIPv4OnlyStackLocked(s, in *computeapi.Subnetwork, creating bool) *APIError {
	if in.Ipv6AccessType != "" && s.Ipv6AccessType == "" {
		return errField("resource.ipv6AccessType", in.Ipv6AccessType,
			"Only valid for a dual-stack or IPv6-only subnetwork.")
	}

	if !creating && s.StackType != stackIPv4Only && b.subnetUsesIPv6Locked(s) {
		return errInvalid("Invalid value for field 'resource.stackType': instances in the subnetwork use IPv6.")
	}

	s.StackType = stackIPv4Only

	return nil
}

// subnetAccess returns the immutable IPv6 access type: the existing one, or the requested one on first use.
func subnetAccess(s, in *computeapi.Subnetwork) (string, *APIError) {
	if in.Ipv6AccessType != "" && s.Ipv6AccessType != "" && in.Ipv6AccessType != s.Ipv6AccessType {
		return "", errField("resource.ipv6AccessType", in.Ipv6AccessType, "The access type is immutable.")
	}

	access := cmpOr(s.Ipv6AccessType, in.Ipv6AccessType)
	if access == "" {
		return "", errRequired("resource.ipv6AccessType")
	}

	if access != accessExternal && access != accessInternal {
		return "", errField("resource.ipv6AccessType", access, "Must be EXTERNAL or INTERNAL.")
	}

	return access, nil
}

func (b *InMemoryBackend) subnetUsesIPv6Locked(s *computeapi.Subnetwork) bool {
	for _, r := range b.instances {
		for _, nic := range r.Inst.NetworkInterfaces {
			if nic.Subnetwork == s.SelfLink && nic.StackType != "" && nic.StackType != stackIPv4Only {
				return true
			}
		}
	}

	for _, a := range b.addresses {
		if a.Subnetwork == s.SelfLink && a.IpVersion == ipv6 {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) usedIPv6Locked(subnet string) map[netip.Addr]bool {
	used := map[netip.Addr]bool{}
	mark := func(raw string) {
		if ip, err := netip.ParseAddr(raw); err == nil {
			used[ip] = true
		}
	}

	for _, r := range b.instances {
		for _, nic := range r.Inst.NetworkInterfaces {
			if nic.Subnetwork != subnet {
				continue
			}

			mark(nic.Ipv6Address)

			for _, ac := range nic.Ipv6AccessConfigs {
				mark(ac.ExternalIpv6)
			}
		}
	}

	for _, a := range b.addresses {
		if a.Subnetwork == subnet && a.IpVersion == ipv6 {
			mark(a.Address)
		}
	}

	return used
}

func nextFreeV6(p netip.Prefix, used map[netip.Addr]bool) (netip.Addr, bool) {
	for n := uint16(1); n != 0; n++ {
		if a := ipv6BlockAddr(p, n); !used[a] {
			return a, true
		}
	}

	return netip.Addr{}, false
}

// pickV6Locked validates a requested /96 address or allocates the next free one in the subnetwork's range.
func (b *InMemoryBackend) pickV6Locked(
	s *computeapi.Subnetwork, field, want string, extra map[netip.Addr]bool,
) (string, *APIError) {
	p, err := netip.ParsePrefix(s.Ipv6CidrRange)
	if err != nil {
		return "", errField(field, want, "The subnetwork has no IPv6 range.")
	}

	used := b.usedIPv6Locked(s.SelfLink)
	for a := range extra {
		used[a] = true
	}

	if want == "" {
		ip, ok := nextFreeV6(p, used)
		if !ok {
			return "", errInvalid("No free IPv6 addresses are available in subnetwork '%s'.", s.Name)
		}

		return ip.String(), nil
	}

	ip, perr := netip.ParseAddr(want)
	if perr != nil || !ip.Is6() || !p.Contains(ip) {
		return "", errField(field, want, "Not an IPv6 address in subnetwork '"+s.Name+"'.")
	}

	ip = netip.PrefixFrom(ip, ipv6Block).Masked().Addr()
	if used[ip] && !b.reservedV6FreeLocked(s.SelfLink, ip) {
		return "", errField(field, want, "The IPv6 address is already in use.")
	}

	return ip.String(), nil
}

// reservedV6FreeLocked reports whether ip is held only by a reserved address that no instance uses yet.
func (b *InMemoryBackend) reservedV6FreeLocked(subnet string, ip netip.Addr) bool {
	for _, a := range b.addresses {
		if a.Subnetwork != subnet || a.IpVersion != ipv6 {
			continue
		}

		if got, err := netip.ParseAddr(a.Address); err != nil || got != ip {
			continue
		}

		return len(b.addressUsersLocked(a)) == 0
	}

	return false
}

func (b *InMemoryBackend) buildNICIPv6Locked(
	s *computeapi.Subnetwork, in, nic *computeapi.NetworkInterface, idx int,
) *APIError {
	stack := in.StackType

	switch {
	case stack == "" && s.StackType == stackIPv6Only:
		stack = stackIPv6Only
	case stack == "":
		stack = stackIPv4Only
	}

	if !slices.Contains([]string{stackIPv4Only, stackIPv4IPv6, stackIPv6Only}, stack) {
		return errField(nicField(idx)+".stackType", stack, "Must be IPV4_ONLY, IPV4_IPV6 or IPV6_ONLY.")
	}

	nic.StackType = stack

	if stack == stackIPv4Only {
		if in.Ipv6Address != "" || len(in.Ipv6AccessConfigs) > 0 {
			return errInvalid(
				"Invalid value for field '%s.ipv6Address': the interface stack type is IPV4_ONLY.",
				nicField(idx),
			)
		}

		return nil
	}

	if s.StackType != stack && s.StackType != stackIPv4IPv6 {
		return errField(
			nicField(idx)+".stackType",
			stack,
			"The subnetwork's stack type is "+cmpOr(s.StackType, stackIPv4Only)+".",
		)
	}

	if s.StackType == stackIPv4IPv6 && stack == stackIPv6Only {
		return errField(nicField(idx)+".stackType", stack, "The subnetwork is dual-stack; use IPV4_IPV6.")
	}

	nic.Ipv6AccessType = s.Ipv6AccessType

	if s.Ipv6AccessType == accessInternal {
		ip, err := b.pickV6Locked(s, nicField(idx)+".ipv6Address", in.Ipv6Address, nil)
		if err != nil {
			return err
		}

		nic.Ipv6Address, nic.InternalIpv6PrefixLength = ip, ipv6Block

		return nil
	}

	if in.Ipv6Address != "" {
		return errField(
			nicField(idx)+".ipv6Address",
			in.Ipv6Address,
			"External IPv6 subnetworks assign addresses through ipv6AccessConfigs.",
		)
	}

	return b.buildV6AccessConfigsLocked(s, in, nic, idx)
}

func nicField(idx int) string { return "resource.networkInterfaces[" + strconv.Itoa(idx) + "]" }

func (b *InMemoryBackend) buildV6AccessConfigsLocked(
	s *computeapi.Subnetwork, in, nic *computeapi.NetworkInterface, idx int,
) *APIError {
	if len(in.Ipv6AccessConfigs) > 1 {
		return errInvalid(
			"Invalid value for field '%s.ipv6AccessConfigs': only one DIRECT_IPV6 config is supported.",
			nicField(idx),
		)
	}

	for _, ac := range in.Ipv6AccessConfigs {
		if ac.Type != "" && ac.Type != directIPv6 {
			return errField(nicField(idx)+".ipv6AccessConfigs.type", ac.Type, "Only DIRECT_IPV6 is supported.")
		}

		ip, err := b.pickV6Locked(s, nicField(idx)+".ipv6AccessConfigs.externalIpv6", ac.ExternalIpv6, nil)
		if err != nil {
			return err
		}

		nic.Ipv6AccessConfigs = append(nic.Ipv6AccessConfigs, &computeapi.AccessConfig{
			Kind: "compute#accessConfig", Type: directIPv6, Name: cmpOr(ac.Name, "External IPv6"),
			ExternalIpv6: ip, ExternalIpv6PrefixLength: ipv6Block, NetworkTier: cmpOr(ac.NetworkTier, tierPremium),
		})
	}

	return nil
}

// buildRegionalIPv6Locked reserves a /96 from a dual-stack subnetwork's IPv6 range.
func (b *InMemoryBackend) buildRegionalIPv6Locked(project, region string, in, a *computeapi.Address) *APIError {
	if in.Subnetwork == "" {
		return errRequired("subnetwork")
	}

	if in.Network != "" {
		return errInvalid("Invalid value for field 'resource.network': regional addresses take a subnetwork.")
	}

	if in.PrefixLength != 0 && in.PrefixLength != ipv6Block {
		return errField(
			"resource.prefixLength",
			strconv.FormatInt(in.PrefixLength, 10),
			"IPV6 addresses are /96 ranges.",
		)
	}

	s, err := b.resolveSubnetLocked(project, region, in.Subnetwork)
	if err != nil {
		return err
	}

	want := accessInternal
	if a.AddressType == addrExternal {
		want = accessExternal
	}

	if s.StackType == stackIPv4Only || s.Ipv6AccessType != want {
		return errField("resource.subnetwork", in.Subnetwork,
			"The subnetwork needs a dual-stack or IPv6-only configuration with "+want+" IPv6 access.")
	}

	ip, perr := b.pickV6Locked(s, "resource.address", in.Address, nil)
	if perr != nil {
		return perr
	}

	a.Address, a.Subnetwork, a.PrefixLength, a.IpVersion = ip, s.SelfLink, ipv6Block, ipv6

	if want == accessExternal {
		return b.finishExternalV6Locked(in, a)
	}

	a.Purpose = cmpOr(in.Purpose, "GCE_ENDPOINT")
	if a.Purpose != "GCE_ENDPOINT" && a.Purpose != "DNS_RESOLVER" {
		return errField("resource.purpose", a.Purpose, "Regional internal addresses use GCE_ENDPOINT or DNS_RESOLVER.")
	}

	return nil
}

func (b *InMemoryBackend) finishExternalV6Locked(in, a *computeapi.Address) *APIError {
	a.Ipv6EndpointType = cmpOr(in.Ipv6EndpointType, endpointVM)
	if a.Ipv6EndpointType != endpointVM && a.Ipv6EndpointType != endpointNetLB {
		return errField("resource.ipv6EndpointType", a.Ipv6EndpointType, "Must be VM or NETLB.")
	}

	a.NetworkTier = cmpOr(in.NetworkTier, tierPremium)
	if a.NetworkTier != tierPremium && a.NetworkTier != tierStandard {
		return errField("resource.networkTier", a.NetworkTier, "")
	}

	return nil
}

func (b *InMemoryBackend) buildPSCLocked(project string, in, a *computeapi.Address) *APIError {
	if in.Network == "" {
		return errRequired("network")
	}

	if in.Subnetwork != "" {
		return errInvalid(
			"Invalid value for field 'resource.subnetwork': global addresses cannot specify a subnetwork.",
		)
	}

	if in.PrefixLength != 0 {
		return errField("resource.prefixLength", strconv.FormatInt(in.PrefixLength, 10),
			"Only VPC_PEERING ranges and IPv6 addresses take a prefix length.")
	}

	n, err := b.resolveNetworkLocked(project, in.Network)
	if err != nil {
		return err
	}

	ip, perr := netip.ParseAddr(in.Address)
	if perr != nil || !ip.Is4() {
		return errField(
			"resource.address",
			in.Address,
			"A Private Service Connect address must be a valid IPv4 address.",
		)
	}

	for _, o := range b.addresses {
		if o.Region == "" && o.Network == n.SelfLink && o.Purpose == purposePSC && o.Address == in.Address {
			return errField("resource.address", in.Address, "The IP address is already in use.")
		}
	}

	a.Address, a.Network, a.Purpose = in.Address, n.SelfLink, purposePSC

	return nil
}

const purposePSC = "PRIVATE_SERVICE_CONNECT"

func usesV6(a *computeapi.Address, nic *computeapi.NetworkInterface) bool {
	if a.AddressType == addrInternal {
		return nic.Subnetwork == a.Subnetwork && nic.Ipv6Address == a.Address
	}

	return slices.ContainsFunc(nic.Ipv6AccessConfigs, func(ac *computeapi.AccessConfig) bool {
		return ac.ExternalIpv6 == a.Address
	})
}

func (b *InMemoryBackend) setupULALocked(n, in *computeapi.Network) *APIError {
	if !in.EnableUlaInternalIpv6 {
		if in.InternalIpv6Range != "" {
			return errField("resource.internalIpv6Range", in.InternalIpv6Range, "Requires enableUlaInternalIpv6.")
		}

		return nil
	}

	n.EnableUlaInternalIpv6 = true

	return b.assignULALocked(n, in.InternalIpv6Range)
}

// patchULALocked applies enableUlaInternalIpv6 and internalIpv6Range from a network patch.
func (b *InMemoryBackend) patchULALocked(n, in *computeapi.Network, raw map[string]any) *APIError {
	_, setEnable := raw["enableUlaInternalIpv6"]
	want, rng := in.EnableUlaInternalIpv6, in.InternalIpv6Range

	if !setEnable {
		want = n.EnableUlaInternalIpv6
	}

	switch {
	case want && !n.EnableUlaInternalIpv6:
		if err := b.assignULALocked(n, rng); err != nil {
			return err
		}

		n.EnableUlaInternalIpv6 = true

		return nil
	case want:
		if rng != "" && rng != n.InternalIpv6Range {
			return errField("resource.internalIpv6Range", rng, "The range cannot be changed.")
		}

		return nil
	case rng != "":
		return errField("resource.internalIpv6Range", rng, "Requires enableUlaInternalIpv6.")
	case !n.EnableUlaInternalIpv6:
		return nil
	}

	for _, s := range b.subnets {
		if s.Network == n.SelfLink && s.Ipv6CidrRange != "" {
			return errField("resource.enableUlaInternalIpv6", "false", "Subnetworks of the network use IPv6 ranges.")
		}
	}

	n.EnableUlaInternalIpv6, n.InternalIpv6Range = false, ""

	return nil
}

func (b *InMemoryBackend) patchSubnetStackLocked(s, in *computeapi.Subnetwork, raw map[string]any) *APIError {
	if _, ok := raw["stackType"]; !ok && raw["ipv6AccessType"] == nil {
		return nil
	}

	n := b.networkBySelfLinkLocked(s.Network)
	if n == nil {
		return errNotFound(trimURL(s.Network))
	}

	return b.applySubnetStackLocked(n, s, in, false)
}
