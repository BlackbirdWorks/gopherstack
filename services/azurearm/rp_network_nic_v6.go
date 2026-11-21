package azurearm

import (
	"net/http"
	"net/netip"
)

// IPv6 subnets must be exactly /64 (learn.microsoft.com/azure/virtual-network/ip-services/ipv6-overview).
const ipv6SubnetBits = 64

func parseNICIPConfigMode(cfgID string, cp map[string]any) (string, string, error) {
	version, ok := canonEnum(firstNonEmpty(strOf(cp, netPropIPVersion), versionV4), []string{versionV4, versionV6})
	if !ok {
		return "", "", netBad("IP configuration %s: privateIPAddressVersion must be IPv4 or IPv6.", cfgID)
	}

	method, ok := canonEnum(firstNonEmpty(strOf(cp, netPropIPMethod), methodDynamic),
		[]string{methodStatic, methodDynamic})
	if !ok {
		return "", "", netBad("privateIPAllocationMethod must be Static or Dynamic.")
	}

	return method, version, nil
}

func nicAssignIP(
	cfgID, method, version, want string, prefixes []netip.Prefix, taken map[string]string, prev string,
) (string, error) {
	if version == versionV6 {
		return assignIPv6(cfgID, method, want, prefixes, taken, prev)
	}

	return assignIP(cfgID, method, want, prefixes, taken, prev)
}

func assignIPv6(
	cfgID, method, want string, prefixes []netip.Prefix, taken map[string]string, prev string,
) (string, error) {
	var v6 []netip.Prefix

	for _, pf := range prefixes {
		if pf.Addr().Is6() {
			v6 = append(v6, pf.Masked())
		}
	}

	if method == methodStatic {
		return assignStaticV6(cfgID, want, v6, taken)
	}

	if a, err := netip.ParseAddr(prev); err == nil && taken[a.String()] == "" {
		for _, pf := range v6 {
			if pf.Contains(a) {
				return a.String(), nil
			}
		}
	}

	for _, pf := range v6 {
		a := pf.Addr()
		for range netReservedLow {
			a = a.Next()
		}

		for ; pf.Contains(a); a = a.Next() {
			if taken[a.String()] == "" {
				return a.String(), nil
			}
		}
	}

	return "", netErr(http.StatusBadRequest, "SubnetIsFull", "Subnet has no free IP addresses left.")
}

func assignStaticV6(cfgID, want string, prefixes []netip.Prefix, taken map[string]string) (string, error) {
	a, err := netip.ParseAddr(want)
	if err != nil || !a.Is6() || a.Is4In6() {
		return "", netBad("IP configuration %s requires a valid IPv6 privateIPAddress for static allocation.", cfgID)
	}

	for _, pf := range prefixes {
		if !pf.Contains(a) {
			continue
		}

		if user := taken[a.String()]; user != "" {
			return "", netErr(http.StatusBadRequest, "PrivateIPAddressIsAllocated",
				"IP configuration %s is using the private IP address %s which is already allocated to resource %s.",
				cfgID, want, user)
		}

		return a.String(), nil
	}

	return "", netErr(http.StatusBadRequest, "PrivateIPAddressNotInSubnet",
		"IP configuration %s is using the private IP address %s which is not in the subnet's address range.",
		cfgID, want)
}

func requireIPv4Config(cfgs []any) error {
	for _, c := range cfgs {
		m, _ := c.(map[string]any)
		if strOf(propsOf(m), netPropIPVersion) == versionV4 {
			return nil
		}
	}

	return netBad("Each network interface must include at least one IPv4 IP configuration.")
}

func firstIPv4Index(cfgs []any) int {
	for i, c := range cfgs {
		m, _ := c.(map[string]any)
		if strOf(propsOf(m), netPropIPVersion) == versionV4 {
			return i
		}
	}

	return 0
}

func checkIPv6SubnetSizes(name, vnet string, prefixes []netip.Prefix) error {
	for _, pf := range prefixes {
		if pf.Addr().Is6() && pf.Bits() != ipv6SubnetBits {
			return netErr(http.StatusBadRequest, "NetcfgInvalidSubnet",
				"Subnet %s is not valid in virtual network %s. IPv6 address prefix %s must be exactly /%d.",
				name, vnet, pf, ipv6SubnetBits)
		}
	}

	return nil
}
