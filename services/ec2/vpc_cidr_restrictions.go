package ec2

import (
	"fmt"
	"net/netip"
)

// IPv4 CIDR block association restrictions table:
// docs.aws.amazon.com/vpc/latest/userguide/vpc-cidr-blocks.html#add-cidr-block-restrictions.
type cidrClass int

const (
	cidrClassPublic cidrClass = iota
	cidrClassRFC10
	cidrClassRFC172
	cidrClassRFC192
	cidrClass198
)

func classifyCIDR(p netip.Prefix) cidrClass {
	a := p.Addr()

	switch {
	case netip.MustParsePrefix("10.0.0.0/8").Contains(a):
		return cidrClassRFC10
	case netip.MustParsePrefix("172.16.0.0/12").Contains(a):
		return cidrClassRFC172
	case netip.MustParsePrefix("192.168.0.0/16").Contains(a):
		return cidrClassRFC192
	case netip.MustParsePrefix("198.19.0.0/16").Contains(a):
		return cidrClass198
	default:
		return cidrClassPublic
	}
}

func isPrivateClass(c cidrClass) bool {
	return c == cidrClassRFC10 || c == cidrClassRFC172 || c == cidrClassRFC192
}

// validateCIDRAssociation applies the documented restrictions of associating cidr with a VPC that already has
// the existing CIDR blocks. Unparseable input is left to the caller's own validation.
func validateCIDRAssociation(existing []string, cidr string) error {
	n, err := netip.ParsePrefix(cidr)
	if err != nil {
		return nil //nolint:nilerr // size/format errors are reported by vpcCIDRPrefixLenValid
	}

	newClass := classifyCIDR(n)

	for _, e := range existing {
		p, perr := netip.ParsePrefix(e)
		if perr != nil {
			continue
		}

		if reason := cidrRestriction(classifyCIDR(p), p, newClass, n); reason != "" {
			return fmt.Errorf("%w: %s cannot be associated with %s: %s", ErrVpcCIDRRange, cidr, e, reason)
		}
	}

	return nil
}

func cidrRestriction(existingClass cidrClass, existing netip.Prefix, newClass cidrClass, n netip.Prefix) string {
	switch {
	case existingClass == cidrClassPublic && (isPrivateClass(newClass) || newClass == cidrClass198):
		return "private ranges cannot be added to a public CIDR block"
	case existingClass == cidrClass198 && isPrivateClass(newClass):
		return "RFC 1918 ranges cannot be added to 198.19.0.0/16"
	case isPrivateClass(existingClass) && newClass == cidrClass198:
		return "198.19.0.0/16 cannot be added to an RFC 1918 CIDR block"
	case isPrivateClass(existingClass) && isPrivateClass(newClass) && existingClass != newClass:
		return "CIDR blocks from other RFC 1918 ranges are restricted"
	}

	return sameRangeRestriction(existingClass, existing, n)
}

func sameRangeRestriction(existingClass cidrClass, existing, n netip.Prefix) string {
	switch existingClass {
	case cidrClassRFC172:
		if netip.MustParsePrefix("172.31.0.0/16").Overlaps(n) {
			return "CIDR blocks from the 172.31.0.0/16 range are restricted"
		}
	case cidrClassRFC10:
		slash16 := netip.MustParsePrefix("10.0.0.0/16")
		slash15 := netip.MustParsePrefix("10.0.0.0/15")
		if slash15.Overlaps(existing) && slash16.Overlaps(n) && !slash16.Overlaps(existing) {
			return "a 10.0.0.0/16 block cannot be added when a 10.0.0.0/15 block is associated"
		}
	default:
	}

	return ""
}
