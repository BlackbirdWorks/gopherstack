package ocicore

import (
	"net/netip"
	"regexp"
	"strings"
)

const (
	minV4Bits    = 16
	maxV4Bits    = 30
	v6SubnetBits = 64
	v6VcnBits    = 56
	maxVcnCidrs  = 16
)

var dnsLabelRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]{0,14}$`)

func validDNSLabel(label, what string) *APIError {
	if !dnsLabelRe.MatchString(label) {
		return errParam(what + " must be an alphanumeric string of at most 15 characters that begins with a letter")
	}

	return nil
}

// parseV4 parses an IPv4 CIDR whose host bits are zero and whose size is /16 to /30.
func parseV4(s, what string) (netip.Prefix, *APIError) {
	p, err := netip.ParsePrefix(s)
	if err != nil || !p.Addr().Is4() {
		return netip.Prefix{}, errParam(what + " " + s + " is not a valid IPv4 CIDR block")
	}

	if p != p.Masked() {
		return netip.Prefix{}, errParam(what + " " + s + " must be a network address, for example " + p.Masked().String())
	}

	if p.Bits() < minV4Bits || p.Bits() > maxV4Bits {
		return netip.Prefix{}, errParam(what + " " + s + " must be between /16 and /30 in size")
	}

	return p, nil
}

func reservedV4(p netip.Prefix) bool {
	for _, r := range []string{"169.254.0.0/16", "224.0.0.0/4", "240.0.0.0/4"} {
		if netip.MustParsePrefix(r).Overlaps(p) {
			return true
		}
	}

	return false
}

func parseAnyCidr(s string) (netip.Prefix, bool) {
	p, err := netip.ParsePrefix(s)

	return p, err == nil
}

// v6Block builds the Oracle-allocated /56 for the n-th IPv6-enabled VCN.
func v6Block(n int64) string {
	return "2603:c020:4000:" + strings.ToLower(hex16(n<<8)) + "::/" + "56"
}

func hex16(n int64) string {
	const digits = "0123456789abcdef"

	out := []byte{digits[(n>>12)&0xf], digits[(n>>8)&0xf], digits[(n>>4)&0xf], digits[n&0xf]}

	return string(out)
}

// firstHost returns the address at offset n from the prefix base.
func hostAt(p netip.Prefix, n int) string {
	a := p.Masked().Addr()
	for range n {
		a = a.Next()
	}

	return a.String()
}

func parseAddr(s string) (netip.Addr, bool) {
	a, err := netip.ParseAddr(s)

	return a, err == nil
}
