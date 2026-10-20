// Package sgfilter evaluates EC2 security-group rules against network flows and fronts container-published
// ports with a TCP proxy that admits or refuses clients by those rules. Enforcement is opt-in: a nil
// *Enforcer admits everything.
package sgfilter

import (
	"net"
	"net/netip"
	"slices"
	"strings"
)

// ProtoAll is the security-group protocol that matches every IP protocol and ignores ports.
const ProtoAll = "-1"

// Protocol names used by flows.
const (
	ProtoTCP  = "tcp"
	ProtoUDP  = "udp"
	ProtoICMP = "icmp"
)

// IANA protocol numbers EC2 accepts as aliases of the names above.
const (
	numICMP   = "1"
	numTCP    = "6"
	numUDP    = "17"
	numICMPv6 = "58"
)

// Rule is one ingress or egress permission. A rule matches a peer when any CIDR contains its address or any
// referenced group is attached to it.
type Rule struct {
	Protocol string
	CIDRs    []netip.Prefix
	Groups   []string
	FromPort int
	ToPort   int
}

// Group is a security group's rules.
type Group struct {
	ID      string
	Ingress []Rule
	Egress  []Rule
}

// Peer is the far end of a flow: its address and the security groups attached to it, if known.
type Peer struct {
	Addr   netip.Addr
	Groups []string
}

// Flow is one connection attempt, described from the protected target's point of view.
type Flow struct {
	Protocol string
	Peer     Peer
	Port     int
}

// AllowIngress reports whether any group admits the flow; with no groups, or no matching rule, it is denied.
func AllowIngress(groups []Group, f Flow) bool {
	for _, g := range groups {
		if anyRuleMatches(g.Ingress, f) {
			return true
		}
	}

	return false
}

// AllowEgress reports whether any group's egress rule admits the flow; with no matching rule it is denied.
func AllowEgress(groups []Group, f Flow) bool {
	for _, g := range groups {
		if anyRuleMatches(g.Egress, f) {
			return true
		}
	}

	return false
}

func anyRuleMatches(rules []Rule, f Flow) bool {
	for _, r := range rules {
		if r.matches(f) {
			return true
		}
	}

	return false
}

func (r Rule) matches(f Flow) bool {
	return r.matchesProtocol(f) && r.matchesPort(f) && r.matchesPeer(f.Peer)
}

func (r Rule) matchesProtocol(f Flow) bool {
	if r.Protocol == ProtoAll {
		return true
	}

	return NormalizeProtocol(r.Protocol) == NormalizeProtocol(f.Protocol)
}

func (r Rule) matchesPort(f Flow) bool {
	if r.Protocol == ProtoAll {
		return true
	}

	switch NormalizeProtocol(r.Protocol) {
	case ProtoTCP, ProtoUDP:
		return f.Port >= r.FromPort && f.Port <= r.ToPort
	default:
		return true
	}
}

func (r Rule) matchesPeer(p Peer) bool {
	if p.Addr.IsValid() {
		addr := p.Addr.Unmap()

		for _, c := range r.CIDRs {
			if c.Contains(addr) {
				return true
			}
		}
	}

	return slices.ContainsFunc(r.Groups, func(g string) bool { return slices.Contains(p.Groups, g) })
}

// NormalizeProtocol maps protocol numbers and mixed-case names onto "tcp", "udp" and "icmp"; "-1" and
// unknown values pass through lower-cased.
func NormalizeProtocol(p string) string {
	switch strings.ToLower(p) {
	case numTCP:
		return ProtoTCP
	case numUDP:
		return ProtoUDP
	case numICMP, numICMPv6, "icmpv6":
		return ProtoICMP
	default:
		return strings.ToLower(p)
	}
}

// AddrOf extracts the IP of a remote address; the zero Addr when it has none.
func AddrOf(a net.Addr) netip.Addr {
	switch v := a.(type) {
	case *net.TCPAddr:
		ip, _ := netip.AddrFromSlice(v.IP)

		return ip.Unmap()
	case *net.UDPAddr:
		ip, _ := netip.AddrFromSlice(v.IP)

		return ip.Unmap()
	case nil:
		return netip.Addr{}
	default:
		ap, err := netip.ParseAddrPort(a.String())
		if err != nil {
			return netip.Addr{}
		}

		return ap.Addr().Unmap()
	}
}
