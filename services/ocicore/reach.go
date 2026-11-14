package ocicore

import (
	"errors"
	"net/netip"
	"slices"
	"strconv"
	"strings"
)

// ErrEndpoint is returned by CanReach when an endpoint does not name a live VNIC.
var ErrEndpoint = errors.New("ocicore: endpoint does not name a VNIC")

const (
	icmpEchoRequest = 8
	icmpEchoReply   = 0
)

// Endpoint names one side of a flow: a VNIC, or a private IP assigned to one.
type Endpoint struct {
	VnicID   string
	IP       string
	SubnetID string
}

// Flow is a new connection from a source to a destination. Protocol is tcp, udp, icmp, icmpv6, all or an IANA number.
type Flow struct {
	Protocol   string
	Port       int
	SourcePort int
	IcmpType   int
	IcmpCode   int
}

// Verdict is the outcome of CanReach.
type Verdict struct {
	Reason  string
	Allowed bool
	Forward bool
	Reply   bool
}

// effRule is a security list or NSG rule reduced to what the evaluator needs.
type effRule struct {
	tcp      *PortOptions
	udp      *PortOptions
	icmp     *IcmpOptions
	protocol string
	peer     string
	peerType string
	stateful bool
}

type reachEnd struct {
	vnic   *Vnic
	subnet *Subnet
	addr   netip.Addr
}

func normalizeProtocol(p string) string {
	switch strings.ToLower(p) {
	case "tcp":
		return protoTCP
	case "udp":
		return protoUDP
	case "icmp":
		return protoICMP
	case "icmpv6":
		return protoICMPv6
	case "":
		return protoAll
	}

	return strings.ToLower(p)
}

func (b *InMemoryBackend) resolveEndpoint(e Endpoint) (reachEnd, error) {
	var v *Vnic

	addr := e.IP

	switch {
	case e.VnicID != "":
		x, ok := b.res[e.VnicID].(*Vnic)
		if !ok || !live(x) {
			return reachEnd{}, ErrEndpoint
		}

		v = x
		addr = cmpOrStr(addr, x.PrivateIP)
	case e.IP != "":
		for _, p := range liveOf(b, func(x *PrivateIP) bool {
			return x.IPAddress == e.IP && x.VnicID != "" && (e.SubnetID == "" || x.SubnetID == e.SubnetID)
		}) {
			if x, ok := b.res[p.VnicID].(*Vnic); ok && live(x) {
				v = x

				break
			}
		}
	}

	if v == nil {
		return reachEnd{}, ErrEndpoint
	}

	s, ok := b.res[v.SubnetID].(*Subnet)
	a, err := netip.ParseAddr(addr)

	if !ok || err != nil {
		return reachEnd{}, ErrEndpoint
	}

	return reachEnd{vnic: v, subnet: s, addr: a}, nil
}

// rulesOf collects the egress or ingress rules that apply to a VNIC: its subnet's security lists plus its NSGs.
func (b *InMemoryBackend) rulesOf(end reachEnd, direction string) []effRule {
	var out []effRule

	for _, id := range end.subnet.SecurityListIDs {
		sl, ok := b.res[id].(*SecurityList)
		if !ok || !live(sl) {
			continue
		}

		if direction == dirEgress {
			for _, r := range sl.EgressRules {
				out = append(out, effRule{protocol: r.Protocol, peer: r.Destination, peerType: r.DestinationType,
					stateful: !flag(r.IsStateless), tcp: r.TCPOptions, udp: r.UDPOptions, icmp: r.IcmpOptions})
			}

			continue
		}

		for _, r := range sl.IngressRules {
			out = append(out, effRule{protocol: r.Protocol, peer: r.Source, peerType: r.SourceType,
				stateful: !flag(r.IsStateless), tcp: r.TCPOptions, udp: r.UDPOptions, icmp: r.IcmpOptions})
		}
	}

	return append(out, b.nsgEffRules(end.vnic, direction)...)
}

func (b *InMemoryBackend) nsgEffRules(v *Vnic, direction string) []effRule {
	var out []effRule

	for _, r := range b.nsgRules {
		if r.Direction != direction || !slices.Contains(v.NsgIDs, r.NsgID) {
			continue
		}

		peer, typ := r.Source, r.SourceType
		if direction == dirEgress {
			peer, typ = r.Destination, r.DestinationType
		}

		out = append(out, effRule{protocol: r.Protocol, peer: peer, peerType: typ, stateful: !flag(r.IsStateless),
			tcp: r.TCPOptions, udp: r.UDPOptions, icmp: r.IcmpOptions})
	}

	return out
}

func (r effRule) matchesPeer(other reachEnd) bool {
	switch r.peerType {
	case ctNSG:
		return slices.Contains(other.vnic.NsgIDs, r.peer)
	case ctCIDR, "":
		p, err := netip.ParsePrefix(r.peer)

		return err == nil && p.Contains(other.addr)
	}

	return false
}

func inRange(pr *PortRange, port int) bool {
	return pr == nil || port == 0 || (port >= pr.Min && port <= pr.Max)
}

func portsOf(r effRule, proto string) *PortOptions {
	if proto == protoUDP {
		return r.udp
	}

	return r.tcp
}

// matchesFlow checks protocol and ports; srcPort and dstPort are the ports the rule sees (0 = unknown).
func (r effRule) matchesFlow(proto string, srcPort, dstPort, icmpType, icmpCode int) bool {
	if r.protocol != protoAll && r.protocol != proto {
		return false
	}

	switch proto {
	case protoTCP, protoUDP:
		o := portsOf(r, proto)

		return o == nil || (inRange(o.SourcePortRange, srcPort) && inRange(o.DestinationPortRange, dstPort))
	case protoICMP, protoICMPv6:
		return r.icmp == nil || (r.icmp.Type == icmpType && (r.icmp.Code == nil || *r.icmp.Code == icmpCode))
	}

	return true
}

// firstMatch returns the best rule for the flow, preferring a stateful one; ok reports whether any rule matched.
func firstMatch(
	rules []effRule,
	peer reachEnd,
	proto string,
	srcPort, dstPort, icmpType, icmpCode int,
) (effRule, bool) {
	var best effRule

	found := false

	for _, r := range rules {
		if !r.matchesPeer(peer) || !r.matchesFlow(proto, srcPort, dstPort, icmpType, icmpCode) {
			continue
		}

		if r.stateful {
			return r, true
		}

		if !found {
			best, found = r, true
		}
	}

	return best, found
}

// CanReach evaluates whether a new connection from src to dst passes the security lists and network security
// groups of both VNICs, including the reply path when a matched rule is stateless. Only traffic inside one VCN
// is evaluated; nothing enforces it.
func (b *InMemoryBackend) CanReach(src, dst Endpoint, f Flow) (Verdict, error) {
	b.mu.RLock("CanReach")
	defer b.mu.RUnlock()

	s, err := b.resolveEndpoint(src)
	if err != nil {
		return Verdict{}, err
	}

	d, err := b.resolveEndpoint(dst)
	if err != nil {
		return Verdict{}, err
	}

	if s.subnet.VcnID != d.subnet.VcnID {
		return Verdict{Reason: "the endpoints are in different VCNs: only same-VCN traffic is evaluated"}, nil
	}

	proto := normalizeProtocol(f.Protocol)

	out, ok := firstMatch(b.rulesOf(s, dirEgress), d, proto, f.SourcePort, f.Port, f.IcmpType, f.IcmpCode)
	if !ok {
		return Verdict{Reason: "no egress rule of the source permits the flow"}, nil
	}

	in, ok := firstMatch(b.rulesOf(d, dirIngress), s, proto, f.SourcePort, f.Port, f.IcmpType, f.IcmpCode)
	if !ok {
		return Verdict{Reason: "no ingress rule of the destination permits the flow"}, nil
	}

	v := Verdict{Forward: true, Reply: true, Allowed: true}

	if !in.stateful && !b.replyAllowed(d, s, proto, f, dirEgress) {
		v.Reply, v.Allowed, v.Reason = false, false, "the ingress rule is stateless and no egress rule permits the reply"
	}

	if v.Reply && !out.stateful && !b.replyAllowed(s, d, proto, f, dirIngress) {
		v.Reply, v.Allowed, v.Reason = false, false, "the egress rule is stateless and no ingress rule permits the reply"
	}

	return v, nil
}

// replyAllowed looks for an explicit rule on end that lets the reply travel (egress at the replier, ingress at
// the requester): the reply carries the request's destination port as source port.
func (b *InMemoryBackend) replyAllowed(end, peer reachEnd, proto string, f Flow, direction string) bool {
	icmpType := f.IcmpType
	if (proto == protoICMP || proto == protoICMPv6) && icmpType == icmpEchoRequest {
		icmpType = icmpEchoReply
	}

	_, ok := firstMatch(b.rulesOf(end, direction), peer, proto, f.Port, f.SourcePort, icmpType, f.IcmpCode)

	return ok
}

// String renders a verdict for logs.
func (v Verdict) String() string {
	if v.Allowed {
		return "allowed"
	}

	return "denied: " + v.Reason + " (forward=" + strconv.FormatBool(v.Forward) + ")"
}
