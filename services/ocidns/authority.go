package ocidns

import (
	"fmt"
	"net"
	"slices"
	"strings"

	"github.com/miekg/dns"
)

const (
	maxCNAMEChain = 8
	classicUDP    = 512
	maxUDPSize    = 65535
)

// authority answers DNS queries from the GLOBAL zones of a backend.
type authority struct{ b *InMemoryBackend }

// Intercept implements pkgs/dns.Interceptor.
func (a authority) Intercept(src net.Addr, req *dns.Msg, _ func() *dns.Msg) *dns.Msg {
	network := "udp"
	if src != nil {
		network = src.Network()
	}

	return a.b.Answer(req, network)
}

// Answer builds the authoritative response to req.
func (b *InMemoryBackend) Answer(req *dns.Msg, network string) *dns.Msg {
	resp := new(dns.Msg)
	resp.SetReply(req)
	resp.RecursionAvailable = false

	switch {
	case req.Opcode != dns.OpcodeQuery:
		resp.Rcode = dns.RcodeNotImplemented
	case len(req.Question) != 1:
		resp.Rcode = dns.RcodeFormatError
	default:
		b.answerQuestion(resp, req.Question[0])
	}

	size := classicUDP

	if opt := req.IsEdns0(); opt != nil {
		size = max(int(opt.UDPSize()), classicUDP)
		resp.SetEdns0(uint16(min(size, maxUDPSize)), opt.Do())
	}

	if strings.HasPrefix(network, "udp") {
		resp.Truncate(size)
	}

	return resp
}

func (b *InMemoryBackend) answerQuestion(resp *dns.Msg, q dns.Question) {
	if q.Qclass != dns.ClassINET || q.Qtype == dns.TypeAXFR || q.Qtype == dns.TypeIXFR {
		resp.Rcode = dns.RcodeRefused

		return
	}

	qname, ok := normName(q.Name, true)
	if !ok {
		resp.Rcode = dns.RcodeRefused

		return
	}

	b.mu.RLock("Answer")
	defer b.mu.RUnlock()

	z := b.globalZoneFor(qname)
	if z == nil {
		resp.Rcode = dns.RcodeRefused

		return
	}

	(&zoneAnswer{b: b, z: z, resp: resp, nodes: b.nodes(z)}).answer(q.Qtype, qname)
}

// globalZoneFor picks the GLOBAL zone with the longest name enclosing qname.
func (b *InMemoryBackend) globalZoneFor(qname string) *Zone {
	var best *Zone

	for _, z := range b.zones {
		if z.Scope != scopeGlobal || !inZone(z.Name, qname) {
			continue
		}

		if best == nil || len(z.Name) > len(best.Name) || len(z.Name) == len(best.Name) && z.Seq < best.Seq {
			best = z
		}
	}

	return best
}

// nodes indexes the zone's record sets, managed ones included, by owner name and type.
func (b *InMemoryBackend) nodes(z *Zone) map[string]map[string]*RRSet {
	out := map[string]map[string]*RRSet{}

	add := func(domain, rtype string, ttl int, rdata ...string) {
		if out[domain] == nil {
			out[domain] = map[string]*RRSet{}
		}

		out[domain][rtype] = &RRSet{Domain: domain, Rtype: rtype, TTL: ttl, Rdata: rdata}
	}

	for _, rs := range z.Records {
		add(rs.Domain, rs.Rtype, rs.TTL, rs.Rdata...)
	}

	var nsData []string

	soa := ""

	for _, m := range b.managed(z) {
		if m.Rtype == rtypeSOA {
			soa = m.Rdata
		} else {
			nsData = append(nsData, m.Rdata)
		}
	}

	add(z.Name, rtypeSOA, soaTTL, soa)

	if len(nsData) > 0 {
		add(z.Name, rtypeNS, nsTTL, nsData...)
	}

	return out
}

type zoneAnswer struct {
	b     *InMemoryBackend
	z     *Zone
	resp  *dns.Msg
	nodes map[string]map[string]*RRSet
}

func (a *zoneAnswer) exists(name string) bool {
	if _, ok := a.nodes[name]; ok {
		return true
	}

	suffix := "." + name

	for owner := range a.nodes {
		if strings.HasSuffix(owner, suffix) {
			return true
		}
	}

	return false
}

// delegation returns the NS set of the highest zone cut below the apex that encloses qname.
func (a *zoneAnswer) delegation(qname string) *RRSet {
	labels := strings.Split(qname, ".")
	apex := len(strings.Split(a.z.Name, "."))

	for n := apex + 1; n <= len(labels); n++ {
		cut := strings.Join(labels[len(labels)-n:], ".")
		if ns := a.nodes[cut][rtypeNS]; ns != nil {
			return ns
		}
	}

	return nil
}

func (a *zoneAnswer) wildcardOwner(qname string) string {
	labels := strings.Split(qname, ".")
	apex := len(strings.Split(a.z.Name, "."))

	for n := len(labels) - 1; n >= apex; n-- {
		encloser := strings.Join(labels[len(labels)-n:], ".")
		if a.exists(encloser) {
			return "*." + encloser
		}
	}

	return ""
}

func toRRs(owner string, rs *RRSet) []dns.RR {
	out := make([]dns.RR, 0, len(rs.Rdata))

	for _, d := range rs.Rdata {
		rtype := rs.Rtype
		if rtype == rtypeALIAS {
			continue
		}

		parsed, err := dns.NewRR(fmt.Sprintf("%s. %d IN %s %s", owner, rs.TTL, rtype, d))
		if err == nil && parsed != nil {
			out = append(out, parsed)
		}
	}

	return out
}

func (a *zoneAnswer) negative(rcode int) {
	a.resp.Rcode = rcode

	if soa := a.nodes[a.z.Name][rtypeSOA]; soa != nil {
		if rrs := toRRs(a.z.Name, soa); len(rrs) > 0 {
			if s, ok := rrs[0].(*dns.SOA); ok {
				s.Hdr.Ttl = min(s.Hdr.Ttl, s.Minttl)
				a.resp.Ns = append(a.resp.Ns, s)
			}
		}
	}
}

func (a *zoneAnswer) answer(qtype uint16, qname string) {
	a.resp.Authoritative = true

	if ns := a.delegation(qname); ns != nil && qtype != dns.TypeDS {
		a.resp.Authoritative = false
		a.resp.Ns = append(a.resp.Ns, toRRs(ns.Domain, ns)...)
		a.glue(a.resp.Ns)

		return
	}

	owner, node := qname, a.nodes[qname]

	if len(node) == 0 && !a.exists(qname) {
		if wc := a.wildcardOwner(qname); wc != "" {
			node = a.nodes[wc]
		}
	}

	if len(node) == 0 {
		if a.exists(qname) {
			a.negative(dns.RcodeSuccess)

			return
		}

		a.negative(dns.RcodeNameError)

		return
	}

	a.fill(qtype, owner, node, 0)

	if len(a.resp.Answer) == 0 {
		a.negative(dns.RcodeSuccess)

		return
	}

	a.glue(a.resp.Answer)
}

func (a *zoneAnswer) fill(qtype uint16, owner string, node map[string]*RRSet, depth int) {
	if cn := node[rtypeCNAME]; cn != nil && qtype != dns.TypeCNAME && qtype != dns.TypeANY {
		a.chase(qtype, owner, cn, depth)

		return
	}

	types := make([]string, 0, len(node))
	for t := range node {
		types = append(types, t)
	}

	slices.Sort(types)

	for _, t := range types {
		if qtype == dns.TypeANY || dns.StringToType[t] == qtype {
			a.resp.Answer = append(a.resp.Answer, toRRs(owner, node[t])...)
		}
	}
}

func (a *zoneAnswer) chase(qtype uint16, owner string, cn *RRSet, depth int) {
	rrs := toRRs(owner, cn)
	a.resp.Answer = append(a.resp.Answer, rrs...)

	if len(rrs) != 1 || depth >= maxCNAMEChain {
		return
	}

	rec, ok := rrs[0].(*dns.CNAME)
	if !ok {
		return
	}

	if target, valid := normName(rec.Target, true); valid && inZone(a.z.Name, target) {
		a.fill(qtype, target, a.nodes[target], depth+1)
	}
}

// glue adds in-zone A and AAAA records for the targets of NS, MX and SRV records.
func (a *zoneAnswer) glue(rrs []dns.RR) {
	seen := map[string]bool{}

	for _, rr := range rrs {
		var target string

		switch v := rr.(type) {
		case *dns.NS:
			target = v.Ns
		case *dns.MX:
			target = v.Mx
		case *dns.SRV:
			target = v.Target
		default:
			continue
		}

		t, ok := normName(target, false)
		if !ok || seen[t] || !inZone(a.z.Name, t) {
			continue
		}

		seen[t] = true

		for _, ty := range []string{"A", "AAAA"} {
			if set := a.nodes[t][ty]; set != nil {
				a.resp.Extra = append(a.resp.Extra, toRRs(t, set)...)
			}
		}
	}
}
