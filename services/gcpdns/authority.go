package gcpdns

import (
	"fmt"
	"net"
	"slices"
	"strings"

	"github.com/miekg/dns"
	dnsapi "google.golang.org/api/dns/v1"
)

const (
	maxCNAMEChain = 8
	classicUDP    = 512
)

// authority answers DNS queries from the public zones of a backend.
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

	if opt := req.IsEdns0(); opt != nil {
		resp.SetEdns0(max(opt.UDPSize(), classicUDP), opt.Do())
	}

	if strings.HasPrefix(network, "udp") {
		size := classicUDP
		if opt := req.IsEdns0(); opt != nil {
			size = max(int(opt.UDPSize()), classicUDP)
		}

		resp.Truncate(size)
	}

	return resp
}

func (b *InMemoryBackend) answerQuestion(resp *dns.Msg, q dns.Question) {
	if q.Qclass != dns.ClassINET || q.Qtype == dns.TypeAXFR || q.Qtype == dns.TypeIXFR {
		resp.Rcode = dns.RcodeRefused

		return
	}

	b.mu.RLock("Answer")
	defer b.mu.RUnlock()

	rec := b.publicZoneFor(canon(q.Name))
	if rec == nil {
		resp.Rcode = dns.RcodeRefused

		return
	}

	(&zoneAnswer{rec: rec, resp: resp}).answer(q)
}

// publicZoneFor picks the public zone with the longest dnsName enclosing qname.
func (b *InMemoryBackend) publicZoneFor(qname string) *zoneRec {
	var best *zoneRec

	for _, rec := range b.zones {
		if rec.Zone.Visibility != visPublic || !dns.IsSubDomain(rec.Zone.DnsName, qname) {
			continue
		}

		if best == nil || len(rec.Zone.DnsName) > len(best.Zone.DnsName) ||
			len(rec.Zone.DnsName) == len(best.Zone.DnsName) && rec.Zone.CreationTime+key(rec.Project, rec.Zone.Name) <
				best.Zone.CreationTime+key(best.Project, best.Zone.Name) {
			best = rec
		}
	}

	return best
}

type zoneAnswer struct {
	rec  *zoneRec
	resp *dns.Msg
}

func (z *zoneAnswer) apex() string { return z.rec.Zone.DnsName }

func (z *zoneAnswer) nodeTypes(name string) map[string]*dnsapi.ResourceRecordSet {
	out := map[string]*dnsapi.ResourceRecordSet{}

	for _, rr := range z.rec.RRSets {
		if rr.Name == name {
			out[rr.Type] = rr
		}
	}

	return out
}

// exists reports whether name owns records or is an empty non-terminal.
func (z *zoneAnswer) exists(name string) bool {
	if name == z.apex() {
		return true
	}

	suffix := "." + name

	for _, rr := range z.rec.RRSets {
		if rr.Name == name || strings.HasSuffix(rr.Name, suffix) {
			return true
		}
	}

	return false
}

// delegation returns the NS rrset of the highest zone cut at or above qname, below the apex.
func (z *zoneAnswer) delegation(qname string) *dnsapi.ResourceRecordSet {
	labels := dns.SplitDomainName(qname)
	apexLabels := len(dns.SplitDomainName(z.apex()))

	for n := apexLabels + 1; n <= len(labels); n++ {
		cut := strings.Join(labels[len(labels)-n:], ".") + "."
		if ns := z.rec.RRSets[rrKey(cut, typeNS)]; ns != nil {
			return ns
		}
	}

	return nil
}

func (z *zoneAnswer) wildcardOwner(qname string) string {
	labels := dns.SplitDomainName(qname)
	apexLabels := len(dns.SplitDomainName(z.apex()))

	for n := len(labels) - 1; n >= apexLabels; n-- {
		encloser := strings.Join(labels[len(labels)-n:], ".") + "."
		if n == 0 {
			encloser = "."
		}

		if z.exists(encloser) {
			return "*." + encloser
		}
	}

	return ""
}

func rrsetData(rr *dnsapi.ResourceRecordSet) []string {
	if len(rr.Rrdatas) > 0 || rr.RoutingPolicy == nil {
		return rr.Rrdatas
	}

	var out []string

	if w := rr.RoutingPolicy.Wrr; w != nil {
		for _, it := range w.Items {
			out = append(out, it.Rrdatas...)
		}
	}

	if g := rr.RoutingPolicy.Geo; g != nil {
		for _, it := range g.Items {
			out = append(out, it.Rrdatas...)
		}
	}

	return out
}

func toRRs(owner string, rr *dnsapi.ResourceRecordSet) []dns.RR {
	data := rrsetData(rr)
	out := make([]dns.RR, 0, len(data))

	for _, d := range data {
		parsed, err := dns.NewRR(fmt.Sprintf("%s %d IN %s %s", owner, rr.Ttl, rr.Type, d))
		if err == nil && parsed != nil {
			out = append(out, parsed)
		}
	}

	return out
}

func (z *zoneAnswer) soaRR() *dns.SOA {
	rr := z.rec.RRSets[rrKey(z.apex(), typeSOA)]
	if rr == nil {
		return nil
	}

	rrs := toRRs(z.apex(), rr)
	if len(rrs) == 0 {
		return nil
	}

	soa, _ := rrs[0].(*dns.SOA)

	return soa
}

func (z *zoneAnswer) negative(rcode int) {
	z.resp.Rcode = rcode

	if soa := z.soaRR(); soa != nil {
		soa.Hdr.Ttl = min(soa.Hdr.Ttl, soa.Minttl)
		z.resp.Ns = append(z.resp.Ns, soa)
	}
}

func (z *zoneAnswer) answer(q dns.Question) {
	qname := canon(q.Name)
	z.resp.Authoritative = true

	if ns := z.delegation(qname); ns != nil && q.Qtype != dns.TypeDS {
		z.resp.Authoritative = false
		z.resp.Ns = append(z.resp.Ns, toRRs(ns.Name, ns)...)
		z.glue(z.resp.Ns)

		return
	}

	owner, node := qname, z.nodeTypes(qname)

	if len(node) == 0 && !z.exists(qname) {
		if wc := z.wildcardOwner(qname); wc != "" {
			if node = z.nodeTypes(wc); len(node) > 0 {
				owner = qname
			}
		}
	}

	if len(node) == 0 {
		if z.exists(qname) {
			z.negative(dns.RcodeSuccess)

			return
		}

		z.negative(dns.RcodeNameError)

		return
	}

	z.fill(q, owner, node, 0)

	if len(z.resp.Answer) == 0 {
		z.negative(dns.RcodeSuccess)

		return
	}

	z.glue(z.resp.Answer)
}

func (z *zoneAnswer) fill(q dns.Question, owner string, node map[string]*dnsapi.ResourceRecordSet, depth int) {
	if cn := node[typeCNAME]; cn != nil && q.Qtype != dns.TypeCNAME && q.Qtype != dns.TypeANY {
		rrs := toRRs(owner, cn)
		z.resp.Answer = append(z.resp.Answer, rrs...)

		if len(rrs) == 1 && depth < maxCNAMEChain {
			if target := canon(rrs[0].(*dns.CNAME).Target); dns.IsSubDomain(z.apex(), target) {
				z.fill(q, target, z.nodeTypes(target), depth+1)
			}
		}

		return
	}

	types := make([]string, 0, len(node))
	for t := range node {
		types = append(types, t)
	}

	slices.Sort(types)

	for _, t := range types {
		if q.Qtype == dns.TypeANY || dns.StringToType[t] == q.Qtype {
			z.resp.Answer = append(z.resp.Answer, toRRs(owner, node[t])...)
		}
	}

	if q.Qtype == dns.TypeDNSKEY && owner == z.apex() && len(z.resp.Answer) == 0 {
		for _, k := range z.rec.Keys {
			z.resp.Answer = append(z.resp.Answer, keyRR(z.apex(), k))
		}
	}
}

// glue adds in-zone A and AAAA records for the targets of NS, MX and SRV records.
func (z *zoneAnswer) glue(rrs []dns.RR) {
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

		target = canon(target)
		if seen[target] || !dns.IsSubDomain(z.apex(), target) {
			continue
		}

		seen[target] = true

		for _, t := range []string{"A", "AAAA"} {
			if set := z.rec.RRSets[rrKey(target, t)]; set != nil {
				z.resp.Extra = append(z.resp.Extra, toRRs(target, set)...)
			}
		}
	}
}
