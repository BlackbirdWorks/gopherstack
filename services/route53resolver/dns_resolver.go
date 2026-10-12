package route53resolver

import (
	"context"
	"net"
	"strings"
	"time"

	"github.com/miekg/dns"
)

const defaultForwardTimeout = 2 * time.Second

// DNSResolver applies Resolver rules, DNS Firewall and query logging to queries
// reaching the embedded DNS server from a VPC.
type DNSResolver struct {
	b       *InMemoryBackend
	owns    func(string) bool
	timeout time.Duration
}

// NewDNSResolver builds a resolver over b. owns reports names the emulator
// itself serves; those are never forwarded.
func (b *InMemoryBackend) NewDNSResolver(owns func(string) bool) *DNSResolver {
	return &DNSResolver{b: b, owns: owns, timeout: defaultForwardTimeout}
}

// Intercept implements the embedded DNS server's interceptor. Queries from
// addresses that map to no VPC fall through to the static records.
func (r *DNSResolver) Intercept(src net.Addr, req *dns.Msg, local func() *dns.Msg) *dns.Msg {
	if len(req.Question) == 0 {
		return nil
	}

	region, vpcID, ok := r.b.locateVPC(addrIP(src))
	if !ok {
		return nil
	}

	q := req.Question[0]
	name := normDomain(q.Name)
	ctx := context.WithValue(context.Background(), regionContextKey{}, region)

	resp, fw := r.applyFirewall(ctx, vpcID, req, name, q.Qtype)
	if resp == nil {
		resp = r.resolve(ctx, region, vpcID, req, name, local)
	}

	r.logQuery(ctx, queryLogEntry{
		region: region, vpcID: vpcID, src: src, req: req, resp: resp, fw: fw,
	})

	return resp
}

func addrIP(a net.Addr) net.IP {
	switch v := a.(type) {
	case *net.UDPAddr:
		return v.IP
	case *net.TCPAddr:
		return v.IP
	default:
		return nil
	}
}

func normDomain(s string) string {
	return strings.ToLower(strings.TrimSuffix(strings.TrimSpace(s), "."))
}

// domainSpecificity is the label count of d; -1 when name is outside d.
func domainSpecificity(d, name string) int {
	d = normDomain(d)
	if d == "" {
		return 0
	}

	if name == d || strings.HasSuffix(name, "."+d) {
		return strings.Count(d, ".") + 1
	}

	return -1
}

// resolve picks the most specific rule or private zone for name. A rule wins a
// tie with a private zone.
func (r *DNSResolver) resolve(
	ctx context.Context, region, vpcID string, req *dns.Msg, name string, local func() *dns.Msg,
) *dns.Msg {
	rule, ruleSpec := r.bestRule(ctx, vpcID, name)
	zone, zoneSpec := r.b.bestPrivateZone(r.b.route53Backend(), vpcID, region, name)

	if rule != nil && ruleSpec >= zoneSpec && rule.RuleType == ruleTypeForward {
		if zoneSpec < 0 && r.owns != nil && r.owns(name) {
			return local()
		}

		return r.forward(rule, req)
	}

	if zone != nil {
		return r.zoneAnswer(zone, req, name)
	}

	return local()
}

func (r *DNSResolver) bestRule(ctx context.Context, vpcID, name string) (*ResolverRule, int) {
	var (
		best     *ResolverRule
		bestSpec = -1
	)

	for _, a := range r.b.ListResolverRuleAssociations(ctx) {
		if a.VPCID != vpcID || a.Status != statusComplete {
			continue
		}

		rule, err := r.b.GetResolverRule(ctx, a.ResolverRuleID)
		if err != nil {
			continue
		}

		spec := domainSpecificity(rule.DomainName, name)
		if spec > bestSpec || (spec == bestSpec && best != nil && rule.ID < best.ID) {
			best, bestSpec = rule, spec
		}
	}

	return best, bestSpec
}

func servfail(req *dns.Msg) *dns.Msg {
	m := new(dns.Msg)
	m.SetRcode(req, dns.RcodeServerFailure)
	m.RecursionAvailable = true

	return m
}
