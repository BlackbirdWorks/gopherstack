package route53resolver

import (
	"context"
	"strings"

	"github.com/miekg/dns"
)

const defaultBlockTTL = 60

type firewallHit struct {
	action       string
	ruleGroupID  string
	domainListID string
}

// applyFirewall evaluates the VPC's associated rule groups by priority. A
// non-nil message is the BLOCK response; ALLOW ends evaluation, ALERT only
// records the hit.
func (r *DNSResolver) applyFirewall(
	ctx context.Context, vpcID string, req *dns.Msg, name string, qtype uint16,
) (*dns.Msg, *firewallHit) {
	var hit *firewallHit

	for _, a := range r.b.ListFirewallRuleGroupAssociations(ctx, vpcID, "") {
		if a.Status != statusComplete {
			continue
		}

		rule := r.matchFirewallRule(ctx, a.FirewallRuleGroupID, name, qtype)
		if rule == nil {
			continue
		}

		hit = &firewallHit{
			action:       rule.Action,
			ruleGroupID:  rule.FirewallRuleGroupID,
			domainListID: rule.FirewallDomainListID,
		}

		switch rule.Action {
		case firewallActionBlock:
			return blockResponse(req, rule), hit
		case firewallActionAllow:
			return nil, hit
		}
	}

	return nil, hit
}

func (r *DNSResolver) matchFirewallRule(ctx context.Context, groupID, name string, qtype uint16) *FirewallRule {
	for _, rule := range r.b.ListFirewallRules(ctx, groupID) {
		if rule.FirewallDomainListID == "" {
			continue
		}

		if rule.Qtype != "" && !strings.EqualFold(rule.Qtype, dns.TypeToString[qtype]) {
			continue
		}

		domains, err := r.b.ListFirewallDomains(ctx, rule.FirewallDomainListID)
		if err == nil && domainListMatches(domains, name) {
			return rule
		}
	}

	return nil
}

func domainListMatches(domains []string, name string) bool {
	for _, d := range domains {
		d = normDomain(d)
		if rest, ok := strings.CutPrefix(d, "*."); ok {
			if strings.HasSuffix(name, "."+rest) {
				return true
			}

			continue
		}

		if d == name {
			return true
		}
	}

	return false
}

func blockResponse(req *dns.Msg, rule *FirewallRule) *dns.Msg {
	m := new(dns.Msg)
	m.SetReply(req)
	m.RecursionAvailable = true

	switch rule.BlockResponse {
	case blockResponseNXDOMAIN:
		m.Rcode = dns.RcodeNameError
	case blockResponseOVERRIDE:
		ttl := uint32(defaultBlockTTL)
		if rule.BlockOverrideTTL > 0 {
			ttl = uint32(rule.BlockOverrideTTL)
		}

		m.Answer = append(m.Answer, &dns.CNAME{
			Hdr:    dns.RR_Header{Name: req.Question[0].Name, Rrtype: dns.TypeCNAME, Class: dns.ClassINET, Ttl: ttl},
			Target: dns.Fqdn(rule.BlockOverrideDomain),
		})
	}

	return m
}
