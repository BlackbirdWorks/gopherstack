package route53resolver

import (
	"math/rand/v2"
	"net"
	"strconv"

	"github.com/miekg/dns"
)

const (
	dnsDefaultPort = 53
	protocolDo53   = "Do53"
)

// forward sends req to the rule's targets in random order, failing over until
// one answers; SERVFAIL when none do. DoH targets are skipped.
func (r *DNSResolver) forward(rule *ResolverRule, req *dns.Msg) *dns.Msg {
	targets := make([]TargetIP, len(rule.TargetIps))
	copy(targets, rule.TargetIps)
	//nolint:gosec // non-cryptographic target ordering
	rand.Shuffle(len(targets), func(i, j int) { targets[i], targets[j] = targets[j], targets[i] })

	for _, t := range targets {
		addr, ok := targetAddr(t)
		if !ok {
			continue
		}

		if resp := r.exchange(addr, req); resp != nil {
			return resp
		}
	}

	return servfail(req)
}

func targetAddr(t TargetIP) (string, bool) {
	if t.Protocol != "" && t.Protocol != protocolDo53 {
		return "", false
	}

	host := t.IP
	if host == "" {
		host = t.Ipv6
	}

	if host == "" {
		return "", false
	}

	port := int(t.Port)
	if port == 0 {
		port = dnsDefaultPort
	}

	return net.JoinHostPort(host, strconv.Itoa(port)), true
}

func (r *DNSResolver) exchange(addr string, req *dns.Msg) *dns.Msg {
	out := req.Copy()
	out.RecursionDesired = true

	c := &dns.Client{Net: "udp", Timeout: r.timeout}

	resp, _, err := c.Exchange(out, addr)
	if err == nil && resp.Truncated {
		c.Net = "tcp"
		resp, _, err = c.Exchange(out, addr)
	}

	if err != nil || resp == nil {
		return nil
	}

	return resp
}
