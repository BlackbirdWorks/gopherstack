package dns

import (
	"net"
	"strings"
	"time"

	"github.com/miekg/dns"
)

const (
	upstreamTimeout = 3 * time.Second
	resolvConfPath  = "/etc/resolv.conf"
)

// DefaultTransparentSuffixes lists the real AWS endpoint zones answered in transparent mode.
func DefaultTransparentSuffixes() []string {
	return []string{"amazonaws.com", "amazonaws.com.cn", "api.aws"}
}

// TransparentConfig makes the server answer real AWS endpoint names with ResolveIP
// and forward every other unowned name to an upstream resolver.
type TransparentConfig struct {
	// Suffixes are zones whose names resolve to ResolveIP; empty disables the mode.
	Suffixes []string
	// Skip lists names (or zones) that always resolve upstream, e.g. "sts.amazonaws.com".
	Skip []string
	// Upstreams are host:port resolvers; empty means /etc/resolv.conf.
	Upstreams []string
}

type transparent struct {
	suffixes  []string
	skip      []string
	upstreams []string
}

func newTransparent(cfg TransparentConfig, self string) *transparent {
	if len(cfg.Suffixes) == 0 {
		return nil
	}

	ups := cfg.Upstreams
	if len(ups) == 0 {
		ups = ResolvConfUpstreams(resolvConfPath)
	}

	t := &transparent{suffixes: normZones(cfg.Suffixes), skip: normZones(cfg.Skip)}

	for _, u := range ups {
		if !isSelfAddr(u, self) {
			t.upstreams = append(t.upstreams, u)
		}
	}

	return t
}

// isSelfAddr reports whether upstream would loop back to the server listening on self.
func isSelfAddr(upstream, self string) bool {
	if upstream == self {
		return true
	}

	uh, up, err := net.SplitHostPort(upstream)
	if err != nil {
		return false
	}

	sh, sp, err := net.SplitHostPort(self)
	if err != nil || up != sp {
		return false
	}

	if uh == sh {
		return true
	}

	uip, sip := net.ParseIP(uh), net.ParseIP(sh)
	selfLocal := sh == "" || (sip != nil && (sip.IsUnspecified() || sip.IsLoopback()))

	return selfLocal && uip != nil && (uip.IsLoopback() || uip.IsUnspecified())
}

func normZones(in []string) []string {
	out := make([]string, 0, len(in))

	for _, z := range in {
		if z = strings.Trim(strings.ToLower(strings.TrimSpace(z)), "."); z != "" {
			out = append(out, z)
		}
	}

	return out
}

func inZone(name string, zones []string) bool {
	for _, z := range zones {
		if name == z || strings.HasSuffix(name, "."+z) {
			return true
		}
	}

	return false
}

// ResolvConfUpstreams returns the nameservers (host:port) from a resolv.conf file.
func ResolvConfUpstreams(path string) []string {
	cc, err := dns.ClientConfigFromFile(path)
	if err != nil {
		return nil
	}

	out := make([]string, 0, len(cc.Servers))
	for _, s := range cc.Servers {
		out = append(out, net.JoinHostPort(s, cc.Port))
	}

	return out
}

// answer returns the reply for an unowned name: the local IP for AWS zones, upstream otherwise.
func (t *transparent) answer(s *Server, r *dns.Msg, q dns.Question) *dns.Msg {
	name := strings.Trim(strings.ToLower(q.Name), ".")

	if inZone(name, t.suffixes) && !inZone(name, t.skip) {
		msg := new(dns.Msg)
		msg.SetReply(r)
		msg.Authoritative = true

		if q.Qtype == dns.TypeA {
			msg.Answer = append(msg.Answer, &dns.A{
				Hdr: dns.RR_Header{Name: q.Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: defaultTTL},
				A:   s.resolveIP,
			})
		}

		return msg
	}

	return t.forward(r)
}

func (t *transparent) forward(r *dns.Msg) *dns.Msg {
	c := &dns.Client{Timeout: upstreamTimeout}

	for _, up := range t.upstreams {
		resp, _, err := c.Exchange(r, up)
		if err == nil && resp != nil && resp.Truncated {
			tc := &dns.Client{Net: "tcp", Timeout: upstreamTimeout}
			resp, _, err = tc.Exchange(r, up)
		}

		if err == nil && resp != nil {
			resp.Id = r.Id

			return resp
		}
	}

	msg := new(dns.Msg)
	msg.SetRcode(r, dns.RcodeServerFailure)

	return msg
}
