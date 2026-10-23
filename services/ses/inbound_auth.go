package ses

import (
	"context"
	"net/mail"
	"net/netip"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	verdictFail             = "FAIL"
	verdictProcessingFailed = "PROCESSING_FAILED"
	maxSPFLookups           = 10
	orgLabels               = 2
)

// InboundTXTResolver answers TXT lookups for SPF, DKIM and DMARC evaluation.
type InboundTXTResolver interface {
	LookupTXT(ctx context.Context, name string) ([]string, error)
}

// SetInboundTXTResolver wires the DNS source for inbound authentication verdicts.
func (b *InMemoryBackend) SetInboundTXTResolver(r InboundTXTResolver) {
	b.mu.Lock("SetInboundTXTResolver")
	defer b.mu.Unlock()

	b.inboundDNS = r
}

// authVerdicts are the SPF, DKIM and DMARC outcomes of one received message.
type authVerdicts struct {
	spf, dkim, dmarc, dmarcPolicy string
}

func grayVerdicts() authVerdicts {
	return authVerdicts{spf: verdictGray, dkim: verdictGray, dmarc: verdictGray}
}

// evaluateAuth computes SPF, DKIM and DMARC verdicts; without a resolver every verdict is GRAY.
func evaluateAuth(ctx context.Context, res InboundTXTResolver, env inboundEnvelope) authVerdicts {
	if res == nil {
		return grayVerdicts()
	}

	spfRes := checkSPF(ctx, res, env.from, env.clientIP)
	dkimRes, dkimDomains := checkDKIM(ctx, res, env.data)
	dmarcRes, policy := checkDMARC(ctx, res, env, spfRes, dkimDomains)

	return authVerdicts{spf: spfRes, dkim: dkimRes, dmarc: dmarcRes, dmarcPolicy: policy}
}

func txtRecords(ctx context.Context, res InboundTXTResolver, name string, prefix string) []string {
	recs, err := res.LookupTXT(ctx, name)
	if err != nil {
		return nil
	}

	var out []string

	for _, r := range recs {
		if strings.HasPrefix(strs.Fold(strings.TrimSpace(r)), prefix) {
			out = append(out, strings.TrimSpace(r))
		}
	}

	return out
}

// checkSPF evaluates the sender domain's v=spf1 record against the client IP.
func checkSPF(ctx context.Context, res InboundTXTResolver, from, clientIP string) string {
	domain := domainOf(from)
	ip, err := netip.ParseAddr(clientIP)

	if domain == "" || domain == strs.Fold(from) || err != nil {
		return verdictGray
	}

	lookups := 0

	return spfEval(ctx, res, domain, ip.Unmap(), &lookups)
}

func spfEval(ctx context.Context, res InboundTXTResolver, domain string, ip netip.Addr, lookups *int) string {
	recs := txtRecords(ctx, res, domain, "v=spf1")
	if len(recs) != 1 {
		if len(recs) > 1 {
			return verdictProcessingFailed
		}

		return verdictGray
	}

	terms := strings.Fields(recs[0])[1:]
	redirect := ""

	for _, t := range terms {
		if k, v, ok := strings.Cut(t, "="); ok && strs.Equal(k, "redirect") {
			redirect = v

			continue
		}

		if out, matched := spfTerm(ctx, res, t, ip, lookups); matched {
			return out
		}
	}

	if redirect != "" && *lookups < maxSPFLookups {
		*lookups++

		return spfEval(ctx, res, strs.Fold(redirect), ip, lookups)
	}

	return verdictGray
}

// spfTerm evaluates one mechanism; matched reports whether it decided the result.
func spfTerm(ctx context.Context, res InboundTXTResolver, term string, ip netip.Addr, lookups *int) (string, bool) {
	result := map[byte]string{'+': verdictPass, '-': verdictFail, '~': verdictFail, '?': verdictGray}

	q := byte('+')
	if _, ok := result[term[0]]; ok {
		q, term = term[0], term[1:]
	}

	name, arg, _ := strings.Cut(term, ":")
	if cidr, ok := strings.CutPrefix(term, "ip4:"); ok {
		arg, name = cidr, "ip4"
	} else if cidr, ok = strings.CutPrefix(term, "ip6:"); ok {
		arg, name = cidr, "ip6"
	}

	switch strs.Fold(name) {
	case "all":
		return result[q], true
	case "ip4", "ip6":
		return result[q], spfIPMatch(arg, ip)
	case "include":
		if *lookups >= maxSPFLookups {
			return verdictProcessingFailed, true
		}

		*lookups++

		if spfEval(ctx, res, strs.Fold(arg), ip, lookups) == verdictPass {
			return result[q], true
		}
	}

	return "", false
}

func spfIPMatch(cidr string, ip netip.Addr) bool {
	if pfx, err := netip.ParsePrefix(cidr); err == nil {
		return pfx.Contains(ip)
	}

	a, err := netip.ParseAddr(cidr)

	return err == nil && a.Unmap() == ip
}

// fromDomain returns the RFC5322.From domain, or "".
func fromDomain(data []byte) string {
	for _, h := range parseHeaderBlock(data) {
		if !strs.Equal(h.Name, "from") {
			continue
		}

		list, err := mail.ParseAddressList(h.Value)
		if err != nil || len(list) == 0 {
			return ""
		}

		return domainOf(list[0].Address)
	}

	return ""
}

// orgDomain approximates the organizational domain without a public suffix list.
func orgDomain(d string) string {
	labels := strings.Split(d, ".")
	if len(labels) <= orgLabels {
		return d
	}

	n := orgLabels

	switch labels[len(labels)-2] {
	case "co", "com", "org", "net", "gov", "ac", "edu":
		if len(labels[len(labels)-1]) == orgLabels {
			n = orgLabels + 1
		}
	}

	return strings.Join(labels[len(labels)-n:], ".")
}

func aligned(a, b string, strict bool) bool {
	if a == "" || b == "" {
		return false
	}

	if strict {
		return a == b
	}

	return orgDomain(a) == orgDomain(b)
}

func parseTags(rec string) map[string]string {
	out := map[string]string{}

	for part := range strings.SplitSeq(rec, ";") {
		if k, v, ok := strings.Cut(part, "="); ok {
			out[strs.Fold(strings.TrimSpace(k))] = strings.TrimSpace(v)
		}
	}

	return out
}

func dmarcRecord(ctx context.Context, res InboundTXTResolver, domain string) (map[string]string, bool) {
	for _, d := range []string{domain, orgDomain(domain)} {
		if recs := txtRecords(ctx, res, "_dmarc."+d, "v=dmarc1"); len(recs) == 1 {
			return parseTags(recs[0]), true
		}
	}

	return nil, false
}

// checkDMARC applies identifier alignment of the SPF and DKIM results to the From domain.
func checkDMARC(
	ctx context.Context, res InboundTXTResolver, env inboundEnvelope, spfRes string, dkimDomains []string,
) (string, string) {
	from := fromDomain(env.data)
	if from == "" {
		return verdictGray, ""
	}

	tags, ok := dmarcRecord(ctx, res, from)
	if !ok {
		return verdictGray, ""
	}

	policy := strs.Fold(tags["p"])
	if sp, has := tags["sp"]; has && from != orgDomain(from) {
		policy = strs.Fold(sp)
	}

	pass := spfRes == verdictPass && aligned(from, domainOf(env.from), strs.Equal(tags["aspf"], "s"))

	for _, d := range dkimDomains {
		pass = pass || aligned(from, d, strs.Equal(tags["adkim"], "s"))
	}

	if pass {
		return verdictPass, policy
	}

	return verdictFail, policy
}
