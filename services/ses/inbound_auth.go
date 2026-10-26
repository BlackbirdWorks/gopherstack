package ses

import (
	"context"
	"net/mail"
	"strings"

	"golang.org/x/net/publicsuffix"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	verdictFail             = "FAIL"
	verdictProcessingFailed = "PROCESSING_FAILED"
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

// orgDomain is the RFC 7489 organizational domain: the public suffix plus one label.
func orgDomain(d string) string {
	if org, err := publicsuffix.EffectiveTLDPlusOne(d); err == nil {
		return org
	}

	return d
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
