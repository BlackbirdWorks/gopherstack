package cloudfront

import (
	"slices"
	"strings"
)

func (b *InMemoryBackend) resolvePolicy(beh *dpBehavior) dpPolicy {
	var p dpPolicy

	if beh.CachePolicyID == "" {
		p = legacyPolicy(beh)
	} else if cp, err := b.GetCachePolicy(beh.CachePolicyID); err == nil {
		p = policyFromCachePolicy(cp)
	} else {
		p = legacyPolicy(beh)
	}

	if beh.OriginRequestPolicyID != "" {
		if orp, err := b.GetOriginRequestPolicy(beh.OriginRequestPolicyID); err == nil {
			applyOriginRequestPolicy(&p, orp)
		}
	}

	return p
}

func policyFromCachePolicy(cp *CachePolicy) dpPolicy {
	p := dpPolicy{ttl: dpTTL{min: cp.MinTTL, def: cp.DefaultTTL, max: cp.MaxTTL}}
	if cp.Params == nil {
		return p
	}

	pr := cp.Params
	p.cacheHeaders = selector{behavior: lower(pr.HeadersConfig.HeaderBehavior), names: pr.HeadersConfig.Headers}
	p.cacheCookies = selector{behavior: lower(pr.CookiesConfig.CookieBehavior), names: pr.CookiesConfig.Cookies}
	p.cacheQuery = selector{
		behavior: lower(pr.QueryStringsConfig.QueryStringBehavior),
		names:    pr.QueryStringsConfig.QueryStrings,
	}
	p.gzip = pr.EnableAcceptEncodingGzip || pr.EnableAcceptEncodingBrotli

	return p
}

func applyOriginRequestPolicy(p *dpPolicy, orp *OriginRequestPolicy) {
	if h := orp.HeadersConfig; h != nil {
		p.originHeaders = selector{behavior: lower(h.HeaderBehavior), names: h.Headers}
		p.cloudFrontHdr = p.originHeaders.behavior == selAllViewerC
	}

	if c := orp.CookiesConfig; c != nil {
		p.originCookies = selector{behavior: lower(c.CookieBehavior), names: c.Cookies}
	}

	if q := orp.QueryStringsConfig; q != nil {
		p.originQuery = selector{behavior: lower(q.QueryStringBehavior), names: q.QueryStrings}
	}
}

// rawConfigHasHost cheaply pre-filters distributions before a full alias parse.
func rawConfigHasHost(raw []byte, host string) bool {
	return strings.Contains(strings.ToLower(string(raw)), host)
}

// DistributionByHost finds the distribution addressed by a viewer Host header.
func (b *InMemoryBackend) DistributionByHost(host string) *Distribution {
	host = strings.ToLower(host)

	b.mu.RLock("DistributionByHost")
	defer b.mu.RUnlock()

	for _, d := range b.distributions.All() {
		if strings.EqualFold(d.DomainName, host) {
			return b.copyDistribution(d)
		}
	}

	for _, d := range b.distributions.All() {
		if !rawConfigHasHost(d.RawConfig, host) {
			continue
		}

		if slices.Contains(rawConfigAliases(d.RawConfig), host) {
			return b.copyDistribution(d)
		}
	}

	return nil
}

// DistributionByIDOrDomain resolves the path-style route identifier.
func (b *InMemoryBackend) DistributionByIDOrDomain(ref string) *Distribution {
	b.mu.RLock("DistributionByIDOrDomain")
	defer b.mu.RUnlock()

	for _, d := range b.distributions.All() {
		if strings.EqualFold(d.ID, ref) || strings.EqualFold(d.DomainName, ref) {
			return b.copyDistribution(d)
		}
	}

	return nil
}
