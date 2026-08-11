package waf

// GetSampledRequests validates WebAclId against real backend state and
// returns WAFNonexistentItemException for an unknown WebACL, matching real
// AWS. The sample itself stays empty: gopherstack does not proxy real HTTP
// traffic through WAF rule evaluation, so there is no request data to sample.
func (b *InMemoryBackend) GetSampledRequests(webACLID, _ string, _ int64) ([]SampledHTTPRequest, error) {
	b.mu.RLock("GetSampledRequests")
	defer b.mu.RUnlock()

	if !b.webACLs.Has(webACLID) {
		return nil, ErrNotFound
	}

	return []SampledHTTPRequest{}, nil
}
