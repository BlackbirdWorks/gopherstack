package waf

// GetSampledRequests returns an empty sample (stub).
func (b *InMemoryBackend) GetSampledRequests(_, _ string, _ int64) []SampledHTTPRequest {
	return []SampledHTTPRequest{}
}
