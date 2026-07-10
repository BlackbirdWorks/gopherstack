package elasticsearch

import "context"

// WithRegionForTest returns a context carrying region as the per-request AWS
// region, the same way getRegion (backend.go) resolves it from a real
// request. Used only in tests that need to exercise more than one region
// from the elasticsearch_test (external) package, which cannot see
// regionContextKey.
func WithRegionForTest(ctx context.Context, region string) context.Context {
	return context.WithValue(ctx, regionContextKey{}, region)
}

// DomainCount returns the total number of domains stored across all regions.
// Used only in tests to verify backend state without going through the HTTP handler.
func (b *InMemoryBackend) DomainCount() int {
	b.mu.RLock("DomainCount")
	defer b.mu.RUnlock()

	return b.domains.Len()
}

// PackageCount returns the total number of packages stored across all regions.
func (b *InMemoryBackend) PackageCount() int {
	b.mu.RLock("PackageCount")
	defer b.mu.RUnlock()

	return b.packages.Len()
}

// InboundConnectionCount returns the total number of inbound cross-cluster connections across all regions.
func (b *InMemoryBackend) InboundConnectionCount() int {
	b.mu.RLock("InboundConnectionCount")
	defer b.mu.RUnlock()

	return b.inboundConnections.Len()
}

// OutboundConnectionCount returns the total number of outbound cross-cluster connections across all regions.
func (b *InMemoryBackend) OutboundConnectionCount() int {
	b.mu.RLock("OutboundConnectionCount")
	defer b.mu.RUnlock()

	return b.outboundConnections.Len()
}

// VpcEndpointCount returns the total number of VPC endpoints stored across all regions.
func (b *InMemoryBackend) VpcEndpointCount() int {
	b.mu.RLock("VpcEndpointCount")
	defer b.mu.RUnlock()

	return b.vpcEndpoints.Len()
}
