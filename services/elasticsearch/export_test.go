package elasticsearch

// DomainCount returns the number of domains stored in the backend.
// Used only in tests to verify backend state without going through the HTTP handler.
func (b *InMemoryBackend) DomainCount() int {
	b.mu.RLock("DomainCount")
	defer b.mu.RUnlock()

	return len(b.domains)
}

// PackageCount returns the number of packages stored in the backend.
func (b *InMemoryBackend) PackageCount() int {
	b.mu.RLock("PackageCount")
	defer b.mu.RUnlock()

	return len(b.packages)
}

// InboundConnectionCount returns the number of inbound cross-cluster connections.
func (b *InMemoryBackend) InboundConnectionCount() int {
	b.mu.RLock("InboundConnectionCount")
	defer b.mu.RUnlock()

	return len(b.inboundConnections)
}

// OutboundConnectionCount returns the number of outbound cross-cluster connections.
func (b *InMemoryBackend) OutboundConnectionCount() int {
	b.mu.RLock("OutboundConnectionCount")
	defer b.mu.RUnlock()

	return len(b.outboundConnections)
}

// VpcEndpointCount returns the number of VPC endpoints stored in the backend.
func (b *InMemoryBackend) VpcEndpointCount() int {
	b.mu.RLock("VpcEndpointCount")
	defer b.mu.RUnlock()

	return len(b.vpcEndpoints)
}
