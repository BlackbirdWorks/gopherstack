package elasticsearch

// DomainCount returns the total number of domains stored across all regions.
// Used only in tests to verify backend state without going through the HTTP handler.
func (b *InMemoryBackend) DomainCount() int {
	b.mu.RLock("DomainCount")
	defer b.mu.RUnlock()

	count := 0
	for _, regionDomains := range b.domains {
		count += len(regionDomains)
	}

	return count
}

// PackageCount returns the total number of packages stored across all regions.
func (b *InMemoryBackend) PackageCount() int {
	b.mu.RLock("PackageCount")
	defer b.mu.RUnlock()

	count := 0
	for _, regionPackages := range b.packages {
		count += len(regionPackages)
	}

	return count
}

// InboundConnectionCount returns the total number of inbound cross-cluster connections across all regions.
func (b *InMemoryBackend) InboundConnectionCount() int {
	b.mu.RLock("InboundConnectionCount")
	defer b.mu.RUnlock()

	count := 0
	for _, regionConns := range b.inboundConnections {
		count += len(regionConns)
	}

	return count
}

// OutboundConnectionCount returns the total number of outbound cross-cluster connections across all regions.
func (b *InMemoryBackend) OutboundConnectionCount() int {
	b.mu.RLock("OutboundConnectionCount")
	defer b.mu.RUnlock()

	count := 0
	for _, regionConns := range b.outboundConnections {
		count += len(regionConns)
	}

	return count
}

// VpcEndpointCount returns the total number of VPC endpoints stored across all regions.
func (b *InMemoryBackend) VpcEndpointCount() int {
	b.mu.RLock("VpcEndpointCount")
	defer b.mu.RUnlock()

	count := 0
	for _, regionEndpoints := range b.vpcEndpoints {
		count += len(regionEndpoints)
	}

	return count
}
