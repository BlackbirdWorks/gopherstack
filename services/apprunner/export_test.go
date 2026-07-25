package apprunner

// ServiceCount returns the number of stored services.
func ServiceCount(b *InMemoryBackend) int {
	b.mu.RLock("ServiceCount")
	defer b.mu.RUnlock()

	return b.services.Len()
}

// AutoScalingConfigCount returns the number of stored auto scaling configs.
func AutoScalingConfigCount(b *InMemoryBackend) int {
	b.mu.RLock("AutoScalingConfigCount")
	defer b.mu.RUnlock()

	return b.autoScalingConfigs.Len()
}

// ConnectionCount returns the number of stored connections.
func ConnectionCount(b *InMemoryBackend) int {
	b.mu.RLock("ConnectionCount")
	defer b.mu.RUnlock()

	return b.connections.Len()
}

// ObservabilityConfigCount returns the number of stored observability configs.
func ObservabilityConfigCount(b *InMemoryBackend) int {
	b.mu.RLock("ObservabilityConfigCount")
	defer b.mu.RUnlock()

	return b.observabilityConfigs.Len()
}

// VpcConnectorCount returns the number of stored VPC connectors.
func VpcConnectorCount(b *InMemoryBackend) int {
	b.mu.RLock("VpcConnectorCount")
	defer b.mu.RUnlock()

	return b.vpcConnectors.Len()
}

// VpcIngressConnectionCount returns the number of stored VPC ingress connections.
func VpcIngressConnectionCount(b *InMemoryBackend) int {
	b.mu.RLock("VpcIngressConnectionCount")
	defer b.mu.RUnlock()

	return b.vpcIngressConnections.Len()
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// ServiceOperationStats returns the length and capacity of a service's operation
// history slice, for leak-detection tests.
func ServiceOperationStats(b *InMemoryBackend, serviceArn string) (int, int) {
	b.mu.RLock("ServiceOperationStats")
	defer b.mu.RUnlock()

	svc, ok := b.services.Get(serviceArn)
	if !ok {
		return 0, 0
	}

	return len(svc.Operations), cap(svc.Operations)
}

// MaxOperationsPerService exposes the per-service operation cap for tests.
const MaxOperationsPerService = maxOperationsPerService

// CustomDomainMapEntries returns the number of serviceArn keys present in the
// backend's raw customDomains map, for leak-detection tests verifying
// DeleteService cascades its custom domain associations rather than leaving
// a ghost, permanently-unreachable entry behind.
func CustomDomainMapEntries(b *InMemoryBackend) int {
	b.mu.RLock("CustomDomainMapEntries")
	defer b.mu.RUnlock()

	return len(b.customDomains)
}
