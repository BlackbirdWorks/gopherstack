package route53

// ZoneCount returns the number of hosted zones in the backend for tests.
func ZoneCount(b *InMemoryBackend) int {
	b.mu.RLock("ZoneCount")
	defer b.mu.RUnlock()

	return len(b.zones)
}

// HealthCheckCount returns the number of health checks in the backend for tests.
func HealthCheckCount(b *InMemoryBackend) int {
	b.mu.RLock("HealthCheckCount")
	defer b.mu.RUnlock()

	return len(b.healthChecks)
}

// KeySigningKeyCount returns the number of KSKs in the backend for tests.
func KeySigningKeyCount(b *InMemoryBackend) int {
	b.mu.RLock("KeySigningKeyCount")
	defer b.mu.RUnlock()

	return len(b.keySigningKeys)
}

// CidrCollectionCount returns the number of CIDR collections for tests.
func CidrCollectionCount(b *InMemoryBackend) int {
	b.mu.RLock("CidrCollectionCount")
	defer b.mu.RUnlock()

	return len(b.cidrCollections)
}

// QueryLoggingConfigCount returns the number of query logging configs for tests.
func QueryLoggingConfigCount(b *InMemoryBackend) int {
	b.mu.RLock("QueryLoggingConfigCount")
	defer b.mu.RUnlock()

	return len(b.queryLoggingConfigs)
}

// DelegationSetCount returns the number of reusable delegation sets for tests.
func DelegationSetCount(b *InMemoryBackend) int {
	b.mu.RLock("DelegationSetCount")
	defer b.mu.RUnlock()

	return len(b.reusableDelegationSets)
}

// TrafficPolicyCount returns the number of traffic policies for tests.
func TrafficPolicyCount(b *InMemoryBackend) int {
	b.mu.RLock("TrafficPolicyCount")
	defer b.mu.RUnlock()

	return len(b.trafficPolicies)
}

// TrafficPolicyInstanceCount returns the number of traffic policy instances for tests.
func TrafficPolicyInstanceCount(b *InMemoryBackend) int {
	b.mu.RLock("TrafficPolicyInstanceCount")
	defer b.mu.RUnlock()

	return len(b.trafficPolicyInstances)
}

// VPCAssociationCount returns the total number of VPC associations across all zones for tests.
func VPCAssociationCount(b *InMemoryBackend) int {
	b.mu.RLock("VPCAssociationCount")
	defer b.mu.RUnlock()

	total := 0
	for _, assocs := range b.vpcAssociations {
		total += len(assocs)
	}

	return total
}

// HandlerOpsLen returns the number of supported operations for tests.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// TagResourceCount returns the number of resource IDs that currently have
// handler-level tags. Used to verify tags are released on resource delete.
func TagResourceCount(h *Handler) int {
	h.tagsMu.RLock("TagResourceCount")
	defer h.tagsMu.RUnlock()

	return len(h.tags)
}
