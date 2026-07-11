package route53

// ZoneCount returns the number of hosted zones in the backend for tests.
func ZoneCount(b *InMemoryBackend) int {
	b.mu.RLock("ZoneCount")
	defer b.mu.RUnlock()

	return b.zones.Len()
}

// HealthCheckCount returns the number of health checks in the backend for tests.
func HealthCheckCount(b *InMemoryBackend) int {
	b.mu.RLock("HealthCheckCount")
	defer b.mu.RUnlock()

	return b.healthChecks.Len()
}

// KeySigningKeyCount returns the number of KSKs in the backend for tests.
func KeySigningKeyCount(b *InMemoryBackend) int {
	b.mu.RLock("KeySigningKeyCount")
	defer b.mu.RUnlock()

	return b.keySigningKeys.Len()
}

// CidrCollectionCount returns the number of CIDR collections for tests.
func CidrCollectionCount(b *InMemoryBackend) int {
	b.mu.RLock("CidrCollectionCount")
	defer b.mu.RUnlock()

	return b.cidrCollections.Len()
}

// QueryLoggingConfigCount returns the number of query logging configs for tests.
func QueryLoggingConfigCount(b *InMemoryBackend) int {
	b.mu.RLock("QueryLoggingConfigCount")
	defer b.mu.RUnlock()

	return b.queryLoggingConfigs.Len()
}

// DelegationSetCount returns the number of reusable delegation sets for tests.
func DelegationSetCount(b *InMemoryBackend) int {
	b.mu.RLock("DelegationSetCount")
	defer b.mu.RUnlock()

	return b.reusableDelegationSets.Len()
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

	return b.trafficPolicyInstances.Len()
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
	b, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return 0
	}
	b.mu.RLock("TagResourceCount")
	defer b.mu.RUnlock()

	return len(b.tags)
}
