package secretsmanager

import "time"

// SecretCount returns the total number of secrets in the backend across all regions.
func SecretCount(b *InMemoryBackend) int {
	b.mu.RLock("SecretCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionSecrets := range b.secrets {
		total += len(regionSecrets)
	}

	return total
}

// ResourcePolicyCount returns the total number of resource policies across all regions.
func ResourcePolicyCount(b *InMemoryBackend) int {
	b.mu.RLock("ResourcePolicyCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionPolicies := range b.resourcePolicies {
		total += len(regionPolicies)
	}

	return total
}

// ReplicationConfigCount returns the total number of replication configs across all regions.
func ReplicationConfigCount(b *InMemoryBackend) int {
	b.mu.RLock("ReplicationConfigCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionConfigs := range b.replicationConfigs {
		total += len(regionConfigs)
	}

	return total
}

// HandlerOpsLen returns the number of operations registered in the handler dispatch table.
func HandlerOpsLen(h *Handler) int {
	return len(h.ops)
}

// NextCronTime exposes nextCronTime for white-box testing.
func NextCronTime(expr string, from time.Time) (time.Time, bool) {
	return nextCronTime(expr, from)
}

// RotationDue exposes rotationDue for white-box testing.
func RotationDue(rules *RotationRulesType, now time.Time, base *float64) bool {
	return rotationDue(rules, now, base)
}

// RunScheduledRotationsForTest exposes runScheduledRotations for deterministic testing.
// Pass a time far enough in the future to ensure all due rotations fire.
func (b *InMemoryBackend) RunScheduledRotationsForTest(now time.Time) {
	b.runScheduledRotations(now)
}
