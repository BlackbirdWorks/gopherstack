package secretsmanager

import "time"

// SecretCount returns the number of secrets in the backend.
func SecretCount(b *InMemoryBackend) int {
	b.mu.RLock("SecretCount")
	defer b.mu.RUnlock()

	return len(b.secrets)
}

// ResourcePolicyCount returns the number of resource policies in the backend.
func ResourcePolicyCount(b *InMemoryBackend) int {
	b.mu.RLock("ResourcePolicyCount")
	defer b.mu.RUnlock()

	return len(b.resourcePolicies)
}

// ReplicationConfigCount returns the number of replication configs in the backend.
func ReplicationConfigCount(b *InMemoryBackend) int {
	b.mu.RLock("ReplicationConfigCount")
	defer b.mu.RUnlock()

	return len(b.replicationConfigs)
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
