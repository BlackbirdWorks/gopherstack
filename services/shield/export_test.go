package shield

// ProtectionCount returns the number of stored protections.
func ProtectionCount(b *InMemoryBackend) int {
	b.mu.RLock("ProtectionCount")
	defer b.mu.RUnlock()

	return len(b.protections)
}

// ProtectionGroupCount returns the number of stored protection groups.
func ProtectionGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("ProtectionGroupCount")
	defer b.mu.RUnlock()

	return len(b.protectionGroups)
}

// AttackCount returns the number of stored attacks.
func AttackCount(b *InMemoryBackend) int {
	b.mu.RLock("AttackCount")
	defer b.mu.RUnlock()

	return len(b.attacks)
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
