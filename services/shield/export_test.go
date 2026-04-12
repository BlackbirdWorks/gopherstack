package shield

// ProtectionCount returns the number of stored protections.
func ProtectionCount(b *InMemoryBackend) int {
	b.mu.RLock("ProtectionCount")
	defer b.mu.RUnlock()

	return len(b.protections)
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
