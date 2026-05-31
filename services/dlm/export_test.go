package dlm

// PolicyCount returns the number of stored lifecycle policies.
func PolicyCount(b *InMemoryBackend) int {
	b.mu.RLock("PolicyCount")
	defer b.mu.RUnlock()

	return len(b.policies)
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
