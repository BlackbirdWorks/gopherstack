package dlm

// PolicyCount returns the number of stored lifecycle policies.
func PolicyCount(b *InMemoryBackend) int {
	b.mu.RLock("PolicyCount")
	defer b.mu.RUnlock()

	return b.policies.Len()
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// ManagedCount returns the number of resources DLM tracks.
func ManagedCount(b *InMemoryBackend) int {
	b.mu.RLock("ManagedCount")
	defer b.mu.RUnlock()

	return len(b.managed.All())
}
