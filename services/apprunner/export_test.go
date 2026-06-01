package apprunner

// ServiceCount returns the number of stored services.
func ServiceCount(b *InMemoryBackend) int {
	b.mu.RLock("ServiceCount")
	defer b.mu.RUnlock()

	return len(b.services)
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
