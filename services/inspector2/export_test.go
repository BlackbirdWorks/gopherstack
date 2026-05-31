package inspector2

// FilterCount returns the number of stored filters.
func FilterCount(b *InMemoryBackend) int {
	b.mu.RLock("FilterCount")
	defer b.mu.RUnlock()

	return len(b.filters)
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
