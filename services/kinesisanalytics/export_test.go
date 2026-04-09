package kinesisanalytics

// ApplicationCount returns the number of applications stored in the backend.
// This is exported for use in tests only.
func ApplicationCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.apps)
}

// HandlerOpsLen returns the number of operations pre-built in the handler dispatch map.
// This is exported for use in tests only.
func HandlerOpsLen(h *Handler) int {
	return len(h.ops)
}
