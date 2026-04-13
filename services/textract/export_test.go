package textract

// NewInMemoryBackendWithCap creates a backend with a custom job history cap for testing.
func NewInMemoryBackendWithCap(maxJobs int) *InMemoryBackend {
	b := NewInMemoryBackend()
	b.maxJobs = maxJobs

	return b
}

// AdapterCount returns the number of adapters stored in the backend (for testing).
func AdapterCount(b *InMemoryBackend) int {
	b.mu.RLock("GetAdapter")
	defer b.mu.RUnlock()

	return len(b.adapters)
}

// AdapterVersionCount returns the number of adapter versions stored in the backend (for testing).
func AdapterVersionCount(b *InMemoryBackend) int {
	b.mu.RLock("GetAdapterVersion")
	defer b.mu.RUnlock()

	return len(b.adapterVersions)
}

// HandlerOpsLen returns the number of operations in the handler's dispatch table.
func HandlerOpsLen(h *Handler) int {
	return len(h.dispatchTable())
}
