package firehose

// StreamCount returns the number of streams in the backend (for white-box testing).
func StreamCount(b *InMemoryBackend) int {
	b.mu.RLock("StreamCount")
	defer b.mu.RUnlock()

	return len(b.streams)
}

// HandlerOpsLen returns the number of pre-built dispatch operations.
func HandlerOpsLen(h *Handler) int {
	return len(h.ops)
}
