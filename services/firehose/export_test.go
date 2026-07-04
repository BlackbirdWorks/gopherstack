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

// StreamFailedRecords returns the FailedRecords delivery metric for a stream, or -1 when
// the stream does not exist (for white-box testing).
func StreamFailedRecords(b *InMemoryBackend, region, name string) int64 {
	b.mu.RLock("StreamFailedRecords")
	defer b.mu.RUnlock()

	streams, ok := b.streams[region]
	if !ok {
		return -1
	}

	s, ok := streams[name]
	if !ok {
		return -1
	}

	return s.Metrics.FailedRecords
}

// PendingFlushCount returns the number of streams flagged for interval-flush inspection
// across all regions (for white-box testing of the flush watch set).
func PendingFlushCount(b *InMemoryBackend) int {
	b.mu.RLock("PendingFlushCount")
	defer b.mu.RUnlock()

	total := 0
	for _, set := range b.pendingFlush {
		total += len(set)
	}

	return total
}
