package timestreamquery

// ExportBackend returns the InMemoryBackend from a Handler for test use.
func ExportBackend(h *Handler) *InMemoryBackend {
	return h.Backend.(*InMemoryBackend)
}

// ScheduledQueryCount returns the total number of scheduled queries across all regions.
// This is exported for use in tests only.
func ScheduledQueryCount(b *InMemoryBackend) int {
	b.mu.RLock("ScheduledQueryCount")
	defer b.mu.RUnlock()

	return b.scheduledQueries.Len()
}

// QueryCount returns the number of active query results stored in the backend.
// This is exported for use in tests only.
func QueryCount(b *InMemoryBackend) int {
	b.mu.RLock("QueryCount")
	defer b.mu.RUnlock()

	return b.queries.Len()
}

// HandlerOpsLen returns the number of operations pre-built in the handler's supported ops set.
// This is exported for use in tests only.
func HandlerOpsLen(h *Handler) int {
	return len(h.supportedOps)
}

// MaxRetainedQueries exposes the cap for tests.
const MaxRetainedQueries = maxRetainedQueries
