package detective

import "time"

// SeedGraph inserts a synthetic graph ARN directly into the backend, bypassing the
// one-graph-per-account constraint of CreateGraph. Used only in tests that need to
// exercise multi-graph pagination.
func SeedGraph(b *InMemoryBackend, arnStr string) {
	b.mu.Lock("SeedGraph")
	defer b.mu.Unlock()
	b.graphs[arnStr] = &storedGraph{Arn: arnStr, CreatedTime: time.Now().UTC()}
}

// GraphCount returns the number of stored behavior graphs.
func GraphCount(b *InMemoryBackend) int {
	b.mu.RLock("GraphCount")
	defer b.mu.RUnlock()

	return len(b.graphs)
}

// MemberCount returns the number of members in a graph.
func MemberCount(b *InMemoryBackend, graphARN string) int {
	b.mu.RLock("MemberCount")
	defer b.mu.RUnlock()

	return len(b.members[graphARN])
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
