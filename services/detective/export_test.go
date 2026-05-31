package detective

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
