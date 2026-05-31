package datasync

// AgentCount returns the number of stored agents.
func AgentCount(b *InMemoryBackend) int {
	b.mu.RLock("AgentCount")
	defer b.mu.RUnlock()

	return len(b.agents)
}

// LocationCount returns the number of stored locations.
func LocationCount(b *InMemoryBackend) int {
	b.mu.RLock("LocationCount")
	defer b.mu.RUnlock()

	return len(b.locations)
}

// TaskCount returns the number of stored tasks.
func TaskCount(b *InMemoryBackend) int {
	b.mu.RLock("TaskCount")
	defer b.mu.RUnlock()

	return len(b.tasks)
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
