package datasync

// AgentCount returns the number of stored agents.
func AgentCount(b *InMemoryBackend) int {
	b.mu.RLock("AgentCount")
	defer b.mu.RUnlock()

	return b.agents.Len()
}

// LocationCount returns the number of stored locations.
func LocationCount(b *InMemoryBackend) int {
	b.mu.RLock("LocationCount")
	defer b.mu.RUnlock()

	return b.locations.Len()
}

// TaskCount returns the number of stored tasks.
func TaskCount(b *InMemoryBackend) int {
	b.mu.RLock("TaskCount")
	defer b.mu.RUnlock()

	return b.tasks.Len()
}

// ExecutionCount returns the number of stored task executions.
func ExecutionCount(b *InMemoryBackend) int {
	b.mu.RLock("ExecutionCount")
	defer b.mu.RUnlock()

	return b.executions.Len()
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
