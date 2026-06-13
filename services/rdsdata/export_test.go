package rdsdata

// ExecutedStatementCount returns the number of executed statements stored in the backend's default region.
func ExecutedStatementCount(b *InMemoryBackend) int {
	b.mu.RLock("ExecutedStatementCount")
	defer b.mu.RUnlock()

	return len(b.executedStatements[b.defaultRegion])
}

// TransactionCount returns the number of active transactions in the backend's default region.
func TransactionCount(b *InMemoryBackend) int {
	b.mu.RLock("TransactionCount")
	defer b.mu.RUnlock()

	return len(b.transactions[b.defaultRegion])
}

// HandlerOpsLen returns the number of operations in GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
