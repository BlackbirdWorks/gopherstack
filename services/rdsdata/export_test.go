package rdsdata

// ExecutedStatementCount returns the number of executed statements stored in the backend.
func ExecutedStatementCount(b *InMemoryBackend) int {
	b.mu.RLock("ExecutedStatementCount")
	defer b.mu.RUnlock()

	return len(b.executedStatements)
}

// TransactionCount returns the number of active transactions in the backend.
func TransactionCount(b *InMemoryBackend) int {
	b.mu.RLock("TransactionCount")
	defer b.mu.RUnlock()

	return len(b.transactions)
}

// HandlerOpsLen returns the number of operations in GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
