package athena

import "time"

// SetQueryExecutionState overrides a query execution's state and completion time.
// If completionDelay is negative the completion time is set to now plus the delay (i.e. in the past).
// Used only in tests.
func (b *InMemoryBackend) SetQueryExecutionState(id, state string, completionDelay time.Duration) {
	b.mu.Lock("SetQueryExecutionState")
	defer b.mu.Unlock()

	qe, ok := b.queryExecutions[id]
	if !ok {
		return
	}

	completionTime := time.Now().Add(completionDelay)
	qe.Status.State = state
	qe.Status.CompletionDateTime = float64(completionTime.UnixMilli()) / millisToSeconds
}

// QueryExecutionCount returns the number of query executions stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) QueryExecutionCount() int {
	b.mu.RLock("QueryExecutionCount")
	defer b.mu.RUnlock()

	return len(b.queryExecutions)
}
