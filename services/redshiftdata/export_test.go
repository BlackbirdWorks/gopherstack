package redshiftdata

// MaxStatementHistoryForTest exposes the maxStatementHistory cap constant for use in tests.
const MaxStatementHistoryForTest = maxStatementHistory

// StatementCount returns the number of statements currently stored.
// Used only in tests.
func (b *InMemoryBackend) StatementCount() int {
	b.mu.RLock("StatementCount")
	defer b.mu.RUnlock()

	return len(b.statements)
}
