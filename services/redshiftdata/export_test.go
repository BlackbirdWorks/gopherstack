package redshiftdata

import "time"

// MaxStatementHistoryForTest exposes the maxStatementHistory cap constant for use in tests.
const MaxStatementHistoryForTest = maxStatementHistory

// StatementCount returns the total number of statements stored across all regions.
// Used only in tests.
func (b *InMemoryBackend) StatementCount() int {
	b.mu.RLock("StatementCount")
	defer b.mu.RUnlock()

	total := 0

	for _, store := range b.stores {
		total += len(store.statements)
	}

	return total
}

// HandlerOpsLen returns the number of operations in GetSupportedOperations.
// Used only in tests.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// AddStatementInternal inserts a pre-built statement directly into the backend
// for the given region, bypassing validation and UUID generation. Used only to seed test fixtures.
func AddStatementInternal(b *InMemoryBackend, region, id, sql, database, status string, hasResultSet bool) *Statement {
	b.mu.Lock("AddStatementInternal")
	defer b.mu.Unlock()

	now := time.Now()
	stmt := &Statement{
		ID:           id,
		QueryString:  sql,
		Database:     database,
		Status:       status,
		HasResultSet: hasResultSet,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	b.storeFor(region).addStatement(stmt)

	return cloneStatement(stmt)
}
