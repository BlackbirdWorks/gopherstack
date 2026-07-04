package athena

import "time"

// DefaultJanitorInterval exposes the package default janitor interval for testing.
const DefaultJanitorInterval = defaultAthenaJanitorInterval

// DefaultExecutionTTL exposes the package default execution TTL for testing.
const DefaultExecutionTTL = defaultAthenaExecutionTTL

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

// GetJanitorTaskTimeout returns the TaskTimeout configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the timeout.
func (h *Handler) GetJanitorTaskTimeout() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.TaskTimeout
}

// GetJanitorExecutionTTL returns the ExecutionTTL configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the TTL.
func (h *Handler) GetJanitorExecutionTTL() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.ExecutionTTL
}

// SetCalculationState overrides a calculation's state for tests.
func (b *InMemoryBackend) SetCalculationState(id, state string) {
	b.mu.Lock("SetCalculationState")
	defer b.mu.Unlock()

	c, ok := b.calculations[id]
	if !ok {
		return
	}

	c.Status.State = state
}

// QueryResultCount returns the number of cached query result sets. Test-only.
func (b *InMemoryBackend) QueryResultCount() int {
	b.mu.RLock("QueryResultCount")
	defer b.mu.RUnlock()

	return len(b.queryResults)
}

// SessionCount returns the number of stored sessions. Test-only.
func (b *InMemoryBackend) SessionCount() int {
	b.mu.RLock("SessionCount")
	defer b.mu.RUnlock()

	return len(b.sessions)
}

// CalculationCount returns the number of stored calculations. Test-only.
func (b *InMemoryBackend) CalculationCount() int {
	b.mu.RLock("CalculationCount")
	defer b.mu.RUnlock()

	return len(b.calculations)
}

// SetSessionTerminated marks a session terminated with an end time offset by
// completionDelay (negative places it in the past). Test-only.
func (b *InMemoryBackend) SetSessionTerminated(id string, completionDelay time.Duration) {
	b.mu.Lock("SetSessionTerminated")
	defer b.mu.Unlock()

	s, ok := b.sessions[id]
	if !ok {
		return
	}

	end := float64(time.Now().Add(completionDelay).UnixMilli()) / millisToSeconds
	s.Status.State = sessionStateTerminated
	s.Status.EndDateTime = end
	s.Status.LastModifiedDateTime = end
}

// SetCalculationCompletion overrides a calculation's state and completion time
// (completionDelay negative places it in the past). Test-only.
func (b *InMemoryBackend) SetCalculationCompletion(id, state string, completionDelay time.Duration) {
	b.mu.Lock("SetCalculationCompletion")
	defer b.mu.Unlock()

	c, ok := b.calculations[id]
	if !ok {
		return
	}

	c.Status.State = state
	c.Status.CompletionDateTime = float64(time.Now().Add(completionDelay).UnixMilli()) / millisToSeconds
}

// TableRowCount returns the number of data rows stored for a table. Test-only.
func (b *InMemoryBackend) TableRowCount(catalog, database, table string) int {
	b.mu.RLock("TableRowCount")
	defer b.mu.RUnlock()

	return len(b.tableData[catalog+"/"+database+"/"+table])
}

// HasTable reports whether the named table exists in the catalog. Test-only.
func (b *InMemoryBackend) HasTable(catalog, database, table string) bool {
	b.mu.RLock("HasTable")
	defer b.mu.RUnlock()

	_, ok := b.tables[catalog+"/"+database][table]

	return ok
}

// HasDatabase reports whether the named database exists in the catalog. Test-only.
func (b *InMemoryBackend) HasDatabase(catalog, database string) bool {
	b.mu.RLock("HasDatabase")
	defer b.mu.RUnlock()

	_, ok := b.databases[catalog][database]

	return ok
}

// GetJanitorInterval returns the Interval configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the interval.
func (h *Handler) GetJanitorInterval() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.Interval
}
