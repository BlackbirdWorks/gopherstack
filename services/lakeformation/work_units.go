package lakeformation

import (
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// StartQueryPlanning registers a new query and returns its ID.
func (b *InMemoryBackend) StartQueryPlanning(queryString string) string {
	id := newTransactionID()
	b.mu.Lock("StartQueryPlanning")
	defer b.mu.Unlock()
	b.queries[id] = "WORKUNITS_AVAILABLE"
	_ = queryString

	return id
}

// GetQueryState returns the current state of a query.
func (b *InMemoryBackend) GetQueryState(queryID string) (string, error) {
	if strings.TrimSpace(queryID) == "" {
		return "", fmt.Errorf("QueryId is required: %w", ErrValidation)
	}
	b.mu.RLock("GetQueryState")
	defer b.mu.RUnlock()
	state, ok := b.queries[queryID]
	if !ok {
		return "", awserr.New("query not found: "+queryID, awserr.ErrNotFound)
	}

	return state, nil
}

// GetQueryStatistics returns synthetic statistics for a query.
func (b *InMemoryBackend) GetQueryStatistics(queryID string) (*ExecutionStatistics, *PlanningStatistics, error) {
	if strings.TrimSpace(queryID) == "" {
		return nil, nil, fmt.Errorf("QueryId is required: %w", ErrValidation)
	}
	b.mu.RLock("GetQueryStatistics")
	defer b.mu.RUnlock()
	if _, ok := b.queries[queryID]; !ok {
		return nil, nil, awserr.New("query not found: "+queryID, awserr.ErrNotFound)
	}
	one := int64(1)
	exec := &ExecutionStatistics{WorkUnitsExecutedCount: &one}
	plan := &PlanningStatistics{WorkUnitsGeneratedCount: &one}

	return exec, plan, nil
}

// GetWorkUnits returns the work unit ranges for a completed query plan.
func (b *InMemoryBackend) GetWorkUnits(queryID string) ([]WorkUnitRange, string, error) {
	if strings.TrimSpace(queryID) == "" {
		return nil, "", fmt.Errorf("QueryId is required: %w", ErrValidation)
	}
	b.mu.RLock("GetWorkUnits")
	defer b.mu.RUnlock()
	if _, ok := b.queries[queryID]; !ok {
		return nil, "", awserr.New("query not found: "+queryID, awserr.ErrNotFound)
	}

	return []WorkUnitRange{{WorkUnitIDMax: 0, WorkUnitIDMin: 0, WorkUnitToken: queryID}}, "", nil
}

// GetWorkUnitResults validates that the query exists and that workUnitID
// names a real work unit -- GetWorkUnits always returns exactly one range
// (WorkUnitIDMin/Max both 0, see GetWorkUnits above), so only 0 is valid
// today, but a caller-supplied ID is no longer silently ignored.
func (b *InMemoryBackend) GetWorkUnitResults(queryID string, workUnitID int64, _ string) (string, error) {
	if strings.TrimSpace(queryID) == "" {
		return "", fmt.Errorf("QueryId is required: %w", ErrValidation)
	}
	b.mu.RLock("GetWorkUnitResults")
	defer b.mu.RUnlock()
	query, ok := b.queries[queryID]
	if !ok {
		return "", awserr.New("query not found: "+queryID, awserr.ErrNotFound)
	}

	if workUnitID != 0 {
		return "", fmt.Errorf("WorkUnitId %d not found for query %q: %w", workUnitID, queryID, ErrValidation)
	}

	return query, nil
}
