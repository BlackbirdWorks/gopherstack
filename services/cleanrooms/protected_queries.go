package cleanrooms

import (
	"sort"

	"github.com/google/uuid"
)

func (b *InMemoryBackend) StartProtectedQuery(
	membershipID, sqlText string,
	resultConfig map[string]any,
	computeConfiguration map[string]any,
) (*ProtectedQuery, error) {
	b.mu.Lock("StartProtectedQuery")
	defer b.mu.Unlock()

	return b.startProtectedQueryLocked(membershipID, sqlText, resultConfig, computeConfiguration)
}

// startProtectedQueryLocked is the shared implementation behind
// StartProtectedQuery and PopulateIntermediateTable (intermediate_tables.go),
// which starts a protected query on behalf of an intermediate table's stored
// populationAnalysisConfiguration while already holding b.mu -- mirroring
// the createMembershipLocked split between CreateMembership and
// CreateCollaboration. Callers must hold b.mu (write lock).
func (b *InMemoryBackend) startProtectedQueryLocked(
	membershipID, sqlText string,
	resultConfig map[string]any,
	computeConfiguration map[string]any,
) (*ProtectedQuery, error) {
	mem, ok := b.memberships.Get(membershipID)
	if !ok {
		return nil, ErrNotFound
	}
	id := uuid.NewString()
	ts := b.now()
	var sqlParams map[string]any
	if sqlText != "" {
		sqlParams = map[string]any{"queryString": sqlText}
	}
	q := &ProtectedQuery{
		ID:                   id,
		MembershipIdentifier: membershipID,
		MembershipArn:        mem.Arn,
		// AWS's StartProtectedQuery response is synchronous and always reports
		// SUBMITTED for a brand-new query; the real service advances it to
		// STARTED/SUCCESS/FAILED asynchronously. There is no real multi-party
		// compute engine behind this emulator, so advanceProtectedQueriesLocked
		// (called from every subsequent read) resolves it to a terminal status
		// instead of leaving it stuck at SUBMITTED forever, which would hang any
		// client that polls GetProtectedQuery for completion.
		Status:               "SUBMITTED",
		SQLParameters:        sqlParams,
		ResultConfiguration:  resultConfig,
		ComputeConfiguration: computeConfiguration,
		CreateTime:           ts,
		MembershipID:         membershipID,
	}
	b.protectedQueries.Put(q)

	return q, nil
}

// advanceProtectedQueriesLocked resolves every non-terminal protected query to
// SUCCESS. Called from read paths so GetProtectedQuery/ListProtectedQueries
// always observe forward progress. Callers must hold b.mu (write lock).
func (b *InMemoryBackend) advanceProtectedQueriesLocked() {
	b.protectedQueries.Range(func(q *ProtectedQuery) bool {
		if !isTerminalProtectedQueryStatus(q.Status) {
			q.Status = protectedQueryStatusSuccess
			q.Statistics = map[string]any{"totalDurationInMillis": mockDurationMillis}
		}

		return true
	})
}

func (b *InMemoryBackend) GetProtectedQuery(membershipID, queryID string) (*ProtectedQuery, error) {
	b.mu.Lock("GetProtectedQuery")
	defer b.mu.Unlock()
	b.advanceProtectedQueriesLocked()
	q, ok := b.protectedQueries.Get(membershipKey(membershipID, queryID))
	if !ok {
		return nil, ErrNotFound
	}

	return q, nil
}

func (b *InMemoryBackend) ListProtectedQueries(
	membershipID, status, maxResults, nextToken string,
) ([]*ProtectedQuerySummary, string, error) {
	b.mu.Lock("ListProtectedQueries")
	defer b.mu.Unlock()
	b.advanceProtectedQueriesLocked()
	if _, ok := b.memberships.Get(membershipID); !ok {
		return nil, "", ErrNotFound
	}
	var items []*ProtectedQuerySummary
	for _, q := range b.protectedQueriesByMembership.Get(membershipID) {
		if status != "" && q.Status != status {
			continue
		}
		items = append(items, &ProtectedQuerySummary{
			ID:                   q.ID,
			MembershipIdentifier: q.MembershipIdentifier,
			MembershipArn:        q.MembershipArn,
			Status:               q.Status,
			CreateTime:           q.CreateTime,
			MembershipID:         q.MembershipID,
		})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	page, next := paginate(items, maxResults, nextToken)

	return page, next, nil
}

func (b *InMemoryBackend) UpdateProtectedQuery(
	membershipID, queryID, targetStatus string,
) (*ProtectedQuery, error) {
	b.mu.Lock("UpdateProtectedQuery")
	defer b.mu.Unlock()
	q, ok := b.protectedQueries.Get(membershipKey(membershipID, queryID))
	if !ok {
		return nil, ErrNotFound
	}
	// UpdateProtectedQuery's only valid TargetStatus is CANCELLED, and AWS
	// rejects cancelling a query that has already reached a terminal status
	// with ConflictException.
	if isTerminalProtectedQueryStatus(q.Status) {
		return nil, ErrConflict
	}
	q.Status = targetStatus

	return q, nil
}

// isTerminalProtectedQueryStatus reports whether a ProtectedQueryStatus value
// is terminal (no further transitions permitted).
func isTerminalProtectedQueryStatus(status string) bool {
	switch status {
	case "SUCCESS", "FAILED", statusCancelled, "TIMED_OUT":
		return true
	default:
		return false
	}
}
