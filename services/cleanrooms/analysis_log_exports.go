package cleanrooms

import (
	"fmt"
	"slices"
	"sort"

	"github.com/google/uuid"
)

const (
	abilityExportQueryAnalysisLog = "CAN_EXPORT_QUERY_ANALYSIS_LOG"
	analysisTypeProtectedQuery    = "PROTECTED_QUERY"
	exportStatusInProgress        = "IN_PROGRESS"
	exportStatusSuccess           = "SUCCESS"
	exportStatusFailed            = "FAILED"
)

func validateExportStatus(status string) error {
	switch status {
	case "", exportStatusInProgress, exportStatusSuccess, exportStatusFailed:
		return nil
	}

	return fmt.Errorf("%w: invalid status %q", ErrValidation, status)
}

func exportOutputBucket(resultConfig map[string]any) string {
	out, _ := resultConfig["outputConfiguration"].(map[string]any)
	s3, _ := out["s3"].(map[string]any)
	bucket, _ := s3["bucket"].(string)

	return bucket
}

// StartAnalysisLogExport starts a Spark-log export of a finished protected query.
func (b *InMemoryBackend) StartAnalysisLogExport(
	membershipID, analysisID, analysisType string, resultConfig map[string]any,
) (*AnalysisLogExport, error) {
	if analysisID == "" {
		return nil, fmt.Errorf("%w: analysisId is required", ErrValidation)
	}

	if analysisType != analysisTypeProtectedQuery {
		return nil, fmt.Errorf("%w: analysisType must be %s", ErrValidation, analysisTypeProtectedQuery)
	}

	if exportOutputBucket(resultConfig) == "" {
		return nil, fmt.Errorf("%w: resultConfiguration.outputConfiguration.s3.bucket is required", ErrValidation)
	}

	b.mu.Lock("StartAnalysisLogExport")
	defer b.mu.Unlock()

	mem, ok := b.memberships.Get(membershipID)
	if !ok {
		return nil, ErrNotFound
	}

	if !slices.Contains(mem.MemberAbilities, abilityExportQueryAnalysisLog) {
		return nil, fmt.Errorf("%w: membership lacks %s", ErrAccessDenied, abilityExportQueryAnalysisLog)
	}

	b.advanceProtectedQueriesLocked()

	q, ok := b.protectedQueries.Get(membershipKey(membershipID, analysisID))
	if !ok {
		return nil, ErrNotFound
	}

	if !isTerminalProtectedQueryStatus(q.Status) {
		return nil, fmt.Errorf("%w: protected query is %s, not in a terminal state", ErrConflict, q.Status)
	}

	ts := b.now()
	e := &AnalysisLogExport{
		ID:                  uuid.NewString(),
		AnalysisID:          analysisID,
		AnalysisType:        analysisType,
		MembershipID:        membershipID,
		Status:              exportStatusInProgress,
		ResultConfiguration: resultConfig,
		CreateTime:          ts,
		UpdateTime:          ts,
	}
	b.analysisLogExports.Put(e)

	cp := *e

	return &cp, nil
}

// advanceAnalysisLogExportsLocked resolves in-progress exports to SUCCESS on
// the same schedule as protected queries. Callers must hold b.mu (write lock).
func (b *InMemoryBackend) advanceAnalysisLogExportsLocked() {
	b.analysisLogExports.Range(func(e *AnalysisLogExport) bool {
		if e.Status == exportStatusInProgress && b.workStageLocked(e.CreateTime) == workDone {
			e.Status = exportStatusSuccess
			e.UpdateTime = b.now()
		}

		return true
	})
}

// GetAnalysisLogExport returns one export of a membership.
func (b *InMemoryBackend) GetAnalysisLogExport(membershipID, exportID string) (*AnalysisLogExport, error) {
	b.mu.Lock("GetAnalysisLogExport")
	defer b.mu.Unlock()

	b.advanceAnalysisLogExportsLocked()

	e, ok := b.analysisLogExports.Get(membershipKey(membershipID, exportID))
	if !ok {
		return nil, ErrNotFound
	}

	cp := *e

	return &cp, nil
}

// ListAnalysisLogExports lists a membership's exports, most recent first.
func (b *InMemoryBackend) ListAnalysisLogExports(
	membershipID, analysisID, status string,
) ([]*AnalysisLogExportSummary, error) {
	if err := validateExportStatus(status); err != nil {
		return nil, err
	}

	b.mu.Lock("ListAnalysisLogExports")
	defer b.mu.Unlock()

	if _, ok := b.memberships.Get(membershipID); !ok {
		return nil, ErrNotFound
	}

	b.advanceAnalysisLogExportsLocked()

	var items []*AnalysisLogExportSummary

	for _, e := range b.analysisLogExportsByMembership.Get(membershipID) {
		if (analysisID != "" && e.AnalysisID != analysisID) || (status != "" && e.Status != status) {
			continue
		}

		items = append(items, &AnalysisLogExportSummary{
			ID:           e.ID,
			AnalysisID:   e.AnalysisID,
			AnalysisType: e.AnalysisType,
			Status:       e.Status,
			CreateTime:   e.CreateTime,
		})
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].CreateTime != items[j].CreateTime {
			return items[i].CreateTime > items[j].CreateTime
		}

		return items[i].ID < items[j].ID
	})

	return items, nil
}
