package ce

import (
	"sort"
	"time"

	"github.com/google/uuid"
)

// CreateCommitmentAnalysis starts a new commitment purchase analysis.
// configuration is stored and echoed back verbatim on Get/List/Start (real
// GetCommitmentPurchaseAnalysisOutput/AnalysisSummary both carry
// CommitmentPurchaseAnalysisConfiguration) -- this backend doesn't simulate
// analysis internals, so it round-trips the request's configuration rather
// than fabricating computed contents.
func (b *InMemoryBackend) CreateCommitmentAnalysis(configuration any) *CommitmentAnalysis {
	b.mu.Lock("CreateCommitmentAnalysis")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	estimated := now.Add(analysisETAMinutes * time.Minute)
	a := &CommitmentAnalysis{
		AnalysisID:              uuid.NewString(),
		AnalysisStatus:          statusProcessing,
		AnalysisStartedTime:     now.Format(time.RFC3339),
		EstimatedCompletionTime: estimated.Format(time.RFC3339),
		Configuration:           configuration,
	}

	b.commitmentAnalyses.Put(a)

	return a
}

// GetCommitmentAnalysis retrieves a commitment analysis by ID.
func (b *InMemoryBackend) GetCommitmentAnalysis(analysisID string) (*CommitmentAnalysis, error) {
	b.mu.RLock("GetCommitmentAnalysis")
	defer b.mu.RUnlock()

	a, ok := b.commitmentAnalyses.Get(analysisID)
	if !ok {
		return nil, ErrNotFound
	}

	cp := *a

	return &cp, nil
}

// ListCommitmentAnalyses returns all commitment analyses sorted by AnalysisStartedTime.
func (b *InMemoryBackend) ListCommitmentAnalyses() []*CommitmentAnalysis {
	b.mu.RLock("ListCommitmentAnalyses")
	defer b.mu.RUnlock()

	all := b.commitmentAnalyses.All()
	result := make([]*CommitmentAnalysis, 0, len(all))
	for _, a := range all {
		cp := *a
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].AnalysisStartedTime > result[j].AnalysisStartedTime
	})

	return result
}
