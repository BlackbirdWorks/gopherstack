package glue

import (
	"sort"
	"time"
)

// BatchGetDataQualityResult retrieves multiple data quality results by ID.
// The real BatchGetDataQualityResultOutput.ResultsNotFound is a list of
// result IDs (glue@v1.152.0 deserializers.go:
// awsAwsjson11_deserializeDocumentDataQualityResultIds decodes into []string,
// not a list of ErrorDetail objects) -- a real client fails to decode
// ErrorDetail objects into that string list.
func (b *InMemoryBackend) BatchGetDataQualityResult(
	resultIDs []string,
) ([]*DataQualityResult, []string) {
	b.mu.RLock("BatchGetDataQualityResult")
	defer b.mu.RUnlock()

	found := make([]*DataQualityResult, 0, len(resultIDs))
	notFound := make([]string, 0, len(resultIDs))

	for _, id := range resultIDs {
		dqr, ok := b.dataQualityResult.Get(id)
		if !ok {
			notFound = append(notFound, id)

			continue
		}

		cp := *dqr
		found = append(found, &cp)
	}

	return found, notFound
}

// AddDataQualityResultInternal adds a data quality result directly to the backend without validation.
func (b *InMemoryBackend) AddDataQualityResultInternal(dqr *DataQualityResult) {
	b.mu.Lock("AddDataQualityResultInternal")
	defer b.mu.Unlock()

	cp := *dqr
	b.dataQualityResult.Put(&cp)
}

// AddDataQualityEvalRunInternal adds an evaluation run directly without validation.
func (b *InMemoryBackend) AddDataQualityEvalRunInternal(run *DataQualityEvaluationRun) {
	b.mu.Lock("AddDataQualityEvalRunInternal")
	defer b.mu.Unlock()

	cp := *run
	cp.RulesetNames = append([]string(nil), run.RulesetNames...)
	b.dataQualityEvalRuns.Put(&cp)
}

func dqAnnotationKey(profileID, statisticID string) string {
	return profileID + "|" + statisticID
}

// PutDataQualityStatisticAnnotation stores (or overwrites) the inclusion
// annotation for a profile/statistic pair.
func (b *InMemoryBackend) PutDataQualityStatisticAnnotation(profileID, statisticID, inclusion string) {
	b.mu.Lock("PutDataQualityStatisticAnnotation")
	defer b.mu.Unlock()

	now := float64(time.Now().Unix())
	key := dqAnnotationKey(profileID, statisticID)

	recordedOn := now
	if existing, ok := b.dqStatisticAnnotations.Get(key); ok {
		recordedOn = existing.RecordedOn
	}

	b.dqStatisticAnnotations.Put(&StatisticAnnotation{
		ProfileID:      profileID,
		StatisticID:    statisticID,
		Inclusion:      inclusion,
		RecordedOn:     recordedOn,
		LastModifiedOn: now,
	})
}

// ListDataQualityStatisticAnnotations returns stored annotations, optionally
// filtered by profile ID and/or statistic ID, sorted by key for a deterministic
// response.
func (b *InMemoryBackend) ListDataQualityStatisticAnnotations(profileID, statisticID string) []*StatisticAnnotation {
	b.mu.RLock("ListDataQualityStatisticAnnotations")
	defer b.mu.RUnlock()

	src := b.dqStatisticAnnotations.Snapshot()
	out := make([]*StatisticAnnotation, 0, len(src))

	for _, e := range src {
		if profileID != "" && e.ProfileID != profileID {
			continue
		}

		if statisticID != "" && e.StatisticID != statisticID {
			continue
		}

		cp := *e
		out = append(out, &cp)
	}

	return out
}

// ListDataQualityEvaluationRuns returns all DataQuality ruleset evaluation runs.
func (b *InMemoryBackend) ListDataQualityEvaluationRuns() []*DataQualityEvaluationRun {
	b.mu.RLock("ListDataQualityEvaluationRuns")
	defer b.mu.RUnlock()

	src := b.dataQualityEvalRuns.All()
	out := make([]*DataQualityEvaluationRun, 0, len(src))
	for _, r := range src {
		cp := *r
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].StartedOn != out[j].StartedOn {
			return out[i].StartedOn < out[j].StartedOn
		}

		return out[i].RunID < out[j].RunID
	})

	return out
}

// ListDataQualityResults returns all DataQuality results.
func (b *InMemoryBackend) ListDataQualityResults() []*DataQualityResult {
	b.mu.RLock("ListDataQualityResults")
	defer b.mu.RUnlock()

	src := b.dataQualityResult.All()
	out := make([]*DataQualityResult, 0, len(src))
	for _, r := range src {
		cp := *r
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ResultID < out[j].ResultID })

	return out
}
