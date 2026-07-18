package xray

import (
	"sort"
	"time"
)

// GetSamplingStatisticSummaries returns accumulated sampling statistic summaries.
func (b *InMemoryBackend) GetSamplingStatisticSummaries() []SamplingStatisticSummary {
	b.mu.RLock("GetSamplingStatisticSummaries")
	defer b.mu.RUnlock()

	all := b.samplingStats.All()
	out := make([]SamplingStatisticSummary, 0, len(all))

	for _, s := range all {
		cp := *s
		out = append(out, cp)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].RuleName < out[j].RuleName
	})

	return out
}

// GetSamplingTargets returns target documents for the provided stat documents.
// Rules that do not exist are returned in the unprocessed list.
// Documents with an empty ClientID are returned in the unprocessed list.
// Statistics from known rules are accumulated for GetSamplingStatisticSummaries.
func (b *InMemoryBackend) GetSamplingTargets(
	docs []SamplingStatisticsDocument,
) ([]SamplingTargetResult, []UnprocessedStatisticsResult) {
	b.mu.Lock("GetSamplingTargets")
	defer b.mu.Unlock()

	targets := make([]SamplingTargetResult, 0, len(docs))
	unprocessed := make([]UnprocessedStatisticsResult, 0)

	for _, d := range docs {
		if d.ClientID == "" {
			unprocessed = append(unprocessed, UnprocessedStatisticsResult{
				RuleName:  d.RuleName,
				ErrorCode: "400",
				Message:   "ClientID is required",
			})

			continue
		}

		r, ok := b.samplingRules.Get(d.RuleName)
		if !ok {
			unprocessed = append(unprocessed, UnprocessedStatisticsResult{
				RuleName:  d.RuleName,
				ErrorCode: "404",
				Message:   "Rule not found",
			})

			continue
		}

		// Accumulate statistics.
		if existing, exists := b.samplingStats.Get(d.RuleName); exists {
			existing.RequestCount += d.RequestCount
			existing.SampledCount += d.SampledCount
			existing.BorrowCount += d.BorrowCount
			existing.Timestamp = time.Now()
		} else {
			b.samplingStats.Put(&SamplingStatisticSummary{
				RuleName:     d.RuleName,
				RequestCount: d.RequestCount,
				SampledCount: d.SampledCount,
				BorrowCount:  d.BorrowCount,
				Timestamp:    time.Now(),
			})
		}

		targets = append(targets, SamplingTargetResult{
			RuleName:          r.RuleName,
			FixedRate:         r.FixedRate,
			ReservoirSize:     r.ReservoirSize,
			ReservoirQuotaTTL: time.Now().Add(samplingTargetInterval * time.Second),
		})
	}

	return targets, unprocessed
}

// LastRuleModification returns the timestamp of the last sampling rule modification.
func (b *InMemoryBackend) LastRuleModification() time.Time {
	b.mu.RLock("LastRuleModification")
	defer b.mu.RUnlock()

	return b.lastRuleModification
}

const (
	// samplingTargetInterval is the recommended polling interval for sampling targets.
	samplingTargetInterval = 10
)
