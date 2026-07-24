package xray

import (
	"fmt"
	"time"
)

// defaultIndexingRules returns the built-in X-Ray indexing rules.
func defaultIndexingRules() []*IndexingRule {
	now := time.Now()

	return []*IndexingRule{
		{
			Name:       "Default",
			ModifiedAt: now,
			Rule: &ProbabilisticRuleValue{
				DesiredSamplingPercentage: defaultIndexingRuleSamplingPct,
				ActualSamplingPercentage:  defaultIndexingRuleSamplingPct,
			},
		},
	}
}

// GetIndexingRules returns all indexing rules.
func (b *InMemoryBackend) GetIndexingRules() []*IndexingRule {
	b.mu.RLock("GetIndexingRules")
	defer b.mu.RUnlock()

	out := make([]*IndexingRule, len(b.indexingRules))
	for i, r := range b.indexingRules {
		cp := *r
		out[i] = &cp
	}

	return out
}

// UpdateIndexingRule updates the named indexing rule's probabilistic sampling
// percentage (the actual point of UpdateIndexingRule per the real API: its request
// carries a required Rule.Probabilistic.DesiredSamplingPercentage). gopherstack applies
// the desired percentage immediately as the actual percentage too, since there is no
// gradual-rollout simulation to model here.
// Returns ErrIndexingRuleNotFound if no rule with that name exists.
func (b *InMemoryBackend) UpdateIndexingRule(name string, rule *ProbabilisticRuleValue) (*IndexingRule, error) {
	b.mu.Lock("UpdateIndexingRule")
	defer b.mu.Unlock()

	for _, r := range b.indexingRules {
		if r.Name == name {
			r.ModifiedAt = time.Now()

			if rule != nil {
				r.Rule = &ProbabilisticRuleValue{
					DesiredSamplingPercentage: rule.DesiredSamplingPercentage,
					ActualSamplingPercentage:  rule.DesiredSamplingPercentage,
				}
			}

			cp := *r

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: indexing rule %s not found", ErrIndexingRuleNotFound, name)
}

const (
	// defaultIndexingRuleSamplingPct is the AWS-documented default indexing
	// percentage for the built-in "Default" Transaction Search indexing rule.
	defaultIndexingRuleSamplingPct = 1.0
)
