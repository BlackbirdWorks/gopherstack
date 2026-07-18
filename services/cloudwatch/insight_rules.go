package cloudwatch

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// aggregateContributorPoint updates the running aggregation for a single metric record point.
func aggregateContributorPoint(
	pt MetricDatum,
	key string,
	rec *metricRecord,
	orderBy string,
	dimSums map[string]float64,
	dimKeys map[string][]string,
) {
	if _, seen := dimKeys[key]; !seen {
		keys := make([]string, len(rec.Dimensions))
		for i, d := range rec.Dimensions {
			keys[i] = d.Value
		}
		dimKeys[key] = keys
	}
	if strings.EqualFold(orderBy, statSum) {
		dimSums[key] += pt.Sum
	} else {
		dimSums[key] += pt.Count
	}
}

// aggregateContributorRecord accumulates a metric record's in-range points into the maps.
func aggregateContributorRecord(
	rec *metricRecord,
	startTime, endTime time.Time,
	orderBy string,
	dimSums map[string]float64,
	dimKeys map[string][]string,
) {
	if len(rec.Dimensions) == 0 {
		return
	}
	key := dimensionSetKey(rec.Dimensions)
	for _, pt := range rec.Points {
		if pt.Timestamp.Before(startTime) || !pt.Timestamp.Before(endTime) {
			continue
		}
		aggregateContributorPoint(pt, key, rec, orderBy, dimSums, dimKeys)
	}
}

// topNContributors converts aggregation maps to a sorted, capped contributor list.
func topNContributors(
	dimSums map[string]float64,
	dimKeys map[string][]string,
	maxN int,
) []AlarmContributor {
	type entry struct {
		key string
		sum float64
	}
	entries := make([]entry, 0, len(dimSums))
	for k, s := range dimSums {
		entries = append(entries, entry{k, s})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].sum > entries[j].sum })
	if len(entries) > maxN {
		entries = entries[:maxN]
	}
	result := make([]AlarmContributor, 0, len(entries))
	for _, e := range entries {
		result = append(result, AlarmContributor{Keys: dimKeys[e.key], Sum: e.sum})
	}

	return result
}

// GetInsightRuleContributors returns top-N contributors for an insight rule by aggregating
// stored metric data along dimension values. This is a best-effort local approximation.
// Caller must hold b.mu (at least read lock).
func (b *InMemoryBackend) GetInsightRuleContributors(
	ruleName string,
	startTime, endTime time.Time,
	maxContributorCount int,
	orderBy string,
) ([]AlarmContributor, error) {
	if !b.insightRules.Has(ruleName) {
		return nil, fmt.Errorf("%w: %s", ErrInsightRuleNotFound, ruleName)
	}
	if maxContributorCount <= 0 {
		maxContributorCount = 10
	}
	dimSums := make(map[string]float64)
	dimKeys := make(map[string][]string)
	for _, nsMap := range b.metrics {
		for _, rec := range nsMap {
			aggregateContributorRecord(rec, startTime, endTime, orderBy, dimSums, dimKeys)
		}
	}

	return topNContributors(dimSums, dimKeys, maxContributorCount), nil
}

// ListManagedInsightRules returns a paginated list of managed (service-linked) insight rules.
// If resourceARN is non-empty only rules whose Arn matches are included; in the emulator the
// ManagedRule flag is used as the primary discriminator.
func (b *InMemoryBackend) ListManagedInsightRules(
	resourceARN, nextToken string,
	maxResults int,
) (page.Page[InsightRule], error) {
	b.mu.RLock("ListManagedInsightRules")
	defer b.mu.RUnlock()

	result := make([]InsightRule, 0)
	for _, rule := range b.insightRules.All() {
		if !rule.ManagedRule {
			continue
		}
		if resourceARN != "" && rule.Arn != resourceARN {
			continue
		}
		result = append(result, *rule)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })

	return page.New(result, nextToken, maxResults, cwDefaultListManagedInsightRulesLimit), nil
}

// DeleteInsightRules removes insight rules by name. Non-existent rules are reported as failures.
func (b *InMemoryBackend) DeleteInsightRules(ruleNames []string) ([]InsightRuleFailure, error) {
	b.mu.Lock("DeleteInsightRules")
	defer b.mu.Unlock()

	var failures []InsightRuleFailure

	for _, name := range ruleNames {
		if !b.insightRules.Has(name) {
			failures = append(failures, InsightRuleFailure{
				RuleName:           name,
				FailureCode:        errResourceNotFoundException,
				FailureDescription: fmt.Sprintf("Insight rule %q does not exist", name),
			})

			continue
		}

		b.insightRules.Delete(name)
	}

	return failures, nil
}

// PutInsightRule creates or updates an insight rule.
func (b *InMemoryBackend) PutInsightRule(rule *InsightRule) error {
	if strings.TrimSpace(rule.Name) == "" {
		return fmt.Errorf("%w: RuleName parameter is required", ErrValidation)
	}

	b.PutInsightRuleInternal(rule)

	return nil
}

// GetInsightRule returns an insight rule by name.
func (b *InMemoryBackend) GetInsightRule(name string) (*InsightRule, error) {
	b.mu.RLock("GetInsightRule")
	defer b.mu.RUnlock()

	rule, ok := b.insightRules.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrInsightRuleNotFound, name)
	}

	cp := *rule

	return &cp, nil
}

// PutInsightRuleInternal creates or updates an insight rule (used for test seeding).
func (b *InMemoryBackend) PutInsightRuleInternal(rule *InsightRule) {
	b.mu.Lock("PutInsightRuleInternal")
	defer b.mu.Unlock()

	cp := *rule
	if cp.State == "" {
		cp.State = insightRuleStateEnabled
	}

	if cp.CreatedAt.IsZero() {
		cp.CreatedAt = time.Now().UTC()
	}

	if cp.Arn == "" {
		cp.Arn = arn.Build("cloudwatch", b.region, b.accountID, "insight-rule/"+rule.Name)
	}

	b.insightRules.Put(&cp)
}

// DescribeInsightRules returns a paginated list of insight rules.
func (b *InMemoryBackend) DescribeInsightRules(
	nextToken string,
	maxResults int,
) (page.Page[InsightRule], error) {
	b.mu.RLock("DescribeInsightRules")
	defer b.mu.RUnlock()

	result := make([]InsightRule, 0, b.insightRules.Len())

	for _, r := range b.insightRules.All() {
		result = append(result, *r)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return page.New(result, nextToken, maxResults, cwDefaultDescribeInsightRulesLimit), nil
}

// DisableInsightRules disables the specified insight rules. Non-existent rules are reported as failures.
func (b *InMemoryBackend) DisableInsightRules(ruleNames []string) ([]InsightRuleFailure, error) {
	b.mu.Lock("DisableInsightRules")
	defer b.mu.Unlock()

	var failures []InsightRuleFailure

	for _, name := range ruleNames {
		rule, ok := b.insightRules.Get(name)
		if !ok {
			failures = append(failures, InsightRuleFailure{
				RuleName:           name,
				FailureCode:        errResourceNotFoundException,
				FailureDescription: fmt.Sprintf("Insight rule %q does not exist", name),
			})

			continue
		}

		rule.State = "DISABLED"
	}

	return failures, nil
}

// EnableInsightRules enables the specified insight rules. Non-existent rules are reported as failures.
func (b *InMemoryBackend) EnableInsightRules(ruleNames []string) ([]InsightRuleFailure, error) {
	b.mu.Lock("EnableInsightRules")
	defer b.mu.Unlock()

	var failures []InsightRuleFailure

	for _, name := range ruleNames {
		rule, ok := b.insightRules.Get(name)
		if !ok {
			failures = append(failures, InsightRuleFailure{
				RuleName:           name,
				FailureCode:        errResourceNotFoundException,
				FailureDescription: fmt.Sprintf("Insight rule %q does not exist", name),
			})

			continue
		}

		rule.State = insightRuleStateEnabled
	}

	return failures, nil
}
