package awsconfig

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// packRuleNamesLocked returns the sorted config rule names deployed by
// packName. Caller must already hold at least a read lock.
func (b *InMemoryBackend) packRuleNamesLocked(packName string) []string {
	links := b.conformancePackRulesByPack.Get(packName)
	names := make([]string, 0, len(links))

	for _, l := range links {
		names = append(names, l.ConfigRuleName)
	}

	sort.Strings(names)

	return names
}

// DescribeConformancePackCompliance returns per-rule compliance for the config
// rules a conformance pack deployed, rolled up from the same b.ruleEvaluations
// state DescribeComplianceByConfigRule reads (real AWS Config's
// DescribeConformancePackCompliance -- verified against
// aws-sdk-go-v2/service/configservice's deserializer, which declares
// NoSuchConformancePackException/NoSuchConfigRuleInConformancePackException).
// ruleNameFilter narrows the result to specific rule names (each must belong
// to the pack); complianceTypeFilter narrows to a single compliance type.
func (b *InMemoryBackend) DescribeConformancePackCompliance(
	packName string,
	ruleNameFilter []string,
	complianceTypeFilter string,
) ([]ConformancePackComplianceItem, error) {
	b.mu.RLock("DescribeConformancePackCompliance")
	defer b.mu.RUnlock()

	if !b.conformancePacks.Has(packName) {
		return nil, fmt.Errorf("%w: %s", ErrNoSuchConformancePack, packName)
	}

	names := b.packRuleNamesLocked(packName)

	if len(ruleNameFilter) > 0 {
		var err error

		names, err = intersectRuleNames(names, ruleNameFilter)
		if err != nil {
			return nil, err
		}
	}

	out := make([]ConformancePackComplianceItem, 0, len(names))

	for _, name := range names {
		ct := ruleComplianceOrInsufficientData(b.ruleEvaluations[name])
		if complianceTypeFilter != "" && ct != complianceTypeFilter {
			continue
		}

		out = append(out, ConformancePackComplianceItem{ConfigRuleName: name, ComplianceType: ct})
	}

	return out, nil
}

// intersectRuleNames validates every name in filter is present in packRules
// (else NoSuchConfigRuleInConformancePackException) and returns filter's
// names in their original order.
func intersectRuleNames(packRules, filter []string) ([]string, error) {
	inPack := make(map[string]struct{}, len(packRules))
	for _, n := range packRules {
		inPack[n] = struct{}{}
	}

	for _, n := range filter {
		if _, ok := inPack[n]; !ok {
			return nil, fmt.Errorf("%w: %s", ErrNoSuchConfigRuleInConformancePack, n)
		}
	}

	return filter, nil
}

// ruleComplianceOrInsufficientData normalizes an empty rollup (no evaluation
// recorded yet) to INSUFFICIENT_DATA, matching the conformance-pack compliance
// family's default when a deployed rule has never been evaluated.
func ruleComplianceOrInsufficientData(ct string) string {
	if ct == "" {
		return complianceInsufficientData
	}

	return ct
}

// GetConformancePackComplianceDetails returns the per-resource evaluation
// results for the config rules a conformance pack deployed, reusing the same
// buildDetailedResults shape GetComplianceDetailsByConfigRule returns (real
// AWS Config's ConformancePackEvaluationResult is wire-shape identical to
// DetailedEvaluationResult -- verified against
// aws-sdk-go-v2/service/configservice's types.ConformancePackEvaluationResult).
func (b *InMemoryBackend) GetConformancePackComplianceDetails(
	packName string,
	ruleNameFilter []string,
	resourceType string,
	resourceIDs []string,
	complianceTypeFilter string,
) ([]DetailedEvaluationResult, error) {
	b.mu.RLock("GetConformancePackComplianceDetails")
	defer b.mu.RUnlock()

	if !b.conformancePacks.Has(packName) {
		return nil, fmt.Errorf("%w: %s", ErrNoSuchConformancePack, packName)
	}

	names := b.packRuleNamesLocked(packName)

	if len(ruleNameFilter) > 0 {
		var err error

		names, err = intersectRuleNames(names, ruleNameFilter)
		if err != nil {
			return nil, err
		}
	}

	complianceTypes := []string(nil)
	if complianceTypeFilter != "" {
		complianceTypes = []string{complianceTypeFilter}
	}

	out := make([]DetailedEvaluationResult, 0)

	for _, name := range names {
		for _, r := range buildDetailedResults(name, b.ruleResourceEvalsByRule.Get(name), complianceTypes) {
			q := r.EvaluationResultIdentifier.EvaluationResultQualifier
			if resourceType != "" && q.ResourceType != resourceType {
				continue
			}

			if len(resourceIDs) > 0 && !slices.Contains(resourceIDs, q.ResourceID) {
				continue
			}

			out = append(out, r)
		}
	}

	return out, nil
}

// GetConformancePackComplianceSummary returns the overall compliance status of
// each named conformance pack: NON_COMPLIANT if any deployed rule is
// NON_COMPLIANT, COMPLIANT if every deployed rule that has been evaluated is
// COMPLIANT, else INSUFFICIENT_DATA -- matching real AWS Config's documented
// rollup ("A conformance pack is compliant if all of the rules... are
// compliant. It is noncompliant if any of the rules are not compliant.").
func (b *InMemoryBackend) GetConformancePackComplianceSummary(
	packNames []string,
) ([]ConformancePackComplianceSummaryEntry, error) {
	b.mu.RLock("GetConformancePackComplianceSummary")
	defer b.mu.RUnlock()

	out := make([]ConformancePackComplianceSummaryEntry, 0, len(packNames))

	for _, name := range packNames {
		if !b.conformancePacks.Has(name) {
			return nil, fmt.Errorf("%w: %s", ErrNoSuchConformancePack, name)
		}

		out = append(out, ConformancePackComplianceSummaryEntry{
			ConformancePackName:             name,
			ConformancePackComplianceStatus: b.packComplianceStatusLocked(name),
		})
	}

	return out, nil
}

// packComplianceStatusLocked rolls up a conformance pack's deployed rules into
// a single compliance status. Caller must already hold at least a read lock.
func (b *InMemoryBackend) packComplianceStatusLocked(packName string) string {
	names := b.packRuleNamesLocked(packName)

	hasCompliant := false

	for _, name := range names {
		switch b.ruleEvaluations[name] {
		case complianceNonCompliant:
			return complianceNonCompliant
		case complianceCompliant:
			hasCompliant = true
		}
	}

	if hasCompliant {
		return complianceCompliant
	}

	return complianceInsufficientData
}

// ListConformancePackComplianceScores returns a compliance score (the
// percentage of compliant rule evaluations among a pack's deployed rules) for
// each conformance pack, or every pack when packNameFilter is empty. A pack
// with no recorded evaluations scores INSUFFICIENT_DATA, matching real AWS
// Config's documented behavior ("Conformance packs with no evaluation results
// will have a compliance score of INSUFFICIENT_DATA").
func (b *InMemoryBackend) ListConformancePackComplianceScores(
	packNameFilter []string,
) []ConformancePackComplianceScoreEntry {
	b.mu.RLock("ListConformancePackComplianceScores")
	defer b.mu.RUnlock()

	names := packNameFilter
	if len(names) == 0 {
		names = make([]string, 0, b.conformancePacks.Len())
		for _, p := range b.conformancePacks.All() {
			names = append(names, p.ConformancePackName)
		}

		sort.Strings(names)
	}

	out := make([]ConformancePackComplianceScoreEntry, 0, len(names))

	for _, name := range names {
		if !b.conformancePacks.Has(name) {
			continue
		}

		out = append(out, b.packComplianceScoreLocked(name))
	}

	return out
}

// packComplianceScoreLocked computes one pack's compliance score entry.
// Caller must already hold at least a read lock.
func (b *InMemoryBackend) packComplianceScoreLocked(packName string) ConformancePackComplianceScoreEntry {
	var compliant, total int
	var lastUpdated float64

	for _, name := range b.packRuleNamesLocked(packName) {
		for _, e := range b.ruleResourceEvalsByRule.Get(name) {
			total++

			if e.ComplianceType == complianceCompliant {
				compliant++
			}

			if e.ResultRecordedTime > lastUpdated {
				lastUpdated = e.ResultRecordedTime
			}
		}
	}

	const percentScale = 100

	score := "INSUFFICIENT_DATA"
	if total > 0 {
		score = strconv.FormatFloat(float64(compliant)/float64(total)*percentScale, 'f', 1, 64)
	}

	return ConformancePackComplianceScoreEntry{
		ConformancePackName: packName,
		Score:               score,
		LastUpdatedTime:     lastUpdated,
	}
}

// DescribeAggregateComplianceByConformancePacks returns every conformance
// pack's compliance as seen through aggregatorName, echoing the requested
// accountID/awsRegion into each result. Mirrors
// GetAggregateComplianceDetailsByConfigRule's approach: this emulator has no
// real multi-account data source, so it reuses local per-pack rule-count
// state once the aggregator's existence is genuinely validated
// (NoSuchConfigurationAggregatorException).
func (b *InMemoryBackend) DescribeAggregateComplianceByConformancePacks(
	aggregatorName, accountID, awsRegion string,
) ([]AggregateComplianceByConformancePack, error) {
	b.mu.RLock("DescribeAggregateComplianceByConformancePacks")
	defer b.mu.RUnlock()

	if err := b.requireAggregatorLocked(aggregatorName); err != nil {
		return nil, err
	}

	packs := b.conformancePacks.All()
	out := make([]AggregateComplianceByConformancePack, 0, len(packs))

	for _, p := range packs {
		compliance := b.packRuleCountsLocked(p.ConformancePackName)
		out = append(out, AggregateComplianceByConformancePack{
			ConformancePackName: p.ConformancePackName,
			AccountID:           accountID,
			AwsRegion:           awsRegion,
			Compliance:          &compliance,
		})
	}

	slices.SortFunc(out, func(a, c AggregateComplianceByConformancePack) int {
		return strings.Compare(a.ConformancePackName, c.ConformancePackName)
	})

	return out, nil
}

// packRuleCountsLocked rolls a conformance pack's deployed rules up into
// AggregateConformancePackCompliance's compliant/noncompliant/total counts and
// overall status. Caller must already hold at least a read lock.
func (b *InMemoryBackend) packRuleCountsLocked(packName string) AggregateConformancePackCompliance {
	names := b.packRuleNamesLocked(packName)

	var compliant, nonCompliant int32

	for _, name := range names {
		switch b.ruleEvaluations[name] {
		case complianceCompliant:
			compliant++
		case complianceNonCompliant:
			nonCompliant++
		}
	}

	return AggregateConformancePackCompliance{
		ComplianceType:        b.packComplianceStatusLocked(packName),
		CompliantRuleCount:    compliant,
		NonCompliantRuleCount: nonCompliant,
		TotalRuleCount:        int32(len(names)), //nolint:gosec // rule count is small and non-negative
	}
}

// GetAggregateConformancePackComplianceSummary returns compliant/noncompliant
// conformance-pack counts grouped by account ID or AWS region (groupByKey;
// ACCOUNT_ID when empty), mirroring GetAggregateConfigRuleComplianceSummary's
// single-group rollup once the aggregator's existence is validated
// (NoSuchConfigurationAggregatorException).
func (b *InMemoryBackend) GetAggregateConformancePackComplianceSummary(
	aggregatorName, groupByKey string,
) ([]AggregateConformancePackComplianceSummary, error) {
	b.mu.RLock("GetAggregateConformancePackComplianceSummary")
	defer b.mu.RUnlock()

	if err := b.requireAggregatorLocked(aggregatorName); err != nil {
		return nil, err
	}

	packs := b.conformancePacks.All()
	if len(packs) == 0 {
		return []AggregateConformancePackComplianceSummary{}, nil
	}

	groupName := b.accountID
	if groupByKey == "AWS_REGION" {
		groupName = b.region
	}

	var compliantPacks, nonCompliantPacks int32

	for _, p := range packs {
		switch b.packComplianceStatusLocked(p.ConformancePackName) {
		case complianceCompliant:
			compliantPacks++
		case complianceNonCompliant:
			nonCompliantPacks++
		}
	}

	return []AggregateConformancePackComplianceSummary{{
		GroupName: groupName,
		ComplianceSummary: AggregateConformancePackComplianceCount{
			CompliantConformancePackCount:    compliantPacks,
			NonCompliantConformancePackCount: nonCompliantPacks,
		},
	}}, nil
}
