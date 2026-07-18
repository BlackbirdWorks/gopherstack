package awsconfig

import (
	"fmt"
	"slices"
	"sort"
	"time"
)

// PutConfigRule creates or updates a config rule with full metadata.
func (b *InMemoryBackend) PutConfigRule(input *ConfigRule) error {
	if input == nil || input.ConfigRuleName == "" {
		return fmt.Errorf("%w: ConfigRuleName is required", ErrValidation)
	}

	b.mu.Lock("PutConfigRule")
	defer b.mu.Unlock()

	existing, ok := b.configRules.Get(input.ConfigRuleName)
	if ok {
		// Preserve ARN and ID on update.
		input.ConfigRuleArn = existing.ConfigRuleArn
		input.ConfigRuleID = existing.ConfigRuleID
	} else {
		b.ruleCounter++
		input.ConfigRuleArn = fmt.Sprintf(
			"arn:aws:config:%s:%s:config-rule/config-rule-%08d",
			b.region, b.accountID, b.ruleCounter,
		)
		input.ConfigRuleID = fmt.Sprintf("config-rule-%08d", b.ruleCounter)
	}

	if input.ConfigRuleState == "" {
		input.ConfigRuleState = "ACTIVE"
	}

	cp := *input
	// Deep-copy Source to avoid shared pointer.
	if input.Source != nil {
		srcCopy := *input.Source
		cp.Source = &srcCopy
	}

	b.configRules.Put(&cp)

	return nil
}

// DescribeConfigRules returns config rules optionally filtered by name list, sorted by name.
func (b *InMemoryBackend) DescribeConfigRules(names []string) []ConfigRule {
	b.mu.RLock("DescribeConfigRules")
	defer b.mu.RUnlock()

	out := make([]ConfigRule, 0, b.configRules.Len())

	if len(names) == 0 {
		for _, r := range b.configRules.All() {
			out = append(out, *r)
		}
	} else {
		for _, n := range names {
			if r, ok := b.configRules.Get(n); ok {
				out = append(out, *r)
			}
		}
	}

	slices.SortFunc(out, func(a, b ConfigRule) int {
		if a.ConfigRuleName < b.ConfigRuleName {
			return -1
		}

		if a.ConfigRuleName > b.ConfigRuleName {
			return 1
		}

		return 0
	})

	return out
}

// DeleteConfigRule deletes a config rule by name.
func (b *InMemoryBackend) DeleteConfigRule(name string) error {
	if name == "" {
		return fmt.Errorf("%w: ConfigRuleName is required", ErrValidation)
	}

	b.mu.Lock("DeleteConfigRule")
	defer b.mu.Unlock()

	if !b.configRules.Has(name) {
		return fmt.Errorf("%w: %s", ErrNoSuchConfigRule, name)
	}

	b.configRules.Delete(name)
	b.clearRuleEvaluationsLocked(name)

	return nil
}

// clearRuleEvaluationsLocked removes every stored evaluation (rollup and
// per-resource) for ruleName. The caller must hold the write lock.
//
// ruleResourceEvals has no bulk "delete everything under this rule" operation
// (unlike the old map[string]map[string]*T's single outer-map delete);
// snapshot the rule's entries via slices.Clone first since Table.Delete
// mutates the very index slice ruleResourceEvalsByRule.Get returns.
func (b *InMemoryBackend) clearRuleEvaluationsLocked(ruleName string) {
	delete(b.ruleEvaluations, ruleName)

	for _, e := range slices.Clone(b.ruleResourceEvalsByRule.Get(ruleName)) {
		b.ruleResourceEvals.Delete(storedEvaluationKeyFn(e))
	}
}

// DeleteEvaluationResults clears the rollup and per-resource evaluation results
// recorded for a config rule (so a subsequent StartConfigRulesEvaluation starts
// from a clean slate), matching real AWS Config which errors
// NoSuchConfigRuleException for an unknown rule (verified against
// aws-sdk-go-v2/service/configservice's DeleteEvaluationResults deserializer).
func (b *InMemoryBackend) DeleteEvaluationResults(ruleName string) error {
	if ruleName == "" {
		return fmt.Errorf("%w: ConfigRuleName is required", ErrValidation)
	}

	b.mu.Lock("DeleteEvaluationResults")
	defer b.mu.Unlock()

	if !b.configRules.Has(ruleName) {
		return fmt.Errorf("%w: %s", ErrNoSuchConfigRule, ruleName)
	}

	b.clearRuleEvaluationsLocked(ruleName)

	return nil
}

// GetConfigRuleComplianceType returns the rolled-up compliance type for a config
// rule after evaluation, or empty string if no evaluation has run for that rule yet.
func (b *InMemoryBackend) GetConfigRuleComplianceType(ruleName string) string {
	b.mu.RLock("GetConfigRuleComplianceType")
	defer b.mu.RUnlock()

	return b.ruleEvaluations[ruleName]
}

// DescribeConfigRuleEvaluationStatus returns evaluation statuses for config rules.
// If names is empty, all rules are returned.
func (b *InMemoryBackend) DescribeConfigRuleEvaluationStatus(names []string) []ConfigRuleEvaluationStatus {
	b.mu.RLock("DescribeConfigRuleEvaluationStatus")
	defer b.mu.RUnlock()

	if len(names) == 0 {
		out := make([]ConfigRuleEvaluationStatus, 0, len(b.ruleEvaluations))
		for name := range b.ruleEvaluations {
			out = append(out, ConfigRuleEvaluationStatus{ConfigRuleName: name})
		}

		return out
	}

	out := make([]ConfigRuleEvaluationStatus, 0, len(names))

	for _, name := range names {
		if _, ok := b.ruleEvaluations[name]; ok {
			out = append(out, ConfigRuleEvaluationStatus{ConfigRuleName: name})
		}
	}

	return out
}

// DescribeComplianceByConfigRule returns compliance info for the given rule names.
// If names is empty, all rules are returned.
func (b *InMemoryBackend) DescribeComplianceByConfigRule(names []string) []ComplianceByConfigRule {
	b.mu.RLock("DescribeComplianceByConfigRule")
	defer b.mu.RUnlock()

	if len(names) == 0 {
		out := make([]ComplianceByConfigRule, 0, len(b.ruleEvaluations))
		for name, ct := range b.ruleEvaluations {
			out = append(out, ComplianceByConfigRule{
				ConfigRuleName: name,
				Compliance:     ComplianceResult{ComplianceType: ct},
			})
		}

		return out
	}

	out := make([]ComplianceByConfigRule, 0, len(names))

	for _, name := range names {
		ct := b.ruleEvaluations[name]
		if ct == "" {
			ct = complianceNotApplicable
		}

		out = append(out, ComplianceByConfigRule{
			ConfigRuleName: name,
			Compliance:     ComplianceResult{ComplianceType: ct},
		})
	}

	return out
}

// GetComplianceSummaryByConfigRule returns a compliance summary aggregated from
// the recorded rule evaluations. AWS returns counts of compliant and
// non-compliant config rules; here we derive those counts from the stored
// per-rule compliance types populated via PutEvaluation(s)/PutExternalEvaluation.
// When no evaluations have been recorded the result is an empty slice.
func (b *InMemoryBackend) GetComplianceSummaryByConfigRule() []ComplianceSummary {
	b.mu.RLock("GetComplianceSummaryByConfigRule")
	defer b.mu.RUnlock()

	if len(b.ruleEvaluations) == 0 {
		return []ComplianceSummary{}
	}

	var compliant, nonCompliant int32

	for _, ct := range b.ruleEvaluations {
		switch ct {
		case "COMPLIANT":
			compliant++
		case "NON_COMPLIANT":
			nonCompliant++
		}
	}

	complianceType := "COMPLIANT"
	if nonCompliant > 0 {
		complianceType = "NON_COMPLIANT"
	}

	return []ComplianceSummary{{
		ComplianceType: complianceType,
		ComplianceSummary: ComplianceSummaryDetail{
			CompliantResourceCount:    ResourceCount{CappedCount: compliant},
			NonCompliantResourceCount: ResourceCount{CappedCount: nonCompliant},
		},
	}}
}

// PutEvaluations stores evaluation results from an AWS Lambda function for a
// config rule. Each result is retained per-(rule, resource) so the compliance
// detail APIs can return real per-resource outcomes.
func (b *InMemoryBackend) PutEvaluations(results []EvaluationResult) error {
	b.mu.Lock("PutEvaluations")
	defer b.mu.Unlock()

	now := float64(time.Now().Unix())

	for _, r := range results {
		b.recordEvaluationLocked(r.ConfigRuleName, r.ResourceType, r.ResourceID, r.ComplianceType, r.Annotation, now)
	}

	return nil
}

// PutExternalEvaluation stores a single external evaluation result per-resource.
func (b *InMemoryBackend) PutExternalEvaluation(result EvaluationResult) error {
	b.mu.Lock("PutExternalEvaluation")
	defer b.mu.Unlock()

	b.recordEvaluationLocked(
		result.ConfigRuleName,
		result.ResourceType,
		result.ResourceID,
		result.ComplianceType,
		result.Annotation,
		float64(time.Now().Unix()),
	)

	return nil
}

// GetCustomRulePolicy returns the policy text for the given custom rule.
func (b *InMemoryBackend) GetCustomRulePolicy(ruleName string) string {
	b.mu.RLock("GetCustomRulePolicy")
	defer b.mu.RUnlock()

	return b.customRulePolicies[ruleName]
}

// GetAggregateComplianceDetailsByConfigRule returns an empty list (intentionally minimal stub).
func (b *InMemoryBackend) GetAggregateComplianceDetailsByConfigRule() []any { return []any{} }

// GetAggregateConfigRuleComplianceSummary returns an empty summary (intentionally minimal stub).
func (b *InMemoryBackend) GetAggregateConfigRuleComplianceSummary() []any { return []any{} }

// DescribeAggregateComplianceByConfigRules returns compliance by rule using ruleEvaluations.
func (b *InMemoryBackend) DescribeAggregateComplianceByConfigRules() []any {
	b.mu.RLock("DescribeAggregateComplianceByConfigRules")
	defer b.mu.RUnlock()

	out := make([]any, 0, len(b.ruleEvaluations))

	for name, ct := range b.ruleEvaluations {
		out = append(out, ComplianceByConfigRule{
			ConfigRuleName: name,
			Compliance:     ComplianceResult{ComplianceType: ct},
		})
	}

	return out
}

// GetComplianceSummaryByResourceType returns compliant/non-compliant resource
// counts grouped by resource type, derived from the same per-(rule, resource)
// evaluation state (b.ruleResourceEvals) that rolls up into b.ruleEvaluations
// for DescribeAggregateComplianceByConfigRules. A resource counts as
// NON_COMPLIANT if any rule evaluated it as such, else COMPLIANT. When
// resourceTypes is non-empty, only those types are included.
func (b *InMemoryBackend) GetComplianceSummaryByResourceType(
	resourceTypes []string,
) []ComplianceSummaryByResourceType {
	b.mu.RLock("GetComplianceSummaryByResourceType")
	defer b.mu.RUnlock()

	nonCompliant := resourceComplianceByType(b.ruleResourceEvals.All(), resourceTypes)

	resourceTypesSeen := make([]string, 0, len(nonCompliant))
	for rt := range nonCompliant {
		resourceTypesSeen = append(resourceTypesSeen, rt)
	}

	sort.Strings(resourceTypesSeen)

	out := make([]ComplianceSummaryByResourceType, 0, len(resourceTypesSeen))
	for _, rt := range resourceTypesSeen {
		out = append(out, summarizeResourceType(rt, nonCompliant[rt]))
	}

	return out
}

// resourceComplianceByType walks per-(rule, resource) evaluations and, for
// every resource matching the optional resourceTypes filter, records whether
// any rule found it NON_COMPLIANT (true) or only COMPLIANT/other (false).
// evals is the flat contents of b.ruleResourceEvals (formerly a nested
// map[string]map[string]*StoredEvaluation keyed by rule then resource; the
// flattened store.Table has no rule-name grouping at this call site's level,
// but nothing here ever used the rule name, only ResourceType/ResourceID/
// ComplianceType off each StoredEvaluation, so a flat slice is equivalent).
func resourceComplianceByType(
	evals []*StoredEvaluation,
	resourceTypes []string,
) map[string]map[string]bool {
	filter := make(map[string]struct{}, len(resourceTypes))
	for _, rt := range resourceTypes {
		filter[rt] = struct{}{}
	}

	nonCompliant := make(map[string]map[string]bool)

	for _, e := range evals {
		if len(filter) > 0 {
			if _, ok := filter[e.ResourceType]; !ok {
				continue
			}
		}

		if nonCompliant[e.ResourceType] == nil {
			nonCompliant[e.ResourceType] = make(map[string]bool)
		}

		nonCompliant[e.ResourceType][e.ResourceID] =
			nonCompliant[e.ResourceType][e.ResourceID] || e.ComplianceType == complianceNonCompliant
	}

	return nonCompliant
}

// summarizeResourceType counts compliant/non-compliant resources for one
// resource type into the wire-shaped summary.
func summarizeResourceType(resourceType string, resources map[string]bool) ComplianceSummaryByResourceType {
	var compliantCount, nonCompliantCount int32

	for _, isNonCompliant := range resources {
		if isNonCompliant {
			nonCompliantCount++
		} else {
			compliantCount++
		}
	}

	return ComplianceSummaryByResourceType{
		ResourceType: resourceType,
		ComplianceSummary: ComplianceSummaryDetail{
			CompliantResourceCount:    ResourceCount{CappedCount: compliantCount},
			NonCompliantResourceCount: ResourceCount{CappedCount: nonCompliantCount},
		},
	}
}

// DescribeComplianceByResource returns an empty list (intentionally minimal stub).
func (b *InMemoryBackend) DescribeComplianceByResource() []any { return []any{} }
