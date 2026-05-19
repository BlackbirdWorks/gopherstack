package awsconfig

// This file contains stub backend methods for AWS Config operations that
// are acknowledged but not yet deeply implemented. All methods follow the
// gopherstack convention of returning empty/success results.

const complianceTypeCompliant = "COMPLIANT"

// DeletePendingAggregationRequest is a no-op stub.
func (b *InMemoryBackend) DeletePendingAggregationRequest(_, _ string) error { return nil }

// DeleteResourceConfig is a no-op stub.
func (b *InMemoryBackend) DeleteResourceConfig(_, _ string) error { return nil }

// DeleteServiceLinkedConfigurationRecorder is a no-op stub.
func (b *InMemoryBackend) DeleteServiceLinkedConfigurationRecorder(_ string) error { return nil }

// DeliverConfigSnapshot is a no-op stub.
func (b *InMemoryBackend) DeliverConfigSnapshot(_ string) error { return nil }

// DescribeAggregateComplianceByConformancePacks returns an empty list.
func (b *InMemoryBackend) DescribeAggregateComplianceByConformancePacks() []any {
	return []any{}
}

// DescribeConfigurationAggregatorSourcesStatus returns an empty list.
func (b *InMemoryBackend) DescribeConfigurationAggregatorSourcesStatus() []any {
	return []any{}
}

// DescribeConfigurationAggregators returns all aggregators sorted by name.
func (b *InMemoryBackend) DescribeConfigurationAggregators() []ConfigurationAggregator {
	b.mu.RLock("DescribeConfigurationAggregators")
	defer b.mu.RUnlock()

	out := make([]ConfigurationAggregator, 0, len(b.aggregators))
	for _, a := range b.aggregators {
		out = append(out, *a)
	}

	return out
}

// DescribeConformancePacks returns all conformance packs.
func (b *InMemoryBackend) DescribeConformancePacks() []ConformancePack {
	b.mu.RLock("DescribeConformancePacks")
	defer b.mu.RUnlock()

	out := make([]ConformancePack, 0, len(b.conformancePacks))
	for _, p := range b.conformancePacks {
		out = append(out, *p)
	}

	return out
}

// DescribeOrganizationConfigRules returns all organization config rules.
func (b *InMemoryBackend) DescribeOrganizationConfigRules() []OrganizationConfigRule {
	b.mu.RLock("DescribeOrganizationConfigRules")
	defer b.mu.RUnlock()

	out := make([]OrganizationConfigRule, 0, len(b.orgConfigRules))
	for _, r := range b.orgConfigRules {
		out = append(out, *r)
	}

	return out
}

// DescribeOrganizationConformancePacks returns all organization conformance packs.
func (b *InMemoryBackend) DescribeOrganizationConformancePacks() []OrganizationConformancePack {
	b.mu.RLock("DescribeOrganizationConformancePacks")
	defer b.mu.RUnlock()

	out := make([]OrganizationConformancePack, 0, len(b.orgConformancePacks))
	for _, p := range b.orgConformancePacks {
		out = append(out, *p)
	}

	return out
}

// DescribePendingAggregationRequests returns an empty list.
func (b *InMemoryBackend) DescribePendingAggregationRequests() []any {
	return []any{}
}

// DisassociateResourceTypes is a no-op stub.
func (b *InMemoryBackend) DisassociateResourceTypes(_, _ string) error { return nil }

// GetDiscoveredResourceCounts returns zero counts.
func (b *InMemoryBackend) GetDiscoveredResourceCounts() int64 { return 0 }

// GetOrganizationConfigRuleDetailedStatus returns an empty list.
func (b *InMemoryBackend) GetOrganizationConfigRuleDetailedStatus() []any {
	return []any{}
}

// GetOrganizationConformancePackDetailedStatus returns an empty list.
func (b *InMemoryBackend) GetOrganizationConformancePackDetailedStatus() []any {
	return []any{}
}

// GetResourceEvaluationSummary returns an empty summary.
func (b *InMemoryBackend) GetResourceEvaluationSummary() *BaseConfigurationItem {
	return &BaseConfigurationItem{}
}

// ListAggregateDiscoveredResources returns an empty list.
func (b *InMemoryBackend) ListAggregateDiscoveredResources() []any {
	return []any{}
}

// ListConfigurationRecorders returns all recorder names.
func (b *InMemoryBackend) ListConfigurationRecorders() []string {
	b.mu.RLock("ListConfigurationRecorders")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.recorders))
	for name := range b.recorders {
		names = append(names, name)
	}

	return names
}

// ListConformancePackComplianceScores returns an empty list.
func (b *InMemoryBackend) ListConformancePackComplianceScores() []any {
	return []any{}
}

// ListResourceEvaluations returns an empty list.
func (b *InMemoryBackend) ListResourceEvaluations() []any { return []any{} }

// ListStoredQueries returns all stored query names.
func (b *InMemoryBackend) ListStoredQueries() []string {
	b.mu.RLock("ListStoredQueries")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.storedQueries))
	for name := range b.storedQueries {
		names = append(names, name)
	}

	return names
}

// PutServiceLinkedConfigurationRecorder is a no-op stub.
func (b *InMemoryBackend) PutServiceLinkedConfigurationRecorder() error { return nil }

// PutStoredQuery stores a query by name.
func (b *InMemoryBackend) PutStoredQuery(name string) error {
	if name == "" {
		return nil
	}

	b.mu.Lock("PutStoredQuery")
	defer b.mu.Unlock()

	b.storedQueries[name] = &StoredQuery{QueryName: name}

	return nil
}

// StartConfigRulesEvaluation triggers an evaluation run for all config rules.
// It marks every rule as COMPLIANT so that GetComplianceDetailsByConfigRule can return results.
func (b *InMemoryBackend) StartConfigRulesEvaluation() error {
	b.mu.Lock("StartConfigRulesEvaluation")
	defer b.mu.Unlock()

	for name := range b.configRules {
		b.ruleEvaluations[name] = complianceTypeCompliant
	}

	return nil
}

// GetConfigRuleComplianceType returns the compliance type for a config rule after evaluation,
// or empty string if no evaluation has run for that rule yet.
func (b *InMemoryBackend) GetConfigRuleComplianceType(ruleName string) string {
	b.mu.RLock("GetConfigRuleComplianceType")
	defer b.mu.RUnlock()

	return b.ruleEvaluations[ruleName]
}

// StartResourceEvaluation returns a stub evaluation ID.
func (b *InMemoryBackend) StartResourceEvaluation() string { return "eval-stub" }
