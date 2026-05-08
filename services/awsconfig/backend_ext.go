package awsconfig

// This file contains stub backend methods for AWS Config operations that
// are acknowledged but not yet deeply implemented. All methods follow the
// gopherstack convention of returning empty/success results.

// DeletePendingAggregationRequest is a no-op stub.
func (b *InMemoryBackend) DeletePendingAggregationRequest(_, _ string) error { return nil }

// DeleteRemediationConfiguration is a no-op stub.
func (b *InMemoryBackend) DeleteRemediationConfiguration(_ string) error { return nil }

// DeleteRemediationExceptions is a no-op stub.
func (b *InMemoryBackend) DeleteRemediationExceptions(_, _ string) error { return nil }

// DeleteResourceConfig is a no-op stub.
func (b *InMemoryBackend) DeleteResourceConfig(_, _ string) error { return nil }

// DeleteRetentionConfiguration is a no-op stub.
func (b *InMemoryBackend) DeleteRetentionConfiguration(_ int32) error { return nil }

// DeleteServiceLinkedConfigurationRecorder is a no-op stub.
func (b *InMemoryBackend) DeleteServiceLinkedConfigurationRecorder(_ string) error { return nil }

// DeleteStoredQuery is a no-op stub.
func (b *InMemoryBackend) DeleteStoredQuery(_ string) error { return nil }

// DeliverConfigSnapshot is a no-op stub.
func (b *InMemoryBackend) DeliverConfigSnapshot(_ string) error { return nil }

// DescribeAggregateComplianceByConfigRules returns an empty list.
func (b *InMemoryBackend) DescribeAggregateComplianceByConfigRules() []any {
	return []any{}
}

// DescribeAggregateComplianceByConformancePacks returns an empty list.
func (b *InMemoryBackend) DescribeAggregateComplianceByConformancePacks() []any {
	return []any{}
}

// DescribeComplianceByConfigRule returns an empty compliance list.
func (b *InMemoryBackend) DescribeComplianceByConfigRule() []any {
	return []any{}
}

// DescribeComplianceByResource returns an empty compliance list.
func (b *InMemoryBackend) DescribeComplianceByResource() []any {
	return []any{}
}

// DescribeConfigRuleEvaluationStatus returns an empty list.
func (b *InMemoryBackend) DescribeConfigRuleEvaluationStatus() []any {
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

// DescribeConformancePackCompliance returns an empty list.
func (b *InMemoryBackend) DescribeConformancePackCompliance() []any {
	return []any{}
}

// DescribeConformancePackStatus returns an empty list.
func (b *InMemoryBackend) DescribeConformancePackStatus() []any {
	return []any{}
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

// DescribeDeliveryChannelStatus returns an empty list.
func (b *InMemoryBackend) DescribeDeliveryChannelStatus() []any {
	return []any{}
}

// DescribeOrganizationConfigRuleStatuses returns an empty list.
func (b *InMemoryBackend) DescribeOrganizationConfigRuleStatuses() []any {
	return []any{}
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

// DescribeOrganizationConformancePackStatuses returns an empty list.
func (b *InMemoryBackend) DescribeOrganizationConformancePackStatuses() []any {
	return []any{}
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

// DescribeRemediationConfigurations returns an empty list.
func (b *InMemoryBackend) DescribeRemediationConfigurations() []any {
	return []any{}
}

// DescribeRemediationExceptions returns an empty list.
func (b *InMemoryBackend) DescribeRemediationExceptions() []any {
	return []any{}
}

// DescribeRemediationExecutionStatus returns an empty list.
func (b *InMemoryBackend) DescribeRemediationExecutionStatus() []any {
	return []any{}
}

// DescribeRetentionConfigurations returns an empty list.
func (b *InMemoryBackend) DescribeRetentionConfigurations() []any {
	return []any{}
}

// DisassociateResourceTypes is a no-op stub.
func (b *InMemoryBackend) DisassociateResourceTypes(_, _ string) error { return nil }

// GetAggregateComplianceDetailsByConfigRule returns an empty list.
func (b *InMemoryBackend) GetAggregateComplianceDetailsByConfigRule() []any {
	return []any{}
}

// GetAggregateConfigRuleComplianceSummary returns an empty summary.
func (b *InMemoryBackend) GetAggregateConfigRuleComplianceSummary() []any {
	return []any{}
}

// GetAggregateConformancePackComplianceSummary returns an empty summary.
func (b *InMemoryBackend) GetAggregateConformancePackComplianceSummary() []any {
	return []any{}
}

// GetAggregateDiscoveredResourceCounts returns zero counts.
func (b *InMemoryBackend) GetAggregateDiscoveredResourceCounts() int32 { return 0 }

// GetAggregateResourceConfig returns an empty config item.
func (b *InMemoryBackend) GetAggregateResourceConfig() *BaseConfigurationItem {
	return &BaseConfigurationItem{}
}

// GetComplianceSummaryByConfigRule returns an empty summary.
func (b *InMemoryBackend) GetComplianceSummaryByConfigRule() []any {
	return []any{}
}

// GetComplianceSummaryByResourceType returns an empty summary.
func (b *InMemoryBackend) GetComplianceSummaryByResourceType() []any {
	return []any{}
}

// GetConformancePackComplianceDetails returns an empty list.
func (b *InMemoryBackend) GetConformancePackComplianceDetails() []any {
	return []any{}
}

// GetConformancePackComplianceSummary returns an empty list.
func (b *InMemoryBackend) GetConformancePackComplianceSummary() []any {
	return []any{}
}

// GetCustomRulePolicy returns an empty policy string.
func (b *InMemoryBackend) GetCustomRulePolicy(_ string) string { return "" }

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

// GetOrganizationCustomRulePolicy returns an empty policy string.
func (b *InMemoryBackend) GetOrganizationCustomRulePolicy(_ string) string { return "" }

// GetResourceConfigHistory returns an empty list.
func (b *InMemoryBackend) GetResourceConfigHistory() []any { return []any{} }

// GetResourceEvaluationSummary returns an empty summary.
func (b *InMemoryBackend) GetResourceEvaluationSummary() *BaseConfigurationItem {
	return &BaseConfigurationItem{}
}

// GetStoredQuery returns nil (not found).
func (b *InMemoryBackend) GetStoredQuery(_ string) *StoredQuery { return nil }

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

// ListDiscoveredResources returns an empty list.
func (b *InMemoryBackend) ListDiscoveredResources() []any { return []any{} }

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

// ListTagsForResource returns an empty tag list.
func (b *InMemoryBackend) ListTagsForResource(_ string) []Tag { return []Tag{} }

// PutEvaluations is a no-op stub.
func (b *InMemoryBackend) PutEvaluations() error { return nil }

// PutExternalEvaluation is a no-op stub.
func (b *InMemoryBackend) PutExternalEvaluation() error { return nil }

// PutRemediationConfigurations is a no-op stub.
func (b *InMemoryBackend) PutRemediationConfigurations() error { return nil }

// PutRemediationExceptions is a no-op stub.
func (b *InMemoryBackend) PutRemediationExceptions() error { return nil }

// PutResourceConfig is a no-op stub.
func (b *InMemoryBackend) PutResourceConfig() error { return nil }

// PutRetentionConfiguration is a no-op stub.
func (b *InMemoryBackend) PutRetentionConfiguration() error { return nil }

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

// SelectAggregateResourceConfig returns an empty result.
func (b *InMemoryBackend) SelectAggregateResourceConfig() []any {
	return []any{}
}

// SelectResourceConfig returns an empty result.
func (b *InMemoryBackend) SelectResourceConfig() []any { return []any{} }

// StartConfigRulesEvaluation triggers an evaluation run for all config rules.
// It marks every rule as COMPLIANT so that GetComplianceDetailsByConfigRule can return results.
func (b *InMemoryBackend) StartConfigRulesEvaluation() error {
	b.mu.Lock("StartConfigRulesEvaluation")
	defer b.mu.Unlock()

	for name := range b.configRules {
		b.ruleEvaluations[name] = "COMPLIANT"
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

// StartRemediationExecution is a no-op stub.
func (b *InMemoryBackend) StartRemediationExecution() error { return nil }

// StartResourceEvaluation returns a stub evaluation ID.
func (b *InMemoryBackend) StartResourceEvaluation() string { return "eval-stub" }

// TagResource is a no-op stub.
func (b *InMemoryBackend) TagResource(_, _ string) error { return nil }

// UntagResource is a no-op stub.
func (b *InMemoryBackend) UntagResource(_, _ string) error { return nil }
