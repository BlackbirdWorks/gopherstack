package awsconfig

import "fmt"

// This file contains stub backend methods for AWS Config operations that
// are acknowledged but not yet deeply implemented. All methods follow the
// gopherstack convention of returning empty/success results.

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
func (b *InMemoryBackend) DisassociateResourceTypes(_ string, _ []string) error { return nil }

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

// ListAggregateDiscoveredResources returns an empty list.
func (b *InMemoryBackend) ListAggregateDiscoveredResources() []any {
	return []any{}
}

// ListConfigurationRecorders returns summaries of all configuration recorders.
func (b *InMemoryBackend) ListConfigurationRecorders() []ConfigurationRecorderSummary {
	b.mu.RLock("ListConfigurationRecorders")
	defer b.mu.RUnlock()

	out := make([]ConfigurationRecorderSummary, 0, len(b.recorders))

	for _, r := range b.recorders {
		arn := fmt.Sprintf(
			"arn:aws:config:%s:%s:config-recorder/%s",
			b.region, b.accountID, r.Name,
		)
		out = append(out, ConfigurationRecorderSummary{
			Arn:            arn,
			Name:           r.Name,
			RecordingScope: "INTERNAL",
		})
	}

	return out
}

// ListConformancePackComplianceScores returns an empty list.
func (b *InMemoryBackend) ListConformancePackComplianceScores() []any {
	return []any{}
}

// ListStoredQueries returns metadata for all stored queries.
func (b *InMemoryBackend) ListStoredQueries() []StoredQueryMetadata {
	b.mu.RLock("ListStoredQueries")
	defer b.mu.RUnlock()

	out := make([]StoredQueryMetadata, 0, len(b.storedQueries))

	for _, q := range b.storedQueries {
		arn := fmt.Sprintf(
			"arn:aws:config:%s:%s:stored-query/%s",
			b.region, b.accountID, q.QueryName,
		)
		out = append(out, StoredQueryMetadata{
			QueryArn:  arn,
			QueryID:   q.QueryID,
			QueryName: q.QueryName,
		})
	}

	return out
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

// GetConfigRuleComplianceType returns the rolled-up compliance type for a config
// rule after evaluation, or empty string if no evaluation has run for that rule yet.
func (b *InMemoryBackend) GetConfigRuleComplianceType(ruleName string) string {
	b.mu.RLock("GetConfigRuleComplianceType")
	defer b.mu.RUnlock()

	return b.ruleEvaluations[ruleName]
}
