package awsconfig

import "fmt"

// This file contains stub backend methods for AWS Config operations that
// are acknowledged but not yet deeply implemented. All methods follow the
// gopherstack convention of returning empty/success results.

// DeletePendingAggregationRequest is a no-op stub.
func (b *InMemoryBackend) DeletePendingAggregationRequest(_, _ string) error { return nil }

// DeleteResourceConfig removes the discovered configuration item for a
// resource from b.resourceConfigs. Deletion is idempotent (no error for an
// already-absent resource), matching real AWS Config's DeleteResourceConfig
// error model (verified against aws-sdk-go-v2/service/configservice's
// deserializer: only NoRunningConfigurationRecorderException/
// ValidationException, never a not-found exception). The resource's history
// (b.resourceHistory) is intentionally preserved, mirroring AWS which keeps
// prior configuration history entries after a resource is deleted.
func (b *InMemoryBackend) DeleteResourceConfig(resourceType, resourceID string) error {
	b.mu.Lock("DeleteResourceConfig")
	defer b.mu.Unlock()

	b.resourceConfigs.Delete(resourceConfigItemKey(resourceType, resourceID))

	return nil
}

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

	all := b.aggregators.All()
	out := make([]ConfigurationAggregator, 0, len(all))

	for _, a := range all {
		out = append(out, *a)
	}

	return out
}

// DescribeConformancePacks returns all conformance packs.
func (b *InMemoryBackend) DescribeConformancePacks() []ConformancePack {
	b.mu.RLock("DescribeConformancePacks")
	defer b.mu.RUnlock()

	all := b.conformancePacks.All()
	out := make([]ConformancePack, 0, len(all))

	for _, p := range all {
		out = append(out, *p)
	}

	return out
}

// DescribeOrganizationConfigRules returns all organization config rules.
func (b *InMemoryBackend) DescribeOrganizationConfigRules() []OrganizationConfigRule {
	b.mu.RLock("DescribeOrganizationConfigRules")
	defer b.mu.RUnlock()

	all := b.orgConfigRules.All()
	out := make([]OrganizationConfigRule, 0, len(all))

	for _, r := range all {
		out = append(out, *r)
	}

	return out
}

// DescribeOrganizationConformancePacks returns all organization conformance packs.
func (b *InMemoryBackend) DescribeOrganizationConformancePacks() []OrganizationConformancePack {
	b.mu.RLock("DescribeOrganizationConformancePacks")
	defer b.mu.RUnlock()

	all := b.orgConformancePacks.All()
	out := make([]OrganizationConformancePack, 0, len(all))

	for _, p := range all {
		out = append(out, *p)
	}

	return out
}

// DescribePendingAggregationRequests returns an empty list.
func (b *InMemoryBackend) DescribePendingAggregationRequests() []any {
	return []any{}
}

// DisassociateResourceTypes removes resourceTypes from a configuration
// recorder's RecordingGroup, the inverse of AssociateResourceTypes.
// recorderARN may be the recorder's bare name or its full ARN. Errors with
// ErrNotFound (wire type NoSuchConfigurationRecorderException) when no
// matching recorder exists, matching the real API's declared error model
// (verified against aws-sdk-go-v2/service/configservice's
// DisassociateResourceTypes deserializer).
func (b *InMemoryBackend) DisassociateResourceTypes(recorderARN string, resourceTypes []string) error {
	if recorderARN == "" {
		return fmt.Errorf("%w: ConfigurationRecorderArn is required", ErrValidation)
	}

	b.mu.Lock("DisassociateResourceTypes")
	defer b.mu.Unlock()

	name := recorderNameFromArn(recorderARN)

	r, ok := b.recorders.Get(name)
	if !ok {
		return fmt.Errorf("%w: %s", ErrNotFound, recorderARN)
	}

	if r.RecordingGroup != nil {
		r.RecordingGroup.ResourceTypes = removeResourceTypes(r.RecordingGroup.ResourceTypes, resourceTypes)
	}

	return nil
}

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

	all := b.recorders.All()
	out := make([]ConfigurationRecorderSummary, 0, len(all))

	for _, r := range all {
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

	all := b.storedQueries.All()
	out := make([]StoredQueryMetadata, 0, len(all))

	for _, q := range all {
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

	b.storedQueries.Put(&StoredQuery{QueryName: name})

	return nil
}

// GetConfigRuleComplianceType returns the rolled-up compliance type for a config
// rule after evaluation, or empty string if no evaluation has run for that rule yet.
func (b *InMemoryBackend) GetConfigRuleComplianceType(ruleName string) string {
	b.mu.RLock("GetConfigRuleComplianceType")
	defer b.mu.RUnlock()

	return b.ruleEvaluations[ruleName]
}
