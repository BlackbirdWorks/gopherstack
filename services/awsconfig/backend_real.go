package awsconfig

import "fmt"

const (
	orgRuleStatusCreateSuccessful = "CREATE_SUCCESSFUL"
	conformancePackStateComplete  = "COMPLETE"
)

// --- Group 1: Tag operations ---

// TagResource adds tags to the resource identified by arn.
func (b *InMemoryBackend) TagResource(arn string, tags []Tag) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	existing := b.resourceTags[arn]
	// Build a map of existing keys for dedup / update.
	idx := make(map[string]int, len(existing))
	for i, t := range existing {
		idx[t.Key] = i
	}

	for _, t := range tags {
		if i, ok := idx[t.Key]; ok {
			existing[i].Value = t.Value
		} else {
			idx[t.Key] = len(existing)
			existing = append(existing, t)
		}
	}

	b.resourceTags[arn] = existing

	return nil
}

// UntagResource removes tags from the resource identified by arn.
func (b *InMemoryBackend) UntagResource(arn string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	remove := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		remove[k] = struct{}{}
	}

	existing := b.resourceTags[arn]
	filtered := existing[:0]

	for _, t := range existing {
		if _, skip := remove[t.Key]; !skip {
			filtered = append(filtered, t)
		}
	}

	b.resourceTags[arn] = filtered

	return nil
}

// ListTagsForResource returns all tags for the resource identified by arn.
func (b *InMemoryBackend) ListTagsForResource(arn string) []Tag {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	tags := b.resourceTags[arn]
	if len(tags) == 0 {
		return []Tag{}
	}

	out := make([]Tag, len(tags))
	copy(out, tags)

	return out
}

// --- Group 2: StoredQuery operations ---

// GetStoredQuery returns the stored query with the given name, or nil if not found.
func (b *InMemoryBackend) GetStoredQuery(name string) *StoredQuery {
	b.mu.RLock("GetStoredQuery")
	defer b.mu.RUnlock()

	q, ok := b.storedQueries[name]
	if !ok {
		return nil
	}

	cp := *q

	return &cp
}

// DeleteStoredQuery removes the stored query with the given name.
func (b *InMemoryBackend) DeleteStoredQuery(name string) error {
	b.mu.Lock("DeleteStoredQuery")
	defer b.mu.Unlock()

	delete(b.storedQueries, name)

	return nil
}

// --- Group 3: RetentionConfiguration operations ---

// PutRetentionConfiguration creates or updates a retention configuration.
func (b *InMemoryBackend) PutRetentionConfiguration(name string, days int32) error {
	if name == "" {
		return fmt.Errorf("%w: RetentionConfiguration name is required", ErrValidation)
	}

	b.mu.Lock("PutRetentionConfiguration")
	defer b.mu.Unlock()

	b.retentionConfigs[name] = &RetentionConfiguration{
		Name:                  name,
		RetentionPeriodInDays: days,
	}

	return nil
}

// DescribeRetentionConfigurations returns all retention configurations.
func (b *InMemoryBackend) DescribeRetentionConfigurations() []RetentionConfiguration {
	b.mu.RLock("DescribeRetentionConfigurations")
	defer b.mu.RUnlock()

	out := make([]RetentionConfiguration, 0, len(b.retentionConfigs))
	for _, rc := range b.retentionConfigs {
		out = append(out, *rc)
	}

	return out
}

// DeleteRetentionConfiguration removes a retention configuration by name.
func (b *InMemoryBackend) DeleteRetentionConfiguration(name string) error {
	b.mu.Lock("DeleteRetentionConfiguration")
	defer b.mu.Unlock()

	delete(b.retentionConfigs, name)

	return nil
}

// --- Group 4: RemediationConfiguration operations ---

// PutRemediationConfigurations stores remediation configurations keyed by rule name.
func (b *InMemoryBackend) PutRemediationConfigurations(configs []RemediationConfiguration) error {
	b.mu.Lock("PutRemediationConfigurations")
	defer b.mu.Unlock()

	for i := range configs {
		cp := configs[i]
		b.remediationConfigs[cp.ConfigRuleName] = &cp
	}

	return nil
}

// DescribeRemediationConfigurations returns remediation configurations for the given rule names.
// If ruleNames is empty, all configurations are returned.
func (b *InMemoryBackend) DescribeRemediationConfigurations(ruleNames []string) []RemediationConfiguration {
	b.mu.RLock("DescribeRemediationConfigurations")
	defer b.mu.RUnlock()

	if len(ruleNames) == 0 {
		out := make([]RemediationConfiguration, 0, len(b.remediationConfigs))
		for _, rc := range b.remediationConfigs {
			out = append(out, *rc)
		}

		return out
	}

	out := make([]RemediationConfiguration, 0, len(ruleNames))

	for _, name := range ruleNames {
		if rc, ok := b.remediationConfigs[name]; ok {
			out = append(out, *rc)
		}
	}

	return out
}

// PutRemediationExceptions stores a remediation exception for a rule + resource.
func (b *InMemoryBackend) PutRemediationExceptions(ruleName, resourceType, resourceID string) error {
	b.mu.Lock("PutRemediationExceptions")
	defer b.mu.Unlock()

	ex := RemediationException{
		ConfigRuleName: ruleName,
		ResourceType:   resourceType,
		ResourceID:     resourceID,
	}

	existing := b.remediationExceptions[ruleName]

	for i, e := range existing {
		if e.ResourceType == resourceType && e.ResourceID == resourceID {
			existing[i] = ex

			b.remediationExceptions[ruleName] = existing

			return nil
		}
	}

	b.remediationExceptions[ruleName] = append(existing, ex)

	return nil
}

// DescribeRemediationExceptions returns all remediation exceptions for the given rule name.
func (b *InMemoryBackend) DescribeRemediationExceptions(ruleName string) []RemediationException {
	b.mu.RLock("DescribeRemediationExceptions")
	defer b.mu.RUnlock()

	exs := b.remediationExceptions[ruleName]
	if len(exs) == 0 {
		return []RemediationException{}
	}

	out := make([]RemediationException, len(exs))
	copy(out, exs)

	return out
}

// DeleteRemediationConfiguration removes the remediation configuration for the given rule.
func (b *InMemoryBackend) DeleteRemediationConfiguration(ruleName string) error {
	b.mu.Lock("DeleteRemediationConfiguration")
	defer b.mu.Unlock()

	delete(b.remediationConfigs, ruleName)

	return nil
}

// DeleteRemediationExceptions removes an exception for a rule + resource.
func (b *InMemoryBackend) DeleteRemediationExceptions(ruleName, resourceID string) error {
	b.mu.Lock("DeleteRemediationExceptions")
	defer b.mu.Unlock()

	existing := b.remediationExceptions[ruleName]
	filtered := existing[:0]

	for _, e := range existing {
		if e.ResourceID != resourceID {
			filtered = append(filtered, e)
		}
	}

	b.remediationExceptions[ruleName] = filtered

	return nil
}

// StartRemediationExecution is an intentionally minimal no-op stub.
func (b *InMemoryBackend) StartRemediationExecution() error { return nil }

// DescribeRemediationExecutionStatus returns an empty list (intentionally minimal stub).
func (b *InMemoryBackend) DescribeRemediationExecutionStatus() []any { return []any{} }

// --- Group 5: ConfigRule evaluation operations ---

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
			ct = "NOT_APPLICABLE"
		}

		out = append(out, ComplianceByConfigRule{
			ConfigRuleName: name,
			Compliance:     ComplianceResult{ComplianceType: ct},
		})
	}

	return out
}

// GetComplianceSummaryByConfigRule returns a stub compliance summary.
func (b *InMemoryBackend) GetComplianceSummaryByConfigRule() []ComplianceSummary {
	return []ComplianceSummary{}
}

// PutEvaluations stores evaluation results from an AWS Lambda function for a config rule.
func (b *InMemoryBackend) PutEvaluations(results []EvaluationResult) error {
	b.mu.Lock("PutEvaluations")
	defer b.mu.Unlock()

	for _, r := range results {
		b.ruleEvaluations[r.ConfigRuleName] = r.ComplianceType
	}

	return nil
}

// PutExternalEvaluation stores a single external evaluation result.
func (b *InMemoryBackend) PutExternalEvaluation(result EvaluationResult) error {
	b.mu.Lock("PutExternalEvaluation")
	defer b.mu.Unlock()

	b.ruleEvaluations[result.ConfigRuleName] = result.ComplianceType

	return nil
}

// --- Group 6: Delivery channel status ---

// DescribeDeliveryChannelStatus returns statuses for delivery channels.
// If names is empty, all channels are returned.
func (b *InMemoryBackend) DescribeDeliveryChannelStatus(names []string) []DeliveryChannelStatus {
	b.mu.RLock("DescribeDeliveryChannelStatus")
	defer b.mu.RUnlock()

	var channelNames []string

	if len(names) == 0 {
		for name := range b.channels {
			channelNames = append(channelNames, name)
		}
	} else {
		for _, name := range names {
			if _, ok := b.channels[name]; ok {
				channelNames = append(channelNames, name)
			}
		}
	}

	out := make([]DeliveryChannelStatus, 0, len(channelNames))

	for _, name := range channelNames {
		out = append(out, DeliveryChannelStatus{
			Name:                      name,
			ConfigHistoryDeliveryInfo: &DeliveryChannelStatusInfo{LastStatus: "SUCCESS"},
			ConfigStreamDeliveryInfo:  &DeliveryChannelStatusInfo{LastStatus: "SUCCESS"},
		})
	}

	return out
}

// --- Group 7: ConformancePack status ---

// DescribeConformancePackStatus returns conformance pack statuses.
// If names is empty, all packs are returned.
func (b *InMemoryBackend) DescribeConformancePackStatus(names []string) []ConformancePackStatus {
	b.mu.RLock("DescribeConformancePackStatus")
	defer b.mu.RUnlock()

	if len(names) == 0 {
		out := make([]ConformancePackStatus, 0, len(b.conformancePacks))
		for _, p := range b.conformancePacks {
			out = append(out, ConformancePackStatus{
				ConformancePackName:  p.ConformancePackName,
				ConformancePackState: conformancePackStateComplete,
				ConformancePackArn: fmt.Sprintf(
					"arn:aws:config:%s:%s:conformance-pack/%s",
					b.region, b.accountID, p.ConformancePackName,
				),
			})
		}

		return out
	}

	out := make([]ConformancePackStatus, 0, len(names))

	for _, name := range names {
		if p, ok := b.conformancePacks[name]; ok {
			out = append(out, ConformancePackStatus{
				ConformancePackName:  p.ConformancePackName,
				ConformancePackState: conformancePackStateComplete,
				ConformancePackArn: fmt.Sprintf(
					"arn:aws:config:%s:%s:conformance-pack/%s",
					b.region, b.accountID, p.ConformancePackName,
				),
			})
		}
	}

	return out
}

// DescribeConformancePackCompliance returns compliance items for a conformance pack.
// Returns an empty list (intentionally minimal stub).
func (b *InMemoryBackend) DescribeConformancePackCompliance(_ string) []ConformancePackComplianceItem {
	return []ConformancePackComplianceItem{}
}

// --- Group 8: Org rule/pack status ---

// DescribeOrganizationConfigRuleStatuses returns statuses for organization config rules.
// If names is empty, all rules are returned.
func (b *InMemoryBackend) DescribeOrganizationConfigRuleStatuses(names []string) []OrganizationConfigRuleStatus {
	b.mu.RLock("DescribeOrganizationConfigRuleStatuses")
	defer b.mu.RUnlock()

	if len(names) == 0 {
		out := make([]OrganizationConfigRuleStatus, 0, len(b.orgConfigRules))
		for _, r := range b.orgConfigRules {
			out = append(out, OrganizationConfigRuleStatus{
				OrganizationConfigRuleName: r.OrganizationConfigRuleName,
				OrganizationRuleStatus:     orgRuleStatusCreateSuccessful,
			})
		}

		return out
	}

	out := make([]OrganizationConfigRuleStatus, 0, len(names))

	for _, name := range names {
		if r, ok := b.orgConfigRules[name]; ok {
			out = append(out, OrganizationConfigRuleStatus{
				OrganizationConfigRuleName: r.OrganizationConfigRuleName,
				OrganizationRuleStatus:     orgRuleStatusCreateSuccessful,
			})
		}
	}

	return out
}

// DescribeOrganizationConformancePackStatuses returns statuses for organization conformance packs.
// If names is empty, all packs are returned.
func (b *InMemoryBackend) DescribeOrganizationConformancePackStatuses(
	names []string,
) []OrganizationConformancePackStatus {
	b.mu.RLock("DescribeOrganizationConformancePackStatuses")
	defer b.mu.RUnlock()

	if len(names) == 0 {
		out := make([]OrganizationConformancePackStatus, 0, len(b.orgConformancePacks))
		for _, p := range b.orgConformancePacks {
			out = append(out, OrganizationConformancePackStatus{
				OrganizationConformancePackName: p.OrganizationConformancePackName,
				Status:                          orgRuleStatusCreateSuccessful,
			})
		}

		return out
	}

	out := make([]OrganizationConformancePackStatus, 0, len(names))

	for _, name := range names {
		if p, ok := b.orgConformancePacks[name]; ok {
			out = append(out, OrganizationConformancePackStatus{
				OrganizationConformancePackName: p.OrganizationConformancePackName,
				Status:                          orgRuleStatusCreateSuccessful,
			})
		}
	}

	return out
}

// --- Group 9: ResourceConfig operations ---

// PutResourceConfig stores configuration for a resource.
func (b *InMemoryBackend) PutResourceConfig(resourceType, resourceID, configuration string) error {
	b.mu.Lock("PutResourceConfig")
	defer b.mu.Unlock()

	if b.resourceConfigs[resourceType] == nil {
		b.resourceConfigs[resourceType] = make(map[string]*ResourceConfigItem)
	}

	b.resourceConfigs[resourceType][resourceID] = &ResourceConfigItem{
		ResourceType:  resourceType,
		ResourceID:    resourceID,
		Configuration: configuration,
	}

	return nil
}

// GetResourceConfigHistory returns configuration history for a resource.
func (b *InMemoryBackend) GetResourceConfigHistory(resourceType, resourceID string) []ResourceConfigItem {
	b.mu.RLock("GetResourceConfigHistory")
	defer b.mu.RUnlock()

	byType := b.resourceConfigs[resourceType]
	if byType == nil {
		return []ResourceConfigItem{}
	}

	item, ok := byType[resourceID]
	if !ok {
		return []ResourceConfigItem{}
	}

	return []ResourceConfigItem{*item}
}

// ListDiscoveredResources returns all discovered resources of the given type.
func (b *InMemoryBackend) ListDiscoveredResources(resourceType string) []ResourceConfigItem {
	b.mu.RLock("ListDiscoveredResources")
	defer b.mu.RUnlock()

	byType := b.resourceConfigs[resourceType]
	if len(byType) == 0 {
		return []ResourceConfigItem{}
	}

	out := make([]ResourceConfigItem, 0, len(byType))
	for _, item := range byType {
		out = append(out, *item)
	}

	return out
}

// --- Group 10: Custom rule policy operations ---

// GetCustomRulePolicy returns the policy text for the given custom rule.
func (b *InMemoryBackend) GetCustomRulePolicy(ruleName string) string {
	b.mu.RLock("GetCustomRulePolicy")
	defer b.mu.RUnlock()

	return b.customRulePolicies[ruleName]
}

// GetOrganizationCustomRulePolicy returns the policy text for the given org custom rule.
func (b *InMemoryBackend) GetOrganizationCustomRulePolicy(ruleName string) string {
	b.mu.RLock("GetOrganizationCustomRulePolicy")
	defer b.mu.RUnlock()

	return b.orgCustomRulePolicies[ruleName]
}

// --- Group 11: Misc operations ---

// GetAggregateDiscoveredResourceCounts returns the total count of discovered resources.
func (b *InMemoryBackend) GetAggregateDiscoveredResourceCounts() int32 {
	b.mu.RLock("GetAggregateDiscoveredResourceCounts")
	defer b.mu.RUnlock()

	var total int32

	for _, byType := range b.resourceConfigs {
		total += int32(len(byType)) //nolint:gosec // len is non-negative and bounded
	}

	return total
}

// GetAggregateResourceConfig returns the first resource config found, or an empty item.
func (b *InMemoryBackend) GetAggregateResourceConfig() *BaseConfigurationItem {
	b.mu.RLock("GetAggregateResourceConfig")
	defer b.mu.RUnlock()

	for _, byType := range b.resourceConfigs {
		for _, item := range byType {
			return &BaseConfigurationItem{
				ResourceType: item.ResourceType,
				ResourceID:   item.ResourceID,
			}
		}
	}

	return &BaseConfigurationItem{}
}

// GetAggregateComplianceDetailsByConfigRule returns an empty list (intentionally minimal stub).
func (b *InMemoryBackend) GetAggregateComplianceDetailsByConfigRule() []any { return []any{} }

// GetAggregateConfigRuleComplianceSummary returns an empty summary (intentionally minimal stub).
func (b *InMemoryBackend) GetAggregateConfigRuleComplianceSummary() []any { return []any{} }

// GetAggregateConformancePackComplianceSummary returns an empty summary (intentionally minimal stub).
func (b *InMemoryBackend) GetAggregateConformancePackComplianceSummary() []any { return []any{} }

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

// GetComplianceSummaryByResourceType returns an empty summary (intentionally minimal stub).
func (b *InMemoryBackend) GetComplianceSummaryByResourceType() []any { return []any{} }

// GetConformancePackComplianceDetails returns an empty list (intentionally minimal stub).
func (b *InMemoryBackend) GetConformancePackComplianceDetails() []any { return []any{} }

// GetConformancePackComplianceSummary returns an empty list (intentionally minimal stub).
func (b *InMemoryBackend) GetConformancePackComplianceSummary() []any { return []any{} }

// DescribeComplianceByResource returns an empty list (intentionally minimal stub).
func (b *InMemoryBackend) DescribeComplianceByResource() []any { return []any{} }

// SelectResourceConfig returns an empty result (intentionally minimal stub).
func (b *InMemoryBackend) SelectResourceConfig() []any { return []any{} }

// SelectAggregateResourceConfig returns an empty result (intentionally minimal stub).
func (b *InMemoryBackend) SelectAggregateResourceConfig() []any { return []any{} }
