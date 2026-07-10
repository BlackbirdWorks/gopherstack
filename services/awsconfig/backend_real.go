package awsconfig

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

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

	q, ok := b.storedQueries.Get(name)
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

	b.storedQueries.Delete(name)

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

	b.retentionConfigs.Put(&RetentionConfiguration{
		Name:                  name,
		RetentionPeriodInDays: days,
	})

	return nil
}

// DescribeRetentionConfigurations returns all retention configurations.
func (b *InMemoryBackend) DescribeRetentionConfigurations() []RetentionConfiguration {
	b.mu.RLock("DescribeRetentionConfigurations")
	defer b.mu.RUnlock()

	all := b.retentionConfigs.All()
	out := make([]RetentionConfiguration, 0, len(all))

	for _, rc := range all {
		out = append(out, *rc)
	}

	return out
}

// DeleteRetentionConfiguration removes a retention configuration by name.
func (b *InMemoryBackend) DeleteRetentionConfiguration(name string) error {
	b.mu.Lock("DeleteRetentionConfiguration")
	defer b.mu.Unlock()

	b.retentionConfigs.Delete(name)

	return nil
}

// --- Group 4: RemediationConfiguration operations ---

// PutRemediationConfigurations stores remediation configurations keyed by rule name.
func (b *InMemoryBackend) PutRemediationConfigurations(configs []RemediationConfiguration) error {
	b.mu.Lock("PutRemediationConfigurations")
	defer b.mu.Unlock()

	for i := range configs {
		cp := configs[i]
		b.remediationConfigs.Put(&cp)
	}

	return nil
}

// DescribeRemediationConfigurations returns remediation configurations for the given rule names.
// If ruleNames is empty, all configurations are returned.
func (b *InMemoryBackend) DescribeRemediationConfigurations(ruleNames []string) []RemediationConfiguration {
	b.mu.RLock("DescribeRemediationConfigurations")
	defer b.mu.RUnlock()

	if len(ruleNames) == 0 {
		all := b.remediationConfigs.All()
		out := make([]RemediationConfiguration, 0, len(all))

		for _, rc := range all {
			out = append(out, *rc)
		}

		return out
	}

	out := make([]RemediationConfiguration, 0, len(ruleNames))

	for _, name := range ruleNames {
		if rc, ok := b.remediationConfigs.Get(name); ok {
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

	b.remediationConfigs.Delete(ruleName)

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

// --- Group 6: Delivery channel status ---

// DescribeDeliveryChannelStatus returns statuses for delivery channels.
// If names is empty, all channels are returned.
func (b *InMemoryBackend) DescribeDeliveryChannelStatus(names []string) []DeliveryChannelStatus {
	b.mu.RLock("DescribeDeliveryChannelStatus")
	defer b.mu.RUnlock()

	var channelNames []string

	if len(names) == 0 {
		for _, c := range b.channels.All() {
			channelNames = append(channelNames, c.Name)
		}
	} else {
		for _, name := range names {
			if b.channels.Has(name) {
				channelNames = append(channelNames, name)
			}
		}
	}

	out := make([]DeliveryChannelStatus, 0, len(channelNames))

	for _, name := range channelNames {
		out = append(out, DeliveryChannelStatus{
			Name:                      name,
			ConfigHistoryDeliveryInfo: &DeliveryChannelStatusInfo{LastStatus: recorderStatusSuccess},
			ConfigStreamDeliveryInfo:  &DeliveryChannelStatusInfo{LastStatus: recorderStatusSuccess},
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
		all := b.conformancePacks.All()
		out := make([]ConformancePackStatus, 0, len(all))

		for _, p := range all {
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
		if p, ok := b.conformancePacks.Get(name); ok {
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
		all := b.orgConfigRules.All()
		out := make([]OrganizationConfigRuleStatus, 0, len(all))

		for _, r := range all {
			out = append(out, OrganizationConfigRuleStatus{
				OrganizationConfigRuleName: r.OrganizationConfigRuleName,
				OrganizationRuleStatus:     orgRuleStatusCreateSuccessful,
			})
		}

		return out
	}

	out := make([]OrganizationConfigRuleStatus, 0, len(names))

	for _, name := range names {
		if r, ok := b.orgConfigRules.Get(name); ok {
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
		all := b.orgConformancePacks.All()
		out := make([]OrganizationConformancePackStatus, 0, len(all))

		for _, p := range all {
			out = append(out, OrganizationConformancePackStatus{
				OrganizationConformancePackName: p.OrganizationConformancePackName,
				Status:                          orgRuleStatusCreateSuccessful,
			})
		}

		return out
	}

	out := make([]OrganizationConformancePackStatus, 0, len(names))

	for _, name := range names {
		if p, ok := b.orgConformancePacks.Get(name); ok {
			out = append(out, OrganizationConformancePackStatus{
				OrganizationConformancePackName: p.OrganizationConformancePackName,
				Status:                          orgRuleStatusCreateSuccessful,
			})
		}
	}

	return out
}

// --- Group 9: ResourceConfig operations ---

// PutResourceConfig stores configuration for a resource. The latest state is kept
// for discovery, and a configuration-history entry is appended whenever the
// configuration actually changes (mirroring AWS Config which records on change).
func (b *InMemoryBackend) PutResourceConfig(resourceType, resourceID, configuration string) error {
	b.mu.Lock("PutResourceConfig")
	defer b.mu.Unlock()

	b.captureCounter++

	item := ResourceConfigItem{
		ResourceType:                 resourceType,
		ResourceID:                   resourceID,
		Configuration:                configuration,
		ConfigurationItemCaptureTime: float64(time.Now().Unix()),
	}

	b.resourceConfigs.Put(&item)

	key := resourceEvalKey(resourceType, resourceID)

	hist := b.resourceHistory[key]
	if len(hist) == 0 || hist[len(hist)-1].Configuration != configuration {
		b.resourceHistory[key] = append(hist, item)
	}

	return nil
}

// resourceHistoryLocked returns the configuration history for a resource ordered
// most-recent first. The caller must hold at least a read lock.
func (b *InMemoryBackend) resourceHistoryLocked(resourceType, resourceID string) []ResourceConfigItem {
	hist := b.resourceHistory[resourceEvalKey(resourceType, resourceID)]
	if len(hist) == 0 {
		return []ResourceConfigItem{}
	}

	out := make([]ResourceConfigItem, len(hist))
	for i, item := range hist {
		out[len(hist)-1-i] = item
	}

	return out
}

// GetResourceConfigHistory returns the full configuration history for a resource,
// most-recent first.
func (b *InMemoryBackend) GetResourceConfigHistory(resourceType, resourceID string) []ResourceConfigItem {
	b.mu.RLock("GetResourceConfigHistory")
	defer b.mu.RUnlock()

	return b.resourceHistoryLocked(resourceType, resourceID)
}

// GetResourceConfigHistoryPage returns a page of a resource's configuration
// history (most-recent first) along with an opaque continuation token.
func (b *InMemoryBackend) GetResourceConfigHistoryPage(
	resourceType, resourceID string,
	limit int,
	token string,
) ([]ResourceConfigItem, string) {
	b.mu.RLock("GetResourceConfigHistoryPage")
	defer b.mu.RUnlock()

	const defaultLimit = 100

	p := page.New(b.resourceHistoryLocked(resourceType, resourceID), token, limit, defaultLimit)

	return p.Data, p.Next
}

// ListDiscoveredResources returns all discovered resources of the given type.
func (b *InMemoryBackend) ListDiscoveredResources(resourceType string) []ResourceConfigItem {
	b.mu.RLock("ListDiscoveredResources")
	defer b.mu.RUnlock()

	byType := b.resourceConfigsByType.Get(resourceType)
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

	return int32(b.resourceConfigs.Len()) //nolint:gosec // Len is non-negative and bounded
}

// GetAggregateResourceConfig returns the first resource config found, or an empty item.
func (b *InMemoryBackend) GetAggregateResourceConfig() *BaseConfigurationItem {
	b.mu.RLock("GetAggregateResourceConfig")
	defer b.mu.RUnlock()

	for _, item := range b.resourceConfigs.All() {
		return &BaseConfigurationItem{
			ResourceType: item.ResourceType,
			ResourceID:   item.ResourceID,
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

// GetConformancePackComplianceDetails returns an empty list (intentionally minimal stub).
func (b *InMemoryBackend) GetConformancePackComplianceDetails() []any { return []any{} }

// GetConformancePackComplianceSummary returns an empty list (intentionally minimal stub).
func (b *InMemoryBackend) GetConformancePackComplianceSummary() []any { return []any{} }

// DescribeComplianceByResource returns an empty list (intentionally minimal stub).
func (b *InMemoryBackend) DescribeComplianceByResource() []any { return []any{} }

// resourceConfigItemsLocked returns every discovered resource configuration
// item across all resource types. Caller must hold at least a read lock.
func (b *InMemoryBackend) resourceConfigItemsLocked() []*ResourceConfigItem {
	return b.resourceConfigs.All()
}

// SelectResourceConfig evaluates a minimal SQL-like "SELECT fields WHERE
// key = value / LIKE pattern" query (see select_query.go) against the
// account's discovered resource configurations, instead of ignoring the
// query entirely.
func (b *InMemoryBackend) SelectResourceConfig(expression string) []string {
	b.mu.RLock("SelectResourceConfig")
	defer b.mu.RUnlock()

	return evaluateSelectQuery(b.resourceConfigItemsLocked(), expression)
}

// SelectAggregateResourceConfig evaluates the same query language as
// SelectResourceConfig. This emulator does not model multi-account
// aggregation separately from the account's own resource-config state, so
// (mirroring DescribeAggregateComplianceByConfigRules, which reuses the
// account's rule evaluations for its aggregate view) it reuses
// resourceConfigItemsLocked rather than returning an empty result.
func (b *InMemoryBackend) SelectAggregateResourceConfig(expression string) []string {
	b.mu.RLock("SelectAggregateResourceConfig")
	defer b.mu.RUnlock()

	return evaluateSelectQuery(b.resourceConfigItemsLocked(), expression)
}
