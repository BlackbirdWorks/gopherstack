package awsconfig

import "fmt"

const orgRuleStatusCreateSuccessful = "CREATE_SUCCESSFUL"

// PutOrganizationConfigRule creates or updates an organization config rule.
func (b *InMemoryBackend) PutOrganizationConfigRule(name string) error {
	if name == "" {
		return fmt.Errorf("%w: OrganizationConfigRuleName is required", ErrValidation)
	}

	b.mu.Lock("PutOrganizationConfigRule")
	defer b.mu.Unlock()

	b.orgConfigRules.Put(&OrganizationConfigRule{OrganizationConfigRuleName: name})

	return nil
}

// DeleteOrganizationConfigRule deletes an organization config rule by name.
func (b *InMemoryBackend) DeleteOrganizationConfigRule(name string) error {
	if name == "" {
		return fmt.Errorf("%w: OrganizationConfigRuleName is required", ErrValidation)
	}

	b.mu.Lock("DeleteOrganizationConfigRule")
	defer b.mu.Unlock()

	if !b.orgConfigRules.Has(name) {
		return fmt.Errorf("%w: %s", ErrNoSuchOrganizationConfigRule, name)
	}

	b.orgConfigRules.Delete(name)

	return nil
}

// PutOrganizationConformancePack creates or updates an organization conformance pack.
func (b *InMemoryBackend) PutOrganizationConformancePack(name string) error {
	if name == "" {
		return fmt.Errorf("%w: OrganizationConformancePackName is required", ErrValidation)
	}

	b.mu.Lock("PutOrganizationConformancePack")
	defer b.mu.Unlock()

	b.orgConformancePacks.Put(&OrganizationConformancePack{OrganizationConformancePackName: name})

	return nil
}

// DeleteOrganizationConformancePack deletes an organization conformance pack by name.
func (b *InMemoryBackend) DeleteOrganizationConformancePack(name string) error {
	if name == "" {
		return fmt.Errorf("%w: OrganizationConformancePackName is required", ErrValidation)
	}

	b.mu.Lock("DeleteOrganizationConformancePack")
	defer b.mu.Unlock()

	if !b.orgConformancePacks.Has(name) {
		return fmt.Errorf("%w: %s", ErrNoSuchOrganizationConformancePack, name)
	}

	b.orgConformancePacks.Delete(name)

	return nil
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

// GetOrganizationConfigRuleDetailedStatus returns an empty list.
func (b *InMemoryBackend) GetOrganizationConfigRuleDetailedStatus() []any {
	return []any{}
}

// GetOrganizationConformancePackDetailedStatus returns an empty list.
func (b *InMemoryBackend) GetOrganizationConformancePackDetailedStatus() []any {
	return []any{}
}

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

// GetOrganizationCustomRulePolicy returns the policy text for the given org custom rule.
func (b *InMemoryBackend) GetOrganizationCustomRulePolicy(ruleName string) string {
	b.mu.RLock("GetOrganizationCustomRulePolicy")
	defer b.mu.RUnlock()

	return b.orgCustomRulePolicies[ruleName]
}
