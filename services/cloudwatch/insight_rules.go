package cloudwatch

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// managedInsightRuleName synthesizes a stable internal name for a managed
// (service-linked) insight rule from its PutManagedInsightRules identity.
// Real AWS's ManagedRule input has no RuleName member at all -- only
// ResourceARN and TemplateName are required (aws-sdk-go-v2 cloudwatch@v1.66.3
// types/types.go:1817) -- so a real client never sends one; this backend
// still needs a stable key to store the rule under and to answer
// ListManagedInsightRules' RuleState.RuleName with something consistent
// across repeated Put calls for the same (ResourceARN, TemplateName) pair.
func managedInsightRuleName(resourceARN, templateName string) string {
	return resourceARN + "/" + templateName
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

	if rule.State != "" && rule.State != insightRuleStateEnabled && rule.State != insightRuleStateDisabled {
		return fmt.Errorf("%w: RuleState must be ENABLED or DISABLED", ErrValidation)
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

		rule.State = insightRuleStateDisabled
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
