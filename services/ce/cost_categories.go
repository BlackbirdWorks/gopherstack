package ce

import (
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) buildCostCategoryARN(name string) string {
	return arn.Build("ce", "", b.accountID, fmt.Sprintf("costcategory/%s", name))
}

func effectiveStart() string {
	now := time.Now().UTC()

	return fmt.Sprintf("%d-%02d-01T00:00:00Z", now.Year(), now.Month())
}

// CreateCostCategoryDefinition creates a new cost category and returns it.
func (b *InMemoryBackend) CreateCostCategoryDefinition(
	name, ruleVersion, defaultValue string,
	rules []CostCategoryRule,
	resourceTags map[string]string,
) (*CostCategory, error) {
	b.mu.Lock("CreateCostCategoryDefinition")
	defer b.mu.Unlock()

	catARN := b.buildCostCategoryARN(name)
	if b.costCategories.Has(catARN) {
		return nil, ErrAlreadyExists
	}

	tagsCopy := make(map[string]string, len(resourceTags))
	maps.Copy(tagsCopy, resourceTags)

	rulesCopy := make([]CostCategoryRule, len(rules))
	copy(rulesCopy, rules)

	cat := &CostCategory{
		ARN:            catARN,
		Name:           name,
		RuleVersion:    ruleVersion,
		DefaultValue:   defaultValue,
		Rules:          rulesCopy,
		EffectiveStart: effectiveStart(),
		CreationDate:   time.Now().UTC(),
		Tags:           tagsCopy,
	}
	b.costCategories.Put(cat)

	out := *cat
	out.Rules = make([]CostCategoryRule, len(cat.Rules))
	copy(out.Rules, cat.Rules)

	return &out, nil
}

// DeleteCostCategoryDefinition removes a cost category by ARN.
func (b *InMemoryBackend) DeleteCostCategoryDefinition(catARN string) (*CostCategory, error) {
	b.mu.Lock("DeleteCostCategoryDefinition")
	defer b.mu.Unlock()

	cat, exists := b.costCategories.Get(catARN)
	if !exists {
		return nil, ErrNotFound
	}

	b.costCategories.Delete(catARN)

	out := *cat

	return &out, nil
}

// DescribeCostCategoryDefinition returns a cost category by ARN.
func (b *InMemoryBackend) DescribeCostCategoryDefinition(catARN string) (*CostCategory, error) {
	b.mu.RLock("DescribeCostCategoryDefinition")
	defer b.mu.RUnlock()

	cat, exists := b.costCategories.Get(catARN)
	if !exists {
		return nil, ErrNotFound
	}

	out := *cat

	return &out, nil
}

// ListCostCategoryDefinitions returns cost categories sorted by name with opaque pagination.
func (b *InMemoryBackend) ListCostCategoryDefinitions(maxResults int, nextPageToken string) ([]*CostCategory, string) {
	b.mu.RLock("ListCostCategoryDefinitions")
	defer b.mu.RUnlock()

	all := b.costCategories.All()
	result := make([]*CostCategory, 0, len(all))
	for _, cat := range all {
		out := *cat
		result = append(result, &out)
	}

	return paginateList(result, maxResults, nextPageToken, func(c *CostCategory) string {
		return c.Name
	})
}

// UpdateCostCategoryDefinition updates an existing cost category.
func (b *InMemoryBackend) UpdateCostCategoryDefinition(
	catARN, ruleVersion, defaultValue string,
	rules []CostCategoryRule,
	splitChargeRules []SplitChargeRule,
) (*CostCategory, error) {
	b.mu.Lock("UpdateCostCategoryDefinition")
	defer b.mu.Unlock()

	cat, exists := b.costCategories.Get(catARN)
	if !exists {
		return nil, ErrNotFound
	}

	cat.RuleVersion = ruleVersion
	cat.DefaultValue = defaultValue
	// Deep-copy both slices so the caller cannot alias backend-owned state.
	rulesCopy := make([]CostCategoryRule, len(rules))
	copy(rulesCopy, rules)
	cat.Rules = rulesCopy

	splitCopy := make([]SplitChargeRule, len(splitChargeRules))
	for i, s := range splitChargeRules {
		sc := s
		if s.Targets != nil {
			sc.Targets = make([]string, len(s.Targets))
			copy(sc.Targets, s.Targets)
		}

		splitCopy[i] = sc
	}

	cat.SplitChargeRules = splitCopy
	cat.EffectiveStart = effectiveStart()

	out := *cat
	out.Rules = make([]CostCategoryRule, len(cat.Rules))
	copy(out.Rules, cat.Rules)
	out.SplitChargeRules = make([]SplitChargeRule, len(cat.SplitChargeRules))
	copy(out.SplitChargeRules, cat.SplitChargeRules)

	return &out, nil
}

// GetCostCategories returns the distinct cost category values stored in the
// backend, optionally filtered by cost category name. Values are sorted alphabetically.
func (b *InMemoryBackend) GetCostCategories(costCategoryName string) []string {
	b.mu.RLock("GetCostCategories")
	defer b.mu.RUnlock()

	seen := make(map[string]struct{})
	var values []string

	for _, cat := range b.costCategories.All() {
		if costCategoryName != "" && cat.Name != costCategoryName {
			continue
		}

		for _, rule := range cat.Rules {
			if _, exists := seen[rule.Value]; !exists && rule.Value != "" {
				seen[rule.Value] = struct{}{}
				values = append(values, rule.Value)
			}
		}
	}

	sort.Strings(values)

	return values
}
