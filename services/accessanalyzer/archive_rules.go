package accessanalyzer

import (
	"sort"
	"time"
)

// CreateArchiveRule adds an archive rule to an analyzer and immediately archives
// all active findings for that analyzer (AWS auto-apply behavior).
func (b *InMemoryBackend) CreateArchiveRule(
	analyzerName, ruleName string,
	filter map[string]FilterCriterion,
) (*ArchiveRule, error) {
	b.mu.Lock("CreateArchiveRule")
	defer b.mu.Unlock()

	if !b.analyzers.Has(analyzerName) {
		return nil, ErrAnalyzerNotFound
	}

	key := archiveRuleKey(analyzerName, ruleName)

	if b.archiveRules.Has(key) {
		return nil, ErrArchiveRuleAlreadyExists
	}

	now := time.Now().UTC()
	rule := &ArchiveRule{
		AnalyzerName: analyzerName,
		RuleName:     ruleName,
		Filter:       cloneFilter(filter),
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	b.archiveRules.Put(rule)

	for _, f := range b.findingsByAnalyzer.Get(analyzerName) {
		if f.Status == FindingStatusActive {
			f.Status = FindingStatusArchived
			f.UpdatedAt = now
		}
	}

	return copyArchiveRule(rule), nil
}

// GetArchiveRule returns the named archive rule.
func (b *InMemoryBackend) GetArchiveRule(analyzerName, ruleName string) (*ArchiveRule, error) {
	b.mu.RLock("GetArchiveRule")
	defer b.mu.RUnlock()

	if !b.analyzers.Has(analyzerName) {
		return nil, ErrAnalyzerNotFound
	}

	rule, exists := b.archiveRules.Get(archiveRuleKey(analyzerName, ruleName))
	if !exists {
		return nil, ErrArchiveRuleNotFound
	}

	return copyArchiveRule(rule), nil
}

// ListArchiveRules returns all archive rules for an analyzer.
func (b *InMemoryBackend) ListArchiveRules(analyzerName string) ([]*ArchiveRule, error) {
	b.mu.RLock("ListArchiveRules")
	defer b.mu.RUnlock()

	if !b.analyzers.Has(analyzerName) {
		return nil, ErrAnalyzerNotFound
	}

	rules := b.archiveRulesByAnalyzer.Get(analyzerName)
	result := make([]*ArchiveRule, 0, len(rules))

	for _, r := range rules {
		result = append(result, copyArchiveRule(r))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].RuleName < result[j].RuleName
	})

	return result, nil
}

// DeleteArchiveRule removes an archive rule.
func (b *InMemoryBackend) DeleteArchiveRule(analyzerName, ruleName string) error {
	b.mu.Lock("DeleteArchiveRule")
	defer b.mu.Unlock()

	if !b.analyzers.Has(analyzerName) {
		return ErrAnalyzerNotFound
	}

	key := archiveRuleKey(analyzerName, ruleName)

	if !b.archiveRules.Has(key) {
		return ErrArchiveRuleNotFound
	}

	b.archiveRules.Delete(key)

	return nil
}

// UpdateArchiveRule replaces the filter on an archive rule.
func (b *InMemoryBackend) UpdateArchiveRule(
	analyzerName, ruleName string,
	filter map[string]FilterCriterion,
) (*ArchiveRule, error) {
	b.mu.Lock("UpdateArchiveRule")
	defer b.mu.Unlock()

	if !b.analyzers.Has(analyzerName) {
		return nil, ErrAnalyzerNotFound
	}

	rule, exists := b.archiveRules.Get(archiveRuleKey(analyzerName, ruleName))
	if !exists {
		return nil, ErrArchiveRuleNotFound
	}

	rule.Filter = cloneFilter(filter)
	rule.UpdatedAt = time.Now().UTC()

	return copyArchiveRule(rule), nil
}

// ApplyArchiveRule applies all archive rules for an analyzer to its findings.
// Findings that match any archive rule filter are archived.
func (b *InMemoryBackend) ApplyArchiveRule(analyzerArn, ruleName string) error {
	b.mu.Lock("ApplyArchiveRule")
	defer b.mu.Unlock()

	var analyzer *Analyzer

	for _, a := range b.analyzers.All() {
		if a.Arn == analyzerArn {
			analyzer = a

			break
		}
	}

	if analyzer == nil {
		return ErrAnalyzerNotFound
	}

	if ruleName != "" {
		if !b.archiveRules.Has(archiveRuleKey(analyzer.Name, ruleName)) {
			return ErrArchiveRuleNotFound
		}
	}

	now := time.Now().UTC()

	for _, f := range b.findingsByAnalyzer.Get(analyzer.Name) {
		if f.Status != FindingStatusActive {
			continue
		}

		f.Status = FindingStatusArchived
		f.UpdatedAt = now
	}

	return nil
}
