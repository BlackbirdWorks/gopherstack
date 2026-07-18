package accessanalyzer

import (
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"
)

// CreateAnalyzer creates a new analyzer.
func (b *InMemoryBackend) CreateAnalyzer(
	name string,
	analyzerType AnalyzerType,
	tags map[string]string,
) (*Analyzer, error) {
	if name == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateAnalyzer")
	defer b.mu.Unlock()

	if b.analyzers.Has(name) {
		return nil, ErrAnalyzerAlreadyExists
	}

	now := time.Now().UTC()
	a := &Analyzer{
		Arn:       b.analyzerARN(name),
		Name:      name,
		Type:      analyzerType,
		Status:    AnalyzerStatusActive,
		CreatedAt: now,
		Tags:      cloneTags(tags),
	}

	b.analyzers.Put(a)

	return copyAnalyzer(a), nil
}

// GetAnalyzer returns the named analyzer.
func (b *InMemoryBackend) GetAnalyzer(name string) (*Analyzer, error) {
	b.mu.RLock("GetAnalyzer")
	defer b.mu.RUnlock()

	a, exists := b.analyzers.Get(name)
	if !exists {
		return nil, ErrAnalyzerNotFound
	}

	return copyAnalyzer(a), nil
}

// ListAnalyzers returns all analyzers, optionally filtered by type.
func (b *InMemoryBackend) ListAnalyzers(analyzerType string) ([]*Analyzer, error) {
	b.mu.RLock("ListAnalyzers")
	defer b.mu.RUnlock()

	all := b.analyzers.All()
	result := make([]*Analyzer, 0, len(all))

	for _, a := range all {
		if analyzerType != "" && string(a.Type) != analyzerType {
			continue
		}

		result = append(result, copyAnalyzer(a))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

// DeleteAnalyzer removes an analyzer and all its findings and archive rules.
func (b *InMemoryBackend) DeleteAnalyzer(name string) error {
	b.mu.Lock("DeleteAnalyzer")
	defer b.mu.Unlock()

	if !b.analyzers.Has(name) {
		return ErrAnalyzerNotFound
	}

	b.analyzers.Delete(name)

	for _, r := range slices.Clone(b.archiveRulesByAnalyzer.Get(name)) {
		b.archiveRules.Delete(archiveRuleKey(r.AnalyzerName, r.RuleName))
	}

	for _, f := range slices.Clone(b.findingsByAnalyzer.Get(name)) {
		b.findings.Delete(f.ID)
	}

	return nil
}

// CreateServiceLinkedAnalyzer creates an analyzer with a generated service-linked name.
func (b *InMemoryBackend) CreateServiceLinkedAnalyzer(analyzerType AnalyzerType) (*Analyzer, error) {
	name := "_AccessAnalyzerForInternalUse-" + uuid.NewString()[:8]

	return b.CreateAnalyzer(name, analyzerType, nil)
}

// UpdateAnalyzer updates an analyzer (currently a no-op — configuration not stored).
func (b *InMemoryBackend) UpdateAnalyzer(name string) (*Analyzer, error) {
	b.mu.RLock("UpdateAnalyzer")
	defer b.mu.RUnlock()

	a, ok := b.analyzers.Get(name)
	if !ok {
		return nil, ErrAnalyzerNotFound
	}

	return copyAnalyzer(a), nil
}
