package accessanalyzer

import (
	"encoding/json"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"
)

// firstConfiguration returns the first element of a variadic
// json.RawMessage configuration argument, or nil if omitted / empty / the
// JSON null literal. CreateAnalyzer, CreateServiceLinkedAnalyzer, and
// UpdateAnalyzer all accept the AnalyzerConfiguration union as an optional
// parameter this way so existing call sites (which never set it) do not
// need to change.
func firstConfiguration(cs []json.RawMessage) json.RawMessage {
	if len(cs) == 0 {
		return nil
	}

	c := cs[0]
	if len(c) == 0 || string(c) == "null" {
		return nil
	}

	return c
}

// CreateAnalyzer creates a new analyzer. configuration, if supplied, is the
// raw AnalyzerConfiguration union body (see Analyzer.Configuration).
func (b *InMemoryBackend) CreateAnalyzer(
	name string,
	analyzerType AnalyzerType,
	tags map[string]string,
	configuration ...json.RawMessage,
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
		Arn:           b.analyzerARN(name),
		Name:          name,
		Type:          analyzerType,
		Status:        AnalyzerStatusActive,
		CreatedAt:     now,
		Tags:          cloneTags(tags),
		Configuration: firstConfiguration(configuration),
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

// DeleteAnalyzer removes an analyzer and cascade-cleans every piece of state
// keyed off it, so no ghost rows survive it: archive rules, findings (and
// their finding recommendations, keyed 1:1 by finding ID), analyzed
// resources, access previews, and tags. Tags in particular used to leak --
// b.tags[analyzerARN] was never removed here, so re-creating an analyzer of
// the same name after deleting the old one would silently resurrect its
// stale tags.
func (b *InMemoryBackend) DeleteAnalyzer(name string) error {
	b.mu.Lock("DeleteAnalyzer")
	defer b.mu.Unlock()

	a, ok := b.analyzers.Get(name)
	if !ok {
		return ErrAnalyzerNotFound
	}

	analyzerARN := a.Arn

	b.analyzers.Delete(name)

	for _, r := range slices.Clone(b.archiveRulesByAnalyzer.Get(name)) {
		b.archiveRules.Delete(archiveRuleKey(r.AnalyzerName, r.RuleName))
	}

	for _, f := range slices.Clone(b.findingsByAnalyzer.Get(name)) {
		b.findings.Delete(f.ID)
		b.findingRecommendations.Delete(f.ID)
	}

	for _, ar := range b.analyzedResources.All() {
		if ar.AnalyzerArn == analyzerARN {
			b.analyzedResources.Delete(analyzedResourceKey(ar.AnalyzerArn, ar.ResourceArn))
		}
	}

	for _, ap := range b.accessPreviews.All() {
		if ap.AnalyzerArn == analyzerARN {
			b.accessPreviews.Delete(ap.ID)
		}
	}

	delete(b.tags, analyzerARN)

	return nil
}

// CreateServiceLinkedAnalyzer creates an analyzer with a generated
// service-linked name. configuration, if supplied, is the raw
// AnalyzerConfiguration union body (see Analyzer.Configuration).
func (b *InMemoryBackend) CreateServiceLinkedAnalyzer(
	analyzerType AnalyzerType,
	configuration ...json.RawMessage,
) (*Analyzer, error) {
	name := "_AccessAnalyzerForInternalUse-" + uuid.NewString()[:8]

	return b.CreateAnalyzer(name, analyzerType, nil, configuration...)
}

// UpdateAnalyzer updates an analyzer's configuration. If configuration is
// supplied (non-empty, non-null), it replaces the analyzer's stored
// AnalyzerConfiguration; otherwise the existing configuration is left
// untouched and simply echoed back, matching UpdateAnalyzerOutput's
// contract of always returning the analyzer's current configuration.
func (b *InMemoryBackend) UpdateAnalyzer(name string, configuration ...json.RawMessage) (*Analyzer, error) {
	b.mu.Lock("UpdateAnalyzer")
	defer b.mu.Unlock()

	a, ok := b.analyzers.Get(name)
	if !ok {
		return nil, ErrAnalyzerNotFound
	}

	if cfg := firstConfiguration(configuration); cfg != nil {
		a.Configuration = cfg
	}

	return copyAnalyzer(a), nil
}
