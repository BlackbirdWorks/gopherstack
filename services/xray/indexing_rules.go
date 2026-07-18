package xray

import (
	"fmt"
	"time"
)

// defaultIndexingRules returns the built-in X-Ray indexing rules.
func defaultIndexingRules() []*IndexingRule {
	now := time.Now()

	return []*IndexingRule{
		{Name: "Default", ModifiedAt: now},
	}
}

// GetIndexingRules returns all indexing rules.
func (b *InMemoryBackend) GetIndexingRules() []*IndexingRule {
	b.mu.RLock("GetIndexingRules")
	defer b.mu.RUnlock()

	out := make([]*IndexingRule, len(b.indexingRules))
	for i, r := range b.indexingRules {
		cp := *r
		out[i] = &cp
	}

	return out
}

// UpdateIndexingRule updates the named indexing rule's ModifiedAt timestamp.
// Returns ErrIndexingRuleNotFound if no rule with that name exists.
func (b *InMemoryBackend) UpdateIndexingRule(name string) (*IndexingRule, error) {
	b.mu.Lock("UpdateIndexingRule")
	defer b.mu.Unlock()

	for _, r := range b.indexingRules {
		if r.Name == name {
			r.ModifiedAt = time.Now()
			cp := *r

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: indexing rule %s not found", ErrIndexingRuleNotFound, name)
}
