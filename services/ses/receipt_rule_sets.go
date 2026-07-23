package ses

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// cloneReceiptRuleSet returns a deep copy of a ReceiptRuleSet.
func cloneReceiptRuleSet(rs *ReceiptRuleSet) ReceiptRuleSet {
	rules := make([]ReceiptRule, len(rs.Rules))
	for i, r := range rs.Rules {
		recipients := make([]string, len(r.Recipients))
		copy(recipients, r.Recipients)
		actions := make([]ReceiptAction, len(r.Actions))
		copy(actions, r.Actions)
		rules[i] = ReceiptRule{
			Name:        r.Name,
			Enabled:     r.Enabled,
			TLSPolicy:   r.TLSPolicy,
			ScanEnabled: r.ScanEnabled,
			Recipients:  recipients,
			Actions:     actions,
		}
	}

	return ReceiptRuleSet{Name: rs.Name, CreatedAt: rs.CreatedAt, Rules: rules}
}

// CreateReceiptRuleSet creates a new receipt rule set.
// Returns ErrReceiptRuleSetExists if it already exists.
func (b *InMemoryBackend) CreateReceiptRuleSet(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("%w: RuleSetName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateReceiptRuleSet")
	defer b.mu.Unlock()

	if b.receiptRuleSets.Has(name) {
		return fmt.Errorf("%w: receipt rule set %s already exists", ErrReceiptRuleSetExists, name)
	}

	b.receiptRuleSets.Put(&ReceiptRuleSet{
		Name:      name,
		CreatedAt: time.Now().UTC(),
		Rules:     []ReceiptRule{},
	})

	return nil
}

// CloneReceiptRuleSet creates a copy of an existing receipt rule set under a new name.
func (b *InMemoryBackend) CloneReceiptRuleSet(originalName, newName string) error {
	if strings.TrimSpace(originalName) == "" {
		return fmt.Errorf("%w: OriginalRuleSetName is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(newName) == "" {
		return fmt.Errorf("%w: RuleSetName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CloneReceiptRuleSet")
	defer b.mu.Unlock()

	src, exists := b.receiptRuleSets.Get(originalName)
	if !exists {
		return fmt.Errorf("%w: %s", ErrReceiptRuleSetNotFound, originalName)
	}

	if b.receiptRuleSets.Has(newName) {
		return fmt.Errorf("%w: receipt rule set %s already exists", ErrReceiptRuleSetExists, newName)
	}

	rules := make([]ReceiptRule, len(src.Rules))
	for i, r := range src.Rules {
		recipients := make([]string, len(r.Recipients))
		copy(recipients, r.Recipients)
		actions := make([]ReceiptAction, len(r.Actions))
		copy(actions, r.Actions)

		rules[i] = ReceiptRule{
			Name:        r.Name,
			Enabled:     r.Enabled,
			TLSPolicy:   r.TLSPolicy,
			ScanEnabled: r.ScanEnabled,
			Recipients:  recipients,
			Actions:     actions,
		}
	}

	b.receiptRuleSets.Put(&ReceiptRuleSet{
		Name:      newName,
		CreatedAt: time.Now().UTC(),
		Rules:     rules,
	})

	return nil
}

// ListReceiptRuleSets returns a sorted slice of all receipt rule sets (name + createdAt only).
func (b *InMemoryBackend) ListReceiptRuleSets() []ReceiptRuleSet {
	b.mu.RLock("ListReceiptRuleSets")
	defer b.mu.RUnlock()

	out := make([]ReceiptRuleSet, 0, b.receiptRuleSets.Len())
	for _, rs := range b.receiptRuleSets.All() {
		out = append(out, ReceiptRuleSet{Name: rs.Name, CreatedAt: rs.CreatedAt})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// DescribeReceiptRuleSet returns a deep copy of the named rule set.
func (b *InMemoryBackend) DescribeReceiptRuleSet(name string) (ReceiptRuleSet, error) {
	if strings.TrimSpace(name) == "" {
		return ReceiptRuleSet{}, fmt.Errorf("%w: RuleSetName is required", ErrInvalidParameter)
	}
	b.mu.RLock("DescribeReceiptRuleSet")
	defer b.mu.RUnlock()
	rs, exists := b.receiptRuleSets.Get(name)
	if !exists {
		return ReceiptRuleSet{}, fmt.Errorf("%w: %s", ErrReceiptRuleSetNotFound, name)
	}

	return cloneReceiptRuleSet(rs), nil
}

// DeleteReceiptRuleSet removes a receipt rule set and its rules.
// Matching real AWS SES ("The currently active rule set cannot be deleted."),
// deleting the currently active rule set is rejected with
// ErrReceiptRuleSetActive (wire code CannotDelete) rather than silently
// clearing the active pointer; the caller must first call
// SetActiveReceiptRuleSet with a different name (or "") before the delete
// will succeed.
func (b *InMemoryBackend) DeleteReceiptRuleSet(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("%w: RuleSetName is required", ErrInvalidParameter)
	}
	b.mu.Lock("DeleteReceiptRuleSet")
	defer b.mu.Unlock()
	if !b.receiptRuleSets.Has(name) {
		return fmt.Errorf("%w: %s", ErrReceiptRuleSetNotFound, name)
	}
	if b.activeRuleSet == name {
		return fmt.Errorf("%w: %s is the currently active rule set", ErrReceiptRuleSetActive, name)
	}
	b.receiptRuleSets.Delete(name)

	return nil
}

// SetActiveReceiptRuleSet sets the named rule set as active.
// Passing an empty name clears the active rule set.
func (b *InMemoryBackend) SetActiveReceiptRuleSet(name string) error {
	b.mu.Lock("SetActiveReceiptRuleSet")
	defer b.mu.Unlock()
	if name != "" {
		if !b.receiptRuleSets.Has(name) {
			return fmt.Errorf("%w: %s", ErrReceiptRuleSetNotFound, name)
		}
	}
	b.activeRuleSet = name

	return nil
}

// DescribeActiveReceiptRuleSet returns the active receipt rule set.
// Returns false if none is set.
func (b *InMemoryBackend) DescribeActiveReceiptRuleSet() (ReceiptRuleSet, bool, error) {
	b.mu.RLock("DescribeActiveReceiptRuleSet")
	defer b.mu.RUnlock()
	if b.activeRuleSet == "" {
		return ReceiptRuleSet{}, false, nil
	}
	rs, exists := b.receiptRuleSets.Get(b.activeRuleSet)
	if !exists {
		return ReceiptRuleSet{}, false, nil
	}

	return cloneReceiptRuleSet(rs), true, nil
}
