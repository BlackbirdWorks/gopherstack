package cloudwatch

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// PutAlarmMuteRule creates or updates an alarm mute rule by name.
func (b *InMemoryBackend) PutAlarmMuteRule(rule *AlarmMuteRule) error {
	if strings.TrimSpace(rule.MuteName) == "" {
		return fmt.Errorf("%w: MuteName parameter is required", ErrValidation)
	}

	b.PutAlarmMuteRuleInternal(rule)

	return nil
}

// DeleteAlarmMuteRule removes an alarm mute rule by name.
// Returns ErrAlarmMuteRuleNotFound if the rule does not exist.
func (b *InMemoryBackend) DeleteAlarmMuteRule(muteName string) error {
	b.mu.Lock("DeleteAlarmMuteRule")
	defer b.mu.Unlock()

	if !b.alarmMuteRules.Has(muteName) {
		return fmt.Errorf("%w: %s", ErrAlarmMuteRuleNotFound, muteName)
	}

	b.alarmMuteRules.Delete(muteName)

	return nil
}

// GetAlarmMuteRule returns an alarm mute rule by name.
// Returns ErrAlarmMuteRuleNotFound if the rule does not exist.
func (b *InMemoryBackend) GetAlarmMuteRule(muteName string) (*AlarmMuteRule, error) {
	b.mu.RLock("GetAlarmMuteRule")
	defer b.mu.RUnlock()

	rule, ok := b.alarmMuteRules.Get(muteName)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrAlarmMuteRuleNotFound, muteName)
	}

	cp := *rule

	return &cp, nil
}

// ListAlarmMuteRules returns a paginated list of all alarm mute rules.
func (b *InMemoryBackend) ListAlarmMuteRules(
	nextToken string,
	maxResults int,
) (page.Page[AlarmMuteRule], error) {
	b.mu.RLock("ListAlarmMuteRules")
	defer b.mu.RUnlock()

	result := make([]AlarmMuteRule, 0, b.alarmMuteRules.Len())
	for _, rule := range b.alarmMuteRules.All() {
		result = append(result, *rule)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].MuteName < result[j].MuteName })

	return page.New(result, nextToken, maxResults, cwDefaultListAlarmMuteRulesLimit), nil
}

// PutAlarmMuteRuleInternal creates or updates an alarm mute rule (used for test seeding).
func (b *InMemoryBackend) PutAlarmMuteRuleInternal(rule *AlarmMuteRule) {
	b.mu.Lock("PutAlarmMuteRuleInternal")
	defer b.mu.Unlock()

	cp := *rule
	if cp.CreationTime.IsZero() {
		cp.CreationTime = time.Now().UTC()
	}

	b.alarmMuteRules.Put(&cp)
}
