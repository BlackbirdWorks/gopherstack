package cloudwatch

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	alarmMuteTypeOneTime   = "ONE_TIME"
	alarmMuteTypeRecurring = "RECURRING"
)

// MuteType classifies the schedule: "at(...)" expressions are one-time
// windows, "cron(...)" expressions recur (types.Schedule doc, aws-sdk-go-v2
// cloudwatch@v1.66.3 types/types.go:3461).
func (r *AlarmMuteRule) MuteType() string {
	if strings.HasPrefix(r.Schedule.Expression, "at(") {
		return alarmMuteTypeOneTime
	}

	return alarmMuteTypeRecurring
}

const (
	alarmMuteStatusScheduled = "SCHEDULED"
	alarmMuteStatusActive    = "ACTIVE"
	alarmMuteStatusExpired   = "EXPIRED"
)

// Status derives SCHEDULED/ACTIVE/EXPIRED from StartDate/ExpireDate relative
// to now (types.AlarmMuteRuleStatus, aws-sdk-go-v2 cloudwatch@v1.66.3
// types/enums.go:26).
func (r *AlarmMuteRule) Status(now time.Time) string {
	if !r.ExpireDate.IsZero() && !now.Before(r.ExpireDate) {
		return alarmMuteStatusExpired
	}
	if !r.StartDate.IsZero() && now.Before(r.StartDate) {
		return alarmMuteStatusScheduled
	}

	return alarmMuteStatusActive
}

// PutAlarmMuteRule creates or updates an alarm mute rule by name. Name and
// Rule.Schedule (Expression, Duration) are required (aws-sdk-go-v2
// cloudwatch@v1.66.3 validators.go:2068,1451,1470).
func (b *InMemoryBackend) PutAlarmMuteRule(rule *AlarmMuteRule) error {
	if strings.TrimSpace(rule.Name) == "" {
		return fmt.Errorf("%w: Name parameter is required", ErrValidation)
	}
	if strings.TrimSpace(rule.Schedule.Expression) == "" {
		return fmt.Errorf("%w: Rule.Schedule.Expression parameter is required", ErrValidation)
	}
	if strings.TrimSpace(rule.Schedule.Duration) == "" {
		return fmt.Errorf("%w: Rule.Schedule.Duration parameter is required", ErrValidation)
	}
	if _, err := parseISO8601Duration(rule.Schedule.Duration); err != nil {
		return fmt.Errorf("%w: Rule.Schedule.Duration: %w", ErrValidation, err)
	}
	if err := validateMuteScheduleExpression(rule.Schedule.Expression); err != nil {
		return fmt.Errorf("%w: Rule.Schedule.Expression: %w", ErrValidation, err)
	}
	if rule.Schedule.Timezone != "" {
		if _, err := time.LoadLocation(rule.Schedule.Timezone); err != nil {
			return fmt.Errorf("%w: Rule.Schedule.Timezone: %w", ErrValidation, err)
		}
	}

	b.PutAlarmMuteRuleInternal(rule)

	return nil
}

// DeleteAlarmMuteRule removes an alarm mute rule by name. Idempotent: deleting
// a rule that does not exist succeeds without error (aws-sdk-go-v2
// cloudwatch@v1.66.3 api_op_DeleteAlarmMuteRule.go:19).
func (b *InMemoryBackend) DeleteAlarmMuteRule(name string) error {
	b.mu.Lock("DeleteAlarmMuteRule")
	defer b.mu.Unlock()

	b.alarmMuteRules.Delete(name)

	return nil
}

// GetAlarmMuteRule returns an alarm mute rule by name.
// Returns ErrAlarmMuteRuleNotFound if the rule does not exist.
func (b *InMemoryBackend) GetAlarmMuteRule(name string) (*AlarmMuteRule, error) {
	b.mu.RLock("GetAlarmMuteRule")
	defer b.mu.RUnlock()

	rule, ok := b.alarmMuteRules.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrAlarmMuteRuleNotFound, name)
	}

	cp := *rule

	return &cp, nil
}

// ListAlarmMuteRules returns a paginated list of alarm mute rules, optionally
// filtered by targeted alarm name and/or status (ListAlarmMuteRulesInput,
// aws-sdk-go-v2 cloudwatch@v1.66.3 api_op_ListAlarmMuteRules.go:42).
func (b *InMemoryBackend) ListAlarmMuteRules(
	nextToken string,
	maxResults int,
	alarmName string,
	statuses []string,
) (page.Page[AlarmMuteRule], error) {
	b.mu.RLock("ListAlarmMuteRules")
	defer b.mu.RUnlock()

	now := time.Now().UTC()
	result := make([]AlarmMuteRule, 0, b.alarmMuteRules.Len())

	for _, rule := range b.alarmMuteRules.All() {
		if alarmName != "" && !slices.Contains(rule.AlarmNames, alarmName) {
			continue
		}
		if len(statuses) > 0 && !slices.Contains(statuses, rule.Status(now)) {
			continue
		}

		result = append(result, *rule)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })

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

	cp.LastUpdatedTimestamp = time.Now().UTC()

	if cp.Arn == "" {
		cp.Arn = arn.Build("cloudwatch", b.region, b.accountID, "alarm-mute-rule:"+rule.Name)
	}

	b.alarmMuteRules.Put(&cp)
}

// activeMuteRule returns the name of an alarm mute rule currently muting
// alarmName at instant now, and whether one was found. A rule mutes an alarm
// when it targets the alarm, its Status(now) is ACTIVE (within
// StartDate/ExpireDate), and its schedule's cron/at window covers now
// ("the targeted alarms continue to evaluate metrics and transition between
// states, but their configured actions are muted", botocore cloudwatch
// 2010-08-01 service-2.json MuteTargets shape). A rule with a schedule that
// fails to parse cannot mute anything -- PutAlarmMuteRule rejects malformed
// schedules, so this only guards stale/pre-validation data.
func (b *InMemoryBackend) activeMuteRule(alarmName string, now time.Time) (string, bool) {
	b.mu.RLock("activeMuteRule")
	defer b.mu.RUnlock()

	for _, rule := range b.alarmMuteRules.All() {
		if rule.Status(now) != alarmMuteStatusActive {
			continue
		}
		if !slices.Contains(rule.AlarmNames, alarmName) {
			continue
		}

		active, err := muteScheduleActive(rule.Schedule, now)
		if err != nil || !active {
			continue
		}

		return rule.Name, true
	}

	return "", false
}

// appendMutedActionHistory records that alarmName's actions were suppressed
// by an active mute rule, mirroring the per-action history entries
// executeActions records when actions do fire.
func (b *InMemoryBackend) appendMutedActionHistory(alarmName, alarmTypeName, muteRuleName string) {
	b.mu.Lock("appendMutedActionHistory")
	defer b.mu.Unlock()

	summary := fmt.Sprintf("Alarm %q actions muted by alarm mute rule %q", alarmName, muteRuleName)
	b.appendHistory(alarmName, alarmTypeName, historyTypeAction, summary, "")
}
