package dlm

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

const (
	maxSchedules   = 4
	maxTargetTags  = 50
	maxRetainCount = 1000

	policyTypeEBSSnapshot = "EBS_SNAPSHOT_MANAGEMENT"
)

var (
	timeOfDayRe = regexp.MustCompile(`^(0[0-9]|1[0-9]|2[0-3]):[0-5][0-9]$`)
)

func invalidRequest(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidRequest, fmt.Sprintf(format, args...))
}

func validateSettableState(state string) error {
	if state != "" && state != stateEnabled && state != "DISABLED" {
		return invalidRequest("State must be ENABLED or DISABLED")
	}

	return nil
}

// validatePolicyDetails checks a custom EBS snapshot policy's documented
// limits (types.PolicyDetails, Schedule, CreateRule, RetainRule). Other
// policy types and default policies are not inspected.
func validatePolicyDetails(details map[string]any, defaultPolicy string) error {
	if details == nil || defaultPolicy != "" {
		return nil
	}

	if _, singular := details["ResourceType"]; singular {
		return nil
	}

	if pt, _ := details["PolicyType"].(string); pt != "" && pt != policyTypeEBSSnapshot {
		return nil
	}

	if err := validateTargeting(details); err != nil {
		return err
	}

	schedules, _ := details["Schedules"].([]any)
	if len(schedules) > maxSchedules {
		return invalidRequest("a policy can have at most %d schedules", maxSchedules)
	}

	for i, raw := range schedules {
		sched, _ := raw.(map[string]any)
		if err := validateSchedule(sched); err != nil {
			return fmt.Errorf("schedule %d: %w", i+1, err)
		}
	}

	return nil
}

func validateTargeting(details map[string]any) error {
	for _, rt := range asSlice(details["ResourceTypes"]) {
		if s, _ := rt.(string); s != "VOLUME" && s != "INSTANCE" {
			return invalidRequest("ResourceTypes values must be VOLUME or INSTANCE")
		}
	}

	if tags := asSlice(details["TargetTags"]); len(tags) > maxTargetTags {
		return invalidRequest("TargetTags can contain at most %d tags", maxTargetTags)
	}

	return nil
}

func asSlice(v any) []any {
	s, _ := v.([]any)

	return s
}

func validateSchedule(sched map[string]any) error {
	if cr, ok := sched["CreateRule"].(map[string]any); ok {
		if err := validateCreateRule(cr); err != nil {
			return err
		}
	}

	if rr, ok := sched["RetainRule"].(map[string]any); ok {
		return validateRetainRule(rr, sched["ArchiveRule"] != nil)
	}

	return nil
}

func validateCreateRule(cr map[string]any) error {
	_, hasInterval := cr["Interval"]
	_, hasCron := cr["CronExpression"]

	if hasInterval && hasCron {
		return invalidRequest("specify either CronExpression or Interval, not both")
	}

	if hasInterval {
		v, _ := cr["Interval"].(float64)
		if !validHourInterval(v) {
			return invalidRequest("CreateRule.Interval must be one of 1, 2, 3, 4, 6, 8, 12, 24")
		}

		if u, _ := cr["IntervalUnit"].(string); u != "HOURS" {
			return invalidRequest("CreateRule.IntervalUnit must be HOURS")
		}
	}

	for _, t := range asSlice(cr["Times"]) {
		if s, _ := t.(string); !timeOfDayRe.MatchString(s) {
			return invalidRequest("CreateRule.Times values must use the hh:mm format")
		}
	}

	return nil
}

// validateRetainRule allows a zero count/interval only on archive-enabled schedules, which archive immediately.
func validateRetainRule(rr map[string]any, archive bool) error {
	count, hasCount := rr["Count"].(float64)
	interval, hasInterval := rr["Interval"].(float64)

	if hasCount == hasInterval {
		return invalidRequest("RetainRule must specify exactly one of Count or Interval")
	}

	minCount := 1.0
	if archive {
		minCount = 0
	}

	if hasCount && (count < minCount || count > maxRetainCount) {
		return invalidRequest("RetainRule.Count must be between %d and %d", int(minCount), maxRetainCount)
	}

	if hasInterval {
		unit, _ := rr["IntervalUnit"].(string)
		if interval < minCount || !validRetentionUnit(unit) {
			return invalidRequest("RetainRule.Interval must be positive with a DAYS, WEEKS, MONTHS or YEARS unit")
		}
	}

	return nil
}

func validRetentionUnit(unit string) bool {
	switch strings.ToUpper(unit) {
	case unitDays, unitWeeks, unitMonths, unitYears:
		return true
	}

	return false
}

func validHourInterval(v float64) bool {
	return slices.Contains([]float64{1, 2, 3, 4, 6, 8, 12, 24}, v)
}
