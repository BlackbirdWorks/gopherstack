package scheduler

import (
	"fmt"
	"strconv"
	"strings"
)

// cronFieldSpec describes one AWS EventBridge Scheduler cron field: its numeric
// range, optional name-alias resolver, and which wildcards are legal in it. Source:
// https://docs.aws.amazon.com/scheduler/latest/UserGuide/schedule-types.html#cron-based
// (fetched 2026-08-11), the "Cron-based schedules" field/wildcard table.
type cronFieldSpec struct {
	resolve    func(string) (int, bool)
	name       string
	min        int
	max        int
	allowQuest bool // "?"
	allowSlash bool // "/" step
	allowL     bool // bare "L"
	allowW     bool // "<n>W" (day-of-month only)
	allowNL    bool // "<n>L" combined form (day-of-week only, e.g. "6L")
	allowHash  bool // "<n>#<m>" (day-of-week only)
}

// Field/wildcard table, row for row, from the cited AWS doc:
//
//	Minutes       0-59              , - * /
//	Hours         0-23              , - * /
//	Day-of-month  1-31              , - * ? / L W
//	Month         1-12 or JAN-DEC   , - * /
//	Day-of-week   1-7 or SUN-SAT    , - * ? L #
//	Year          1970-2199         , - * /
const (
	cronMinuteMin, cronMinuteMax         = 0, 59
	cronHourMin, cronHourMax             = 0, 23
	cronDayOfMonthMin, cronDayOfMonthMax = 1, 31
	cronMonthMin, cronMonthMax           = 1, 12
	cronDayOfWeekMin, cronDayOfWeekMax   = 1, 7
	cronYearMin, cronYearMax             = 1970, 2199
)

// validateCronFields checks each of the six cron fields against its AWS-documented
// range, name aliases, and legal wildcards, plus the day-of-month/day-of-week
// cross-field rule. Without this, matchesCronField silently treats any token it
// can't parse as "no match" rather than erroring, so a structurally valid but
// semantically garbage field (e.g. cron(0 12 * * ? GARBAGE)) is accepted at create
// time and then never fires.
func validateCronFields(cf *cronFields) error {
	fields := []struct {
		value string
		spec  cronFieldSpec
	}{
		{value: cf.minutes, spec: cronFieldSpec{
			name: "minutes", min: cronMinuteMin, max: cronMinuteMax, allowSlash: true,
		}},
		{value: cf.hours, spec: cronFieldSpec{
			name: "hours", min: cronHourMin, max: cronHourMax, allowSlash: true,
		}},
		{value: cf.dayOfMonth, spec: cronFieldSpec{
			name: "day-of-month", min: cronDayOfMonthMin, max: cronDayOfMonthMax,
			allowQuest: true, allowSlash: true, allowL: true, allowW: true,
		}},
		{value: cf.month, spec: cronFieldSpec{
			name: "month", min: cronMonthMin, max: cronMonthMax, resolve: cronMonthValue, allowSlash: true,
		}},
		{value: cf.dayOfWeek, spec: cronFieldSpec{
			name: "day-of-week", min: cronDayOfWeekMin, max: cronDayOfWeekMax, resolve: cronDOWValue,
			allowQuest: true, allowL: true, allowNL: true, allowHash: true,
		}},
		{value: cf.year, spec: cronFieldSpec{name: "year", min: cronYearMin, max: cronYearMax, allowSlash: true}},
	}

	for _, f := range fields {
		if err := validateCronField(f.spec, f.value); err != nil {
			return err
		}
	}

	return validateCronDayFields(cf.dayOfMonth, cf.dayOfWeek)
}

// validateCronDayFields enforces the AWS doc's day-of-month/day-of-week rule:
// "You can't use * in both the Day-of-month and Day-of-week fields. If you use
// it in one, you must use ? in the other".
func validateCronDayFields(dayOfMonth, dayOfWeek string) error {
	if dayOfMonth == "*" && dayOfWeek != "?" {
		return fmt.Errorf("%w: day-of-month is '*', day-of-week must be '?', got %q", ErrUnknownCronValue, dayOfWeek)
	}

	if dayOfWeek == "*" && dayOfMonth != "?" {
		return fmt.Errorf("%w: day-of-week is '*', day-of-month must be '?', got %q", ErrUnknownCronValue, dayOfMonth)
	}

	return nil
}

// validateCronField checks one field's raw value against spec: the whole-field "*"
// and "?" wildcards (subject to spec.allowQuest), then each comma-separated part.
func validateCronField(spec cronFieldSpec, field string) error {
	switch field {
	case "*":
		return nil
	case "?":
		if !spec.allowQuest {
			return fmt.Errorf("%w: %s field does not accept '?': %q", ErrUnknownCronValue, spec.name, field)
		}

		return nil
	}

	parts := strings.Split(field, ",")
	if spec.allowHash && len(parts) > 1 {
		for _, p := range parts {
			if strings.Contains(p, "#") {
				// AWS doc note: "If you use a '#' character, you can define only one
				// expression in the day-of-week field" -- e.g. "3#1,6#3" is invalid.
				return fmt.Errorf("%w: %s field: '#' cannot be combined with a comma list: %q",
					ErrUnknownCronValue, spec.name, field)
			}
		}
	}

	for _, part := range parts {
		if err := validateCronPart(spec, strings.TrimSpace(part)); err != nil {
			return err
		}
	}

	return nil
}

// validateCronPart validates a single comma-separated token: a step (base/n), an
// L/W/# wildcard, a range (lo-hi), a name alias, or a plain integer.
func validateCronPart(spec cronFieldSpec, part string) error {
	if strings.Contains(part, "/") {
		if !spec.allowSlash {
			return fmt.Errorf("%w: %s field does not accept '/' steps: %q", ErrUnknownCronValue, spec.name, part)
		}

		return validateCronStepPart(spec, part)
	}

	if handled, err := validateCronWildcardToken(spec, part); handled {
		return err
	}

	if lo, hi, isRange := strings.Cut(part, "-"); isRange {
		return validateCronRange(spec, lo, hi)
	}

	return validateCronValue(spec, part)
}

// validateCronWildcardToken recognizes the L/W/# tokens AWS documents for
// day-of-month and day-of-week. It reports handled=true when part matched one of
// these shapes, along with any validation error for that shape.
//
// This emulator's matcher (matchesCronPart in schedule_expression.go) does not
// implement L/W/# matching semantics, so a schedule using one still never fires --
// a pre-existing gap noted in 4f588177c's PARITY notes, unchanged by this pass.
// These are accepted here rather than rejected because AWS documents them as legal:
// rejecting real AWS syntax is the worse bug class this pass was warned against.
func validateCronWildcardToken(spec cronFieldSpec, part string) (bool, error) {
	if part == "L" {
		if !spec.allowL {
			return true, fmt.Errorf("%w: %s field does not accept 'L': %q", ErrUnknownCronValue, spec.name, part)
		}

		return true, nil
	}

	if spec.allowL {
		if handled, err := validateCronLOffsetToken(spec, part); handled {
			return true, err
		}
	}

	if spec.allowW {
		if handled, err := validateCronWToken(spec, part); handled {
			return true, err
		}
	}

	if spec.allowNL {
		if prefix, ok := strings.CutSuffix(part, "L"); ok && prefix != "" {
			if err := validateCronValue(spec, prefix); err != nil {
				return true, fmt.Errorf("%w: %s field: invalid 'L' token: %q", ErrUnknownCronValue, spec.name, part)
			}

			return true, nil
		}
	}

	if spec.allowHash && strings.Contains(part, "#") {
		return true, validateCronHashToken(spec, part)
	}

	return false, nil
}

// validateCronLOffsetToken recognizes the Quartz "L-<n>" offset idiom (n days/units
// before the last one, e.g. "L-2"). Neither the fetched EventBridge Scheduler doc
// (schedule-types.html) nor the legacy EventBridge cron doc
// (eb-scheduled-rule-pattern.html, fetched 2026-08-11) confirms or rules out this
// form in either day-of-month or day-of-week -- both document only bare "L".
// Accepted per under-enforcement bias rather than guessed-and-rejected; like the
// other L/W/# forms, matching semantics for it remain unimplemented.
func validateCronLOffsetToken(spec cronFieldSpec, part string) (bool, error) {
	offset, ok := strings.CutPrefix(part, "L-")
	if !ok {
		return false, nil
	}

	if _, err := strconv.Atoi(offset); err != nil {
		return true, fmt.Errorf("%w: %s field: invalid 'L-' offset token: %q", ErrUnknownCronValue, spec.name, part)
	}

	return true, nil
}

// validateCronWToken validates a day-of-month "<n>W" (nearest weekday to day n)
// token. "LW" (last weekday of month) is a common Quartz idiom but is not confirmed
// by the fetched AWS doc text, so it is accepted rather than guessed-and-rejected.
func validateCronWToken(spec cronFieldSpec, part string) (bool, error) {
	day, ok := strings.CutSuffix(part, "W")
	if !ok {
		return false, nil
	}

	if day == "L" {
		return true, nil
	}

	n, err := strconv.Atoi(day)
	if err != nil || n < spec.min || n > spec.max {
		return true, fmt.Errorf("%w: %s field: invalid 'W' token: %q", ErrUnknownCronValue, spec.name, part)
	}

	return true, nil
}

// validateCronHashToken validates an AWS "N#M" day-of-week token (Nth occurrence of
// weekday N in the month, e.g. "3#2" = second Tuesday). The fetched AWS doc text
// does not state an upper bound on the instance number M, so none is enforced here.
func validateCronHashToken(spec cronFieldSpec, part string) error {
	dow, instance, ok := strings.Cut(part, "#")
	if !ok {
		return fmt.Errorf("%w: %s field: invalid '#' token: %q", ErrUnknownCronValue, spec.name, part)
	}

	if err := validateCronValue(spec, dow); err != nil {
		return fmt.Errorf("%w: %s field: invalid '#' token: %q", ErrUnknownCronValue, spec.name, part)
	}

	n, err := strconv.Atoi(instance)
	if err != nil || n <= 0 {
		return fmt.Errorf("%w: %s field: invalid '#' instance: %q", ErrUnknownCronValue, spec.name, part)
	}

	return nil
}

// validateCronStepPart validates a base/step token (*/n, ?/n, n/n, n-m/n).
func validateCronStepPart(spec cronFieldSpec, part string) error {
	base, stepStr, _ := strings.Cut(part, "/")

	step, err := strconv.Atoi(stepStr)
	if err != nil || step <= 0 {
		return fmt.Errorf("%w: %s field: invalid step %q", ErrUnknownCronValue, spec.name, part)
	}

	switch base {
	case "*", "?":
		return nil
	}

	if lo, hi, isRange := strings.Cut(base, "-"); isRange {
		return validateCronRange(spec, lo, hi)
	}

	return validateCronValue(spec, base)
}

// validateCronRange validates a "lo-hi" token against spec's numeric range.
func validateCronRange(spec cronFieldSpec, lo, hi string) error {
	if err := validateCronValue(spec, lo); err != nil {
		return err
	}

	return validateCronValue(spec, hi)
}

// validateCronValue validates a single integer or name-alias token against spec's
// numeric range (name aliases are resolved to their numeric value first).
func validateCronValue(spec cronFieldSpec, token string) error {
	n, err := strconv.Atoi(token)
	if err != nil {
		if spec.resolve == nil {
			return fmt.Errorf("%w: %s field: %q", ErrUnknownCronValue, spec.name, token)
		}

		resolved, ok := spec.resolve(strings.ToUpper(token))
		if !ok {
			return fmt.Errorf("%w: %s field: %q", ErrUnknownCronValue, spec.name, token)
		}

		n = resolved
	}

	if n < spec.min || n > spec.max {
		return fmt.Errorf("%w: %s field: %d not in range [%d, %d]",
			ErrUnknownCronValue, spec.name, n, spec.min, spec.max)
	}

	return nil
}
