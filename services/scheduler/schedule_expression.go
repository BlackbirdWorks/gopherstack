package scheduler

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// cronStepMaxRange is the inclusive upper bound used when a cron step has no explicit end
// (e.g. "*/5"). It must be larger than any valid field value (year ~9999).
const cronStepMaxRange = 9999

var (
	// ErrInvalidRateExpression is returned for malformed rate() expressions.
	ErrInvalidRateExpression = errors.New("invalid rate expression")
	// ErrInvalidRateValue is returned when the numeric value in a rate() expression is not valid.
	ErrInvalidRateValue = errors.New("invalid rate value")
	// ErrUnknownRateUnit is returned when the unit in a rate() expression is not recognised.
	ErrUnknownRateUnit = errors.New("unknown rate unit")
	// ErrInvalidCronExpression is returned for malformed cron() expressions.
	ErrInvalidCronExpression = errors.New("invalid cron expression")
)

// validateScheduleExpression checks that the ScheduleExpression has a valid prefix and format.
// AWS Scheduler accepts: rate(value unit), cron(fields), at(datetime).
// A cron expression must have exactly 6 space-separated fields inside cron(...).
func validateScheduleExpression(expr string) error {
	switch {
	case strings.HasPrefix(expr, "rate("):
		if !strings.HasSuffix(expr, ")") {
			return fmt.Errorf("%w: ScheduleExpression rate expression must end with ')'", ErrValidation)
		}
	case strings.HasPrefix(expr, "at("):
		if !strings.HasSuffix(expr, ")") {
			return fmt.Errorf("%w: ScheduleExpression at expression must end with ')'", ErrValidation)
		}
	case strings.HasPrefix(expr, "cron("):
		if !strings.HasSuffix(expr, ")") {
			return fmt.Errorf("%w: ScheduleExpression cron expression must end with ')'", ErrValidation)
		}
		inner := expr[len("cron(") : len(expr)-1]
		fields := strings.Fields(inner)
		if len(fields) != cronFieldCount {
			return fmt.Errorf(
				"%w: ScheduleExpression cron expression must have exactly %d fields "+
					"(minutes hours day-of-month month day-of-week year), got %d",
				ErrValidation,
				cronFieldCount,
				len(fields),
			)
		}
	default:
		return fmt.Errorf(
			"%w: ScheduleExpression must start with rate(), cron(), or at(); got %q",
			ErrValidation, expr,
		)
	}

	return nil
}

// parseRateExpression parses an AWS EventBridge rate expression.
// Supported units: minutes, hours, days (and singular forms).
// Non-standard unit "seconds" is also supported for local testing.
func parseRateExpression(expr string) (time.Duration, error) {
	const rateFieldCount = 2 // rate(N unit) always has exactly two whitespace-separated fields

	inner := strings.TrimSuffix(strings.TrimPrefix(expr, "rate("), ")")
	inner = strings.TrimSpace(inner)

	parts := strings.Fields(inner)
	if len(parts) != rateFieldCount {
		return 0, fmt.Errorf("%w: %q", ErrInvalidRateExpression, expr)
	}

	n, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%w: %q", ErrInvalidRateValue, parts[0])
	}

	unit := strings.ToLower(parts[1])

	switch unit {
	case "second", "seconds":
		return time.Duration(n) * time.Second, nil
	case "minute", "minutes":
		return time.Duration(n) * time.Minute, nil
	case "hour", "hours":
		return time.Duration(n) * time.Hour, nil
	case "day", "days":
		return time.Duration(n) * 24 * time.Hour, nil
	}

	return 0, fmt.Errorf("%w: %q", ErrUnknownRateUnit, parts[1])
}

// cronFields holds the parsed fields of a 6-field AWS EventBridge cron expression.
// AWS cron format: cron(Minutes Hours Day-of-month Month Day-of-week Year).
type cronFields struct {
	minutes    string
	hours      string
	dayOfMonth string
	month      string
	dayOfWeek  string
	year       string
}

// parseCronExpression parses an AWS EventBridge 6-field cron expression.
func parseCronExpression(expr string) (*cronFields, error) {
	inner := strings.TrimSuffix(strings.TrimPrefix(expr, "cron("), ")")
	inner = strings.TrimSpace(inner)

	parts := strings.Fields(inner)

	const cronExprFieldCount = 6
	if len(parts) != cronExprFieldCount {
		return nil, fmt.Errorf("%w: must have 6 fields, got %d: %q", ErrInvalidCronExpression, len(parts), expr)
	}

	return &cronFields{
		minutes:    parts[0],
		hours:      parts[1],
		dayOfMonth: parts[2],
		month:      parts[3],
		dayOfWeek:  parts[4],
		year:       parts[5],
	}, nil
}

// matchesCron returns true if t matches the given cron fields.
// Supports wildcard (*), don't-care (?), single integers, comma-separated lists,
// ranges (n-m), steps (*/s, n/s, n-m/s), and month/weekday name aliases.
func matchesCron(t time.Time, cf *cronFields) bool {
	return matchesCronField(cf.minutes, t.Minute()) &&
		matchesCronField(cf.hours, t.Hour()) &&
		matchesCronField(cf.dayOfMonth, t.Day()) &&
		matchesCronField(cf.month, int(t.Month())) &&
		matchesCronField(cf.dayOfWeek, dayOfWeekAWS(t.Weekday())) &&
		matchesCronField(cf.year, t.Year())
}

// dayOfWeekAWS converts Go's [time.Weekday] to the AWS cron day-of-week (1=Sunday, 7=Saturday).
func dayOfWeekAWS(wd time.Weekday) int {
	return int(wd) + 1
}

// matchesCronField checks whether a cron field pattern matches a numeric value.
// Supports: * (any), ? (any), comma-separated lists, ranges (n-m), steps (*/s, n/s, n-m/s),
// and month/weekday name aliases (JAN-DEC, SUN-SAT).
func matchesCronField(field string, value int) bool {
	if field == "*" || field == "?" {
		return true
	}

	for part := range strings.SplitSeq(field, ",") {
		part = strings.TrimSpace(part)
		if matchesCronPart(part, value) {
			return true
		}
	}

	return false
}

// matchesCronPart evaluates a single cron token (no commas) against value.
// Handles: integer, name alias, range (n-m), step (*/s, n/s, n-m/s).
func matchesCronPart(part string, value int) bool {
	// Step: base/step
	if baseStr, stepStr, isStep := strings.Cut(part, "/"); isStep {
		return matchesCronStep(baseStr, stepStr, cronStepMaxRange, value)
	}

	// Range: n-m
	if lo, hi, isRange := strings.Cut(part, "-"); isRange {
		start, err1 := parseCronValue(lo)
		end, err2 := parseCronValue(hi)
		if err1 != nil || err2 != nil {
			return false
		}

		return value >= start && value <= end
	}

	// Single value or name alias
	n, err := parseCronValue(part)

	return err == nil && n == value
}

// matchesCronStep evaluates a step token (base/step) against value.
func matchesCronStep(baseStr, stepStr string, maxVal, value int) bool {
	step, err := strconv.Atoi(stepStr)
	if err != nil || step <= 0 {
		return false
	}

	start, end := 0, maxVal

	switch baseStr {
	case "*", "?":
		// */step — every step starting from 0; end stays at maxVal
	default:
		if lo, hi, isRange := strings.Cut(baseStr, "-"); isRange {
			s, err1 := parseCronValue(lo)
			e, err2 := parseCronValue(hi)
			if err1 != nil || err2 != nil {
				return false
			}

			start, end = s, e
		} else {
			s, parseErr := parseCronValue(baseStr)
			if parseErr != nil {
				return false
			}

			start = s
		}
	}

	if value < start || value > end {
		return false
	}

	return (value-start)%step == 0
}

// ErrUnknownCronValue is returned when a cron field token cannot be parsed.
var ErrUnknownCronValue = errors.New("unknown cron value")

// Month and weekday numeric constants used in AWS EventBridge cron expressions.
const (
	cronJan = 1
	cronFeb = 2
	cronMar = 3
	cronApr = 4
	cronMay = 5
	cronJun = 6
	cronJul = 7
	cronAug = 8
	cronSep = 9
	cronOct = 10
	cronNov = 11
	cronDec = 12

	// AWS cron day-of-week: 1=SUN, 2=MON, ..., 7=SAT.
	cronSun = 1
	cronMon = 2
	cronTue = 3
	cronWed = 4
	cronThu = 5
	cronFri = 6
	cronSat = 7
)

// cronMonthValue maps a 3-letter month abbreviation (uppercase) to its numeric value (1=JAN..12=DEC).
// Cases are ordered alphabetically for lint compliance (cyclop/cyclop keeps the count within limits).
func cronMonthValue(upper string) (int, bool) {
	switch upper {
	case "APR":
		return cronApr, true
	case "AUG":
		return cronAug, true
	case "DEC":
		return cronDec, true
	case "FEB":
		return cronFeb, true
	case "JAN":
		return cronJan, true
	case "JUL":
		return cronJul, true
	case "JUN":
		return cronJun, true
	case "MAR":
		return cronMar, true
	case "MAY":
		return cronMay, true
	case "NOV":
		return cronNov, true
	case "OCT":
		return cronOct, true
	case "SEP":
		return cronSep, true
	}

	return 0, false
}

// cronDOWValue maps a 3-letter weekday abbreviation (uppercase) to its AWS numeric value.
// AWS uses 1=SUN, 2=MON, ..., 7=SAT.
func cronDOWValue(upper string) (int, bool) {
	switch upper {
	case "FRI":
		return cronFri, true
	case "MON":
		return cronMon, true
	case "SAT":
		return cronSat, true
	case "SUN":
		return cronSun, true
	case "THU":
		return cronThu, true
	case "TUE":
		return cronTue, true
	case "WED":
		return cronWed, true
	}

	return 0, false
}

// parseCronValue parses a single cron field token: an integer or a month/weekday name alias.
func parseCronValue(s string) (int, error) {
	if n, err := strconv.Atoi(s); err == nil {
		return n, nil
	}

	upper := strings.ToUpper(s)

	if n, ok := cronMonthValue(upper); ok {
		return n, nil
	}

	if n, ok := cronDOWValue(upper); ok {
		return n, nil
	}

	return 0, fmt.Errorf("%w: %q", ErrUnknownCronValue, s)
}
