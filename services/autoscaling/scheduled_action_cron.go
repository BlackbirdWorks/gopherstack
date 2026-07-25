package autoscaling

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ErrInvalidRecurrence is returned when a ScheduledUpdateGroupAction's
// Recurrence field is not a valid 5-field Unix cron expression.
var ErrInvalidRecurrence = errors.New("invalid Recurrence cron expression")

// recurrenceFields is the number of whitespace-separated fields AWS documents
// for ScheduledUpdateGroupAction.Recurrence: "minute hour day-of-month month
// day-of-week", the standard 5-field Unix cron format (no seconds, no year --
// that distinguishes it from EventBridge's 6-field
// "minute hour day-of-month month day-of-week year" cron() expressions).
const recurrenceFields = 5

// Field value bounds used by matchRecurrenceField's step-expansion loop.
const (
	recurrenceMinuteMin     = 0
	recurrenceMinuteMax     = 59
	recurrenceHourMin       = 0
	recurrenceHourMax       = 23
	recurrenceDayOfMonthMin = 1
	recurrenceDayOfMonthMax = 31
	recurrenceMonthMin      = 1
	recurrenceMonthMax      = 12
	recurrenceDayOfWeekMin  = 0
	recurrenceDayOfWeekMax  = 6
	// recurrenceScanYears bounds how far into the future NextAfter will scan
	// looking for a match before giving up (protects against an expression
	// that can never match, e.g. "31 * * 2 *" -- Feb 31st never exists).
	recurrenceScanYears = 2
)

// recurrenceSchedule is a parsed 5-field Unix cron expression, minute-resolution.
// Supports numeric values, "*", comma-separated lists, "-" ranges, and "/" steps
// in each field -- the AWS docs for ScheduledUpdateGroupAction.Recurrence give
// only numeric examples (e.g. "0 10 * * *"), so named months/weekdays are
// intentionally not supported here (unlike EventBridge's richer cron()).
type recurrenceSchedule struct {
	minute     string
	hour       string
	dayOfMonth string
	month      string
	dayOfWeek  string
}

// parseRecurrence parses a Recurrence string like "0 10 * * *" (fires daily at
// 10:00 UTC). Returns an error if it does not have exactly 5 fields.
func parseRecurrence(expr string) (*recurrenceSchedule, error) {
	fields := strings.Fields(strings.TrimSpace(expr))
	if len(fields) != recurrenceFields {
		return nil, fmt.Errorf("%w: requires %d fields, got %d: %q",
			ErrInvalidRecurrence, recurrenceFields, len(fields), expr)
	}

	return &recurrenceSchedule{
		minute:     fields[0],
		hour:       fields[1],
		dayOfMonth: fields[2],
		month:      fields[3],
		dayOfWeek:  fields[4],
	}, nil
}

// NextAfter returns the next minute-resolution UTC time strictly after t that
// matches the schedule, scanning forward up to recurrenceScanYears before
// giving up (in which case it returns the scan limit itself, a time that will
// never compare <= any real "now" the caller checks it against).
func (r *recurrenceSchedule) NextAfter(t time.Time) time.Time {
	candidate := t.UTC().Truncate(time.Minute).Add(time.Minute)
	limit := t.UTC().AddDate(recurrenceScanYears, 0, 0)

	for candidate.Before(limit) {
		if r.matches(candidate) {
			return candidate
		}

		candidate = candidate.Add(time.Minute)
	}

	return limit
}

// matches reports whether t satisfies every field of the schedule. Unlike
// EventBridge's cron (where day-of-month/day-of-week use "?" wildcarding and
// an OR relationship), standard Unix cron ANDs every field -- both must match
// when both are non-"*".
func (r *recurrenceSchedule) matches(t time.Time) bool {
	return matchRecurrenceField(r.minute, t.Minute(), recurrenceMinuteMin, recurrenceMinuteMax) &&
		matchRecurrenceField(r.hour, t.Hour(), recurrenceHourMin, recurrenceHourMax) &&
		matchRecurrenceField(r.dayOfMonth, t.Day(), recurrenceDayOfMonthMin, recurrenceDayOfMonthMax) &&
		matchRecurrenceField(r.month, int(t.Month()), recurrenceMonthMin, recurrenceMonthMax) &&
		matchRecurrenceField(r.dayOfWeek, int(t.Weekday()), recurrenceDayOfWeekMin, recurrenceDayOfWeekMax)
}

// matchRecurrenceField checks if val matches a single cron field (which may be
// a comma-separated list of tokens).
func matchRecurrenceField(field string, val, fieldMin, fieldMax int) bool {
	if field == "*" {
		return true
	}

	for part := range strings.SplitSeq(field, ",") {
		if matchRecurrenceToken(strings.TrimSpace(part), val, fieldMin, fieldMax) {
			return true
		}
	}

	return false
}

// matchRecurrenceToken checks whether a single cron token (a step, a range, or
// an exact value) matches val.
func matchRecurrenceToken(token string, val, fieldMin, fieldMax int) bool {
	switch {
	case strings.Contains(token, "/"):
		return matchRecurrenceStep(token, val, fieldMin, fieldMax)
	case strings.Contains(token, "-"):
		return matchRecurrenceRange(token, val)
	default:
		n, err := strconv.Atoi(token)

		return err == nil && n == val
	}
}

// matchRecurrenceRange returns true if val falls within the inclusive range
// "lo-hi".
func matchRecurrenceRange(token string, val int) bool {
	const rangeParts = 2

	parts := strings.SplitN(token, "-", rangeParts)
	if len(parts) != rangeParts {
		return false
	}

	lo, errLo := strconv.Atoi(parts[0])
	hi, errHi := strconv.Atoi(parts[1])

	if errLo != nil || errHi != nil {
		return false
	}

	return val >= lo && val <= hi
}

// matchRecurrenceStep returns true if val matches a step pattern like
// "*/step" or "start/step".
func matchRecurrenceStep(token string, val, fieldMin, fieldMax int) bool {
	const stepParts = 2

	parts := strings.SplitN(token, "/", stepParts)
	if len(parts) != stepParts {
		return false
	}

	step, err := strconv.Atoi(parts[1])
	if err != nil || step <= 0 {
		return false
	}

	start := fieldMin

	if parts[0] != "*" {
		n, startErr := strconv.Atoi(parts[0])
		if startErr != nil {
			return false
		}

		start = n
	}

	for v := start; v <= fieldMax; v += step {
		if v == val {
			return true
		}
	}

	return false
}
