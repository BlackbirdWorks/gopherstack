// Package awscron implements the field-matching primitives shared by AWS's
// cron(...) schedule expression dialects: numeric values, three-letter
// names, *, ?, ranges, steps, and comma lists for a single field, plus AWS's
// day-of-month/day-of-week OR semantics.
//
// AWS does not use one dialect. EventBridge Rules and Redshift
// ScheduledAction both require six fields ending in Year (aws-sdk-go-v2
// eventbridge@v1.48.4 api_op_PutRule.go:118, example "cron(0 20 * * ? *)";
// redshift@v1.65.4 api_op_CreateScheduledAction.go:104-105, example
// "cron(0 10 ? * MON *)"), while CloudWatch alarm mute schedules use five
// fields with no Year (cloudwatch@v1.66.3 types/types.go:3441-3442, example
// "cron(0 2 * * *)"). Redshift's at() form carries seconds
// (api_op_CreateScheduledAction.go:101-102, "at(2016-03-04T17:27:00)");
// CloudWatch's does not (types/types.go:3459, "at(2024-05-10T14:00)");
// EventBridge Rules has no at() form at all.
//
// Because the field count and the at() layout both vary by caller, this
// package does not assemble a cron expression type of its own -- doing so
// would force every caller onto one dialect and silently accept expressions
// the real service rejects. Instead each caller defines its own expression
// struct with its own field count and at() parsing, and calls MatchField per
// field and MatchDayFields for the day-of-month/day-of-week rule.
package awscron

import (
	"strconv"
	"strings"
	"time"
)

// FieldKind identifies which cron field a token belongs to, so numeric/name
// resolution can apply AWS's field-specific conventions: month and
// day-of-week accept three-letter names, and AWS's day-of-week numbering
// (1-7, where 1 = Sunday) differs from Go's time.Weekday (0-6, where 0 =
// Sunday).
type FieldKind int

const (
	FieldMinute FieldKind = iota
	FieldHour
	FieldDayOfMonth
	FieldMonth
	FieldDayOfWeek
	FieldYear
)

const (
	MinuteMin = 0
	MinuteMax = 59

	HourMin = 0
	HourMax = 23

	MonthMin = 1
	MonthMax = 12

	DayOfMonthMin = 1
	DayOfMonthMax = 31

	DayOfWeekMin = 0
	DayOfWeekMax = 6

	YearMin = 1970
	YearMax = 2199
)

//nolint:gochecknoglobals // read-only lookup table initialized once at startup
var monthNames = map[string]int{
	"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, //nolint:mnd // calendar month numbers, not magic constants
	"MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8, //nolint:mnd // calendar month numbers, not magic constants
	"SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12, //nolint:mnd // calendar month numbers, not magic constants
}

// dowNames maps AWS's three-letter day-of-week names directly to Go's
// time.Weekday numbering (0 = Sunday), so no offset is needed after lookup.
//
//nolint:gochecknoglobals // read-only lookup table initialized once at startup
var dowNames = map[string]int{
	"SUN": 0, "MON": 1, "TUE": 2, "WED": 3, //nolint:mnd // time.Weekday numbers, not magic constants
	"THU": 4, "FRI": 5, "SAT": 6, //nolint:mnd // time.Weekday numbers, not magic constants
}

// awsDayOfWeekBase is AWS's numeric base for day-of-week fields (1-7, 1 =
// Sunday), vs. Go's time.Weekday (0-6, 0 = Sunday).
const awsDayOfWeekBase = 1

// TokenValue resolves a single cron token to its comparable integer value
// for the given field kind: three-letter names for month/day-of-week,
// otherwise a plain number. Numeric day-of-week tokens are converted from
// AWS's 1-7 (Sunday = 1) convention to Go's time.Weekday 0-6 (Sunday = 0) so
// callers can compare directly against int(t.Weekday()).
func TokenValue(kind FieldKind, token string) (int, bool) {
	switch kind {
	case FieldMonth:
		if n, ok := monthNames[strings.ToUpper(token)]; ok {
			return n, true
		}
	case FieldDayOfWeek:
		if n, ok := dowNames[strings.ToUpper(token)]; ok {
			return n, true
		}
	case FieldMinute, FieldHour, FieldDayOfMonth, FieldYear:
		// Numeric-only fields; fall through to the plain-integer parse below.
	}

	n, err := strconv.Atoi(token)
	if err != nil {
		return 0, false
	}

	if kind == FieldDayOfWeek {
		const daysPerWeek = 7
		n = ((n-awsDayOfWeekBase)%daysPerWeek + daysPerWeek) % daysPerWeek
	}

	return n, true
}

// MatchField reports whether val matches a cron field: *, ?, or a
// comma-separated list of tokens (numeric, name, range, or step).
func MatchField(kind FieldKind, field string, val, fieldMin, fieldMax int) bool {
	if field == "*" || field == "?" {
		return true
	}

	for part := range strings.SplitSeq(field, ",") {
		if matchToken(kind, strings.TrimSpace(part), val, fieldMin, fieldMax) {
			return true
		}
	}

	return false
}

func matchToken(kind FieldKind, token string, val, fieldMin, fieldMax int) bool {
	switch {
	case strings.Contains(token, "/"):
		// Checked before "-": a step's start may itself be a range (e.g.
		// "0-30/10"), which must not be misrouted to matchRange.
		return matchStep(kind, token, val, fieldMin, fieldMax)
	case strings.Contains(token, "-"):
		return matchRange(kind, token, val)
	default:
		n, ok := TokenValue(kind, token)

		return ok && n == val
	}
}

const rangeParts = 2

func matchRange(kind FieldKind, token string, val int) bool {
	parts := strings.SplitN(token, "-", rangeParts)
	if len(parts) != rangeParts {
		return false
	}

	lo, ok1 := TokenValue(kind, parts[0])
	hi, ok2 := TokenValue(kind, parts[1])

	if !ok1 || !ok2 {
		return false
	}

	if kind == FieldDayOfWeek && hi < lo {
		// A day-of-week range may wrap past Saturday back to Sunday after the
		// AWS->Go conversion (e.g. AWS "FRI-MON" spans Fri,Sat,Sun,Mon).
		return val >= lo || val <= hi
	}

	return val >= lo && val <= hi
}

func matchStep(kind FieldKind, token string, val, fieldMin, fieldMax int) bool {
	parts := strings.SplitN(token, "/", rangeParts)
	if len(parts) != rangeParts {
		return false
	}

	step, err := strconv.Atoi(parts[1])
	if err != nil || step <= 0 {
		return false
	}

	start, end, ok := stepBounds(kind, parts[0], fieldMin, fieldMax)
	if !ok {
		return false
	}

	for v := start; v <= end; v += step {
		if v == val {
			return true
		}
	}

	return false
}

// stepBounds resolves the "start" portion of a step token ("*/N", "5/N", or
// a range "0-30/N") into the [start, end] the step iterates over.
func stepBounds(kind FieldKind, start string, fieldMin, fieldMax int) (int, int, bool) {
	switch {
	case start == "*":
		return fieldMin, fieldMax, true
	case strings.Contains(start, "-"):
		parts := strings.SplitN(start, "-", rangeParts)
		if len(parts) != rangeParts {
			return 0, 0, false
		}

		lo, ok1 := TokenValue(kind, parts[0])
		hi, ok2 := TokenValue(kind, parts[1])

		return lo, hi, ok1 && ok2
	default:
		n, ok := TokenValue(kind, start)

		return n, fieldMax, ok
	}
}

// MatchDayFields evaluates the AWS day-of-month/day-of-week rule: a
// wildcard ("*" or "?") field defers entirely to the other; when both are
// concrete, AWS matches if either is satisfied.
func MatchDayFields(dayOfMonth, dayOfWeek string, t time.Time) bool {
	domWild := dayOfMonth == "?" || dayOfMonth == "*"
	dowWild := dayOfWeek == "?" || dayOfWeek == "*"

	switch {
	case domWild && dowWild:
		return true
	case domWild:
		return MatchField(FieldDayOfWeek, dayOfWeek, int(t.Weekday()), DayOfWeekMin, DayOfWeekMax)
	case dowWild:
		return MatchField(FieldDayOfMonth, dayOfMonth, t.Day(), DayOfMonthMin, DayOfMonthMax)
	default:
		domMatch := MatchField(FieldDayOfMonth, dayOfMonth, t.Day(), DayOfMonthMin, DayOfMonthMax)
		dowMatch := MatchField(FieldDayOfWeek, dayOfWeek, int(t.Weekday()), DayOfWeekMin, DayOfWeekMax)

		return domMatch || dowMatch
	}
}
