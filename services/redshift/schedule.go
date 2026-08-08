package redshift

import (
	"strconv"
	"strings"
	"time"
)

// maxNextInvocations bounds how many upcoming ScheduledActionTime entries this
// backend computes -- AWS does not document an exact cap, this keeps the list
// finite and matches the small number the console/API typically surfaces.
const maxNextInvocations = 5

// nextInvocations computes up to maxNextInvocations future invocation times for a
// Schedule expression, evaluating the real at()/cron() expression the caller
// supplied rather than fabricating timestamps. Unrecognized or malformed
// expressions (including "rate(...)", which real Redshift does not accept for
// ScheduledAction.Schedule) yield an empty, honest result instead of a guess.
func nextInvocations(schedule string, after time.Time) []time.Time {
	schedule = strings.TrimSpace(schedule)

	switch {
	case strings.HasPrefix(schedule, "at(") && strings.HasSuffix(schedule, ")"):
		return nextAtInvocation(schedule, after)
	case strings.HasPrefix(schedule, "cron(") && strings.HasSuffix(schedule, ")"):
		return nextCronInvocations(schedule, after)
	default:
		return nil
	}
}

// nextAtInvocation parses "at(yyyy-mm-ddThh:mm:ss)" (real Redshift's one-time
// schedule format) and returns the single invocation if it is still in the future.
func nextAtInvocation(schedule string, after time.Time) []time.Time {
	inner := schedule[len("at(") : len(schedule)-1]

	t, err := time.ParseInLocation("2006-01-02T15:04:05", inner, time.UTC)
	if err != nil {
		return nil
	}

	if t.Before(after) {
		return nil
	}

	return []time.Time{t}
}

// redshiftCronFields is the field count real Redshift cron expressions require:
// Minutes Hours Day-of-month Month Day-of-week Year (confirmed against the
// CreateScheduledActionInput.Schedule doc comment in
// aws-sdk-go-v2/service/redshift@v1.65.4/types/types.go).
const redshiftCronFields = 6

// nextCronInvocations parses "cron(Minutes Hours Day-of-month Month Day-of-week
// Year)" and returns up to maxNextInvocations matching times via a minute-resolution
// forward scan, capped at cronScanLimit to bound worst-case work for an
// expression that never matches (e.g. a Year field already in the past).
func nextCronInvocations(schedule string, after time.Time) []time.Time {
	inner := schedule[len("cron(") : len(schedule)-1]
	fields := strings.Fields(inner)

	if len(fields) != redshiftCronFields {
		return nil
	}

	c := &cronExpr{
		minute: fields[0], hour: fields[1], dayOfMonth: fields[2],
		month: fields[3], dayOfWeek: fields[4], year: fields[5],
	}

	const cronScanLimit = 2 * 365 * 24 * time.Hour

	limit := after.UTC().Add(cronScanLimit)
	candidate := after.UTC().Truncate(time.Minute).Add(time.Minute)

	var out []time.Time

	for candidate.Before(limit) && len(out) < maxNextInvocations {
		if c.matches(candidate) {
			out = append(out, candidate)
		}

		candidate = candidate.Add(time.Minute)
	}

	return out
}

// cronExpr is a parsed 6-field AWS cron(min hour dom month dow year) expression.
// Supports numeric values/names, *, ?, ranges, steps, and comma lists.
type cronExpr struct {
	minute     string
	hour       string
	dayOfMonth string
	month      string
	dayOfWeek  string
	year       string
}

const (
	cronMinuteMin, cronMinuteMax         = 0, 59
	cronHourMin, cronHourMax             = 0, 23
	cronMonthMin, cronMonthMax           = 1, 12
	cronDayOfMonthMin, cronDayOfMonthMax = 1, 31
	cronDayOfWeekMin, cronDayOfWeekMax   = 0, 6
	cronYearMin, cronYearMax             = 1970, 2199
)

func (c *cronExpr) matches(t time.Time) bool {
	if !matchCronField(cronFieldMinute, c.minute, t.Minute(), cronMinuteMin, cronMinuteMax) {
		return false
	}

	if !matchCronField(cronFieldHour, c.hour, t.Hour(), cronHourMin, cronHourMax) {
		return false
	}

	if !matchCronField(cronFieldMonth, c.month, int(t.Month()), cronMonthMin, cronMonthMax) {
		return false
	}

	if !matchCronField(cronFieldYear, c.year, t.Year(), cronYearMin, cronYearMax) {
		return false
	}

	return c.matchDayFields(t)
}

// matchDayFields evaluates dayOfMonth/dayOfWeek: a wildcard field defers entirely
// to the other; when both are concrete, AWS matches if either is satisfied.
func (c *cronExpr) matchDayFields(t time.Time) bool {
	domWild := c.dayOfMonth == "?" || c.dayOfMonth == "*"
	dowWild := c.dayOfWeek == "?" || c.dayOfWeek == "*"

	switch {
	case domWild && dowWild:
		return true
	case domWild:
		return matchCronField(
			cronFieldDayOfWeek,
			c.dayOfWeek,
			int(t.Weekday()),
			cronDayOfWeekMin,
			cronDayOfWeekMax,
		)
	case dowWild:
		return matchCronField(
			cronFieldDayOfMonth,
			c.dayOfMonth,
			t.Day(),
			cronDayOfMonthMin,
			cronDayOfMonthMax,
		)
	default:
		domMatch := matchCronField(
			cronFieldDayOfMonth,
			c.dayOfMonth,
			t.Day(),
			cronDayOfMonthMin,
			cronDayOfMonthMax,
		)
		dowMatch := matchCronField(
			cronFieldDayOfWeek,
			c.dayOfWeek,
			int(t.Weekday()),
			cronDayOfWeekMin,
			cronDayOfWeekMax,
		)

		return domMatch || dowMatch
	}
}

type cronFieldKind int

const (
	cronFieldMinute cronFieldKind = iota
	cronFieldHour
	cronFieldDayOfMonth
	cronFieldMonth
	cronFieldDayOfWeek
	cronFieldYear
)

//nolint:gochecknoglobals // read-only lookup table initialized once at startup
var cronMonthNames = map[string]int{
	"JAN": 1,
	"FEB": 2,  //nolint:mnd // calendar month number, not a magic constant
	"MAR": 3,  //nolint:mnd // calendar month number, not a magic constant
	"APR": 4,  //nolint:mnd // calendar month number, not a magic constant
	"MAY": 5,  //nolint:mnd // calendar month number, not a magic constant
	"JUN": 6,  //nolint:mnd // calendar month number, not a magic constant
	"JUL": 7,  //nolint:mnd // calendar month number, not a magic constant
	"AUG": 8,  //nolint:mnd // calendar month number, not a magic constant
	"SEP": 9,  //nolint:mnd // calendar month number, not a magic constant
	"OCT": 10, //nolint:mnd // calendar month number, not a magic constant
	"NOV": 11, //nolint:mnd // calendar month number, not a magic constant
	"DEC": 12, //nolint:mnd // calendar month number, not a magic constant
}

// cronDowNames maps AWS's three-letter day-of-week names directly to Go's
// time.Weekday numbering (0 = Sunday), so no offset is needed after lookup.
//
//nolint:gochecknoglobals // read-only lookup table initialized once at startup
var cronDowNames = map[string]int{
	"SUN": 0,
	"MON": 1,
	"TUE": 2, //nolint:mnd // time.Weekday number, not a magic constant
	"WED": 3, //nolint:mnd // time.Weekday number, not a magic constant
	"THU": 4, //nolint:mnd // time.Weekday number, not a magic constant
	"FRI": 5, //nolint:mnd // time.Weekday number, not a magic constant
	"SAT": 6, //nolint:mnd // time.Weekday number, not a magic constant
}

// awsDayOfWeekBase is AWS's numeric base for day-of-week fields (1-7, 1 = Sunday),
// vs. Go's time.Weekday (0-6, 0 = Sunday).
const awsDayOfWeekBase = 1

func cronTokenValue(kind cronFieldKind, token string) (int, bool) {
	switch kind {
	case cronFieldMonth:
		if n, ok := cronMonthNames[strings.ToUpper(token)]; ok {
			return n, true
		}
	case cronFieldDayOfWeek:
		if n, ok := cronDowNames[strings.ToUpper(token)]; ok {
			return n, true
		}
	case cronFieldMinute, cronFieldHour, cronFieldDayOfMonth, cronFieldYear:
		// Numeric-only fields; fall through to the plain-integer parse below.
	}

	n, err := strconv.Atoi(token)
	if err != nil {
		return 0, false
	}

	if kind == cronFieldDayOfWeek {
		const daysPerWeek = 7
		n = ((n-awsDayOfWeekBase)%daysPerWeek + daysPerWeek) % daysPerWeek
	}

	return n, true
}

func matchCronField(kind cronFieldKind, field string, val, fieldMin, fieldMax int) bool {
	if field == "*" || field == "?" {
		return true
	}

	for part := range strings.SplitSeq(field, ",") {
		if matchCronToken(kind, strings.TrimSpace(part), val, fieldMin, fieldMax) {
			return true
		}
	}

	return false
}

func matchCronToken(kind cronFieldKind, token string, val, fieldMin, fieldMax int) bool {
	switch {
	case strings.Contains(token, "/"):
		// Checked before "-": a step's start may itself be a range (e.g.
		// "0-30/10"), which must not be misrouted to matchCronRange.
		return matchCronStep(kind, token, val, fieldMin, fieldMax)
	case strings.Contains(token, "-"):
		return matchCronRange(kind, token, val)
	default:
		n, ok := cronTokenValue(kind, token)

		return ok && n == val
	}
}

const cronRangeParts = 2

func matchCronRange(kind cronFieldKind, token string, val int) bool {
	parts := strings.SplitN(token, "-", cronRangeParts)
	if len(parts) != cronRangeParts {
		return false
	}

	lo, ok1 := cronTokenValue(kind, parts[0])
	hi, ok2 := cronTokenValue(kind, parts[1])

	if !ok1 || !ok2 {
		return false
	}

	if kind == cronFieldDayOfWeek && hi < lo {
		// A day-of-week range may wrap past Saturday back to Sunday after the
		// AWS->Go conversion (e.g. AWS "FRI-MON" spans Fri,Sat,Sun,Mon).
		return val >= lo || val <= hi
	}

	return val >= lo && val <= hi
}

func matchCronStep(kind cronFieldKind, token string, val, fieldMin, fieldMax int) bool {
	parts := strings.SplitN(token, "/", cronRangeParts)
	if len(parts) != cronRangeParts {
		return false
	}

	step, err := strconv.Atoi(parts[1])
	if err != nil || step <= 0 {
		return false
	}

	start, end, ok := cronStepBounds(kind, parts[0], fieldMin, fieldMax)
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

// cronStepBounds resolves the "start" portion of a step token ("*/N",
// "5/N", or a range "0-30/N") into the [start, end] the step iterates over.
func cronStepBounds(kind cronFieldKind, start string, fieldMin, fieldMax int) (int, int, bool) {
	switch {
	case start == "*":
		return fieldMin, fieldMax, true
	case strings.Contains(start, "-"):
		parts := strings.SplitN(start, "-", cronRangeParts)
		if len(parts) != cronRangeParts {
			return 0, 0, false
		}

		lo, ok1 := cronTokenValue(kind, parts[0])
		hi, ok2 := cronTokenValue(kind, parts[1])

		return lo, hi, ok1 && ok2
	default:
		n, ok := cronTokenValue(kind, start)

		return n, fieldMax, ok
	}
}
