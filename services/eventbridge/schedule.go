package eventbridge

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Sentinel errors for schedule expression parsing.
var (
	ErrUnsupportedScheduleExpression = errors.New("unsupported schedule expression")
	ErrInvalidRateExpression         = errors.New("invalid rate expression")
	ErrInvalidRateValue              = errors.New("invalid rate value")
	ErrUnsupportedRateUnit           = errors.New("unsupported rate unit")
	ErrInvalidCronExpression         = errors.New("invalid cron expression")
)

// scheduleExpression represents a parsed schedule expression.
type scheduleExpression interface {
	// NextAfter returns the next fire time at or after t, or the zero Time
	// if the expression has no match at or after t.
	NextAfter(t time.Time) time.Time
}

// rateExpression represents a rate(N unit) schedule.
type rateExpression struct {
	interval time.Duration
}

// NextAfter returns the next fire time at or after t by rounding to interval multiples from epoch.
func (r *rateExpression) NextAfter(t time.Time) time.Time {
	epoch := time.Unix(0, 0).UTC()
	since := t.Sub(epoch)
	// n is a dimensionless multiple (stored as int64 to avoid duration*duration lint error).
	n := int64(since/r.interval) + 1

	return epoch.Add(time.Duration(n) * r.interval)
}

// parseScheduleExpression parses a rate() or cron() schedule expression.
// Returns an error if the expression is not recognized.
func parseScheduleExpression(expr string) (scheduleExpression, error) {
	expr = strings.TrimSpace(expr)

	if strings.HasPrefix(expr, "rate(") && strings.HasSuffix(expr, ")") {
		return parseRate(expr)
	}

	if strings.HasPrefix(expr, "cron(") && strings.HasSuffix(expr, ")") {
		return parseCron(expr)
	}

	return nil, fmt.Errorf("%w: %q", ErrUnsupportedScheduleExpression, expr)
}

// rateExpressionFields is the expected number of fields in a rate expression.
const rateExpressionFields = 2

// parseRate parses expressions like "rate(5 minutes)" or "rate(1 hour)".
func parseRate(expr string) (*rateExpression, error) {
	inner := expr[len("rate(") : len(expr)-1]
	inner = strings.TrimSpace(inner)
	parts := strings.Fields(inner)

	if len(parts) != rateExpressionFields {
		return nil, fmt.Errorf("%w: %q", ErrInvalidRateExpression, expr)
	}

	n, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || n <= 0 {
		return nil, fmt.Errorf("%w: %q", ErrInvalidRateValue, parts[0])
	}

	unit := strings.ToLower(parts[1])
	// Remove trailing 's' to normalize "minutes" -> "minute" etc.
	unit = strings.TrimSuffix(unit, "s")

	var d time.Duration
	switch unit {
	case "second":
		d = time.Duration(n) * time.Second
	case "minute":
		d = time.Duration(n) * time.Minute
	case "hour":
		d = time.Duration(n) * time.Hour
	case "day":
		d = time.Duration(n) * cronHoursPerDay * time.Hour
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedRateUnit, parts[1])
	}

	return &rateExpression{interval: d}, nil
}

// cronFieldRanges defines the valid bounds for cron fields.
const (
	cronFields        = 6  // required number of fields in a cron expression
	cronHoursPerDay   = 24 // hours in a day (for rate day computation)
	cronMinuteMin     = 0
	cronMinuteMax     = 59
	cronHourMin       = 0
	cronHourMax       = 23
	cronMonthMin      = 1
	cronMonthMax      = 12
	cronDayOfMonthMin = 1
	cronDayOfMonthMax = 31
	cronDayOfWeekMin  = 0
	cronDayOfWeekMax  = 6
	cronYearMin       = 1970
	cronYearMax       = 2199
	cronScanYears     = 2 // forward scan limit in years
)

// cronExpression represents a parsed cron(min hour day month weekday year) schedule.
// Fields: minute, hour, dayOfMonth, month, dayOfWeek, year
// Supports: numeric values, *, ?, and comma-separated lists.
type cronExpression struct {
	minute     string
	hour       string
	dayOfMonth string
	month      string
	dayOfWeek  string
	year       string
}

// parseCron parses expressions like "cron(0 12 * * ? *)".
func parseCron(expr string) (*cronExpression, error) {
	inner := expr[len("cron(") : len(expr)-1]
	fields := strings.Fields(inner)

	if len(fields) != cronFields {
		return nil, fmt.Errorf(
			"%w: requires %d fields, got %d: %q",
			ErrInvalidCronExpression,
			cronFields,
			len(fields),
			expr,
		)
	}

	return &cronExpression{
		minute:     fields[0],
		hour:       fields[1],
		dayOfMonth: fields[2],
		month:      fields[3],
		dayOfWeek:  fields[4],
		year:       fields[5],
	}, nil
}

// NextAfter returns the next time at or after t that matches the cron expression,
// or the zero Time if nothing matches within the scan window (callers must check
// IsZero() rather than treat it as a real fire time).
// Implementation is a simple minute-resolution forward scan (max 2 years ahead).
func (c *cronExpression) NextAfter(t time.Time) time.Time {
	// Start from the next minute.
	candidate := t.UTC().Truncate(time.Minute).Add(time.Minute)

	limit := t.UTC().Add(cronScanYears * 365 * cronHoursPerDay * time.Hour)

	for candidate.Before(limit) {
		if c.matches(candidate) {
			return candidate
		}

		candidate = candidate.Add(time.Minute)
	}

	return time.Time{}
}

// cronFieldKind identifies which of the six cron fields a token belongs to,
// so numeric/name resolution can apply AWS's field-specific conventions:
// month and day-of-week accept three-letter names, and AWS's day-of-week
// numbering (1-7, where 1 = Sunday) differs from Go's time.Weekday (0-6,
// where 0 = Sunday).
type cronFieldKind int

const (
	cronFieldMinute cronFieldKind = iota
	cronFieldHour
	cronFieldDayOfMonth
	cronFieldMonth
	cronFieldDayOfWeek
	cronFieldYear
)

// cronMonthNames maps AWS's three-letter month abbreviations to their 1-12
// numeric value (same numbering as Go's time.Month, so no offset is needed).
//
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

// cronDowNames maps AWS's three-letter day-of-week abbreviations directly to
// Go's time.Weekday numbering (0 = Sunday ... 6 = Saturday), so values
// resolved here need no further offset before comparison with t.Weekday().
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

// awsDayOfWeekBase is AWS's numeric base for day-of-week fields: AWS uses
// 1-7 with 1 = Sunday, while Go's time.Weekday uses 0-6 with 0 = Sunday.
const awsDayOfWeekBase = 1

// cronTokenValue resolves a single cron token to its comparable integer
// value for the given field kind: three-letter names for month/day-of-week,
// otherwise a plain number. Numeric day-of-week tokens are converted from
// AWS's 1-7 (Sunday = 1) convention to Go's time.Weekday 0-6 (Sunday = 0) so
// callers can compare directly against int(t.Weekday()).
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

// matches checks whether a time matches all cron fields.
func (c *cronExpression) matches(t time.Time) bool {
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

// matchDayFields evaluates the dayOfMonth and dayOfWeek cron fields.
// If one is a wildcard (? or *), only the other is checked.
// If both are specified, either must match (AWS behavior).
func (c *cronExpression) matchDayFields(t time.Time) bool {
	domWild := c.dayOfMonth == "?" || c.dayOfMonth == "*"
	dowWild := c.dayOfWeek == "?" || c.dayOfWeek == "*"

	switch {
	case domWild && dowWild:
		return true
	case domWild:
		return matchCronField(
			cronFieldDayOfWeek, c.dayOfWeek, int(t.Weekday()), cronDayOfWeekMin, cronDayOfWeekMax,
		)
	case dowWild:
		return matchCronField(cronFieldDayOfMonth, c.dayOfMonth, t.Day(), cronDayOfMonthMin, cronDayOfMonthMax)
	default:
		domMatch := matchCronField(cronFieldDayOfMonth, c.dayOfMonth, t.Day(), cronDayOfMonthMin, cronDayOfMonthMax)
		dowMatch := matchCronField(
			cronFieldDayOfWeek, c.dayOfWeek, int(t.Weekday()), cronDayOfWeekMin, cronDayOfWeekMax,
		)

		return domMatch || dowMatch
	}
}

// matchCronField checks if val matches a cron field (numeric, name, *, ?, or comma-list).
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

// matchCronToken checks whether a single cron token (range, step, or exact) matches val.
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

// matchCronRange returns true if val falls within the range "lo-hi" (numeric
// or three-letter names, e.g. "MON-FRI" or "JAN-DEC").
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
		// AWS->Go conversion (e.g. AWS "FRI-MON" / "6-2" spans Fri,Sat,Sun,Mon).
		return val >= lo || val <= hi
	}

	return val >= lo && val <= hi
}

// matchCronStep returns true if val matches a step pattern like "*/N", "5/N", or "0-30/N".
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
