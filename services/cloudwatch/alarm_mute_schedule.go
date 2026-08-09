package cloudwatch

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	ErrInvalidMuteDuration   = errors.New("invalid mute rule duration")
	ErrInvalidMuteExpression = errors.New("invalid mute rule schedule expression")
	ErrInvalidMuteTimezone   = errors.New("invalid mute rule timezone")
)

// minMuteDuration/maxMuteDuration mirror the documented range for
// Schedule.Duration: 1 minute (PT1M) to 15 days (P15D) (aws-sdk-go-v2
// cloudwatch@v1.66.3 types/types.go:3408-3412).
const (
	minMuteDuration = time.Minute
	maxMuteDuration = 15 * 24 * time.Hour
)

// muteDurationPattern accepts the PnDTnHnMnS subset of ISO 8601 that AWS's
// documented examples use (PT4H, P2DT12H, P7D); year/month designators are
// omitted because they are calendar-ambiguous and no valid AWS duration
// (max 15 days) would need them.
var muteDurationPattern = regexp.MustCompile(`^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$`)

// parseISO8601Duration parses an AlarmMuteRuleSchedule.Duration value.
func parseISO8601Duration(s string) (time.Duration, error) {
	m := muteDurationPattern.FindStringSubmatch(s)
	if m == nil {
		return 0, fmt.Errorf("%w: %q", ErrInvalidMuteDuration, s)
	}

	var total time.Duration
	for i, unit := range [...]time.Duration{24 * time.Hour, time.Hour, time.Minute, time.Second} {
		if m[i+1] == "" {
			continue
		}

		n, err := strconv.Atoi(m[i+1])
		if err != nil {
			return 0, fmt.Errorf("%w: %q", ErrInvalidMuteDuration, s)
		}

		total += time.Duration(n) * unit
	}

	if total < minMuteDuration || total > maxMuteDuration {
		return 0, fmt.Errorf(
			"%w: %q must be between %s and %s",
			ErrInvalidMuteDuration, s, minMuteDuration, maxMuteDuration,
		)
	}

	return total, nil
}

// muteCronFields is the AWS mute-rule cron field count: Minutes Hours
// Day-of-month Month Day-of-week -- unlike eventbridge/redshift schedule
// expressions, there is no trailing Year field (types.Schedule doc,
// aws-sdk-go-v2 cloudwatch@v1.66.3 types/types.go:3441).
const muteCronFields = 5

// muteCronExpr is a parsed 5-field AWS cron(min hour dom month dow) mute
// schedule. The field-matching primitives below (matchCronField and callees)
// are lifted from services/redshift/schedule.go, which already covers this
// same AWS cron dialect; eventbridge/schedule.go has a third copy. All three
// are tracked for consolidation by bd issue gopherstack-wx3l.
type muteCronExpr struct {
	minute     string
	hour       string
	dayOfMonth string
	month      string
	dayOfWeek  string
}

func parseMuteCron(expr string) (*muteCronExpr, error) {
	inner := expr[len("cron(") : len(expr)-1]
	fields := strings.Fields(inner)

	if len(fields) != muteCronFields {
		return nil, fmt.Errorf(
			"%w: cron expression requires %d fields, got %d: %q",
			ErrInvalidMuteExpression, muteCronFields, len(fields), expr,
		)
	}

	return &muteCronExpr{
		minute: fields[0], hour: fields[1], dayOfMonth: fields[2],
		month: fields[3], dayOfWeek: fields[4],
	}, nil
}

func (c *muteCronExpr) matches(t time.Time) bool {
	if !matchCronField(cronFieldMinute, c.minute, t.Minute(), cronMinuteMin, cronMinuteMax) {
		return false
	}

	if !matchCronField(cronFieldHour, c.hour, t.Hour(), cronHourMin, cronHourMax) {
		return false
	}

	if !matchCronField(cronFieldMonth, c.month, int(t.Month()), cronMonthMin, cronMonthMax) {
		return false
	}

	return c.matchDayFields(t)
}

// matchDayFields evaluates dayOfMonth/dayOfWeek: a wildcard field defers
// entirely to the other; when both are concrete, AWS matches if either is
// satisfied (same rule as the eventbridge/redshift cron dialects).
func (c *muteCronExpr) matchDayFields(t time.Time) bool {
	domWild := c.dayOfMonth == "?" || c.dayOfMonth == "*"
	dowWild := c.dayOfWeek == "?" || c.dayOfWeek == "*"

	switch {
	case domWild && dowWild:
		return true
	case domWild:
		return matchCronField(cronFieldDayOfWeek, c.dayOfWeek, int(t.Weekday()), cronDayOfWeekMin, cronDayOfWeekMax)
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

type cronFieldKind int

const (
	cronFieldMinute cronFieldKind = iota
	cronFieldHour
	cronFieldDayOfMonth
	cronFieldMonth
	cronFieldDayOfWeek
)

const (
	cronMinuteMin, cronMinuteMax         = 0, 59
	cronHourMin, cronHourMax             = 0, 23
	cronMonthMin, cronMonthMax           = 1, 12
	cronDayOfMonthMin, cronDayOfMonthMax = 1, 31
	cronDayOfWeekMin, cronDayOfWeekMax   = 0, 6
)

//nolint:gochecknoglobals // read-only lookup table initialized once at startup
var cronMonthNames = map[string]int{
	"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, //nolint:mnd // calendar month numbers, not magic constants
	"MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8, //nolint:mnd // calendar month numbers, not magic constants
	"SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12, //nolint:mnd // calendar month numbers, not magic constants
}

// cronDowNames maps AWS's three-letter day-of-week names directly to Go's
// time.Weekday numbering (0 = Sunday), so no offset is needed after lookup.
//
//nolint:gochecknoglobals // read-only lookup table initialized once at startup
var cronDowNames = map[string]int{
	"SUN": 0, "MON": 1, "TUE": 2, "WED": 3, //nolint:mnd // time.Weekday numbers, not magic constants
	"THU": 4, "FRI": 5, "SAT": 6, //nolint:mnd // time.Weekday numbers, not magic constants
}

// awsDayOfWeekBase is AWS's numeric base for day-of-week fields (1-7, 1 =
// Sunday), vs. Go's time.Weekday (0-6, 0 = Sunday).
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
	case cronFieldMinute, cronFieldHour, cronFieldDayOfMonth:
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

// muteAtLayout is AWS's one-time mute expression format: at(yyyy-MM-ddThh:mm)
// -- no seconds, unlike redshift's at() dialect (types.Schedule doc,
// aws-sdk-go-v2 cloudwatch@v1.66.3 types/types.go:3455).
const muteAtLayout = "2006-01-02T15:04"

func parseMuteAt(expr string, loc *time.Location) (time.Time, error) {
	inner := expr[len("at(") : len(expr)-1]

	t, err := time.ParseInLocation(muteAtLayout, inner, loc)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: %q: %w", ErrInvalidMuteExpression, expr, err)
	}

	return t, nil
}

// validateMuteScheduleExpression checks that expr is a well-formed cron() or
// at() mute schedule expression, without evaluating it against any instant.
func validateMuteScheduleExpression(expr string) error {
	expr = strings.TrimSpace(expr)

	switch {
	case strings.HasPrefix(expr, "cron(") && strings.HasSuffix(expr, ")"):
		_, err := parseMuteCron(expr)

		return err
	case strings.HasPrefix(expr, "at(") && strings.HasSuffix(expr, ")"):
		_, err := parseMuteAt(expr, time.UTC)

		return err
	default:
		return fmt.Errorf("%w: %q", ErrInvalidMuteExpression, expr)
	}
}

// muteScheduleActive reports whether sched's cron/at expression, combined
// with its Duration and Timezone, covers instant now. Timezone governs how
// the wall-clock expression is interpreted ("the time zone affects how cron
// and at expressions are interpreted", aws-sdk-go-v2 cloudwatch@v1.66.3
// types/types.go:3466); once an occurrence's absolute instant is resolved,
// adding the duration to it is DST-safe because time.Time.Add operates on
// the absolute instant, not on wall-clock fields.
func muteScheduleActive(sched AlarmMuteRuleSchedule, now time.Time) (bool, error) {
	dur, err := parseISO8601Duration(sched.Duration)
	if err != nil {
		return false, err
	}

	loc := time.UTC

	if sched.Timezone != "" {
		loc, err = time.LoadLocation(sched.Timezone)
		if err != nil {
			return false, fmt.Errorf("%w: %q: %w", ErrInvalidMuteTimezone, sched.Timezone, err)
		}
	}

	expr := strings.TrimSpace(sched.Expression)

	switch {
	case strings.HasPrefix(expr, "at(") && strings.HasSuffix(expr, ")"):
		occ, atErr := parseMuteAt(expr, loc)
		if atErr != nil {
			return false, atErr
		}

		return !now.Before(occ) && now.Before(occ.Add(dur)), nil

	case strings.HasPrefix(expr, "cron(") && strings.HasSuffix(expr, ")"):
		cron, cronErr := parseMuteCron(expr)
		if cronErr != nil {
			return false, cronErr
		}

		return cronMuteActive(cron, now.In(loc), dur), nil

	default:
		return false, fmt.Errorf("%w: %q", ErrInvalidMuteExpression, expr)
	}
}

// cronMuteActive reports whether now falls within dur of the most recent
// cron occurrence at or before now, scanning backward at minute resolution.
// Only occurrences strictly after now-dur can still be active, which bounds
// the scan and means the first match found (nearest to now) is decisive: an
// even earlier occurrence cannot be active if this one is not.
func cronMuteActive(c *muteCronExpr, now time.Time, dur time.Duration) bool {
	candidate := now.Truncate(time.Minute)
	cutoff := now.Add(-dur)

	for candidate.After(cutoff) {
		if c.matches(candidate) {
			return true
		}

		candidate = candidate.Add(-time.Minute)
	}

	return false
}
