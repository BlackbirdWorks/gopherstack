package cloudwatch

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awscron"
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
// schedule; field matching is shared with eventbridge/redshift's 6-field
// dialects via pkgs/awscron.
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
	if !awscron.MatchField(awscron.FieldMinute, c.minute, t.Minute(), awscron.MinuteMin, awscron.MinuteMax) {
		return false
	}

	if !awscron.MatchField(awscron.FieldHour, c.hour, t.Hour(), awscron.HourMin, awscron.HourMax) {
		return false
	}

	if !awscron.MatchField(awscron.FieldMonth, c.month, int(t.Month()), awscron.MonthMin, awscron.MonthMax) {
		return false
	}

	return awscron.MatchDayFields(c.dayOfMonth, c.dayOfWeek, t)
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
