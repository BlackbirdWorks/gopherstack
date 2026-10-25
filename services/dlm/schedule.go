package dlm

import (
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awscron"
)

const (
	cronFieldCount = 6
	maxCronScan    = 7 * 24 * 60
	hoursPerDay    = 24
	daysPerWeek    = 7
)

func numberOf(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	}

	return 0, false
}

// createRuleDue returns the latest scheduled occurrence in (baseline, now] at or after created.
func createRuleDue(rule map[string]any, created, baseline, now time.Time) (time.Time, bool) {
	if expr, _ := rule["CronExpression"].(string); expr != "" {
		return cronDue(expr, created, baseline, now)
	}

	n, ok := numberOf(rule["Interval"])
	if !ok || n < 1 {
		return time.Time{}, false
	}

	unit, _ := rule["IntervalUnit"].(string)
	step := time.Duration(n) * time.Hour

	if strings.EqualFold(unit, "DAYS") {
		step *= hoursPerDay
	}

	anchor := created.UTC()

	if times := asSlice(rule["Times"]); len(times) > 0 {
		s, _ := times[0].(string)

		hh, mm, found := strings.Cut(s, ":")
		h, errH := strconv.Atoi(hh)
		m, errM := strconv.Atoi(mm)

		if found && errH == nil && errM == nil {
			anchor = time.Date(anchor.Year(), anchor.Month(), anchor.Day(), h, m, 0, 0, time.UTC)
		}
	}

	if now.Before(anchor) {
		return time.Time{}, false
	}

	occ := anchor.Add(now.Sub(anchor).Truncate(step))
	if occ.After(baseline) && !occ.Before(created) {
		return occ, true
	}

	return time.Time{}, false
}

func cronDue(expr string, created, baseline, now time.Time) (time.Time, bool) {
	body := strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(expr), "cron("), ")")

	f := strings.Fields(body)
	if len(f) != cronFieldCount {
		return time.Time{}, false
	}

	floor := created.UTC().Truncate(time.Minute)
	end := now.UTC().Truncate(time.Minute)

	start := baseline.UTC().Truncate(time.Minute).Add(time.Minute)
	if start.Before(floor) {
		start = floor
	}

	var latest time.Time

	for i, t := 0, start; !t.After(end) && i < maxCronScan; i, t = i+1, t.Add(time.Minute) {
		if cronMatches(f, t) {
			latest = t
		}
	}

	return latest, !latest.IsZero()
}

func cronMatches(f []string, t time.Time) bool {
	return awscron.MatchField(awscron.FieldMinute, f[0], t.Minute(), awscron.MinuteMin, awscron.MinuteMax) &&
		awscron.MatchField(awscron.FieldHour, f[1], t.Hour(), awscron.HourMin, awscron.HourMax) &&
		awscron.MatchField(awscron.FieldMonth, f[3], int(t.Month()), awscron.MonthMin, awscron.MonthMax) &&
		awscron.MatchField(awscron.FieldYear, f[5], t.Year(), awscron.YearMin, awscron.YearMax) &&
		awscron.MatchDayFields(f[2], f[4], t)
}

// retainCutoff returns the age beyond which a set expires, or false when the rule is count-based.
func retainCutoff(rule map[string]any, now time.Time) (time.Time, bool) {
	n, ok := numberOf(rule["Interval"])
	if !ok {
		return time.Time{}, false
	}

	count := int(n)
	unit, _ := rule["IntervalUnit"].(string)

	switch strings.ToUpper(unit) {
	case "WEEKS":
		return now.AddDate(0, 0, -count*daysPerWeek), true
	case "MONTHS":
		return now.AddDate(0, -count, 0), true
	case "YEARS":
		return now.AddDate(-count, 0, 0), true
	default:
		return now.AddDate(0, 0, -count), true
	}
}
