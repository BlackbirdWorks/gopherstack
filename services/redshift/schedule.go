package redshift

import (
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awscron"
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

func (c *cronExpr) matches(t time.Time) bool {
	if !awscron.MatchField(awscron.FieldMinute, c.minute, t.Minute(), awscron.MinuteMin, awscron.MinuteMax) {
		return false
	}

	if !awscron.MatchField(awscron.FieldHour, c.hour, t.Hour(), awscron.HourMin, awscron.HourMax) {
		return false
	}

	if !awscron.MatchField(awscron.FieldMonth, c.month, int(t.Month()), awscron.MonthMin, awscron.MonthMax) {
		return false
	}

	if !awscron.MatchField(awscron.FieldYear, c.year, t.Year(), awscron.YearMin, awscron.YearMax) {
		return false
	}

	return awscron.MatchDayFields(c.dayOfMonth, c.dayOfWeek, t)
}
