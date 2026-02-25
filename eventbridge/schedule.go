package eventbridge

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// scheduleExpression represents a parsed schedule expression.
type scheduleExpression interface {
	// NextAfter returns the next fire time at or after t.
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
	// Next multiple of interval after t.
	n := since/r.interval + 1

	return epoch.Add(n * r.interval)
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

	return nil, fmt.Errorf("unsupported schedule expression: %q", expr)
}

// parseRate parses expressions like "rate(5 minutes)" or "rate(1 hour)".
func parseRate(expr string) (*rateExpression, error) {
	inner := expr[len("rate(") : len(expr)-1]
	inner = strings.TrimSpace(inner)
	parts := strings.Fields(inner)

	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid rate expression: %q", expr)
	}

	n, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || n <= 0 {
		return nil, fmt.Errorf("invalid rate value: %q", parts[0])
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
		d = time.Duration(n) * 24 * time.Hour
	default:
		return nil, fmt.Errorf("unsupported rate unit: %q", parts[1])
	}

	return &rateExpression{interval: d}, nil
}

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

	const cronFields = 6
	if len(fields) != cronFields {
		return nil, fmt.Errorf("cron expression requires 6 fields, got %d: %q", len(fields), expr)
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

// NextAfter returns the next time at or after t that matches the cron expression.
// Implementation is a simple minute-resolution forward scan (max 2 years ahead).
func (c *cronExpression) NextAfter(t time.Time) time.Time {
	// Start from the next minute.
	candidate := t.UTC().Truncate(time.Minute).Add(time.Minute)
	limit := t.UTC().Add(2 * 365 * 24 * time.Hour)

	for candidate.Before(limit) {
		if c.matches(candidate) {
			return candidate
		}

		candidate = candidate.Add(time.Minute)
	}

	// Fallback: return a far-future time.
	return limit
}

// matches checks whether a time matches all cron fields.
func (c *cronExpression) matches(t time.Time) bool {
	if !matchCronField(c.minute, t.Minute(), 0, 59) {
		return false
	}

	if !matchCronField(c.hour, t.Hour(), 0, 23) {
		return false
	}

	if !matchCronField(c.month, int(t.Month()), 1, 12) {
		return false
	}

	if !matchCronField(c.year, t.Year(), 1970, 2199) {
		return false
	}

	// dayOfMonth and dayOfWeek: if one is ?, only check the other.
	domWild := c.dayOfMonth == "?" || c.dayOfMonth == "*"
	dowWild := c.dayOfWeek == "?" || c.dayOfWeek == "*"

	switch {
	case domWild && dowWild:
		// both wildcards: always match
	case domWild:
		if !matchCronField(c.dayOfWeek, int(t.Weekday()), 0, 6) {
			return false
		}
	case dowWild:
		if !matchCronField(c.dayOfMonth, t.Day(), 1, 31) {
			return false
		}
	default:
		// Both specified: either must match (AWS behavior).
		domMatch := matchCronField(c.dayOfMonth, t.Day(), 1, 31)
		dowMatch := matchCronField(c.dayOfWeek, int(t.Weekday()), 0, 6)
		if !domMatch && !dowMatch {
			return false
		}
	}

	return true
}

// matchCronField checks if val matches a cron field (numeric, *, ?, or comma-list).
func matchCronField(field string, val, min, max int) bool {
	if field == "*" || field == "?" {
		return true
	}

	// Comma-separated list.
	for _, part := range strings.Split(field, ",") {
		part = strings.TrimSpace(part)

		// Range a-b.
		if strings.Contains(part, "-") {
			rangeParts := strings.SplitN(part, "-", 2)
			lo, err1 := strconv.Atoi(rangeParts[0])
			hi, err2 := strconv.Atoi(rangeParts[1])

			if err1 == nil && err2 == nil && val >= lo && val <= hi {
				return true
			}

			continue
		}

		// Step */step or a/step.
		if strings.Contains(part, "/") {
			stepParts := strings.SplitN(part, "/", 2)
			step, err := strconv.Atoi(stepParts[1])

			if err != nil || step <= 0 {
				continue
			}

			start := min
			if stepParts[0] != "*" {
				start, _ = strconv.Atoi(stepParts[0])
			}

			for v := start; v <= max; v += step {
				if v == val {
					return true
				}
			}

			continue
		}

		// Exact value.
		n, err := strconv.Atoi(part)
		if err == nil && n == val {
			return true
		}
	}

	return false
}
