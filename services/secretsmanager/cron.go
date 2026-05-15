package secretsmanager

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// AWS cron expression: cron(Minutes Hours Day-of-month Month Day-of-week Year)
// Supported special chars: * ? , - /
// Month names: JAN-DEC; Day-of-week names: SUN-SAT (SUN=1 … SAT=7)

//nolint:gochecknoglobals
var (
	cronMonthAliases = map[string]int{
		"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
		"JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
	}
	cronDOWAliases = map[string]int{
		"SUN": 1, "MON": 2, "TUE": 3, "WED": 4, "THU": 5, "FRI": 6, "SAT": 7,
	}
)

// nextCronTime returns the next time strictly after `from` that matches the AWS cron
// expression `expr` (format: cron(m h dom mon dow year)), evaluated in UTC.
// Returns (time.Time{}, false) if expr cannot be parsed or no match exists within 4 years.
func nextCronTime(expr string, from time.Time) (time.Time, bool) {
	expr = strings.TrimSpace(expr)
	if !strings.HasPrefix(expr, "cron(") || !strings.HasSuffix(expr, ")") {
		return time.Time{}, false
	}

	inner := expr[5 : len(expr)-1]
	fields := strings.Fields(inner)

	const expectedCronFields = 6
	if len(fields) != expectedCronFields {
		return time.Time{}, false
	}

	minuteSet, err := parseCronField(fields[0], 0, 59, nil)
	if err != nil {
		return time.Time{}, false
	}

	hourSet, err := parseCronField(fields[1], 0, 23, nil)
	if err != nil {
		return time.Time{}, false
	}

	domSet, err := parseCronField(fields[2], 1, 31, nil)
	if err != nil {
		return time.Time{}, false
	}

	monthSet, err := parseCronField(fields[3], 1, 12, cronMonthAliases)
	if err != nil {
		return time.Time{}, false
	}

	dowSet, err := parseCronField(fields[4], 1, 7, cronDOWAliases)
	if err != nil {
		return time.Time{}, false
	}

	yearSet, err := parseCronField(fields[5], 1970, 2199, nil)
	if err != nil {
		return time.Time{}, false
	}

	domWild := fields[2] == "*" || fields[2] == "?"
	dowWild := fields[4] == "*" || fields[4] == "?"

	// Search forward from the next minute.
	t := from.UTC().Truncate(time.Minute).Add(time.Minute)
	deadline := t.Add(4 * 365 * 24 * time.Hour)

	for t.Before(deadline) {
		if !cronSetContains(yearSet, t.Year()) {
			// Advance to start of next year.
			t = time.Date(t.Year()+1, 1, 1, 0, 0, 0, 0, time.UTC)
			continue
		}

		if !cronSetContains(monthSet, int(t.Month())) {
			// Advance to first day of next month.
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, time.UTC)
			continue
		}

		if !cronDayMatch(t, domSet, dowSet, domWild, dowWild) {
			// Advance to next day.
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, time.UTC)
			continue
		}

		if !cronSetContains(hourSet, t.Hour()) {
			// Advance to next hour.
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, time.UTC)
			continue
		}

		if !cronSetContains(minuteSet, t.Minute()) {
			t = t.Add(time.Minute)
			continue
		}

		return t, true
	}

	return time.Time{}, false
}

// cronDayMatch returns true when time t satisfies the day-of-month / day-of-week criteria.
// AWS semantics: if both dom and dow are non-wildcard, either matching is sufficient (OR).
// If one is wildcard (? or *) only the other is evaluated.
func cronDayMatch(t time.Time, domSet, dowSet []int, domWild, dowWild bool) bool {
	// awsWeekday converts time.Weekday to AWS DOW (SUN=1 … SAT=7).
	awsWeekday := func(wd time.Weekday) int { return int(wd) + 1 }

	switch {
	case domWild && dowWild:
		return true
	case domWild:
		return cronSetContains(dowSet, awsWeekday(t.Weekday()))
	case dowWild:
		return cronSetContains(domSet, t.Day())
	default:
		return cronSetContains(domSet, t.Day()) || cronSetContains(dowSet, awsWeekday(t.Weekday()))
	}
}

// cronSetContains returns true if val is present in the pre-sorted set.
func cronSetContains(set []int, val int) bool {
	for _, v := range set {
		if v == val {
			return true
		}
	}
	return false
}

// parseCronField parses a single cron field expression and returns the sorted set of matching
// integer values within [min, max]. aliases maps upper-case name strings to their integer values.
func parseCronField(expr string, min, max int, aliases map[string]int) ([]int, error) {
	expr = strings.TrimSpace(expr)

	if expr == "*" || expr == "?" {
		return makeRange(min, max, 1), nil
	}

	// Comma-separated list of sub-expressions.
	if strings.Contains(expr, ",") {
		parts := strings.Split(expr, ",")
		seen := make(map[int]struct{})
		for _, p := range parts {
			vals, err := parseCronField(p, min, max, aliases)
			if err != nil {
				return nil, err
			}
			for _, v := range vals {
				seen[v] = struct{}{}
			}
		}
		return setToSorted(seen), nil
	}

	// Step expression: base/step where base is * or a value.
	if idx := strings.Index(expr, "/"); idx >= 0 {
		base := expr[:idx]
		stepStr := expr[idx+1:]

		step, err := strconv.Atoi(stepStr)
		if err != nil || step <= 0 {
			return nil, fmt.Errorf("invalid cron step %q", stepStr)
		}

		var start int
		if base == "*" || base == "?" {
			start = min
		} else {
			start, err = parseSingleCronValue(base, min, max, aliases)
			if err != nil {
				return nil, err
			}
		}

		return makeRange(start, max, step), nil
	}

	// Range expression: n-m
	if idx := strings.Index(expr, "-"); idx >= 0 {
		lo, err := parseSingleCronValue(expr[:idx], min, max, aliases)
		if err != nil {
			return nil, err
		}

		hi, err := parseSingleCronValue(expr[idx+1:], min, max, aliases)
		if err != nil {
			return nil, err
		}

		if lo > hi {
			return nil, fmt.Errorf("invalid cron range %q: lo > hi", expr)
		}

		return makeRange(lo, hi, 1), nil
	}

	// Single value.
	v, err := parseSingleCronValue(expr, min, max, aliases)
	if err != nil {
		return nil, err
	}

	return []int{v}, nil
}

// parseSingleCronValue converts a single token (numeric or alias) to its integer value,
// validated within [min, max].
func parseSingleCronValue(token string, min, max int, aliases map[string]int) (int, error) {
	upper := strings.ToUpper(strings.TrimSpace(token))

	if aliases != nil {
		if v, ok := aliases[upper]; ok {
			return v, nil
		}
	}

	v, err := strconv.Atoi(token)
	if err != nil {
		return 0, fmt.Errorf("invalid cron value %q", token)
	}

	if v < min || v > max {
		return 0, fmt.Errorf("cron value %d out of range [%d, %d]", v, min, max)
	}

	return v, nil
}

// makeRange returns a slice of values from start to max (inclusive) with the given step.
func makeRange(start, max, step int) []int {
	out := make([]int, 0, (max-start)/step+1)
	for i := start; i <= max; i += step {
		out = append(out, i)
	}
	return out
}

// setToSorted converts a map[int]struct{} to a sorted []int.
func setToSorted(seen map[int]struct{}) []int {
	out := make([]int, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	// Simple insertion sort — field sets are small.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// isCronExpression returns true if expr is an AWS cron(…) expression.
func isCronExpression(expr string) bool {
	s := strings.TrimSpace(expr)
	return strings.HasPrefix(s, "cron(") && strings.HasSuffix(s, ")")
}
