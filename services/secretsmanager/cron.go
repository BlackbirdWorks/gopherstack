package secretsmanager

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// AWS cron expression: cron(Minutes Hours Day-of-month Month Day-of-week Year)
// Supported special chars: * ? , - /
// Month names: JAN-DEC; Day-of-week names: SUN-SAT (SUN=1 … SAT=7)

// Cron field bounds.
const (
	cronMinMinute      = 0
	cronMaxMinute      = 59
	cronMinHour        = 0
	cronMaxHour        = 23
	cronMinDOM         = 1
	cronMaxDOM         = 31
	cronMinMonth       = 1
	cronMaxMonth       = 12
	cronMinDOW         = 1
	cronMaxDOW         = 7
	cronMinYear        = 1970
	cronMaxYear        = 2199
	cronFieldCount     = 6
	cronMaxSearchYears = 4
	// awsDOWOffset maps time.Weekday (SUN=0) to AWS DOW (SUN=1).
	awsDOWOffset = 1
)

// Sentinel errors for cron parsing — only used internally; never leaked to callers.
var (
	errCronInvalidStep  = errors.New("invalid cron step")
	errCronInvalidRange = errors.New("invalid cron range")
	errCronInvalidValue = errors.New("invalid cron value")
)

// cronMonthAliases maps upper-case 3-letter month abbreviations to their numeric value (1-12).
// Initialised from time.Month to avoid magic numbers.
//
//nolint:gochecknoglobals // package-level lookup table initialised from stdlib types
var cronMonthAliases = func() map[string]int {
	m := make(map[string]int, cronMaxMonth)
	for month := time.January; month <= time.December; month++ {
		m[strings.ToUpper(month.String()[:3])] = int(month)
	}

	return m
}()

// cronDOWAliases maps upper-case 3-letter weekday abbreviations to AWS day-of-week (SUN=1..SAT=7).
// Initialised from time.Weekday to avoid magic numbers.
//
//nolint:gochecknoglobals // package-level lookup table initialised from stdlib types
var cronDOWAliases = func() map[string]int {
	days := [...]time.Weekday{
		time.Sunday, time.Monday, time.Tuesday, time.Wednesday,
		time.Thursday, time.Friday, time.Saturday,
	}
	m := make(map[string]int, len(days))
	for i, d := range days {
		m[strings.ToUpper(d.String()[:3])] = i + awsDOWOffset
	}

	return m
}()

// cronFields holds the parsed field sets for an AWS cron expression.
type cronFields struct {
	minutes []int
	hours   []int
	dom     []int
	months  []int
	dow     []int
	years   []int
	domWild bool
	dowWild bool
}

// parseCronExpr parses an AWS cron(…) expression into cronFields.
// Returns an error when the expression cannot be parsed.
func parseCronExpr(expr string) (*cronFields, error) {
	expr = strings.TrimSpace(expr)
	if !strings.HasPrefix(expr, "cron(") || !strings.HasSuffix(expr, ")") {
		return nil, fmt.Errorf("%w: not a cron() expression", errCronInvalidValue)
	}

	inner := expr[5 : len(expr)-1]
	parts := strings.Fields(inner)

	if len(parts) != cronFieldCount {
		return nil, fmt.Errorf(
			"%w: expected %d fields, got %d",
			errCronInvalidValue,
			cronFieldCount,
			len(parts),
		)
	}

	minutes, err := parseCronField(parts[0], cronMinMinute, cronMaxMinute, nil)
	if err != nil {
		return nil, err
	}

	hours, err := parseCronField(parts[1], cronMinHour, cronMaxHour, nil)
	if err != nil {
		return nil, err
	}

	dom, err := parseCronField(parts[2], cronMinDOM, cronMaxDOM, nil)
	if err != nil {
		return nil, err
	}

	months, err := parseCronField(parts[3], cronMinMonth, cronMaxMonth, cronMonthAliases)
	if err != nil {
		return nil, err
	}

	dow, err := parseCronField(parts[4], cronMinDOW, cronMaxDOW, cronDOWAliases)
	if err != nil {
		return nil, err
	}

	years, err := parseCronField(parts[5], cronMinYear, cronMaxYear, nil)
	if err != nil {
		return nil, err
	}

	return &cronFields{
		minutes: minutes,
		hours:   hours,
		dom:     dom,
		months:  months,
		dow:     dow,
		years:   years,
		domWild: parts[2] == "*" || parts[2] == "?",
		dowWild: parts[4] == "*" || parts[4] == "?",
	}, nil
}

// nextCronTime returns the next time strictly after `from` that matches the AWS cron
// expression `expr` (format: cron(m h dom mon dow year)), evaluated in UTC.
// Returns (time.Time{}, false) if expr cannot be parsed or no match exists within 4 years.
func nextCronTime(expr string, from time.Time) (time.Time, bool) {
	cf, err := parseCronExpr(expr)
	if err != nil {
		return time.Time{}, false
	}

	return searchCronNextTime(cf, from)
}

// cronAdvanceFn is a function that advances a time to the next matching value for a field.
// Returns t unchanged when already matching, or a later time when not matching.
// Returns zero time when no match can be found.
type cronAdvanceFn func(*cronFields, time.Time) time.Time

// searchCronNextTime iterates forward in time from `from+1min` looking for the next match.
// After any field advances the clock, we restart from the coarsest field to re-validate
// all earlier fields against the new time.
func searchCronNextTime(cf *cronFields, from time.Time) (time.Time, bool) {
	t := from.UTC().Truncate(time.Minute).Add(time.Minute)
	deadline := t.Add(cronMaxSearchYears * 365 * 24 * time.Hour)

	advances := [...]cronAdvanceFn{
		advanceCronYear, advanceCronMonth, advanceCronDay,
		advanceCronHour, advanceCronMinute,
	}

	for t.Before(deadline) {
		next, done := applyAdvances(cf, t, advances[:])
		if done {
			break
		}

		if next.Equal(t) {
			return t, true
		}

		t = next
	}

	return time.Time{}, false
}

// applyAdvances runs each advance function in order and returns (newT, done).
// done=true signals that a zero time was returned and no match is possible.
// When newT != t, the caller should restart from the first advance with newT.
func applyAdvances(cf *cronFields, t time.Time, advances []cronAdvanceFn) (time.Time, bool) {
	for _, adv := range advances {
		next := adv(cf, t)
		if next.IsZero() {
			return time.Time{}, true
		}

		if !next.Equal(t) {
			return next, false
		}
	}

	return t, false
}

// advanceCronYear returns t unchanged if the year matches, or advances to the next matching year.
// Returns zero time if no matching year exists within the set.
func advanceCronYear(cf *cronFields, t time.Time) time.Time {
	if slices.Contains(cf.years, t.Year()) {
		return t
	}

	for _, y := range cf.years {
		if y > t.Year() {
			return time.Date(y, 1, 1, 0, 0, 0, 0, time.UTC)
		}
	}

	return time.Time{}
}

// advanceCronMonth returns t unchanged if the month matches, or advances to the first day of
// the next matching month. Wraps to the next year when no match exists in the current year.
func advanceCronMonth(cf *cronFields, t time.Time) time.Time {
	if slices.Contains(cf.months, int(t.Month())) {
		return t
	}

	for _, m := range cf.months {
		if m > int(t.Month()) {
			return time.Date(t.Year(), time.Month(m), 1, 0, 0, 0, 0, time.UTC)
		}
	}

	return time.Date(t.Year()+1, 1, 1, 0, 0, 0, 0, time.UTC)
}

// advanceCronDay returns t unchanged if the day matches, or advances to the next day that
// satisfies the DOM/DOW criteria. Returns the first day of the next month when none match.
func advanceCronDay(cf *cronFields, t time.Time) time.Time {
	if cronDayMatch(t, cf) {
		return t
	}

	next := time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, time.UTC)
	for next.Month() == t.Month() {
		if cronDayMatch(next, cf) {
			return next
		}

		next = time.Date(next.Year(), next.Month(), next.Day()+1, 0, 0, 0, 0, time.UTC)
	}

	return next
}

// advanceCronHour returns t unchanged if the hour matches, or advances to the next matching hour.
// Wraps to the next day when no match exists in the current day.
func advanceCronHour(cf *cronFields, t time.Time) time.Time {
	if slices.Contains(cf.hours, t.Hour()) {
		return t
	}

	for _, h := range cf.hours {
		if h > t.Hour() {
			return time.Date(t.Year(), t.Month(), t.Day(), h, 0, 0, 0, time.UTC)
		}
	}

	return time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, time.UTC)
}

// advanceCronMinute returns t unchanged if the minute matches, or advances to the next
// matching minute. Wraps to the next hour when no match exists in the current hour.
func advanceCronMinute(cf *cronFields, t time.Time) time.Time {
	if slices.Contains(cf.minutes, t.Minute()) {
		return t
	}

	for _, m := range cf.minutes {
		if m > t.Minute() {
			return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), m, 0, 0, time.UTC)
		}
	}

	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, time.UTC)
}

// cronDayMatch returns true when time t satisfies the day-of-month / day-of-week criteria.
// AWS semantics: if both dom and dow are non-wildcard, either matching is sufficient (OR).
// If one is wildcard (? or *) only the other is evaluated.
func cronDayMatch(t time.Time, cf *cronFields) bool {
	awsWeekday := int(t.Weekday()) + awsDOWOffset

	switch {
	case cf.domWild && cf.dowWild:
		return true
	case cf.domWild:
		return slices.Contains(cf.dow, awsWeekday)
	case cf.dowWild:
		return slices.Contains(cf.dom, t.Day())
	default:
		return slices.Contains(cf.dom, t.Day()) || slices.Contains(cf.dow, awsWeekday)
	}
}

// parseCronField parses a single cron field expression and returns the sorted set of matching
// integer values within [lo, hi]. aliases maps upper-case name strings to their integer values.
func parseCronField(expr string, lo, hi int, aliases map[string]int) ([]int, error) {
	expr = strings.TrimSpace(expr)

	if expr == "*" || expr == "?" {
		return makeRange(lo, hi, 1), nil
	}

	if strings.Contains(expr, ",") {
		return parseCronList(expr, lo, hi, aliases)
	}

	if base, stepStr, found := strings.Cut(expr, "/"); found {
		return parseCronStep(base, stepStr, lo, hi, aliases)
	}

	if loStr, hiStr, found := strings.Cut(expr, "-"); found {
		return parseCronRange(loStr, hiStr, lo, hi, aliases)
	}

	v, err := parseSingleCronValue(expr, lo, hi, aliases)
	if err != nil {
		return nil, err
	}

	return []int{v}, nil
}

// parseCronList parses a comma-separated list of cron sub-expressions.
func parseCronList(expr string, lo, hi int, aliases map[string]int) ([]int, error) {
	parts := strings.Split(expr, ",")
	seen := make(map[int]struct{})

	for _, p := range parts {
		vals, err := parseCronField(p, lo, hi, aliases)
		if err != nil {
			return nil, err
		}

		for _, v := range vals {
			seen[v] = struct{}{}
		}
	}

	return setToSorted(seen), nil
}

// parseCronStep parses a step expression of the form base/step.
func parseCronStep(base, stepStr string, lo, hi int, aliases map[string]int) ([]int, error) {
	step, err := strconv.Atoi(strings.TrimSpace(stepStr))
	if err != nil || step <= 0 {
		return nil, fmt.Errorf("%w: %q", errCronInvalidStep, stepStr)
	}

	var start int
	if base == "*" || base == "?" {
		start = lo
	} else {
		start, err = parseSingleCronValue(base, lo, hi, aliases)
		if err != nil {
			return nil, err
		}
	}

	return makeRange(start, hi, step), nil
}

// parseCronRange parses a range expression of the form lo-hi.
func parseCronRange(loStr, hiStr string, lo, hi int, aliases map[string]int) ([]int, error) {
	low, err := parseSingleCronValue(loStr, lo, hi, aliases)
	if err != nil {
		return nil, err
	}

	high, err := parseSingleCronValue(hiStr, lo, hi, aliases)
	if err != nil {
		return nil, err
	}

	if low > high {
		return nil, fmt.Errorf("%w: %d > %d", errCronInvalidRange, low, high)
	}

	return makeRange(low, high, 1), nil
}

// parseSingleCronValue converts a single token (numeric or alias) to its integer value,
// validated within [lo, hi].
func parseSingleCronValue(token string, lo, hi int, aliases map[string]int) (int, error) {
	upper := strings.ToUpper(strings.TrimSpace(token))

	if aliases != nil {
		if v, ok := aliases[upper]; ok {
			return v, nil
		}
	}

	v, err := strconv.Atoi(token)
	if err != nil {
		return 0, fmt.Errorf("%w: %q", errCronInvalidValue, token)
	}

	if v < lo || v > hi {
		return 0, fmt.Errorf("%w: %d not in [%d, %d]", errCronInvalidValue, v, lo, hi)
	}

	return v, nil
}

// makeRange returns a slice of values from start to end (inclusive) with the given step.
func makeRange(start, end, step int) []int {
	out := make([]int, 0, (end-start)/step+1)
	for i := start; i <= end; i += step {
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

	// Insertion sort — field sets are small (at most 60 elements for minutes).
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
