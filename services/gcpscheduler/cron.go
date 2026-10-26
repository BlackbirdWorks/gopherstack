package gcpscheduler

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	cronFieldCount = 5
	maxMinute      = 59
	maxHour        = 23
	maxDom         = 31
	maxMonth       = 12
	maxSearchDays  = 366 * 9
	dowSunday      = 0
	dowAlt         = 7
)

// cronSchedule is a parsed five-field unix-cron expression.
type cronSchedule struct {
	minute, hour, dom, month, dow uint64
	domStar, dowStar              bool
}

type cronField struct {
	names    []string
	min, max int
}

var errCron = errors.New("invalid cron expression")

func cronFields() [5]cronField {
	return [5]cronField{
		{min: 0, max: maxMinute},
		{min: 0, max: maxHour},
		{min: 1, max: maxDom},
		{min: 1, max: maxMonth, names: []string{
			"jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
		}},
		{min: 0, max: dowAlt, names: []string{"sun", "mon", "tue", "wed", "thu", "fri", "sat"}},
	}
}

func cronMacro(name string) (string, bool) {
	switch strings.ToLower(name) {
	case "@yearly", "@annually":
		return "0 0 1 1 *", true
	case "@monthly":
		return "0 0 1 * *", true
	case "@weekly":
		return "0 0 * * 0", true
	case "@daily", "@midnight":
		return "0 0 * * *", true
	case "@hourly":
		return "0 * * * *", true
	}

	return "", false
}

// parseUnixCron parses a unix-cron expression: five fields supporting *, lists, ranges, steps, month and weekday
// names, plus the @hourly-style macros.
func parseUnixCron(expr string) (*cronSchedule, error) {
	expr = strings.TrimSpace(expr)
	if m, ok := cronMacro(expr); ok {
		expr = m
	}

	f := strings.Fields(expr)
	if len(f) != 5 { //nolint:mnd // unix-cron has five fields
		return nil, fmt.Errorf("%w: %q must have 5 space-separated fields", errCron, expr)
	}

	var (
		s   cronSchedule
		err error
	)

	fields := cronFields()

	if s.minute, _, err = parseCronField(f[0], fields[0]); err != nil {
		return nil, fmt.Errorf("minute: %w", err)
	}

	if s.hour, _, err = parseCronField(f[1], fields[1]); err != nil {
		return nil, fmt.Errorf("hour: %w", err)
	}

	if s.dom, s.domStar, err = parseCronField(f[2], fields[2]); err != nil {
		return nil, fmt.Errorf("day of month: %w", err)
	}

	if s.month, _, err = parseCronField(f[3], fields[3]); err != nil {
		return nil, fmt.Errorf("month: %w", err)
	}

	if s.dow, s.dowStar, err = parseCronField(f[4], fields[4]); err != nil {
		return nil, fmt.Errorf("day of week: %w", err)
	}

	if s.dow&(1<<dowAlt) != 0 {
		s.dow = s.dow&^(1<<dowAlt) | 1<<dowSunday
	}

	return &s, nil
}

// parseCronField returns the bitmask of allowed values and whether the field was an unrestricted "*".
func parseCronField(expr string, f cronField) (uint64, bool, error) {
	var (
		mask uint64
		star bool
	)

	for part := range strings.SplitSeq(expr, ",") {
		bits, isStar, err := parseCronTerm(part, f)
		if err != nil {
			return 0, false, err
		}

		mask |= bits
		star = star || isStar
	}

	return mask, star, nil
}

func parseCronTerm(term string, f cronField) (uint64, bool, error) {
	rng, stepStr, hasStep := strings.Cut(term, "/")

	step := 1

	if hasStep {
		n, err := strconv.Atoi(stepStr)
		if err != nil || n < 1 {
			return 0, false, fmt.Errorf("%w: invalid step in %q", errCron, term)
		}

		step = n
	}

	lo, hi, star, err := cronRange(rng, f, hasStep)
	if err != nil {
		return 0, false, fmt.Errorf("%w in %q", err, term)
	}

	var mask uint64
	for v := lo; v <= hi; v += step {
		mask |= 1 << uint(v)
	}

	return mask, star, nil
}

func cronRange(rng string, f cronField, hasStep bool) (int, int, bool, error) {
	if rng == "*" {
		return f.min, f.max, true, nil
	}

	loStr, hiStr, isRange := strings.Cut(rng, "-")

	lo, err := cronValue(loStr, f)
	if err != nil {
		return 0, 0, false, err
	}

	switch {
	case isRange:
		hi, herr := cronValue(hiStr, f)
		if herr != nil {
			return 0, 0, false, herr
		}

		if hi < lo {
			return 0, 0, false, fmt.Errorf("%w: range start exceeds end", errCron)
		}

		return lo, hi, false, nil
	case hasStep:
		return lo, f.max, false, nil
	}

	return lo, lo, false, nil
}

func cronValue(s string, f cronField) (int, error) {
	if i := slices.Index(f.names, strings.ToLower(s)); i >= 0 {
		return f.min + i, nil
	}

	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid value %q", errCron, s)
	}

	if v < f.min || v > f.max {
		return 0, fmt.Errorf("%w: value %d out of range %d-%d", errCron, v, f.min, f.max)
	}

	return v, nil
}

func (s *cronSchedule) dayMatches(t time.Time) bool {
	if s.month&(1<<uint(t.Month())) == 0 {
		return false
	}

	domOK := s.dom&(1<<uint(t.Day())) != 0
	dowOK := s.dow&(1<<uint(t.Weekday())) != 0

	switch {
	case s.domStar || s.dowStar:
		return domOK && dowOK
	default:
		return domOK || dowOK
	}
}

// next returns the first fire time strictly after t in loc, or the zero time when none exists within nine years.
func (s *cronSchedule) next(t time.Time, loc *time.Location) time.Time {
	t = t.In(loc)
	day := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc)

	for range maxSearchDays {
		if s.dayMatches(day) {
			if c, ok := s.firstOnDay(day, t); ok {
				return c
			}
		}

		day = time.Date(day.Year(), day.Month(), day.Day()+1, 0, 0, 0, 0, loc)
	}

	return time.Time{}
}

// firstOnDay returns the earliest fire time on day that is strictly after t.
func (s *cronSchedule) firstOnDay(day, t time.Time) (time.Time, bool) {
	for h := range 24 {
		for m := range 60 {
			if s.hour&(1<<uint(h)) == 0 || s.minute&(1<<uint(m)) == 0 {
				continue
			}

			if c := time.Date(day.Year(), day.Month(), day.Day(), h, m, 0, 0, day.Location()); c.After(t) {
				return c, true
			}
		}
	}

	return time.Time{}, false
}
