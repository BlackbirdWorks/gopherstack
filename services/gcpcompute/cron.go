package gcpcompute

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var errCron = errors.New("invalid unix-cron expression")

const (
	cronFields   = 5
	maxNextYears = 5
	maxLookback  = 7 * hoursPerDay * 60
	maxMinute    = 59
	maxHour      = 23
	maxDom       = 31
	maxMonth     = 12
	maxDow       = 7
)

// cronSpec is a parsed five-field unix-cron expression: minute, hour, day of month, month, day of week.
type cronSpec struct {
	min, hour, dom, mon, dow uint64
	domAny, dowAny           bool
}

func monthNames() []string {
	return []string{"JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"}
}

func dowNames() []string { return []string{"SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"} }

func parseCron(expr string) (cronSpec, error) {
	f := splitFields(expr)
	if len(f) != cronFields {
		return cronSpec{}, fmt.Errorf("%w: five fields required", errCron)
	}

	var (
		c   cronSpec
		err error
	)

	if c.min, _, err = parseCronField(f[0], 0, maxMinute, nil, 0); err != nil {
		return c, err
	}

	if c.hour, _, err = parseCronField(f[1], 0, maxHour, nil, 0); err != nil {
		return c, err
	}

	if c.dom, c.domAny, err = parseCronField(f[2], 1, maxDom, nil, 0); err != nil {
		return c, err
	}

	if c.mon, _, err = parseCronField(f[3], 1, maxMonth, monthNames(), 1); err != nil {
		return c, err
	}

	if c.dow, c.dowAny, err = parseCronField(f[4], 0, maxDow, dowNames(), 0); err != nil {
		return c, err
	}

	if c.dow&(1<<maxDow) != 0 {
		c.dow |= 1
	}

	return c, nil
}

func parseCronField(tok string, lo, hi int, names []string, nameBase int) (uint64, bool, error) {
	var mask uint64

	wild := tok == "*" || tok == "?"

	for part := range strings.SplitSeq(tok, ",") {
		m, err := parseCronPart(part, lo, hi, names, nameBase)
		if err != nil {
			return 0, false, err
		}

		mask |= m
	}

	return mask, wild || strings.HasPrefix(tok, "*/"), nil
}

func cronValue(s string, lo, hi int, names []string, nameBase int) (int, error) {
	for i, n := range names {
		if strings.EqualFold(s, n) {
			return i + nameBase, nil
		}
	}

	v, err := strconv.Atoi(s)
	if err != nil || v < lo || v > hi {
		return 0, fmt.Errorf("%w: %q out of range %d-%d", errCron, s, lo, hi)
	}

	return v, nil
}

func parseCronPart(part string, lo, hi int, names []string, nameBase int) (uint64, error) {
	rng, stepS, hasStep := strings.Cut(part, "/")

	step := 1

	if hasStep {
		n, err := strconv.Atoi(stepS)
		if err != nil || n < 1 {
			return 0, fmt.Errorf("%w: bad step %q", errCron, stepS)
		}

		step = n
	}

	from, to := lo, hi

	switch {
	case rng == "*" || rng == "?":
	case strings.Contains(rng, "-"):
		a, b, _ := strings.Cut(rng, "-")

		var err error
		if from, err = cronValue(a, lo, hi, names, nameBase); err != nil {
			return 0, err
		}

		if to, err = cronValue(b, lo, hi, names, nameBase); err != nil {
			return 0, err
		}

		if from > to {
			return 0, fmt.Errorf("%w: reversed range %q", errCron, rng)
		}
	default:
		v, err := cronValue(rng, lo, hi, names, nameBase)
		if err != nil {
			return 0, err
		}

		from = v
		if !hasStep {
			to = v
		}
	}

	var mask uint64

	for v := from; v <= to; v += step {
		mask |= 1 << uint(v)
	}

	return mask, nil
}

func (c cronSpec) dayMatches(t time.Time) bool {
	domOK := c.dom&(1<<uint(t.Day())) != 0
	dowOK := c.dow&(1<<uint(t.Weekday())) != 0

	switch {
	case c.domAny && c.dowAny:
		return true
	case c.domAny:
		return dowOK
	case c.dowAny:
		return domOK
	}

	return domOK || dowOK
}

func (c cronSpec) matches(t time.Time) bool {
	return c.min&(1<<uint(t.Minute())) != 0 && c.hour&(1<<uint(t.Hour())) != 0 &&
		c.mon&(1<<uint(t.Month())) != 0 && c.dayMatches(t)
}

// latestFire returns the latest minute in (after, upTo] at which the schedule fires, looking back at most a week.
func (c cronSpec) latestFire(loc *time.Location, after, upTo time.Time) (time.Time, bool) {
	t := upTo.UTC().Truncate(time.Minute)

	for range maxLookback {
		if !t.After(after) {
			return time.Time{}, false
		}

		if c.matches(t.In(loc)) {
			return t, true
		}

		t = t.Add(-time.Minute)
	}

	return time.Time{}, false
}

// nextFire returns the first minute strictly after from at which the schedule fires.
func (c cronSpec) nextFire(loc *time.Location, from time.Time) (time.Time, bool) {
	t := from.In(loc).Truncate(time.Minute).Add(time.Minute)
	limit := t.AddDate(maxNextYears, 0, 0)

	for t.Before(limit) {
		switch {
		case c.mon&(1<<uint(t.Month())) == 0:
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, loc)
		case !c.dayMatches(t):
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, loc)
		case c.hour&(1<<uint(t.Hour())) == 0:
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, loc)
		case c.min&(1<<uint(t.Minute())) == 0:
			t = t.Add(time.Minute)
		default:
			return t.UTC(), true
		}
	}

	return time.Time{}, false
}
