package azurearm

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	acaCronMinuteMax = 59
	acaCronHourMax   = 23
	acaCronDomMax    = 31
	acaCronMonthMax  = 12
	acaCronDowMax    = 7
	acaCronLookahead = 8
	acaCronParts     = 2
)

// acaCron is a standard five-field cron expression; Container Apps evaluates it in UTC.
type acaCron struct {
	minute, hour, dom, month, dow uint64
	domAny, dowAny                bool
}

const (
	acaCronMonthNames = "jan feb mar apr may jun jul aug sep oct nov dec"
	acaCronDowNames   = "sun mon tue wed thu fri sat"
)

var errACACron = errors.New("invalid cron expression")

func parseACACron(expr string) (*acaCron, error) {
	f := strings.Fields(expr)
	if len(f) != acaCronFields {
		return nil, fmt.Errorf("%w: need five fields", errACACron)
	}

	var (
		c   acaCron
		err error
	)

	fields := []struct {
		dst      *uint64
		spec     string
		names    []string
		lo, hi   int
		nameBase int
	}{
		{&c.minute, f[0], nil, 0, acaCronMinuteMax, 0},
		{&c.hour, f[1], nil, 0, acaCronHourMax, 0},
		{&c.dom, f[2], nil, 1, acaCronDomMax, 0},
		{&c.month, f[3], strings.Fields(acaCronMonthNames), 1, acaCronMonthMax, 1},
		{&c.dow, f[4], strings.Fields(acaCronDowNames), 0, acaCronDowMax, 0},
	}

	for _, fd := range fields {
		if *fd.dst, err = acaCronField(fd.spec, fd.lo, fd.hi, fd.names, fd.nameBase); err != nil {
			return nil, err
		}
	}

	if c.dow&(1<<acaCronDowMax) != 0 {
		c.dow = c.dow&^(1<<acaCronDowMax) | 1
	}

	c.domAny, c.dowAny = strings.HasPrefix(f[2], "*"), strings.HasPrefix(f[4], "*")

	return &c, nil
}

func acaCronField(spec string, lo, hi int, names []string, base int) (uint64, error) {
	var mask uint64

	for part := range strings.SplitSeq(spec, ",") {
		rng, stepText, hasStep := strings.Cut(part, "/")
		step := 1

		if hasStep {
			n, err := strconv.Atoi(stepText)
			if err != nil || n < 1 {
				return 0, fmt.Errorf("%w: bad step %q", errACACron, part)
			}

			step = n
		}

		from, to := lo, hi

		if rng != "*" {
			a, b, isRange := strings.Cut(rng, "-")

			var err error
			if from, err = acaCronValue(a, names, base); err != nil {
				return 0, err
			}

			to = from
			if isRange {
				if to, err = acaCronValue(b, names, base); err != nil {
					return 0, err
				}
			} else if hasStep {
				to = hi
			}
		}

		if from < lo || to > hi || from > to {
			return 0, fmt.Errorf("%w: %q out of range %d-%d", errACACron, part, lo, hi)
		}

		for v := from; v <= to; v += step {
			mask |= 1 << v
		}
	}

	return mask, nil
}

func acaCronValue(s string, names []string, base int) (int, error) {
	if n, err := strconv.Atoi(s); err == nil {
		return n, nil
	}

	for i, name := range names {
		if strings.EqualFold(s, name) {
			return i + base, nil
		}
	}

	return 0, fmt.Errorf("%w: bad value %q", errACACron, s)
}

func (c *acaCron) dayMatches(t time.Time) bool {
	dom := c.dom&(1<<t.Day()) != 0
	dow := c.dow&(1<<int(t.Weekday())) != 0

	switch {
	case c.domAny && c.dowAny:
		return true
	case c.domAny:
		return dow
	case c.dowAny:
		return dom
	}

	return dom || dow
}

// next returns the first matching minute strictly after t, or the zero time when none falls within eight years.
func (c *acaCron) next(t time.Time) time.Time {
	t = t.UTC().Truncate(time.Minute).Add(time.Minute)
	limit := t.AddDate(acaCronLookahead, 0, 0)

	for t.Before(limit) {
		switch {
		case c.month&(1<<int(t.Month())) == 0:
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, time.UTC)
		case !c.dayMatches(t):
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, time.UTC)
		case c.hour&(1<<t.Hour()) == 0:
			t = t.Truncate(time.Hour).Add(time.Hour)
		case c.minute&(1<<t.Minute()) == 0:
			t = t.Add(time.Minute)
		default:
			return t
		}
	}

	return time.Time{}
}
