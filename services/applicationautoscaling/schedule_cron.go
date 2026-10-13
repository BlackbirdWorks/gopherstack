package applicationautoscaling

import (
	"strconv"
	"strings"
	"time"
)

// cronField is the set of values one cron(...) field matches; any is true for * and ?.
type cronField struct {
	allowed     map[int]bool
	any         bool
	unsupported bool
}

func cronMonthNames() []string {
	return []string{
		"JAN",
		"FEB",
		"MAR",
		"APR",
		"MAY",
		"JUN",
		"JUL",
		"AUG",
		"SEP",
		"OCT",
		"NOV",
		"DEC",
	}
}

func cronDayNames() []string {
	return []string{"SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"}
}

type cronBounds struct {
	names    []string
	min, max int
	nameBase int
}

const (
	cronMaxMinute = 59
	cronMaxHour   = 23
	cronMaxDay    = 31
	cronMaxMonth  = 12
	cronMaxDow    = 7
	cronMinYear   = 1970
	cronMaxYear   = 2199
)

func cronFieldBounds() [cronFieldCount]cronBounds {
	return [cronFieldCount]cronBounds{
		{min: 0, max: cronMaxMinute},
		{min: 0, max: cronMaxHour},
		{min: 1, max: cronMaxDay},
		{min: 1, max: cronMaxMonth, names: cronMonthNames(), nameBase: 1},
		{min: 1, max: cronMaxDow, names: cronDayNames(), nameBase: 1},
		{min: cronMinYear, max: cronMaxYear},
	}
}

func parseCron(body string) (scheduleSpec, bool) {
	parts := strings.Fields(body)
	if len(parts) != cronFieldCount {
		return scheduleSpec{}, false
	}

	if parts[2] != "?" && parts[4] != "?" {
		return scheduleSpec{}, false
	}

	spec := scheduleSpec{kind: scheduleKindCron}
	bounds := cronFieldBounds()

	for i, p := range parts {
		f, ok := parseCronField(p, bounds[i])
		if !ok {
			return scheduleSpec{}, false
		}

		spec.cron[i] = f
	}

	return spec, true
}

func cronValue(tok string, b cronBounds) (int, bool) {
	if n, err := strconv.Atoi(tok); err == nil {
		return n, n >= b.min && n <= b.max
	}

	for i, name := range b.names {
		if strings.EqualFold(tok, name) {
			return i + b.nameBase, true
		}
	}

	return 0, false
}

func parseCronField(s string, b cronBounds) (cronField, bool) {
	if s == "*" || s == "?" {
		return cronField{any: true}, true
	}

	if strings.ContainsAny(s, "LW#") && !isNameToken(s, b) {
		return cronField{unsupported: true}, true
	}

	f := cronField{allowed: map[int]bool{}}

	for item := range strings.SplitSeq(s, ",") {
		if !addCronItem(f.allowed, item, b) {
			return cronField{}, false
		}
	}

	return f, true
}

func isNameToken(s string, b cronBounds) bool {
	_, ok := cronValue(s, b)

	return ok
}

func addCronItem(set map[int]bool, item string, b cronBounds) bool {
	rng, stepStr, hasStep := strings.Cut(item, "/")
	step := 1

	if hasStep {
		n, err := strconv.Atoi(stepStr)
		if err != nil || n < 1 {
			return false
		}

		step = n
	}

	lo, hi := b.min, b.max

	switch {
	case rng == "*":
	case strings.Contains(rng, "-"):
		a, z, _ := strings.Cut(rng, "-")
		l, ok1 := cronValue(a, b)
		h, ok2 := cronValue(z, b)

		if !ok1 || !ok2 || l > h {
			return false
		}

		lo, hi = l, h
	default:
		v, ok := cronValue(rng, b)
		if !ok {
			return false
		}

		lo, hi = v, v
		if hasStep {
			hi = b.max
		}
	}

	for v := lo; v <= hi; v += step {
		set[v] = true
	}

	return true
}

func (f cronField) matches(v int) bool {
	if f.unsupported {
		return false
	}

	return f.any || f.allowed[v]
}

// matchesMinute reports whether t (already in the schedule's timezone, truncated to the minute) fires.
func (s scheduleSpec) matchesMinute(t time.Time) bool {
	dow := int(t.Weekday()) + 1

	return s.cron[0].matches(t.Minute()) && s.cron[1].matches(t.Hour()) &&
		s.cron[2].matches(t.Day()) && s.cron[3].matches(int(t.Month())) &&
		s.cron[4].matches(dow) && s.cron[5].matches(t.Year())
}
