package gcpscheduler

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	minutesPerHour = 60
	minutesPerDay  = 24 * minutesPerHour
	maxOrdinal     = 5
	daysPerWeek    = 7
	timeParts      = 2
	hhmmLen        = 5
)

type schedule interface {
	next(t time.Time, loc *time.Location) time.Time
}

//nolint:gochecknoglobals // immutable lookup tables
var (
	grocDays = map[string]int{
		"sunday": 0, "sun": 0, "monday": 1, "mon": 1, "tuesday": 2, "tue": 2, "wednesday": 3, "wed": 3,
		"thursday": 4, "thu": 4, "friday": 5, "fri": 5, "saturday": 6, "sat": 6,
	}
	grocMonths = map[string]int{
		"january": 1, "jan": 1, "february": 2, "feb": 2, "march": 3, "mar": 3, "april": 4, "apr": 4,
		"may": 5, "june": 6, "jun": 6, "july": 7, "jul": 7, "august": 8, "aug": 8,
		"september": 9, "sep": 9, "october": 10, "oct": 10, "november": 11, "nov": 11, "december": 12, "dec": 12,
	}
	grocOrdinals = map[string]int{
		"1st": 1, "first": 1, "2nd": 2, "second": 2, "3rd": 3, "third": 3,
		"4th": 4, "fourth": 4, "5th": 5, "fifth": 5,
	}
)

// parseCron accepts unix-cron or the App Engine cron (groc) format Cloud Scheduler documents.
func parseCron(expr string) (schedule, error) {
	s, err := parseUnixCron(expr)
	if err == nil {
		return s, nil
	}

	if g, gerr := parseGroc(expr); gerr == nil {
		return g, nil
	} else if len(strings.Fields(expr)) != cronFieldCount {
		return nil, gerr
	}

	return nil, err
}

// grocInterval is "every N minutes|hours [from HH:MM to HH:MM | synchronized]", on a lattice anchored at from.
type grocInterval struct {
	step, from, to int
}

func (g *grocInterval) next(t time.Time, loc *time.Location) time.Time {
	t = t.In(loc)

	for d := range 2 {
		for m := g.from; m <= g.to; m += g.step {
			c := time.Date(t.Year(), t.Month(), t.Day()+d, m/minutesPerHour, m%minutesPerHour, 0, 0, loc)
			if c.After(t) {
				return c
			}
		}
	}

	return time.Time{}
}

// grocCalendar is the day-and-time form: ordinals/weekdays or month days, in months, at HH:MM.
type grocCalendar struct {
	ordinals, dows, doms, months uint64
	minute                       int
}

func (g *grocCalendar) dayMatches(d time.Time) bool {
	if g.months&(1<<uint(d.Month())) == 0 {
		return false
	}

	if g.doms != 0 {
		return g.doms&(1<<uint(d.Day())) != 0
	}

	if g.dows&(1<<uint(d.Weekday())) == 0 {
		return false
	}

	return g.ordinals == 0 || g.ordinals&(1<<uint((d.Day()-1)/daysPerWeek+1)) != 0
}

func (g *grocCalendar) next(t time.Time, loc *time.Location) time.Time {
	t = t.In(loc)
	day := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, loc)

	for range maxSearchDays {
		if g.dayMatches(day) {
			c := time.Date(day.Year(), day.Month(), day.Day(), g.minute/minutesPerHour, g.minute%minutesPerHour, 0, 0, loc)
			if c.After(t) {
				return c
			}
		}

		day = time.Date(day.Year(), day.Month(), day.Day()+1, 0, 0, 0, 0, loc)
	}

	return time.Time{}
}

func grocErr(format string, a ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{errCron}, a...)...)
}

func parseGroc(expr string) (schedule, error) {
	toks := strings.Fields(strings.ToLower(expr))
	every := len(toks) > 0 && toks[0] == "every"

	if every {
		toks = toks[1:]
	}

	if len(toks) == 0 {
		return nil, grocErr("empty schedule")
	}

	if n, err := strconv.Atoi(toks[0]); err == nil && len(toks) > 1 && isIntervalUnit(toks[1]) {
		if !every {
			return nil, grocErr("interval schedules must start with \"every\"")
		}

		return parseGrocInterval(n, toks[1], toks[2:])
	}

	return parseGrocCalendar(toks, every)
}

func isIntervalUnit(s string) bool { return s == "minutes" || s == "mins" || s == "hours" }

func parseGrocInterval(n int, unit string, rest []string) (schedule, error) {
	if n < 1 {
		return nil, grocErr("interval must be at least 1")
	}

	g := &grocInterval{step: n, to: minutesPerDay - 1}
	if unit == "hours" {
		g.step = n * minutesPerHour
	}

	switch {
	case len(rest) == 0 || (len(rest) == 1 && rest[0] == "synchronized"):
	case len(rest) == 4 && rest[0] == "from" && rest[2] == "to": //nolint:mnd // from HH:MM to HH:MM
		var err error

		if g.from, err = parseHHMM(rest[1]); err != nil {
			return nil, err
		}

		if g.to, err = parseHHMM(rest[3]); err != nil {
			return nil, err
		}

		if g.to < g.from {
			return nil, grocErr("\"to\" time precedes \"from\"")
		}
	default:
		return nil, grocErr("unexpected %q after interval", strings.Join(rest, " "))
	}

	return g, nil
}

func parseHHMM(s string) (int, error) {
	h, m, ok := strings.Cut(s, ":")
	hv, herr := strconv.Atoi(h)
	mv, merr := strconv.Atoi(m)

	if !ok || len(s) != hhmmLen || herr != nil || merr != nil || hv < 0 || hv > maxHour || mv < 0 || mv > maxMinute {
		return 0, grocErr("time %q must be HH:MM in 24 hour format", s)
	}

	return hv*minutesPerHour + mv, nil
}

func nameMask(list string, names map[string]int, all string, allMask uint64) (uint64, bool) {
	if list == all {
		return allMask, true
	}

	var mask uint64

	for item := range strings.SplitSeq(list, ",") {
		v, ok := names[item]
		if !ok {
			return 0, false
		}

		mask |= 1 << uint(v)
	}

	return mask, true
}

func numMask(list string, lo, hi int) (uint64, bool) {
	var mask uint64

	for item := range strings.SplitSeq(list, ",") {
		v, err := strconv.Atoi(item)
		if err != nil || v < lo || v > hi {
			return 0, false
		}

		mask |= 1 << uint(v)
	}

	return mask, true
}

func parseGrocCalendar(toks []string, every bool) (schedule, error) {
	g := &grocCalendar{}
	ok := false

	if m, found := nameMask(toks[0], grocOrdinals, "", 0); found {
		g.ordinals, toks = m, toks[1:]
		every = true
	} else if m, found = numMask(toks[0], 1, maxDom); found {
		g.doms, toks = m, toks[1:]

		if len(toks) == 0 || toks[0] != "of" {
			return nil, grocErr("a day-of-month list must be followed by \"of\"")
		}
	}

	if g.doms == 0 {
		if len(toks) == 0 {
			return nil, grocErr("a day of the week is required")
		}

		if g.dows, ok = nameMask(toks[0], grocDays, "day", 1<<daysPerWeek-1); !ok {
			return nil, grocErr("unknown day %q", toks[0])
		}

		if !every && g.ordinals == 0 {
			return nil, grocErr("a daily schedule must start with \"every\"")
		}

		toks = toks[1:]
	}

	g.months = 1<<13 - 2 //nolint:mnd // bits 1..12

	if len(toks) > 0 && toks[0] == "of" {
		if len(toks) < timeParts {
			return nil, grocErr("\"of\" must be followed by months")
		}

		if g.months, ok = nameMask(toks[1], grocMonths, "month", g.months); !ok {
			return nil, grocErr("unknown month %q", toks[1])
		}

		toks = toks[timeParts:]
	}

	switch len(toks) {
	case 0:
	case 1:
		var err error
		if g.minute, err = parseHHMM(toks[0]); err != nil {
			return nil, err
		}
	default:
		return nil, grocErr("unexpected %q", strings.Join(toks, " "))
	}

	return g, nil
}
