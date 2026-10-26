package gcpscheduler

import (
	"fmt"
	"slices"
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

func grocDays() [][]string {
	return [][]string{
		{"sunday", "sun"}, {"monday", "mon"}, {"tuesday", "tue"}, {"wednesday", "wed"},
		{"thursday", "thu"}, {"friday", "fri"}, {"saturday", "sat"},
	}
}

func grocMonths() [][]string {
	return [][]string{
		{"january", "jan"}, {"february", "feb"}, {"march", "mar"}, {"april", "apr"}, {"may"}, {"june", "jun"},
		{"july", "jul"}, {"august", "aug"}, {"september", "sep"}, {"october", "oct"}, {"november", "nov"},
		{"december", "dec"},
	}
}

func grocOrdinals() [][]string {
	return [][]string{{"first", "1st"}, {"second", "2nd"}, {"third", "3rd"}, {"fourth", "4th"}, {"fifth", "5th"}}
}

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
			c := time.Date(
				day.Year(),
				day.Month(),
				day.Day(),
				g.minute/minutesPerHour,
				g.minute%minutesPerHour,
				0,
				0,
				loc,
			)
			if c.After(t) {
				return c
			}
		}

		day = time.Date(day.Year(), day.Month(), day.Day()+1, 0, 0, 0, 0, loc)
	}

	return time.Time{}
}

func grocErr(format string, a ...any) error {
	return fmt.Errorf("%w: %s", errCron, fmt.Sprintf(format, a...))
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
	case len(rest) == 4 && rest[0] == "from" && rest[2] == "to":
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

// nameMask resolves a comma list of names (or the all keyword) to a bit mask; base is the value of table[0].
func nameMask(list string, table [][]string, base int, all string) (uint64, bool) {
	if list == all && all != "" {
		var m uint64
		for i := range table {
			m |= 1 << uint(base+i)
		}

		return m, true
	}

	var mask uint64

	for item := range strings.SplitSeq(list, ",") {
		found := false

		for i, names := range table {
			if slices.Contains(names, item) {
				mask |= 1 << uint(base+i)
				found = true
			}
		}

		if !found {
			return 0, false
		}
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

	toks, err := parseGrocDays(g, toks, every)
	if err != nil {
		return nil, err
	}

	if toks, err = parseGrocMonths(g, toks); err != nil {
		return nil, err
	}

	switch len(toks) {
	case 0:
	case 1:
		if g.minute, err = parseHHMM(toks[0]); err != nil {
			return nil, err
		}
	default:
		return nil, grocErr("unexpected %q", strings.Join(toks, " "))
	}

	return g, nil
}

// parseGrocDays fills the ordinal/weekday or month-day part and returns the remaining tokens.
func parseGrocDays(g *grocCalendar, toks []string, every bool) ([]string, error) {
	if m, found := nameMask(toks[0], grocOrdinals(), 1, ""); found {
		g.ordinals, toks, every = m, toks[1:], true
	} else if m, found = numMask(toks[0], 1, maxDom); found {
		g.doms, toks = m, toks[1:]

		if len(toks) == 0 || toks[0] != "of" {
			return nil, grocErr("a day-of-month list must be followed by \"of\"")
		}

		return toks, nil
	}

	if len(toks) == 0 {
		return nil, grocErr("a day of the week is required")
	}

	var ok bool

	if g.dows, ok = nameMask(toks[0], grocDays(), 0, "day"); !ok {
		return nil, grocErr("unknown day %q", toks[0])
	}

	if !every {
		return nil, grocErr("a daily schedule must start with \"every\"")
	}

	return toks[1:], nil
}

// parseGrocMonths reads the optional "of <months>" part; no part means every month.
func parseGrocMonths(g *grocCalendar, toks []string) ([]string, error) {
	months := grocMonths()
	g.months, _ = nameMask("month", months, 1, "month")

	if len(toks) == 0 || toks[0] != "of" {
		return toks, nil
	}

	if len(toks) < 2 { //nolint:mnd // "of" plus the month list
		return nil, grocErr("\"of\" must be followed by months")
	}

	var ok bool

	if g.months, ok = nameMask(toks[1], months, 1, "month"); !ok {
		return nil, grocErr("unknown month %q", toks[1])
	}

	return toks[2:], nil
}
