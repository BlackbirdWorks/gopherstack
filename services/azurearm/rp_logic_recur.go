package azurearm

import (
	"slices"
	"strings"
	"time"
)

const (
	logicMaxConcurrency = 50
	logicDefaultConc    = 20
	logicDaysPerWeek    = 7
	logicMaxPeriods     = 200000
)

//nolint:gochecknoglobals // static weekday table
var logicWeekdays = map[string]time.Weekday{
	"sunday": time.Sunday, "monday": time.Monday, "tuesday": time.Tuesday, "wednesday": time.Wednesday,
	"thursday": time.Thursday, "friday": time.Friday, "saturday": time.Saturday,
}

type logicRecurrence struct {
	anchor    time.Time
	end       time.Time
	frequency string
	hours     []int
	minutes   []int
	monthDays []int
	weekDays  []time.Weekday
	interval  int
	scheduled bool
}

func intList(v any) []int {
	arr, _ := v.([]any)
	out := make([]int, 0, len(arr))

	for _, x := range arr {
		if n, ok := logicInt(normLogic(x)); ok {
			out = append(out, int(n))
		}
	}

	return out
}

// parseLogicRecurrence reads a validated Recurrence trigger; anchor is used when no startTime is given.
func parseLogicRecurrence(trigger map[string]any, anchor time.Time) logicRecurrence {
	rc, _ := objectOf(trigger["recurrence"])
	n, _ := logicInt(normLogic(rc["interval"]))
	r := logicRecurrence{frequency: strings.ToLower(stringOf(rc["frequency"])), interval: int(n), anchor: anchor}

	if st := stringOf(rc[logicKStartTime]); st != "" {
		if ts, err := parseLogicTime(st); err == nil {
			r.anchor = ts.t
		}
	}

	if et := stringOf(rc[logicKEndTime]); et != "" {
		if ts, err := parseLogicTime(et); err == nil {
			r.end = ts.t
		}
	}

	if sc, ok := objectOf(rc["schedule"]); ok {
		r.scheduled = true
		r.hours, r.minutes, r.monthDays = intList(sc["hours"]), intList(sc["minutes"]), intList(sc["monthDays"])

		days, _ := sc["weekDays"].([]any)
		for _, d := range days {
			r.weekDays = append(r.weekDays, logicWeekdays[strings.ToLower(stringOf(d))])
		}
	}

	return r
}

func (r logicRecurrence) unit() (time.Duration, bool) {
	switch r.frequency {
	case logicUnitSecond:
		return time.Second, true
	case logicUnitMinute:
		return time.Minute, true
	case logicUnitHour:
		return time.Hour, true
	case logicUnitDay:
		return logicHoursPerDay * time.Hour, true
	case logicUnitWeek:
		return logicDaysPerWeek * 24 * time.Hour, true
	}

	return 0, false
}

func (r logicRecurrence) periodStart(k int) time.Time {
	if d, ok := r.unit(); ok {
		return r.anchor.Add(time.Duration(k*r.interval) * d)
	}

	return r.anchor.AddDate(0, k*r.interval, 0)
}

// firstPeriod skips periods that end before after, so a restored trigger does not walk years of periods.
func (r logicRecurrence) firstPeriod(after time.Time) int {
	d, ok := r.unit()
	if !ok || !after.After(r.anchor) {
		return 0
	}

	step := d * time.Duration(r.interval)
	k := int(after.Sub(r.anchor)/step) - 1

	return max(k, 0)
}

// next returns the first occurrence strictly after the given time.
func (r logicRecurrence) next(after time.Time) (time.Time, bool) {
	for k := r.firstPeriod(after); k < r.firstPeriod(after)+logicMaxPeriods; k++ {
		ps := r.periodStart(k)
		if !r.end.IsZero() && ps.After(r.end) {
			return time.Time{}, false
		}

		for _, c := range r.candidates(ps) {
			if c.After(after) && !c.Before(r.anchor) && (r.end.IsZero() || !c.After(r.end)) {
				return c, true
			}
		}
	}

	return time.Time{}, false
}

func orDefault(v []int, d int) []int {
	if len(v) == 0 {
		return []int{d}
	}

	return v
}

func (r logicRecurrence) candidates(ps time.Time) []time.Time {
	if !r.scheduled {
		return []time.Time{ps}
	}

	minutes := orDefault(r.minutes, r.anchor.Minute())
	hours := orDefault(r.hours, r.anchor.Hour())

	var out []time.Time

	at := func(day time.Time, h, m int) {
		out = append(out, time.Date(day.Year(), day.Month(), day.Day(), h, m, ps.Second(), ps.Nanosecond(), time.UTC))
	}

	switch r.frequency {
	case logicUnitHour:
		for _, m := range minutes {
			at(ps, ps.Hour(), m)
		}
	case logicUnitDay:
		for _, h := range hours {
			for _, m := range minutes {
				at(ps, h, m)
			}
		}
	case logicUnitWeek:
		weekStart := time.Date(ps.Year(), ps.Month(), ps.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -int(ps.Weekday()))
		days := r.weekDays

		if len(days) == 0 {
			days = []time.Weekday{r.anchor.Weekday()}
		}

		for _, wd := range days {
			for _, h := range hours {
				for _, m := range minutes {
					at(weekStart.AddDate(0, 0, int(wd)), h, m)
				}
			}
		}
	case logicUnitMonth:
		out = r.monthCandidates(ps, hours, minutes)
	default:
		out = append(out, ps)
	}

	slices.SortFunc(out, func(a, b time.Time) int { return a.Compare(b) })

	return out
}

func (r logicRecurrence) monthCandidates(ps time.Time, hours, minutes []int) []time.Time {
	var out []time.Time

	last := time.Date(ps.Year(), ps.Month()+1, 0, 0, 0, 0, 0, time.UTC).Day()

	for _, md := range orDefault(r.monthDays, r.anchor.Day()) {
		day := md
		if md < 0 {
			day = last + md + 1
		}

		if day < 1 || day > last {
			continue
		}

		for _, h := range hours {
			for _, m := range minutes {
				out = append(out, time.Date(ps.Year(), ps.Month(), day, h, m, ps.Second(), ps.Nanosecond(), time.UTC))
			}
		}
	}

	return out
}
