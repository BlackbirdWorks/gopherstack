package ocimonitoring

import (
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	freqDaily      = "DAILY"
	freqWeekly     = "WEEKLY"
	minRecurrence  = time.Minute
	maxRecurrence  = 24 * time.Hour
	maxSpans       = 1000
	maxScanDays    = 3660
	hoursInDay     = 24
	minutesPerHour = 60
	secondsPerMin  = 60
	partCount      = 2
)

func weekdayOf(code string) (time.Weekday, bool) {
	for i, c := range []string{"SU", "MO", "TU", "WE", "TH", "FR", "SA"} {
		if c == code {
			return time.Weekday(i), true
		}
	}

	return 0, false
}

// recurrence is the RFC 5545 subset Monitoring documents for suppressions: FREQ=DAILY|WEEKLY with
// BYDAY, BYHOUR, BYMINUTE and BYSECOND. INTERVAL, COUNT, UNTIL and every other rule part are rejected.
type recurrence struct {
	days    []time.Weekday
	hours   []int
	minutes []int
	seconds []int
	dur     time.Duration
	weekly  bool
}

type span struct{ start, end time.Time }

func parseIntList(key, val string, hi int) ([]int, *APIError) {
	var out []int

	for p := range strings.SplitSeq(val, ",") {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 || n > hi {
			return nil, errParam("suppressionRecurrence " + key + " values must be integers between 0 and " +
				strconv.Itoa(hi))
		}

		out = append(out, n)
	}

	slices.Sort(out)

	return slices.Compact(out), nil
}

func parseDays(val string) ([]time.Weekday, *APIError) {
	var out []time.Weekday

	for p := range strings.SplitSeq(val, ",") {
		d, ok := weekdayOf(p)
		if !ok {
			return nil, errParam("suppressionRecurrence BYDAY values must be MO, TU, WE, TH, FR, SA or SU")
		}

		out = append(out, d)
	}

	return out, nil
}

func parseRecurrence(rule, duration string) (*recurrence, *APIError) {
	dur, ok := parseISODuration(duration)
	if !ok || dur < minRecurrence || dur > maxRecurrence {
		return nil, errParam("suppressionDuration must be an ISO 8601 duration between PT1M and PT24H")
	}

	r := &recurrence{dur: dur, hours: []int{0}, minutes: []int{0}, seconds: []int{0}}
	seen := map[string]bool{}

	for part := range strings.SplitSeq(rule, ";") {
		kv := strings.SplitN(part, "=", partCount)
		if len(kv) != partCount || kv[1] == "" || seen[kv[0]] {
			return nil, errParam("suppressionRecurrence must be KEY=VALUE rule parts separated by semicolons")
		}

		seen[kv[0]] = true

		if aerr := r.setPart(kv[0], kv[1]); aerr != nil {
			return nil, aerr
		}
	}

	switch {
	case !seen["FREQ"]:
		return nil, errParam("suppressionRecurrence requires FREQ")
	case seen["BYDAY"] && !r.weekly:
		return nil, errParam("suppressionRecurrence BYDAY can only be used with FREQ=WEEKLY")
	}

	return r, nil
}

func (r *recurrence) setPart(key, val string) *APIError {
	var aerr *APIError

	switch key {
	case "FREQ":
		switch val {
		case freqDaily:
		case freqWeekly:
			r.weekly = true
		default:
			return errParam("suppressionRecurrence FREQ must be DAILY or WEEKLY")
		}
	case "BYDAY":
		r.days, aerr = parseDays(val)
	case "BYHOUR":
		r.hours, aerr = parseIntList(key, val, hoursInDay-1)
	case "BYMINUTE":
		r.minutes, aerr = parseIntList(key, val, minutesPerHour-1)
	case "BYSECOND":
		r.seconds, aerr = parseIntList(key, val, secondsPerMin-1)
	default:
		return errParam("suppressionRecurrence rule part " + key +
			" is not supported; supported parts are FREQ, BYDAY, BYHOUR, BYMINUTE and BYSECOND")
	}

	return aerr
}

func (r *recurrence) onDay(d time.Weekday, from time.Time) bool {
	if !r.weekly {
		return true
	}

	if len(r.days) == 0 {
		return d == from.Weekday()
	}

	return slices.Contains(r.days, d)
}

// spans returns the occurrences starting within [from, until] that overlap [lo, hi], capped at maxSpans.
func (r *recurrence) spans(from, until, lo, hi time.Time) []span {
	first := from
	if w := lo.Add(-r.dur); w.After(from) {
		first = w
	}
	day := time.Date(first.Year(), first.Month(), first.Day(), 0, 0, 0, 0, time.UTC)

	var out []span

	for i := 0; i < maxScanDays && !day.After(hi) && !day.After(until) && len(out) < maxSpans; i++ {
		if r.onDay(day.Weekday(), from) {
			out = r.appendDay(out, day, from, until, lo, hi)
		}

		day = day.AddDate(0, 0, 1)
	}

	return out
}

func (r *recurrence) appendDay(out []span, day, from, until, lo, hi time.Time) []span {
	for _, h := range r.hours {
		for _, m := range r.minutes {
			for _, s := range r.seconds {
				start := day.Add(
					time.Duration(h)*time.Hour + time.Duration(m)*time.Minute + time.Duration(s)*time.Second,
				)
				if start.Before(from) || start.After(until) || start.After(hi) {
					continue
				}

				end := start.Add(r.dur)
				if end.After(until) {
					end = until
				}

				if end.Before(lo) {
					continue
				}

				out = append(out, span{start: start, end: end})
			}
		}
	}

	return out
}
