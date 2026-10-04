package cloudwatch

import (
	"fmt"
	"regexp"
	"strconv"
	"time"
	_ "time/tzdata" // IANA zones for wall clock windows in minimal containers
)

// AlarmEvaluationWindow is MetricAlarm.EvaluationWindow: nil or WallClock=false is a sliding window.
type AlarmEvaluationWindow struct {
	Timezone  string `json:"Timezone,omitempty"`
	WallClock bool   `json:"WallClock,omitempty"`
}

const (
	secondsPerMinute = 60
	secondsPerHour   = 3600
	fiveMinutes      = 300
	daysPerWeek      = 7
	secondsPerDay    = 86400
	secondsPerWeek   = 604800
	offsetStepMin    = 5
)

var tzOffsetPattern = regexp.MustCompile(`^(?:UTC)?([+-])(\d{2}):(\d{2})$`)

// wallClockPeriodOK reports whether period (seconds) is a supported wall clock period.
func wallClockPeriodOK(period int32) bool {
	switch period {
	case secondsPerMinute, fiveMinutes, secondsPerHour, secondsPerDay, secondsPerWeek:
		return true
	default:
		return false
	}
}

// resolveWindowTimezone parses an IANA name or a fixed offset (+05:30, UTC+05:30) in 5-minute steps.
func resolveWindowTimezone(name string) (*time.Location, error) {
	if name == "" {
		return time.UTC, nil
	}

	if m := tzOffsetPattern.FindStringSubmatch(name); m != nil {
		hours, _ := strconv.Atoi(m[2])
		mins, _ := strconv.Atoi(m[3])

		if hours > 23 || mins > 59 || mins%offsetStepMin != 0 {
			return nil, fmt.Errorf("%w: Timezone offset %q must be a multiple of 5 minutes", ErrValidation, name)
		}

		secs := hours*secondsPerHour + mins*secondsPerMinute
		if m[1] == "-" {
			secs = -secs
		}

		return time.FixedZone(name, secs), nil
	}

	loc, err := time.LoadLocation(name)
	if err != nil {
		return nil, fmt.Errorf("%w: Timezone %q is not a valid IANA zone or UTC offset", ErrValidation, name)
	}

	return loc, nil
}

// validateEvaluationWindow checks a wall clock window against the alarm period (types.WallClockWindow).
func validateEvaluationWindow(w *AlarmEvaluationWindow, period int32) error {
	if w == nil || !w.WallClock {
		return nil
	}

	if period > 0 && !wallClockPeriodOK(period) {
		return fmt.Errorf(
			"%w: a wall clock window requires a period of 60, 300, 3600, 86400 or 604800 seconds", ErrValidation,
		)
	}

	_, err := resolveWindowTimezone(w.Timezone)

	return err
}

// alarmEvaluationTime returns the instant an alarm evaluates up to: now for sliding windows, otherwise the
// latest clock boundary at or before now. Weekly windows start on Monday.
func alarmEvaluationTime(a MetricAlarm, now time.Time) time.Time {
	if a.EvaluationWindow == nil || !a.EvaluationWindow.WallClock || !wallClockPeriodOK(a.Period) {
		return now
	}

	loc, err := resolveWindowTimezone(a.EvaluationWindow.Timezone)
	if err != nil {
		return now
	}

	local := now.In(loc)

	switch a.Period {
	case secondsPerDay:
		return time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, loc)
	case secondsPerWeek:
		daysSinceMonday := (int(local.Weekday()) + daysPerWeek - 1) % daysPerWeek

		return time.Date(local.Year(), local.Month(), local.Day()-daysSinceMonday, 0, 0, 0, 0, loc)
	default:
		_, offset := local.Zone()
		p := int64(a.Period)

		return time.Unix((now.Unix()+int64(offset))/p*p-int64(offset), 0).UTC()
	}
}
