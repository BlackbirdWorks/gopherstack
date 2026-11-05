package timestreamquery

import (
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awscron"
)

const (
	cronSearchYears = 8
	hoursInDay      = 24
	minutesInHour   = 60
	cronMinuteIdx   = 0
	cronHourIdx     = 1
	cronDomIdx      = 2
	cronMonthIdx    = 3
	cronDowIdx      = 4
	cronYearIdx     = 5
)

// schedule is a parsed ScheduleExpression: a fixed rate or a six-field EventBridge-style cron.
type schedule struct {
	cron [cronFieldCount]string
	rate time.Duration
}

func parseSchedule(expr string) (schedule, bool) {
	if m := rateCapture.FindStringSubmatch(expr); m != nil {
		n, err := strconv.Atoi(m[1])
		if err != nil || n <= 0 {
			return schedule{}, false
		}

		unit := time.Minute

		switch {
		case strings.HasPrefix(m[2], "hour"):
			unit = time.Hour
		case strings.HasPrefix(m[2], "day"):
			unit = hoursInDay * time.Hour
		}

		return schedule{rate: time.Duration(n) * unit}, true
	}

	m := cronParts.FindStringSubmatch(expr)
	if m == nil {
		return schedule{}, false
	}

	fields := strings.Fields(m[1])
	if len(fields) != cronFieldCount {
		return schedule{}, false
	}

	var s schedule

	copy(s.cron[:], fields)

	return s, true
}

func (s schedule) dayMatches(d time.Time) bool {
	return awscron.MatchField(awscron.FieldYear, s.cron[cronYearIdx], d.Year(), awscron.YearMin, awscron.YearMax) &&
		awscron.MatchField(awscron.FieldMonth, s.cron[cronMonthIdx], int(d.Month()), awscron.MonthMin, awscron.MonthMax) &&
		awscron.MatchDayFields(s.cron[cronDomIdx], s.cron[cronDowIdx], d)
}

// next returns the first invocation strictly after t, or the zero time when none exists.
func (s schedule) next(t time.Time) time.Time {
	if s.rate > 0 {
		return t.Add(s.rate)
	}

	t = t.UTC().Truncate(time.Minute).Add(time.Minute)
	day := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	end := day.AddDate(cronSearchYears, 0, 0)

	for ; day.Before(end); day = day.AddDate(0, 0, 1) {
		if !s.dayMatches(day) {
			continue
		}

		if at, ok := s.firstInDay(day, t); ok {
			return at
		}
	}

	return time.Time{}
}

func (s schedule) firstInDay(day, notBefore time.Time) (time.Time, bool) {
	for h := range hoursInDay {
		if !awscron.MatchField(awscron.FieldHour, s.cron[cronHourIdx], h, awscron.HourMin, awscron.HourMax) {
			continue
		}

		for m := range minutesInHour {
			at := day.Add(time.Duration(h)*time.Hour + time.Duration(m)*time.Minute)
			if at.Before(notBefore) {
				continue
			}

			if awscron.MatchField(awscron.FieldMinute, s.cron[cronMinuteIdx], m, awscron.MinuteMin, awscron.MinuteMax) {
				return at, true
			}
		}
	}

	return time.Time{}, false
}
