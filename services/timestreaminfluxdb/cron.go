package timestreaminfluxdb

import (
	"regexp"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awscron"
)

// customScheduleRE is the documented customSchedule pattern: cron( + six fields + ).
var customScheduleRE = regexp.MustCompile(`^cron\(\S+ \S+ \S+ \S+ \S+ \S+\)$`) //nolint:gochecknoglobals // immutable

const (
	cronFieldCount     = 6
	cronSearchYears    = 8
	minCustomSchedule  = 9
	maxCustomSchedule  = 256
	cronHoursPerDay    = 24
	minutesPerHour     = 60
	cronFieldMinuteIdx = 0
	cronFieldHourIdx   = 1
	cronFieldDomIdx    = 2
	cronFieldMonthIdx  = 3
	cronFieldDowIdx    = 4
	cronFieldYearIdx   = 5
)

type cronSpec struct{ f [cronFieldCount]string }

// parseCustomSchedule parses cron(min hour dom month dow year), the AWS six-field dialect.
func parseCustomSchedule(s string) (cronSpec, bool) {
	if len(s) < minCustomSchedule || len(s) > maxCustomSchedule || !customScheduleRE.MatchString(s) {
		return cronSpec{}, false
	}

	var spec cronSpec

	copy(spec.f[:], strings.Fields(s[len("cron("):len(s)-1]))

	if (spec.f[cronFieldDomIdx] == "?") == (spec.f[cronFieldDowIdx] == "?") {
		return cronSpec{}, false
	}

	kinds := [cronFieldCount]struct {
		kind     awscron.FieldKind
		min, max int
	}{
		{awscron.FieldMinute, awscron.MinuteMin, awscron.MinuteMax},
		{awscron.FieldHour, awscron.HourMin, awscron.HourMax},
		{awscron.FieldDayOfMonth, awscron.DayOfMonthMin, awscron.DayOfMonthMax},
		{awscron.FieldMonth, awscron.MonthMin, awscron.MonthMax},
		{awscron.FieldDayOfWeek, awscron.DayOfWeekMin, awscron.DayOfWeekMax},
		{awscron.FieldYear, awscron.YearMin, awscron.YearMax},
	}

	for i, k := range kinds {
		if !fieldHasValue(k.kind, spec.f[i], k.min, k.max) {
			return cronSpec{}, false
		}
	}

	return spec, true
}

func fieldHasValue(kind awscron.FieldKind, field string, lo, hi int) bool {
	for v := lo; v <= hi; v++ {
		if awscron.MatchField(kind, field, v, lo, hi) {
			return true
		}
	}

	return false
}

func (c cronSpec) dayMatches(d time.Time) bool {
	return awscron.MatchField(awscron.FieldYear, c.f[cronFieldYearIdx], d.Year(), awscron.YearMin, awscron.YearMax) &&
		awscron.MatchField(awscron.FieldMonth, c.f[cronFieldMonthIdx], int(d.Month()), awscron.MonthMin, awscron.MonthMax) &&
		awscron.MatchDayFields(c.f[cronFieldDomIdx], c.f[cronFieldDowIdx], d)
}

// next returns the first matching minute strictly after t (UTC), or zero when none falls within the search window.
func (c cronSpec) next(t time.Time) time.Time {
	t = t.UTC().Truncate(time.Minute).Add(time.Minute)
	day := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	end := day.AddDate(cronSearchYears, 0, 0)

	for ; day.Before(end); day = day.AddDate(0, 0, 1) {
		if !c.dayMatches(day) {
			continue
		}

		for h := range cronHoursPerDay {
			if !awscron.MatchField(awscron.FieldHour, c.f[cronFieldHourIdx], h, awscron.HourMin, awscron.HourMax) {
				continue
			}

			for m := range minutesPerHour {
				at := day.Add(time.Duration(h)*time.Hour + time.Duration(m)*time.Minute)
				if at.Before(t) {
					continue
				}

				if awscron.MatchField(awscron.FieldMinute, c.f[cronFieldMinuteIdx], m, awscron.MinuteMin, awscron.MinuteMax) {
					return at
				}
			}
		}
	}

	return time.Time{}
}

// nextBackupFor is the next run of cfg after t; zero when the type has no computed schedule.
func nextBackupFor(t time.Time, cfg *BackupConfig) time.Time {
	if cfg.Type == "CUSTOM_SCHEDULE" {
		spec, ok := parseCustomSchedule(cfg.CustomSchedule)
		if !ok {
			return time.Time{}
		}

		return spec.next(t)
	}

	return nextBackup(t, cfg.Type)
}
