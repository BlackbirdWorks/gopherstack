package applicationautoscaling

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata" // IANA zones for scheduled-action timezones in minimal containers
)

const (
	scheduleKindAt   = "at"
	scheduleKindRate = "rate"
	scheduleKindCron = "cron"
	cronFieldCount   = 6
	atLayout         = "2006-01-02T15:04:05"
)

type scheduleSpec struct {
	at       time.Time
	cron     [cronFieldCount]cronField
	kind     string
	interval time.Duration
}

func errInvalidSchedule(expr string) error {
	return fmt.Errorf("%w: Invalid schedule expression %q; use at(yyyy-mm-ddThh:mm:ss), "+
		"rate(value unit) or cron(fields)", ErrValidation, expr)
}

// parseSchedule parses an at(), rate() or cron() expression; at() is interpreted in loc.
func parseSchedule(expr string, loc *time.Location) (scheduleSpec, error) {
	kind, body, ok := strings.Cut(expr, "(")
	if !ok || !strings.HasSuffix(body, ")") {
		return scheduleSpec{}, errInvalidSchedule(expr)
	}

	body = strings.TrimSuffix(body, ")")

	switch kind {
	case scheduleKindAt:
		t, err := time.ParseInLocation(atLayout, strings.TrimSpace(body), loc)
		if err != nil {
			return scheduleSpec{}, errInvalidSchedule(expr)
		}

		return scheduleSpec{kind: kind, at: t}, nil
	case scheduleKindRate:
		d, rateOK := parseRate(body)
		if !rateOK {
			return scheduleSpec{}, errInvalidSchedule(expr)
		}

		return scheduleSpec{kind: kind, interval: d}, nil
	case scheduleKindCron:
		spec, cronOK := parseCron(body)
		if !cronOK {
			return scheduleSpec{}, errInvalidSchedule(expr)
		}

		return spec, nil
	default:
		return scheduleSpec{}, errInvalidSchedule(expr)
	}
}

func parseRate(body string) (time.Duration, bool) {
	fields := strings.Fields(body)
	if len(fields) != 2 { //nolint:mnd // "value unit"
		return 0, false
	}

	n, err := strconv.Atoi(fields[0])
	if err != nil || n < 1 {
		return 0, false
	}

	unit := fields[1]
	if n == 1 {
		unit += "s"
	}

	switch unit {
	case "minutes":
		return time.Duration(n) * time.Minute, true
	case "hours":
		return time.Duration(n) * time.Hour, true
	case "days":
		return time.Duration(n) * 24 * time.Hour, true
	default:
		return 0, false
	}
}

// validateScheduleInput checks the schedule expression and timezone of a PutScheduledAction request.
func validateScheduleInput(schedule, timezone string, startTime, endTime *time.Time) error {
	loc := time.UTC

	if timezone != "" {
		l, err := time.LoadLocation(timezone)
		if err != nil {
			return fmt.Errorf("%w: Invalid timezone %q", ErrValidation, timezone)
		}

		loc = l
	}

	if schedule != "" {
		if _, err := parseSchedule(schedule, loc); err != nil {
			return err
		}
	}

	if startTime != nil && endTime != nil && !startTime.Before(*endTime) {
		return fmt.Errorf("%w: EndTime must be after StartTime", ErrValidation)
	}

	return nil
}
