package timestreamquery

import (
	"database/sql/driver"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	reDurationText = regexp.MustCompile(
		`(?i)^\s*(\d+(?:\.\d+)?)\s*(nanoseconds?|ns|microseconds?|us|milliseconds?|ms|seconds?|s|minutes?|m|hours?|h|` +
			`days?|d)\s*$`,
	)
	reIntervalText = regexp.MustCompile(`^(-?\d+) (\d\d):(\d\d):(\d\d)\.(\d{9})$`)
)

// formatInterval renders an interval day to second the way Timestream does: "d hh:mm:ss.nnnnnnnnn".
func formatInterval(ns int64) string {
	sign := ""
	if ns < 0 {
		sign, ns = "-", -ns
	}

	const nsPerDay = int64(hoursInDay) * int64(time.Hour)

	days, rest := ns/nsPerDay, time.Duration(ns%nsPerDay)

	return fmt.Sprintf("%s%d %02d:%02d:%02d.%09d", sign, days, int(rest.Hours()), int(rest.Minutes())%minutesInHour,
		int(rest.Seconds())%minutesInHour, rest%time.Second)
}

func parseIntervalText(s string) (int64, bool) {
	m := reIntervalText.FindStringSubmatch(s)
	if m == nil {
		return 0, false
	}

	days, _ := strconv.ParseInt(m[1], 10, 64)
	h, _ := strconv.ParseInt(m[2], 10, 64)
	mi, _ := strconv.ParseInt(m[3], 10, 64)
	sec, _ := strconv.ParseInt(m[4], 10, 64)
	frac, _ := strconv.ParseInt(m[5], 10, 64)
	total := ((days*hoursInDay+h)*minutesInHour+mi)*minutesInHour + sec

	if strings.HasPrefix(m[1], "-") {
		return total*int64(time.Second) - frac, true
	}

	return total*int64(time.Second) + frac, true
}

// udfParseDuration implements parse_duration('42.8ms').
func udfParseDuration(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	if args[0] == nil {
		return nil, nil //nolint:nilnil // SQL NULL
	}

	s, ok := textOf(args[0])
	if !ok {
		return nil, errBadArg
	}

	m := reDurationText.FindStringSubmatch(s)
	if m == nil {
		return nil, fmt.Errorf("%w: cannot parse duration %q", errBadArg, s)
	}

	n, _ := strconv.ParseFloat(m[1], 64)

	return formatInterval(int64(n * float64(unitNanos(m[2])))), nil
}
