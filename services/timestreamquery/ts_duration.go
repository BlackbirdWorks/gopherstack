package timestreamquery

import (
	"database/sql/driver"
	"fmt"
	"math"
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

// intervalNanos reads an interval day to second held as nanoseconds, "d hh:mm:ss.nnnnnnnnn" text or an interval blob.
func intervalNanos(v driver.Value) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case string:
		return parseIntervalText(x)
	case []byte:
		if kind, n, isIv := decodeIntervalBlob(x); isIv {
			return n * msToNs, kind == 'D'
		}

		return parseIntervalText(string(x))
	}

	return 0, false
}

func intervalPair(args []driver.Value) (int64, int64, bool, error) {
	if args[0] == nil || args[1] == nil {
		return 0, 0, true, nil
	}

	a, okA := intervalNanos(args[0])
	b, okB := intervalNanos(args[1])

	if !okA || !okB {
		return 0, 0, false, errOperandType
	}

	return a, b, false, nil
}

func udfIntervalAdd(sign int64) scalarImpl {
	return func(_ *fnCtx, args []driver.Value) (driver.Value, error) {
		a, b, isNull, err := intervalPair(args)
		if err != nil || isNull {
			return nil, err
		}

		if sign < 0 {
			if r, ok := subExact(a, b); ok {
				return r, nil
			}

			return nil, evalError(fmt.Sprintf("interval day to second subtraction overflow: %d - %d", a, b))
		}

		if r, ok := addExact(a, b); ok {
			return r, nil
		}

		return nil, evalError(fmt.Sprintf("interval day to second addition overflow: %d + %d", a, b))
	}
}

func udfIntervalNegate(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	if args[0] == nil {
		return nil, nil //nolint:nilnil // SQL NULL
	}

	n, ok := intervalNanos(args[0])
	if !ok {
		return nil, errOperandType
	}

	if n == math.MinInt64 {
		return nil, evalError("interval day to second negation overflow")
	}

	return -n, nil
}

// udfIntervalScale is interval * factor (divide=false) or interval / factor, rounded to a whole nanosecond.
func udfIntervalScale(divide, factorFirst bool) scalarImpl {
	return func(_ *fnCtx, args []driver.Value) (driver.Value, error) {
		if args[0] == nil || args[1] == nil {
			return nil, nil
		}

		ivArg, factorArg := args[0], args[1]
		if factorFirst {
			ivArg, factorArg = factorArg, ivArg
		}

		n, okN := intervalNanos(ivArg)
		f, okF := toFloat64(factorArg)

		if !okN || !okF || math.IsNaN(f) {
			return nil, errOperandType
		}

		if divide && f == 0 {
			return nil, evalError("Division by zero")
		}

		r := float64(n) * f
		if divide {
			r = float64(n) / f
		}

		if math.IsInf(r, 0) || math.Abs(r) >= twoPow63 {
			return nil, evalError("interval day to second out of range")
		}

		return int64(math.Round(r)), nil
	}
}

// udfTimestampShift is timestamp + interval (sign 1), timestamp - interval (sign -1), or interval + timestamp.
func udfTimestampShift(sign int64, intervalFirst bool) scalarImpl {
	return func(_ *fnCtx, args []driver.Value) (driver.Value, error) {
		if args[0] == nil || args[1] == nil {
			return nil, nil
		}

		tsArg, ivArg := args[0], args[1]
		if intervalFirst {
			tsArg, ivArg = ivArg, tsArg
		}

		t, okT := parseTSValue(tsArg)
		n, okN := intervalNanos(ivArg)

		if !okT || !okN {
			return nil, errOperandType
		}

		shifted := t.UnixNano() + sign*n
		if (sign*n > 0 && shifted < t.UnixNano()) || (sign*n < 0 && shifted > t.UnixNano()) {
			return nil, evalError("timestamp out of range")
		}

		return formatTS(time.Unix(0, shifted)), nil
	}
}
