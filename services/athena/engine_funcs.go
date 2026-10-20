package athena

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	sqlitedriver "modernc.org/sqlite"
)

const (
	tsLayout       = "2006-01-02 15:04:05.000"
	dateLayout     = "2006-01-02"
	hoursPerDay    = 24
	daysPerWeek    = 7
	monthsPerQuart = 3
)

//nolint:gochecknoglobals // the driver's function registry is process-wide
var registerFuncsOnce sync.Once

var errBadUnit = errors.New("invalid unit")

func sqlNull() driver.Value { return nil }

// registerEngineFuncs installs the athena_* scalar functions the SQL
// translator targets; the driver registry is process-wide.
func registerEngineFuncs() {
	registerFuncsOnce.Do(func() {
		reg := sqlitedriver.MustRegisterDeterministicScalarFunction
		reg("athena_date_trunc", argsTwo, udfDateTrunc)
		reg("athena_date_add", argsThree, udfDateAdd)
		reg("athena_date_diff", argsThree, udfDateDiff)
		reg("athena_year", 1, datePart(func(t time.Time) int64 { return int64(t.Year()) }))
		reg("athena_month", 1, datePart(func(t time.Time) int64 { return int64(t.Month()) }))
		reg("athena_day", 1, datePart(func(t time.Time) int64 { return int64(t.Day()) }))
		reg("athena_hour", 1, datePart(func(t time.Time) int64 { return int64(t.Hour()) }))
		reg("athena_regexp_like", argsTwo, udfRegexpLike)
		registerExtraFuncs()
		registerComplexFuncs()
		registerAggregates()
		registerUnnestModule()
	})
}

type fnCtx = sqlitedriver.FunctionContext

func tsFormats() []string {
	return []string{
		"2006-01-02 15:04:05.999999999", "2006-01-02T15:04:05.999999999", "2006-01-02T15:04:05Z07:00",
		"2006-01-02 15:04:05", "2006-01-02",
	}
}

func parseTS(v driver.Value) (time.Time, bool) {
	s, ok := asString(v)
	if !ok {
		return time.Time{}, false
	}

	s = strings.TrimSpace(s)

	for _, f := range tsFormats() {
		if t, err := time.Parse(f, s); err == nil {
			return t.UTC(), true
		}
	}

	return time.Time{}, false
}

func asString(v driver.Value) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case []byte:
		return string(t), true
	default:
		return "", false
	}
}

func isDateOnly(v driver.Value) bool {
	s, _ := asString(v)

	return len(strings.TrimSpace(s)) == len(dateLayout)
}

func formatLike(t time.Time, dateOnly bool) string {
	if dateOnly {
		return t.Format(dateLayout)
	}

	return t.Format(tsLayout)
}

func truncateTo(t time.Time, unit string) (time.Time, bool) {
	y, m, d := t.Date()

	switch unit {
	case unitSecond:
		return t.Truncate(time.Second), true
	case unitMinute:
		return t.Truncate(time.Minute), true
	case unitHour:
		return t.Truncate(time.Hour), true
	case unitDay:
		return time.Date(y, m, d, 0, 0, 0, 0, time.UTC), true
	case unitWeek:
		off := (int(t.Weekday()) + daysPerWeek - 1) % daysPerWeek

		return time.Date(y, m, d-off, 0, 0, 0, 0, time.UTC), true
	case unitMonth:
		return time.Date(y, m, 1, 0, 0, 0, 0, time.UTC), true
	case unitQuarter:
		return time.Date(y, time.Month((int(m)-1)/monthsPerQuart*monthsPerQuart+1), 1, 0, 0, 0, 0, time.UTC), true
	case unitYear:
		return time.Date(y, 1, 1, 0, 0, 0, 0, time.UTC), true
	default:
		return time.Time{}, false
	}
}

func unitArg(v driver.Value) string {
	s, _ := asString(v)

	return strings.ToLower(strings.TrimSpace(s))
}

func udfDateTrunc(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	t, ok := parseTS(args[1])
	if !ok {
		return sqlNull(), nil
	}

	r, ok := truncateTo(t, unitArg(args[0]))
	if !ok {
		return nil, fmt.Errorf("%w: date_trunc unit %v", errBadUnit, args[0])
	}

	return formatLike(r, isDateOnly(args[1])), nil
}

func toInt(v driver.Value) (int64, bool) {
	switch t := v.(type) {
	case int64:
		return t, true
	case float64:
		return int64(t), true
	case string:
		n, err := strconv.ParseInt(t, 10, 64)

		return n, err == nil
	default:
		return 0, false
	}
}

func udfDateAdd(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	n, nok := toInt(args[1])
	t, ok := parseTS(args[2])

	if !ok || !nok || n > math.MaxInt32 || n < math.MinInt32 {
		return sqlNull(), nil
	}

	switch unitArg(args[0]) {
	case unitSecond:
		t = t.Add(time.Duration(n) * time.Second)
	case unitMinute:
		t = t.Add(time.Duration(n) * time.Minute)
	case unitHour:
		t = t.Add(time.Duration(n) * time.Hour)
	case unitDay:
		t = t.AddDate(0, 0, int(n))
	case unitWeek:
		t = t.AddDate(0, 0, int(n)*daysPerWeek)
	case unitMonth:
		t = addMonthsClamped(t, int(n))
	case unitQuarter:
		t = addMonthsClamped(t, int(n)*monthsPerQuart)
	case unitYear:
		t = addMonthsClamped(t, int(n)*monthsPerYear)
	default:
		return nil, fmt.Errorf("%w: date_add unit %v", errBadUnit, args[0])
	}

	return formatLike(t, isDateOnly(args[2])), nil
}

// addMonthsClamped adds n months, clamping to the last day of a shorter month as Trino does.
func addMonthsClamped(t time.Time, n int) time.Time {
	y, m, d := t.Date()
	first := time.Date(y, m+time.Month(n), 1, t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
	last := time.Date(first.Year(), first.Month()+1, 0, 0, 0, 0, 0, time.UTC).Day()

	return first.AddDate(0, 0, min(d, last)-1)
}

func udfDateDiff(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	a, aok := parseTS(args[1])
	b, bok := parseTS(args[2])

	if !aok || !bok {
		return sqlNull(), nil
	}

	d := b.Sub(a)

	switch unitArg(args[0]) {
	case unitSecond:
		return int64(d / time.Second), nil
	case unitMinute:
		return int64(d / time.Minute), nil
	case unitHour:
		return int64(d / time.Hour), nil
	case unitDay:
		return int64(d / (hoursPerDay * time.Hour)), nil
	case unitWeek:
		return int64(d / (daysPerWeek * hoursPerDay * time.Hour)), nil
	case unitMonth:
		return monthsBetween(a, b), nil
	case unitQuarter:
		return monthsBetween(a, b) / monthsPerQuart, nil
	case unitYear:
		return monthsBetween(a, b) / monthsPerYear, nil
	default:
		return nil, fmt.Errorf("%w: date_diff unit %v", errBadUnit, args[0])
	}
}

func monthsBetween(a, b time.Time) int64 {
	m := int64(b.Year()-a.Year())*monthsPerYear + int64(b.Month()) - int64(a.Month())
	if m > 0 && b.Day() < a.Day() {
		m--
	} else if m < 0 && b.Day() > a.Day() {
		m++
	}

	return m
}

func datePart(f func(time.Time) int64) func(*fnCtx, []driver.Value) (driver.Value, error) {
	return func(_ *fnCtx, args []driver.Value) (driver.Value, error) {
		t, ok := parseTS(args[0])
		if !ok {
			return sqlNull(), nil
		}

		return f(t), nil
	}
}

func udfRegexpLike(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, sok := asString(args[0])
	p, pok := asString(args[1])

	if !sok || !pok {
		return sqlNull(), nil
	}

	re, err := regexp.Compile(p)
	if err != nil {
		return nil, fmt.Errorf("invalid regular expression: %w", err)
	}

	if re.MatchString(s) {
		return int64(1), nil
	}

	return int64(0), nil
}
