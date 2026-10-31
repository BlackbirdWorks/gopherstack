package timestreamquery

import (
	"database/sql/driver"
	"errors"
	"sync"
	"time"

	sqlitedriver "modernc.org/sqlite"
)

//nolint:gochecknoglobals // the sqlite driver's function registry is process-wide
var registerFuncsOnce sync.Once

var errBadArg = errors.New("invalid argument")

const argsTwo = 2

var tsParseLayouts = []string{
	"2006-01-02 15:04:05.999999999", "2006-01-02T15:04:05.999999999", "2006-01-02T15:04:05Z07:00", "2006-01-02",
}

func parseTSValue(v driver.Value) (time.Time, bool) {
	s, ok := v.(string)
	if !ok {
		if b, isBytes := v.([]byte); isBytes {
			s, ok = string(b), true
		}
	}

	if !ok {
		return time.Time{}, false
	}

	for _, l := range tsParseLayouts {
		if t, err := time.Parse(l, s); err == nil {
			return t.UTC(), true
		}
	}

	return time.Time{}, false
}

func intArg(v driver.Value) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case float64:
		return int64(n), true
	default:
		return 0, false
	}
}

// udfBin implements bin(x, interval): x rounded down to a multiple of the interval (nanoseconds for timestamps).
func udfBin(_ *sqlitedriver.FunctionContext, args []driver.Value) (driver.Value, error) {
	if args[0] == nil || args[1] == nil {
		return nil, nil //nolint:nilnil // SQL NULL
	}

	step, ok := intArg(args[1])
	if !ok || step <= 0 {
		return nil, errBadArg
	}

	if t, isTS := parseTSValue(args[0]); isTS {
		ns := t.UnixNano()

		return formatTS(time.Unix(0, ns-floorMod(ns, step))), nil
	}

	x, isNum := args[0].(int64)
	if !isNum {
		f, isFloat := args[0].(float64)
		if !isFloat {
			return nil, errBadArg
		}

		return float64(int64(f)-floorMod(int64(f), step)), nil
	}

	return x - floorMod(x, step), nil
}

func floorMod(a, m int64) int64 {
	r := a % m
	if r < 0 {
		r += m
	}

	return r
}

// udfAgo implements ago(interval) against the query's fixed now (first argument).
func udfAgo(_ *sqlitedriver.FunctionContext, args []driver.Value) (driver.Value, error) {
	now, ok := parseTSValue(args[0])
	d, isInt := intArg(args[1])

	if !ok || !isInt {
		return nil, errBadArg
	}

	return formatTS(now.Add(-time.Duration(d))), nil
}

func unitFunc(toTime func(int64) time.Time) func(*sqlitedriver.FunctionContext, []driver.Value) (driver.Value, error) {
	return func(_ *sqlitedriver.FunctionContext, args []driver.Value) (driver.Value, error) {
		if args[0] == nil {
			return nil, nil //nolint:nilnil // SQL NULL
		}

		n, ok := intArg(args[0])
		if !ok {
			return nil, errBadArg
		}

		return formatTS(toTime(n)), nil
	}
}

func toUnit(per func(time.Time) int64) func(*sqlitedriver.FunctionContext, []driver.Value) (driver.Value, error) {
	return func(_ *sqlitedriver.FunctionContext, args []driver.Value) (driver.Value, error) {
		if args[0] == nil {
			return nil, nil //nolint:nilnil // SQL NULL
		}

		t, ok := parseTSValue(args[0])
		if !ok {
			return nil, errBadArg
		}

		return per(t), nil
	}
}

func registerTimestreamFuncs() {
	registerFuncsOnce.Do(func() {
		reg := sqlitedriver.MustRegisterDeterministicScalarFunction
		reg("ts_bin", argsTwo, udfBin)
		reg("ts_ago", argsTwo, udfAgo)
		reg("from_milliseconds", 1, unitFunc(time.UnixMilli))
		reg("from_nanoseconds", 1, unitFunc(func(n int64) time.Time { return time.Unix(0, n) }))
		reg("to_milliseconds", 1, toUnit(func(t time.Time) int64 { return t.UnixMilli() }))
		reg("to_nanoseconds", 1, toUnit(func(t time.Time) int64 { return t.UnixNano() }))
	})
}
