package timestreamquery

import (
	"database/sql/driver"
	"errors"
	"math"
	"slices"
	"sync"
	"time"

	sqlitedriver "modernc.org/sqlite"
)

//nolint:gochecknoglobals // the sqlite driver's function registry is process-wide
var registerFuncsOnce sync.Once

var errBadArg = errors.New("invalid argument")

const (
	argsTwo   = 2
	argsThree = 3
)

type fnCtx = sqlitedriver.FunctionContext

func tsParseLayouts() []string {
	return []string{
		"2006-01-02 15:04:05.999999999", "2006-01-02T15:04:05.999999999", "2006-01-02T15:04:05Z07:00", "2006-01-02",
	}
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

	for _, l := range tsParseLayouts() {
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

		return float64(int64(f) - floorMod(int64(f), step)), nil
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
			return nil, nil
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
			return nil, nil
		}

		if ns, isInterval := intervalNanos(args[0]); isInterval {
			return per(time.Unix(0, ns).UTC()) - per(time.Unix(0, 0).UTC()), nil
		}

		t, ok := parseTSValue(args[0])
		if !ok {
			return nil, errBadArg
		}

		return per(t), nil
	}
}

type scalarImpl = func(*sqlitedriver.FunctionContext, []driver.Value) (driver.Value, error)

type scalarFunc struct {
	impl  scalarImpl
	name  string
	nargs int32
	// volatile keeps SQLite from evaluating a constant call ahead of the guard around it.
	volatile bool
}

func scalarFn(name string, nargs int32, impl scalarImpl) scalarFunc {
	return scalarFunc{name: name, nargs: nargs, impl: impl}
}

// raisingFn is scalarFn for a function that can fail the query, so it is evaluated only where the query reaches it.
func raisingFn(name string, nargs int32, impl scalarImpl) scalarFunc {
	return scalarFunc{name: name, nargs: nargs, impl: impl, volatile: true}
}

func registerScalar(f scalarFunc) {
	if f.volatile {
		sqlitedriver.MustRegisterScalarFunction(engineFunc(f.name), f.nargs, f.impl)

		return
	}

	sqlitedriver.MustRegisterDeterministicScalarFunction(engineFunc(f.name), f.nargs, f.impl)
}

// tsScalarFuncs lists the scalar functions this package registers with the engine.
func tsScalarFuncs() []scalarFunc {
	isoYear := toUnit(func(t time.Time) int64 {
		y, _ := t.ISOWeek()

		return int64(y)
	})

	return slices.Concat([]scalarFunc{
		scalarFn("ts_bin", argsTwo, udfBin),
		scalarFn("ts_ago", argsTwo, udfAgo),
		scalarFn("ts_tsdiff", argsTwo, udfTimestampDiff),
		scalarFn("from_milliseconds", 1, unitFunc(time.UnixMilli)),
		scalarFn("from_nanoseconds", 1, unitFunc(func(n int64) time.Time { return time.Unix(0, n) })),
		scalarFn("to_milliseconds", 1, toUnit(func(t time.Time) int64 { return t.UnixMilli() })),
		scalarFn("to_nanoseconds", 1, toUnit(func(t time.Time) int64 { return t.UnixNano() })),
		scalarFn("year_of_week", 1, isoYear),
		scalarFn("yow", 1, isoYear),
		scalarFn("nanosecond", 1, toUnit(func(t time.Time) int64 { return int64(t.Nanosecond()) })),
		scalarFn("parse_duration", 1, udfParseDuration),
		scalarFn("interpolate_linear", argsTwo, udfInterpolate(linearAt)),
		scalarFn("interpolate_spline_cubic", argsTwo, udfInterpolate(splineAt)),
		scalarFn("interpolate_locf", argsTwo, udfInterpolate(locfAt)),
		scalarFn("interpolate_fill", argsThree, udfInterpolateFill),
		scalarFn("derivative_linear", argsTwo, udfDerivative(false)),
		scalarFn("non_negative_derivative_linear", argsTwo, udfDerivative(true)),
		raisingFn("integral_trapezoidal", -1, withArgRange(1, argsTwo, udfIntegral)),
		scalarFn("correlate_pearson", argsTwo, udfCorrelate(false)),
		scalarFn("correlate_spearman", argsTwo, udfCorrelate(true)),
		scalarFn("sequence", -1, withArgRange(argsTwo, argsThree, udfSequence)),
		scalarFn("ts_series_json", 1, udfSeriesJSON),
	}, tsTextFuncs(), tsArrayFuncs(), clockFuncs())
}

// withArgRange guards a variadic registration against calls with fewer than lo or more than hi arguments.
func withArgRange(lo, hi int, impl scalarImpl) scalarImpl {
	return func(c *fnCtx, args []driver.Value) (driver.Value, error) {
		if len(args) < lo || len(args) > hi {
			return nil, evalError("wrong number of arguments")
		}

		return impl(c, args)
	}
}

// withMaxArgs guards a variadic registration against calls with fewer than two or more than hi arguments.
func withMaxArgs(hi int, impl scalarImpl) scalarImpl { return withArgRange(argsTwo, hi, impl) }

func tsTextFuncs() []scalarFunc {
	return []scalarFunc{
		raisingFn("from_base", argsTwo, udfFromBase),
		raisingFn("to_base", argsTwo, udfToBase),
		raisingFn("bitwise_not", 1, udfBitwiseNot),
		raisingFn("bit_count", argsTwo, udfBitCount),
		raisingFn("hamming_distance", argsTwo, udfHamming),
		raisingFn("strrpos", -1, withMaxArgs(argsThree, positionOf(true))),
		raisingFn("ts_strpos", -1, withMaxArgs(argsThree, positionOf(false))),
		raisingFn("ts_replace", -1, withMaxArgs(argsThree, udfReplace)),
		scalarFn("infinity", 0, udfInfinity),
		scalarFn("nan", 0, func(*fnCtx, []driver.Value) (driver.Value, error) { return nanText, nil }),
	}
}

func tsArrayFuncs() []scalarFunc {
	return []scalarFunc{
		raisingFn("array_intersect", argsTwo, setOp(func(inY bool) bool { return inY }, false)),
		raisingFn("array_union", argsTwo, setOp(func(bool) bool { return true }, true)),
		raisingFn("array_except", argsTwo, setOp(func(inY bool) bool { return !inY }, false)),
		raisingFn("arrays_overlap", argsTwo, udfArraysOverlap),
		raisingFn("repeat", argsTwo, udfRepeat),
		raisingFn("zip", -1, withMaxArgs(maxZipArrays, udfZip)),
		raisingFn("ts_reverse_array", 1, udfReverseArray),
		raisingFn("ts_regexp_groups", argsTwo, udfRegexpGroups),
		raisingFn("ts_regexp_splice", argsThree, udfRegexpSplice),
		raisingFn("ts_concat_array", -1, withArgRange(1, math.MaxInt, udfConcatArrays)),
	}
}

// udfTimestampDiff implements timestamp - timestamp as an INTERVAL DAY TO SECOND.
func udfTimestampDiff(_ *sqlitedriver.FunctionContext, args []driver.Value) (driver.Value, error) {
	if args[0] == nil || args[1] == nil {
		return nil, nil //nolint:nilnil // SQL NULL
	}

	a, okA := parseTSValue(args[0])
	b, okB := parseTSValue(args[1])

	if !okA || !okB {
		return nil, errBadArg
	}

	return formatInterval(a.Sub(b).Nanoseconds()), nil
}

//nolint:gochecknoinits // the driver reads its registry unlocked on Open; lazy registration crashes
func init() { registerTimestreamFuncs() }

func registerTimestreamFuncs() {
	registerFuncsOnce.Do(func() {
		registerUnnestModule()
		registerArithFuncs()
		registerCastFuncs()
		registerSetAggs()
		sqlitedriver.MustRegisterScalarFunction(engineFunc("shuffle"), 1, udfShuffle)

		for _, f := range tsScalarFuncs() {
			registerScalar(f)
		}

		sqlitedriver.MustRegisterFunction(engineFunc("create_time_series"), &sqlitedriver.FunctionImpl{
			NArgs: argsTwo,
			MakeAggregate: func(_ fnCtx) (sqlitedriver.AggregateFunction, error) {
				return &seriesAgg{}, nil
			},
		})
	})
}
