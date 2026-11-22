package timestreamquery

import (
	"database/sql/driver"
	"fmt"
	"time"
)

const (
	unitDay         = "day"
	unitHour        = "hour"
	unitMinute      = "minute"
	unitSecond      = "second"
	unitMillisecond = "millisecond"
	unitNanosecond  = "nanosecond"
	layoutClock     = "15:04:05"
	layoutClockFrac = "15:04:05.999999999"
	kwHaving        = "having"
	kwLimit         = "limit"
	kwWhere         = "where"
	unitDayOfMonth  = "day_of_month"
	nsPerSecond     = int64(time.Second)
	nsPerDay        = int64(hoursInDay) * int64(time.Hour)
	clockFormat     = "15:04:05.000000000"
)

func parseClock(v driver.Value) (int64, bool) {
	s, ok := textOf(v)
	if !ok {
		return 0, false
	}

	for _, l := range []string{layoutClockFrac, layoutClock, "15:04"} {
		if t, err := time.Parse(l, s); err == nil {
			return int64(t.Hour())*int64(time.Hour) + int64(t.Minute())*int64(time.Minute) +
				int64(t.Second())*nsPerSecond + int64(t.Nanosecond()), true
		}
	}

	return 0, false
}

func formatClock(ns int64) string {
	return time.Unix(0, floorMod(ns, nsPerDay)).UTC().Format(clockFormat)
}

type clockPart struct {
	extract func(ns int64) int64
	name    string
}

func clockParts() []clockPart {
	return []clockPart{
		{name: unitHour, extract: func(ns int64) int64 { return ns % nsPerDay / int64(time.Hour) }},
		{name: unitMinute, extract: func(ns int64) int64 { return ns / int64(time.Minute) % minutesInHour }},
		{name: unitSecond, extract: func(ns int64) int64 { return ns / nsPerSecond % minutesInHour }},
		{name: unitMillisecond, extract: func(ns int64) int64 { return ns % nsPerSecond / int64(time.Millisecond) }},
		{name: unitNanosecond, extract: func(ns int64) int64 { return ns % nsPerSecond }},
	}
}

func timePartFunc(extract func(int64) int64) scalarImpl {
	return func(_ *fnCtx, args []driver.Value) (driver.Value, error) {
		if args[0] == nil {
			return nil, nil
		}

		ns, ok := parseClock(args[0])
		if !ok {
			return nil, errOperandType
		}

		return extract(ns), nil
	}
}

func intervalPartFunc(extract func(int64) int64) scalarImpl {
	return func(_ *fnCtx, args []driver.Value) (driver.Value, error) {
		if args[0] == nil {
			return nil, nil
		}

		ns, ok := intervalNanos(args[0])
		if !ok {
			return nil, errOperandType
		}

		return extract(ns), nil
	}
}

func clockFuncs() []scalarFunc {
	out := make([]scalarFunc, 0, 2*len(clockParts())+argsThree)

	for _, p := range clockParts() {
		out = append(out,
			raisingFn("ts_time_"+p.name, 1, timePartFunc(p.extract)),
			raisingFn("ts_iv_"+p.name, 1, intervalPartFunc(p.extract)),
		)
	}

	out = append(out,
		raisingFn("ts_iv_"+unitDay, 1, intervalPartFunc(func(ns int64) int64 { return ns / nsPerDay })),
		raisingFn("ts_time_add", argsThree, udfTimeAdd),
		raisingFn("ts_time_diff", argsThree, udfTimeDiff),
	)

	return out
}

func clockUnit(v driver.Value) (int64, error) {
	u, ok := textOf(v)
	if !ok {
		return 0, errOperandType
	}

	switch u {
	case unitMillisecond:
		return int64(time.Millisecond), nil
	case unitSecond:
		return nsPerSecond, nil
	case unitMinute:
		return int64(time.Minute), nil
	case unitHour:
		return int64(time.Hour), nil
	}

	return 0, evalError(fmt.Sprintf("'%s' is not a valid TIME field", u))
}

// udfTimeAdd is date_add(unit, n, time): the time of day wraps around midnight.
func udfTimeAdd(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	if args[0] == nil || args[1] == nil || args[argsTwo] == nil {
		return nil, nil //nolint:nilnil // SQL NULL
	}

	unit, err := clockUnit(args[0])
	if err != nil {
		return nil, err
	}

	n, okN := toInt64(args[1])
	base, okT := parseClock(args[argsTwo])

	if !okN || !okT {
		return nil, errOperandType
	}

	step, ok := mulExact(n%(nsPerDay/unit), unit)
	if !ok {
		return nil, evalError("time addition overflow")
	}

	return formatClock(base + step), nil
}

// udfTimeDiff is date_diff(unit, time, time): whole units from the first time to the second.
func udfTimeDiff(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	if args[0] == nil || args[1] == nil || args[argsTwo] == nil {
		return nil, nil //nolint:nilnil // SQL NULL
	}

	unit, err := clockUnit(args[0])
	if err != nil {
		return nil, err
	}

	a, okA := parseClock(args[1])
	b, okB := parseClock(args[argsTwo])

	if !okA || !okB {
		return nil, errOperandType
	}

	return (b - a) / unit, nil
}
