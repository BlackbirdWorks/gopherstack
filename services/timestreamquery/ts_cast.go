package timestreamquery

import (
	"database/sql/driver"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	sqlitedriver "modernc.org/sqlite"
)

func castFailure(try bool, format string, args ...any) (driver.Value, error) {
	if try {
		return nil, nil //nolint:nilnil // TRY_CAST maps failures to SQL NULL
	}

	return nil, evalError(fmt.Sprintf(format, args...))
}

func castSources(dst tsKind) []tsKind {
	switch dst {
	case tyInteger, tyBigint:
		return []tsKind{tyInteger, tyBigint, tyDouble, tyVarchar, tyBoolean}
	case tyDouble:
		return []tsKind{tyInteger, tyBigint, tyVarchar}
	case tyBoolean:
		return []tsKind{tyInteger, tyBigint, tyDouble, tyVarchar}
	case tyVarchar:
		return []tsKind{tyInteger, tyBigint, tyDouble, tyBoolean}
	case tyTimestamp, tyDate, tyTime:
		return []tsKind{tyVarchar}
	default:
		return nil
	}
}

func castSupported(src, dst tsKind) bool {
	return src != dst && slices.Contains(castSources(dst), src)
}

// udfCast implements CAST/TRY_CAST for the conversions that can fail; args are value, source, target, try.
func udfCast(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	if args[0] == nil {
		return nil, nil //nolint:nilnil // SQL NULL
	}

	src, _ := textOf(args[1])
	dst, _ := textOf(args[2])
	try, _ := toInt64(args[3])

	return castValue(args[0], src, dst, try != 0)
}

func castValue(v driver.Value, src, dst string, try bool) (driver.Value, error) {
	switch dst {
	case kwInteger:
		return castToInt(v, src, bits32, try)
	case typBigint:
		return castToInt(v, src, bits64, try)
	case typDouble:
		return castToDouble(v, src, try)
	case typBoolean:
		return castToBool(v, src, try)
	case typVarchar:
		return castToVarchar(v, src)
	default:
		return castToTemporal(v, dst, try)
	}
}

func castToInt(v driver.Value, src string, bits int, try bool) (driver.Value, error) {
	name := bitsName(bits)

	var n int64

	switch src {
	case typDouble:
		f, ok := toFloat64(v)
		if !ok {
			return nil, errOperandType
		}

		if math.IsNaN(f) {
			return castFailure(try, "Unable to cast NaN to %s", name)
		}

		r := math.Round(f)
		if r < -twoPow63 || r >= twoPow63 || !fitsBits(int64(r), bits) {
			return castFailure(try, "Out of range for %s: %s", name, javaDouble(f))
		}

		n = int64(r)
	case typVarchar:
		s, _ := textOf(v)

		p, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
		if err != nil || !fitsBits(p, bits) {
			return castFailure(try, "Cannot cast '%s' to %s", s, strings.ToUpper(name))
		}

		n = p
	default:
		i, ok := toInt64(v)
		if !ok {
			return nil, errOperandType
		}

		if !fitsBits(i, bits) {
			return castFailure(try, "Out of range for %s: %d", name, i)
		}

		n = i
	}

	return n, nil
}

func parseJavaDouble(s string) (float64, bool) {
	t := strings.TrimSpace(s)

	switch t {
	case nanText:
		return math.NaN(), true
	case "Infinity", "+Infinity":
		return math.Inf(1), true
	case "-Infinity":
		return math.Inf(-1), true
	}

	if t == "" || strings.ContainsAny(t, "_nN") {
		return 0, false
	}

	f, err := strconv.ParseFloat(t, 64)
	if err != nil || math.IsInf(f, 0) {
		return 0, false
	}

	return f, true
}

func castToDouble(v driver.Value, src string, try bool) (driver.Value, error) {
	if src == typVarchar {
		s, _ := textOf(v)

		f, ok := parseJavaDouble(s)
		if !ok {
			return castFailure(try, "Cannot cast '%s' to DOUBLE", s)
		}

		if math.IsNaN(f) {
			return nanText, nil
		}

		return f, nil
	}

	f, ok := toFloat64(v)
	if !ok {
		return nil, errOperandType
	}

	return f, nil
}

func castToBool(v driver.Value, src string, try bool) (driver.Value, error) {
	if src == typVarchar {
		s, _ := textOf(v)

		switch strings.ToLower(s) {
		case "true", "t", "1":
			return int64(1), nil
		case "false", "f", "0":
			return int64(0), nil
		}

		return castFailure(try, "Cannot cast '%s' to BOOLEAN", s)
	}

	f, ok := toFloat64(v)
	if !ok {
		return nil, errOperandType
	}

	if f != 0 {
		return int64(1), nil
	}

	return int64(0), nil
}

func castToVarchar(v driver.Value, src string) (driver.Value, error) {
	switch src {
	case typBoolean:
		f, ok := toFloat64(v)
		if !ok {
			if s, isText := textOf(v); isText {
				return strconv.FormatBool(strings.EqualFold(s, "true") || s == "1"), nil
			}

			return nil, errOperandType
		}

		return strconv.FormatBool(f != 0), nil
	case typDouble:
		f, ok := toFloat64(v)
		if !ok {
			return nil, errOperandType
		}

		return javaDouble(f), nil
	default:
		i, ok := toInt64(v)
		if !ok {
			return nil, errOperandType
		}

		return strconv.FormatInt(i, 10), nil
	}
}

// parseCastTimestamp accepts the forms the engine's own datetime parser did before casts were checked.
func parseCastTimestamp(s string) (time.Time, bool) {
	if t, ok := parseTSValue(s); ok {
		return t, true
	}

	for _, l := range []string{
		"2006-01-02 15:04:05Z07:00", "2006-01-02 15:04Z07:00", "2006-01-02T15:04Z07:00",
		"2006-01-02 15:04", "2006-01-02T15:04",
	} {
		if t, err := time.Parse(l, s); err == nil {
			return t.UTC(), true
		}
	}

	return time.Time{}, false
}

func castToTemporal(v driver.Value, dst string, try bool) (driver.Value, error) {
	s, _ := textOf(v)

	if dst == colTime {
		for _, l := range []string{"15:04:05.999999999", "15:04:05"} {
			if t, err := time.Parse(l, strings.TrimSpace(s)); err == nil {
				return t.Format("15:04:05.000000000"), nil
			}
		}

		return castFailure(try, "Value cannot be cast to time: %s", s)
	}

	t, ok := parseCastTimestamp(strings.TrimSpace(s))
	if !ok {
		return castFailure(try, "Value cannot be cast to %s: %s", dst, s)
	}

	if dst == kwDate {
		return t.Format(time.DateOnly), nil
	}

	return formatTS(t), nil
}

func registerCastFuncs() {
	sqlitedriver.MustRegisterDeterministicScalarFunction("ts_cast", argsFour, udfCast)
}
