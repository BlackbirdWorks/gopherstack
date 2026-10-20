package bqsql

import (
	"database/sql/driver"
	"math"
	"strconv"
	"strings"
	"time"
)

func addCastUDFs(d map[string]udfDef) {
	d["cast"] = udfDef{n: argc3, fn: udfCast}
}

func udfCast(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	target, src := toStr(a[1]), toStr(a[2])
	k, ok := KindOf(target)

	if !ok {
		return nil, errBQ("unsupported cast target %s", target)
	}

	switch k {
	case KindInt:
		return castInt(a[0], src)
	case KindFloat:
		return toFloat(a[0])
	case KindBool:
		return castBool(a[0])
	case KindString:
		return castString(a[0], src)
	case KindBytes:
		return bytesOf(a[0]), nil
	case KindNumeric, KindBigNumeric:
		return castNumeric(a[0])
	case KindTimestamp, KindDate, KindDatetime, KindTime:
		return castTemporal(k, a[0])
	case KindJSON:
		return toStr(a[0]), nil
	case KindUnknown, KindGeography, KindStruct, KindArray:
	}

	return nil, errBQ("unsupported cast to %s", target)
}

func castInt(v driver.Value, src string) (driver.Value, error) {
	switch t := v.(type) {
	case int64:
		return t, nil
	case float64:
		if math.IsNaN(t) || math.IsInf(t, 0) || math.Abs(t) >= math.MaxInt64 {
			return nil, errBQ("Illegal conversion of non-finite floating point number to an integer: %v", t)
		}

		return int64(math.Round(t)), nil
	}

	s := strings.TrimSpace(toStr(v))
	if src == litString {
		n, err := strconv.ParseInt(s, 0, bitSize)
		if err != nil {
			return nil, errBQ("Bad int64 value: %s", s)
		}

		return n, nil
	}

	return toInt(v)
}

func castBool(v driver.Value) (driver.Value, error) {
	switch t := v.(type) {
	case int64:
		return boolVal(t != 0), nil
	case float64:
		return boolVal(t != 0), nil
	}

	switch strings.ToLower(strings.TrimSpace(toStr(v))) {
	case litTrueX, "t":
		return int64(1), nil
	case litFalseX, "f":
		return int64(0), nil
	}

	return nil, errBQ("Bad bool value: %s", toStr(v))
}

func castString(v driver.Value, src string) (driver.Value, error) {
	switch src {
	case litBool:
		n, _ := toInt(v)

		return strconv.FormatBool(n != 0), nil
	case litFloat64:
		f, _ := toFloat(v)

		return strings.TrimSuffix(FormatFloat(f), ".0"), nil
	case litNumeric, litBignumeric:
		f, _ := toFloat(v)

		return FormatNumeric(f), nil
	case litTimestamp:
		return udfStringOf([]driver.Value{v})
	case litDatetime, litTime:
		return trimFraction(toStr(v)), nil
	}

	return toStr(v), nil
}

func castNumeric(v driver.Value) (driver.Value, error) {
	if n, ok := v.(int64); ok {
		return float64(n), nil
	}

	f, err := toFloat(v)
	if err != nil {
		return nil, errBQ("Invalid NUMERIC value: %s", toStr(v))
	}

	return f, nil
}

func castTemporal(k Kind, v driver.Value) (driver.Value, error) {
	t, from, err := parseAny(v)
	if err != nil {
		return nil, errBQ("Invalid %s value: %s", k.RESTName(), toStr(v))
	}

	if k == KindTimestamp && (from == KindDate || from == KindDatetime) {
		t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
	}

	return FormatTemporal(k, t), nil
}
