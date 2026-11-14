package athena

import (
	"database/sql/driver"
	"math"
	"math/bits"
	"strconv"
	"strings"
)

// numericOf reads a SQLite value as a number the way SQLite's arithmetic does.
func numericOf(v driver.Value) (float64, bool) {
	n, isInt, ok := sqliteNumber(v)
	if !ok {
		return 0, false
	}

	if isInt {
		return float64(toI64(n)), true
	}

	return math.Float64frombits(n), true
}

// sqliteNumber returns the int64 bits (isInt) or float64 bits of v.
func sqliteNumber(v driver.Value) (uint64, bool, bool) {
	switch t := v.(type) {
	case int64:
		return toU64(t), true, true
	case float64:
		return math.Float64bits(t), false, true
	case bool:
		if t {
			return 1, true, true
		}

		return 0, true, true
	case string:
		return textNumber(t)
	case []byte:
		return textNumber(string(t))
	default:
		return 0, false, false
	}
}

func textNumber(s string) (uint64, bool, bool) {
	s = strings.TrimSpace(s)
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return toU64(n), true, true
	}

	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return math.Float64bits(f), false, true
	}

	return 0, true, true
}

func addInt(a, b int64) (int64, bool) {
	r := a + b

	return r, (a >= 0) != (b >= 0) || (r >= 0) == (a >= 0)
}

func mulInt(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}

	hi, lo := bits.Mul64(toU64(abs64(a)), toU64(abs64(b)))
	if hi != 0 || lo > math.MaxInt64 {
		return 0, false
	}

	r := int64(lo)
	if (a < 0) != (b < 0) {
		r = -r
	}

	return r, true
}

func abs64(n int64) int64 {
	if n < 0 {
		return -n
	}

	return n
}

// numericOp is SQLite's arithmetic for + - * /, used when neither operand is an interval.
func numericOp(op string, a, b driver.Value) (driver.Value, error) {
	an, aInt, aok := sqliteNumber(a)
	bn, bInt, bok := sqliteNumber(b)

	if !aok || !bok {
		return sqlNull(), nil
	}

	if aInt && bInt {
		if r, ok := intOp(op, toI64(an), toI64(bn)); ok {
			return r, nil
		}
	}

	af, _ := numericOf(a)
	bf, _ := numericOf(b)

	switch op {
	case "+":
		return af + bf, nil
	case "-":
		return af - bf, nil
	case "*":
		return af * bf, nil
	default:
		if bf == 0 {
			return sqlNull(), nil
		}

		return af / bf, nil
	}
}

func intOp(op string, a, b int64) (driver.Value, bool) {
	switch op {
	case "+":
		r, ok := addInt(a, b)

		return r, ok
	case "-":
		if b == math.MinInt64 {
			return nil, false
		}

		r, ok := addInt(a, -b)

		return r, ok
	case "*":
		r, ok := mulInt(a, b)

		return r, ok
	default:
		if b == 0 {
			return nil, true
		}

		if a == math.MinInt64 && b == -1 {
			return nil, false
		}

		return a / b, true
	}
}
