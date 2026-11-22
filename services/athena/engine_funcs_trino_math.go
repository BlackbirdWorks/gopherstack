package athena

import (
	"database/sql/driver"
	"math"
	"math/bits"
	"strconv"
	"time"
)

const (
	minRadix     = 2
	maxRadix     = 36
	minBitCount  = 2
	maxBitCount  = 64
	argsFour     = 4
	int64Bits    = 64
	hammingError = "The input strings to hamming_distance function must have the same length"
)

func errUnexpected(fn string) error {
	return errArg("Unexpected parameters for function %s", fn)
}

// bigintArg accepts an integer value; SQLite hands whole doubles back for some expressions.
func bigintArg(v driver.Value) (int64, bool) {
	switch t := v.(type) {
	case int64:
		return t, true
	case float64:
		return int64(t), t == math.Trunc(t) && math.Abs(t) < 1<<53
	default:
		return 0, false
	}
}

func radixArg(v driver.Value) (int, error) {
	r, ok := bigintArg(v)
	if !ok {
		return 0, errUnexpected("radix")
	}

	if r < minRadix || r > maxRadix {
		return 0, errArg("Radix must be between 2 and 36")
	}

	return int(r), nil
}

func udfFromBase(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	if args[0] == nil || args[1] == nil {
		return sqlNull(), nil
	}

	s, ok := asString(args[0])
	if !ok {
		return nil, errUnexpected("from_base")
	}

	radix, err := radixArg(args[1])
	if err != nil {
		return nil, err
	}

	n, perr := strconv.ParseInt(s, radix, int64Bits)
	if perr != nil {
		return nil, errArg("Not a valid base-%d number: %s", radix, s)
	}

	return n, nil
}

func udfToBase(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	if args[0] == nil || args[1] == nil {
		return sqlNull(), nil
	}

	n, ok := bigintArg(args[0])
	if !ok {
		return nil, errUnexpected("to_base")
	}

	radix, err := radixArg(args[1])
	if err != nil {
		return nil, err
	}

	return strconv.FormatInt(n, radix), nil
}

func udfBitwiseNot(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	if args[0] == nil {
		return sqlNull(), nil
	}

	n, ok := bigintArg(args[0])
	if !ok {
		return nil, errUnexpected("bitwise_not")
	}

	return ^n, nil
}

func udfBitCount(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	if args[0] == nil || args[1] == nil {
		return sqlNull(), nil
	}

	n, nok := bigintArg(args[0])
	width, wok := bigintArg(args[1])

	if !nok || !wok {
		return nil, errUnexpected("bit_count")
	}

	if width < minBitCount || width > maxBitCount {
		return nil, errArg("Bits specified in bit_count must be between 2 and 64, got %d", width)
	}

	mask := ^uint64(0)

	if width < maxBitCount {
		limit := int64(1) << (width - 1)
		if n < -limit || n >= limit {
			return nil, errArg(
				"Number must be representable with the bits specified. %d can not be represented with %d bits",
				n,
				width,
			)
		}

		mask = uint64(1)<<width - 1
	}

	return int64(bits.OnesCount64(uint64(n) & mask)), nil //nolint:gosec // two's-complement bit pattern
}

func shiftFunc(name string, shift func(v int64, by uint) int64) scalarFn {
	return func(_ *fnCtx, args []driver.Value) (driver.Value, error) {
		if args[0] == nil || args[1] == nil {
			return sqlNull(), nil
		}

		v, vok := bigintArg(args[0])
		by, bok := bigintArg(args[1])

		if !vok || !bok {
			return nil, errUnexpected(name)
		}

		if by < 0 {
			return nil, errArg("Shift must be non-negative")
		}

		return shift(v, uint(min(by, int64Bits))), nil
	}
}

func shiftLeft(v int64, by uint) int64 {
	if by >= int64Bits {
		return 0
	}

	return v << by
}

func shiftRight(v int64, by uint) int64 {
	if by >= int64Bits {
		return 0
	}

	return int64(uint64(v) >> by) //nolint:gosec // logical shift on the bit pattern
}

func shiftRightArithmetic(v int64, by uint) int64 {
	if by >= int64Bits {
		by = int64Bits - 1
	}

	return v >> by
}

func udfHammingDistance(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	a, aok := asString(args[0])
	b, bok := asString(args[1])

	if !aok || !bok {
		if args[0] == nil || args[1] == nil {
			return sqlNull(), nil
		}

		return nil, errUnexpected("hamming_distance")
	}

	ra, rb := []rune(a), []rune(b)
	if len(ra) != len(rb) {
		return nil, errArg(hammingError)
	}

	var diff int64

	for i := range ra {
		if ra[i] != rb[i] {
			diff++
		}
	}

	return diff, nil
}

func isoYearOf(t time.Time) int64 {
	y, _ := t.ISOWeek()

	return int64(y)
}

func udfIsInfinite(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	f, ok := floatOrNaN(args[0])
	if !ok {
		return sqlNull(), nil
	}

	return boolInt(math.IsInf(f, 0)), nil
}

func checkWidthBucket(x, b1, b2 float64, n int64) error {
	switch {
	case n <= 0:
		return errArg("bucketCount must be greater than 0")
	case math.IsNaN(x):
		return errArg("operand must not be NaN")
	case b1 == b2:
		return errArg("bounds cannot equal each other")
	case math.IsNaN(b1) || math.IsNaN(b2) || math.IsInf(b1, 0) || math.IsInf(b2, 0):
		return errArg("first and second bound must be finite")
	}

	return nil
}

func udfWidthBucket(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	if slicesHasNil(args) {
		return sqlNull(), nil
	}

	x, xok := toFloatArg(args[0])
	b1, aok := toFloatArg(args[1])
	b2, bok := toFloatArg(args[argsTwo])
	n, nok := bigintArg(args[argsThree])

	if !xok || !aok || !bok || !nok {
		return nil, errUnexpected("width_bucket")
	}

	if err := checkWidthBucket(x, b1, b2, n); err != nil {
		return nil, err
	}

	lo, hi, from := b1, b2, x
	if b1 > b2 {
		lo, hi, from = -b1, -b2, -x
	}

	switch {
	case from < lo:
		return int64(0), nil
	case from >= hi:
		return n + 1, nil
	}

	return int64(float64(n)*(from-lo)/(hi-lo)) + 1, nil
}

func slicesHasNil(args []driver.Value) bool {
	for _, a := range args {
		if a == nil {
			return true
		}
	}

	return false
}

func constFunc(v float64) scalarFn {
	return func(*fnCtx, []driver.Value) (driver.Value, error) { return v, nil }
}

func registerTrinoMathFuncs() {
	registerNondeterministic("from_base", argsTwo, udfFromBase)
	registerNondeterministic("to_base", argsTwo, udfToBase)
	registerNondeterministic("bit_count", argsTwo, udfBitCount)
	registerNondeterministic("bitwise_not", 1, udfBitwiseNot)
	registerNondeterministic("bitwise_left_shift", argsTwo, shiftFunc("bitwise_left_shift", shiftLeft))
	registerNondeterministic("bitwise_right_shift", argsTwo, shiftFunc("bitwise_right_shift", shiftRight))
	registerNondeterministic("bitwise_right_shift_arithmetic", argsTwo,
		shiftFunc("bitwise_right_shift_arithmetic", shiftRightArithmetic))
	registerNondeterministic("hamming_distance", argsTwo, udfHammingDistance)
	registerNondeterministic("width_bucket", argsFour, udfWidthBucket)
	registerScalar("infinity", 0, constFunc(math.Inf(1)))
	registerScalar("nan", 0, func(*fnCtx, []driver.Value) (driver.Value, error) { return nanText, nil })
	registerScalar("e", 0, constFunc(math.E))
	registerScalar("is_infinite", 1, udfIsInfinite)
	registerScalar("year_of_week", 1, tsPart(isoYearOf))
	registerScalar("yow", 1, tsPart(isoYearOf))
}
