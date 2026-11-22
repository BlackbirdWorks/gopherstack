package timestreamquery

import (
	"database/sql/driver"
	"math"
	"math/bits"
	"strconv"
	"strings"
)

const (
	minRadix       = 2
	maxRadix       = 36
	minBitCount    = 2
	maxBitCount    = 64
	hammingMessage = "The input strings to hamming_distance function must have the same length"
)

func textArgs(args []driver.Value) ([]string, bool, error) {
	out := make([]string, len(args))

	for i, a := range args {
		if a == nil {
			return nil, true, nil
		}

		s, ok := textOf(a)
		if !ok {
			return nil, false, errOperandType
		}

		out[i] = s
	}

	return out, false, nil
}

func checkRadix(v driver.Value) (int, error) {
	r, ok := toInt64(v)
	if !ok {
		return 0, errOperandType
	}

	if r < minRadix || r > maxRadix {
		return 0, evalError("Radix must be between 2 and 36")
	}

	return int(r), nil
}

func udfFromBase(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	if args[0] == nil || args[1] == nil {
		return nil, nil //nolint:nilnil // SQL NULL
	}

	s, ok := textOf(args[0])
	if !ok {
		return nil, errOperandType
	}

	radix, err := checkRadix(args[1])
	if err != nil {
		return nil, err
	}

	n, parseErr := strconv.ParseInt(s, radix, bits64)
	if parseErr != nil {
		return nil, evalError("Not a valid base-" + strconv.Itoa(radix) + " number: " + s)
	}

	return n, nil
}

func udfToBase(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	if args[0] == nil || args[1] == nil {
		return nil, nil //nolint:nilnil // SQL NULL
	}

	n, ok := toInt64(args[0])
	if !ok {
		return nil, errOperandType
	}

	radix, err := checkRadix(args[1])
	if err != nil {
		return nil, err
	}

	return strconv.FormatInt(n, radix), nil
}

func udfBitwiseNot(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	if args[0] == nil {
		return nil, nil //nolint:nilnil // SQL NULL
	}

	n, ok := toInt64(args[0])
	if !ok {
		return nil, errOperandType
	}

	return ^n, nil
}

func udfBitCount(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	if args[0] == nil || args[1] == nil {
		return nil, nil //nolint:nilnil // SQL NULL
	}

	n, ok1 := toInt64(args[0])
	width, ok2 := toInt64(args[1])

	if !ok1 || !ok2 {
		return nil, errOperandType
	}

	if width < minBitCount || width > maxBitCount {
		return nil, evalError(
			"Bits specified in bit_count must be between 2 and 64, got " + strconv.FormatInt(width, 10),
		)
	}

	mask := ^uint64(0)
	if width < maxBitCount {
		limit := int64(1) << (width - 1)
		if n < -limit || n >= limit {
			return nil, evalError("Number must be representable with the bits specified. " +
				strconv.FormatInt(n, 10) + " can not be represented with " + strconv.FormatInt(width, 10) + " bits")
		}

		mask = uint64(1)<<width - 1
	}

	return int64(bits.OnesCount64(uint64(n) & mask)), nil //nolint:gosec // two's complement bit pattern
}

func udfHamming(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	s, isNull, err := textArgs(args)
	if err != nil || isNull {
		return nil, err
	}

	a, b := []rune(s[0]), []rune(s[1])
	if len(a) != len(b) {
		return nil, evalError(hammingMessage)
	}

	var diff int64

	for i := range a {
		if a[i] != b[i] {
			diff++
		}
	}

	return diff, nil
}

// runeIndexes returns the rune offsets of every occurrence of sub in s, overlapping ones included.
func runeIndexes(s, sub []rune) []int {
	var out []int

	for i := 0; i+len(sub) <= len(s); i++ {
		if string(s[i:i+len(sub)]) == string(sub) {
			out = append(out, i)
		}
	}

	return out
}

// positionOf implements strpos and strrpos: the 1-based position of the nth occurrence, 0 when absent.
func positionOf(fromEnd bool) scalarImpl {
	return func(_ *fnCtx, args []driver.Value) (driver.Value, error) {
		for _, a := range args {
			if a == nil {
				return nil, nil
			}
		}

		s, sub := []rune(argText(args[0])), []rune(argText(args[1]))
		instance := int64(1)

		if len(args) > argsTwo {
			n, ok := toInt64(args[argsTwo])
			if !ok {
				return nil, errOperandType
			}

			if n <= 0 {
				return nil, evalError("'instance' must be a positive number")
			}

			instance = n
		}

		if len(sub) == 0 {
			return int64(1), nil
		}

		idx := runeIndexes(s, sub)
		if fromEnd {
			slicesReverse(idx)
		}

		if instance > int64(len(idx)) {
			return int64(0), nil
		}

		return int64(idx[instance-1] + 1), nil
	}
}

func slicesReverse(s []int) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func argText(v driver.Value) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		return javaDouble(x)
	}

	return ""
}

// udfReplace is replace(s, search[, replacement]); a missing replacement removes every match.
func udfReplace(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	for _, a := range args {
		if a == nil {
			return nil, nil //nolint:nilnil // SQL NULL
		}
	}

	repl := ""
	if len(args) > argsTwo {
		repl = argText(args[argsTwo])
	}

	return strings.ReplaceAll(argText(args[0]), argText(args[1]), repl), nil
}

func udfInfinity(*fnCtx, []driver.Value) (driver.Value, error) { return math.Inf(1), nil }
