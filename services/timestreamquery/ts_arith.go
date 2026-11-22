package timestreamquery

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"strconv"

	sqlitedriver "modernc.org/sqlite"
)

const (
	bits32       = 32
	bits64       = 64
	tryNameSufx  = "t"
	errDivByZero = "Division by zero"
	nanText      = "NaN"
)

// evalMarker tags a message raised by a checked function so a failed query can tell a runtime error from a bad rewrite.
const evalMarker = "\x1e"

var errOperandType = errors.New("operand does not match its static type")

type evalError string

func (e evalError) Error() string { return evalMarker + string(e) }

func bitsName(bits int) string {
	if bits == bits32 {
		return kwInteger
	}

	return typBigint
}

func fitsBits(v int64, bits int) bool {
	return bits == bits64 || (v >= math.MinInt32 && v <= math.MaxInt32)
}

func addExact(a, b int64) (int64, bool) {
	r := a + b

	return r, (a >= 0) != (b >= 0) || (r >= 0) == (a >= 0)
}

func subExact(a, b int64) (int64, bool) {
	r := a - b

	return r, (a >= 0) == (b >= 0) || (r >= 0) == (a >= 0)
}

func mulExact(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}

	r := a * b
	if (a == -1 && b == math.MinInt64) || (b == -1 && a == math.MinInt64) || r/b != a {
		return r, false
	}

	return r, true
}

func toInt64(v driver.Value) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case float64:
		if n == math.Trunc(n) && n >= -twoPow63 && n < twoPow63 {
			return int64(n), true
		}
	case bool:
		if n {
			return 1, true
		}

		return 0, true
	case string, []byte:
		txt, _ := textOf(n)
		if i, err := strconv.ParseInt(txt, 10, 64); err == nil {
			return i, true
		}

		if f, err := strconv.ParseFloat(txt, 64); err == nil && f == math.Trunc(f) && f >= -twoPow63 && f < twoPow63 {
			return int64(f), true
		}
	}

	return 0, false
}

func toFloat64(v driver.Value) (float64, bool) {
	switch n := v.(type) {
	case int64:
		return float64(n), true
	case float64:
		return n, true
	case string, []byte:
		txt, _ := textOf(n)
		f, err := strconv.ParseFloat(txt, 64)

		return f, err == nil
	}

	return 0, false
}

type checkedOp struct {
	apply func(a, b int64) (int64, bool)
	name  string
	word  string
	sym   string
}

func checkedOps() []checkedOp {
	return []checkedOp{
		{name: "add", word: "addition", sym: "+", apply: addExact},
		{name: "sub", word: "subtraction", sym: "-", apply: subExact},
		{name: "mul", word: "multiplication", sym: "*", apply: mulExact},
	}
}

func arithFailure(try bool, format string, args ...any) (driver.Value, error) {
	if try {
		return nil, nil //nolint:nilnil // SQL NULL
	}

	return nil, evalError(fmt.Sprintf(format, args...))
}

func wrapBits(v int64, bits int) int64 {
	if bits == bits32 {
		return int64(int32(v)) //nolint:gosec // 32-bit wrap-around intended
	}

	return v
}

func checkedBinary(op checkedOp, bits int, try bool) scalarImpl {
	return func(_ *fnCtx, args []driver.Value) (driver.Value, error) {
		if args[0] == nil || args[1] == nil {
			return nil, nil
		}

		a, okA := toInt64(args[0])
		b, okB := toInt64(args[1])

		if !okA || !okB {
			return nil, errOperandType
		}

		r, ok := op.apply(a, b)
		if !ok || !fitsBits(r, bits) {
			return arithFailure(try, "%s %s overflow: %d %s %d", bitsName(bits), op.word, a, op.sym, b)
		}

		return r, nil
	}
}

func checkedDivide(bits int, mod, try bool) scalarImpl {
	return func(_ *fnCtx, args []driver.Value) (driver.Value, error) {
		if args[0] == nil || args[1] == nil {
			return nil, nil
		}

		a, okA := toInt64(args[0])
		b, okB := toInt64(args[1])

		if !okA || !okB {
			return nil, errOperandType
		}

		if b == 0 {
			return arithFailure(try, errDivByZero)
		}

		if mod {
			return a % b, nil
		}

		return wrapBits(a/b, bits), nil
	}
}

func checkedNegate(bits int, try bool) scalarImpl {
	return func(_ *fnCtx, args []driver.Value) (driver.Value, error) {
		if args[0] == nil {
			return nil, nil
		}

		a, ok := toInt64(args[0])
		if !ok {
			return nil, errOperandType
		}

		if a == math.MinInt64 || !fitsBits(-a, bits) {
			return arithFailure(try, "%s negation overflow: %d", bitsName(bits), a)
		}

		return -a, nil
	}
}

func checkedAbs(bits int, try bool) scalarImpl {
	return func(_ *fnCtx, args []driver.Value) (driver.Value, error) {
		if args[0] == nil {
			return nil, nil
		}

		a, ok := toInt64(args[0])
		if !ok {
			return nil, errOperandType
		}

		if a >= 0 {
			return a, nil
		}

		if a == math.MinInt64 || !fitsBits(-a, bits) {
			return arithFailure(try, "Value %d is out of range for %s", a, bitsName(bits))
		}

		return -a, nil
	}
}

// floatBinary is DOUBLE addition, subtraction or multiplication that tolerates NaN passed as text.
func floatBinary(op func(a, b float64) float64, keepNaN bool) scalarImpl {
	return domainMath(func(a []float64) float64 { return op(a[0], a[1]) }, keepNaN)
}

// floatDivide is DOUBLE division; NaN is text only where keepNaN says the result is rendered, else NULL like SQLite.
func floatDivide(mod, keepNaN bool) scalarImpl {
	return func(_ *fnCtx, args []driver.Value) (driver.Value, error) {
		if args[0] == nil || args[1] == nil {
			return nil, nil
		}

		a, okA := toFloat64(args[0])
		b, okB := toFloat64(args[1])

		if !okA || !okB {
			return nil, errOperandType
		}

		r := a / b
		if mod {
			r = math.Mod(a, b)
		}

		if math.IsNaN(r) {
			if keepNaN {
				return nanText, nil
			}

			return nil, nil
		}

		return r, nil
	}
}

func udfRandom(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	if args[0] == nil {
		return nil, nil //nolint:nilnil // SQL NULL
	}

	n, ok := toInt64(args[0])
	if !ok || n <= 0 {
		return nil, evalError("bound must be positive")
	}

	return rand.Int64N(n), nil //nolint:gosec // not security sensitive
}

func domainMath(apply func(args []float64) float64, keepNaN bool) scalarImpl {
	return func(_ *fnCtx, args []driver.Value) (driver.Value, error) {
		fs := make([]float64, len(args))

		for i, a := range args {
			if a == nil {
				return nil, nil
			}

			f, ok := toFloat64(a)
			if !ok {
				return nil, errOperandType
			}

			fs[i] = f
		}

		r := apply(fs)
		if math.IsNaN(r) {
			if keepNaN {
				return nanText, nil
			}

			return nil, nil
		}

		return r, nil
	}
}

// floatCompare compares two DOUBLE values with IEEE semantics: every comparison with NaN is false except <>.
func floatCompare(test func(a, b float64) bool) scalarImpl {
	return func(_ *fnCtx, args []driver.Value) (driver.Value, error) {
		if args[0] == nil || args[1] == nil {
			return nil, nil
		}

		a, okA := toFloat64(args[0])
		b, okB := toFloat64(args[1])

		if !okA || !okB {
			return nil, errOperandType
		}

		return boolInt(test(a, b)), nil
	}
}

// log10 is math.Log10 made exact at powers of ten, where Go's log2-based form is off by an ulp (log10(0.1)).
func log10(x float64) float64 {
	r := math.Log10(x)
	n := math.Round(r)

	if r != n && math.Abs(r-n) < 1e-9 && math.Pow10(int(n)) == x {
		return n
	}

	return r
}

func domainMathFuncs() []scalarFunc {
	unary := map[string]func(float64) float64{
		"sqrt": math.Sqrt, "ln": math.Log, "log10": log10, "log2": math.Log2, "acos": math.Acos,
		"asin": math.Asin, "acosh": math.Acosh, "atanh": math.Atanh,
	}

	var out []scalarFunc

	for _, keep := range []bool{true, false} {
		suffix := ""
		if !keep {
			suffix = "n"
		}

		for name, f := range unary {
			out = append(
				out,
				raisingFn("ts_"+name+suffix, 1, domainMath(func(a []float64) float64 { return f(a[0]) }, keep)),
			)
		}

		out = append(
			out,
			raisingFn(
				"ts_power"+suffix,
				argsTwo,
				domainMath(func(a []float64) float64 { return math.Pow(a[0], a[1]) }, keep),
			),
		)
	}

	for _, keep := range []bool{true, false} {
		suffix := ""
		if !keep {
			suffix = "n"
		}

		out = append(out,
			raisingFn("ts_dadd"+suffix, argsTwo, floatBinary(func(a, b float64) float64 { return a + b }, keep)),
			raisingFn("ts_dsub"+suffix, argsTwo, floatBinary(func(a, b float64) float64 { return a - b }, keep)),
			raisingFn("ts_dmul"+suffix, argsTwo, floatBinary(func(a, b float64) float64 { return a * b }, keep)),
		)
	}

	out = append(out,
		raisingFn("ts_deq", argsTwo, floatCompare(func(a, b float64) bool { return a == b })),
		raisingFn("ts_dne", argsTwo, floatCompare(func(a, b float64) bool { return a != b })),
		raisingFn("ts_dlt", argsTwo, floatCompare(func(a, b float64) bool { return a < b })),
		raisingFn("ts_dle", argsTwo, floatCompare(func(a, b float64) bool { return a <= b })),
		raisingFn("ts_dgt", argsTwo, floatCompare(func(a, b float64) bool { return a > b })),
		raisingFn("ts_dge", argsTwo, floatCompare(func(a, b float64) bool { return a >= b })),
	)

	return append(out, raisingFn("ts_nann", 0, func(*fnCtx, []driver.Value) (driver.Value, error) { return nil, nil }))
}

func floatClass(test func(float64) bool) scalarImpl {
	return func(_ *fnCtx, args []driver.Value) (driver.Value, error) {
		if args[0] == nil {
			return nil, nil
		}

		f, ok := toFloat64(args[0])
		if !ok {
			return nil, errOperandType
		}

		return boolInt(test(f)), nil
	}
}

func boolInt(b bool) int64 {
	if b {
		return 1
	}

	return 0
}

func udfE(*fnCtx, []driver.Value) (driver.Value, error) { return math.E, nil }

func tryFlag(try bool) string {
	if try {
		return tryNameSufx
	}

	return ""
}

func arithName(base string, bits int, try bool) string {
	return "ts_" + base + strconv.Itoa(bits) + tryFlag(try)
}

func tsArithFuncs() []scalarFunc {
	var out []scalarFunc

	for _, bits := range []int{bits32, bits64} {
		for _, try := range []bool{false, true} {
			for _, op := range checkedOps() {
				out = append(out, raisingFn(arithName(op.name, bits, try), argsTwo, checkedBinary(op, bits, try)))
			}

			out = append(out,
				raisingFn(arithName("div", bits, try), argsTwo, checkedDivide(bits, false, try)),
				raisingFn(arithName("mod", bits, try), argsTwo, checkedDivide(bits, true, try)),
				raisingFn(arithName("neg", bits, try), 1, checkedNegate(bits, try)),
				raisingFn(arithName("abs", bits, try), 1, checkedAbs(bits, try)),
			)
		}
	}

	out = append(out, domainMathFuncs()...)
	out = append(out,
		raisingFn("ts_ivadd", argsTwo, udfIntervalAdd(1)),
		raisingFn("ts_ivsub", argsTwo, udfIntervalAdd(-1)),
		raisingFn("ts_ivneg", 1, udfIntervalNegate),
		raisingFn("ts_tsadd", argsTwo, udfTimestampShift(1, false)),
		raisingFn("ts_tssub", argsTwo, udfTimestampShift(-1, false)),
		raisingFn("ts_tsaddr", argsTwo, udfTimestampShift(1, true)),
		raisingFn("ts_ivmul", argsTwo, udfIntervalScale(false, false)),
		raisingFn("ts_ivmulr", argsTwo, udfIntervalScale(false, true)),
		raisingFn("ts_ivdiv", argsTwo, udfIntervalScale(true, false)),
	)

	return append(out,
		raisingFn("ts_ddiv", argsTwo, floatDivide(false, true)),
		raisingFn("ts_dmod", argsTwo, floatDivide(true, true)),
		raisingFn("ts_ddivn", argsTwo, floatDivide(false, false)),
		raisingFn("ts_dmodn", argsTwo, floatDivide(true, false)),
		scalarFn("e", 0, udfE),
		raisingFn("ts_is_nan", 1, floatClass(math.IsNaN)),
		raisingFn("ts_is_infinite", 1, floatClass(func(f float64) bool { return math.IsInf(f, 0) })),
		raisingFn("ts_is_finite", 1, floatClass(func(f float64) bool { return !math.IsNaN(f) && !math.IsInf(f, 0) })),
	)
}

// sumAgg is SUM over integral values; a 64-bit overflow fails the query like the other checked arithmetic.
type sumAgg struct {
	total int64
	count int64
}

func (a *sumAgg) Step(_ *fnCtx, args []driver.Value) error {
	if args[0] == nil {
		return nil
	}

	v, ok := toInt64(args[0])
	if !ok {
		return errOperandType
	}

	r, fits := addExact(a.total, v)
	if !fits {
		return evalError(fmt.Sprintf("bigint addition overflow: %d + %d", a.total, v))
	}

	a.total = r
	a.count++

	return nil
}

func (a *sumAgg) WindowInverse(_ *fnCtx, args []driver.Value) error {
	if args[0] == nil {
		return nil
	}

	v, ok := toInt64(args[0])
	if !ok {
		return errOperandType
	}

	a.total -= v
	a.count--

	return nil
}

func (a *sumAgg) WindowValue(_ *fnCtx) (driver.Value, error) {
	if a.count == 0 {
		return nil, nil //nolint:nilnil // SQL NULL
	}

	return a.total, nil
}

func (*sumAgg) Final(_ *fnCtx) {}

func registerArithFuncs() {
	for _, f := range tsArithFuncs() {
		registerScalar(f)
	}

	registerAggFuncs()
	sqlitedriver.MustRegisterScalarFunction("ts_random", 1, udfRandom)
	sqlitedriver.MustRegisterFunction("ts_sum", &sqlitedriver.FunctionImpl{
		NArgs: 1, Deterministic: true,
		MakeAggregate: func(_ fnCtx) (sqlitedriver.AggregateFunction, error) { return &sumAgg{}, nil },
	})
}
