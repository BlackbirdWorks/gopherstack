package timestreamquery

import (
	"database/sql/driver"
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

type evalError string

func (e evalError) Error() string { return string(e) }

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
		return nil, nil //nolint:nilnil // try() maps failures to SQL NULL
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
			return arithFailure(try, "%s operands must be integers", bitsName(bits))
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
			return arithFailure(try, "%s operands must be integers", bitsName(bits))
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
			return arithFailure(try, "%s operand must be an integer", bitsName(bits))
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
			return arithFailure(try, "%s operand must be an integer", bitsName(bits))
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

func floatDivide(mod bool) scalarImpl {
	return func(_ *fnCtx, args []driver.Value) (driver.Value, error) {
		if args[0] == nil || args[1] == nil {
			return nil, nil
		}

		a, okA := toFloat64(args[0])
		b, okB := toFloat64(args[1])

		if !okA || !okB {
			return nil, evalError("operands must be numbers")
		}

		r := a / b
		if mod {
			r = math.Mod(a, b)
		}

		if math.IsNaN(r) {
			return nanText, nil
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
				out = append(out, scalarFn(arithName(op.name, bits, try), argsTwo, checkedBinary(op, bits, try)))
			}

			out = append(out,
				scalarFn(arithName("div", bits, try), argsTwo, checkedDivide(bits, false, try)),
				scalarFn(arithName("mod", bits, try), argsTwo, checkedDivide(bits, true, try)),
				scalarFn(arithName("neg", bits, try), 1, checkedNegate(bits, try)),
				scalarFn(arithName("abs", bits, try), 1, checkedAbs(bits, try)),
			)
		}
	}

	return append(out,
		scalarFn("ts_ddiv", argsTwo, floatDivide(false)),
		scalarFn("ts_dmod", argsTwo, floatDivide(true)),
		scalarFn("e", 0, udfE),
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
		return evalError("sum operand must be an integer")
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
		return evalError("sum operand must be an integer")
	}

	a.total -= v
	a.count--

	return nil
}

func (a *sumAgg) WindowValue(_ *fnCtx) (driver.Value, error) {
	if a.count == 0 {
		return nil, nil //nolint:nilnil // SUM of no rows is NULL
	}

	return a.total, nil
}

func (*sumAgg) Final(_ *fnCtx) {}

func registerArithFuncs() {
	for _, f := range tsArithFuncs() {
		sqlitedriver.MustRegisterDeterministicScalarFunction(f.name, f.nargs, f.impl)
	}

	sqlitedriver.MustRegisterScalarFunction("ts_random", 1, udfRandom)
	sqlitedriver.MustRegisterFunction("ts_sum", &sqlitedriver.FunctionImpl{
		NArgs: 1, Deterministic: true,
		MakeAggregate: func(_ fnCtx) (sqlitedriver.AggregateFunction, error) { return &sumAgg{}, nil },
	})
}
