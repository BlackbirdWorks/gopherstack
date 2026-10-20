package bqsql

import (
	"database/sql/driver"
	"math"
	"math/rand/v2"
	"strings"
)

func unaryFloat(name string, f func(float64) (float64, bool)) udfDef {
	return udfDef{n: 1, fn: func(a []driver.Value) (driver.Value, error) {
		if a[0] == nil {
			return nil, nil
		}

		x, err := toFloat(a[0])
		if err != nil {
			return nil, err
		}

		r, ok := f(x)
		if !ok {
			return nil, errBQ("an invalid argument to %s: %v", strings.ToUpper(name), x)
		}

		return r, nil
	}}
}

func always(f func(float64) float64) func(float64) (float64, bool) {
	return func(x float64) (float64, bool) { return f(x), true }
}

func addMathUDFs(d map[string]udfDef) {
	d["sqrt"] = unaryFloat("sqrt", func(x float64) (float64, bool) { return math.Sqrt(x), x >= 0 })
	d["ln"] = unaryFloat("ln", func(x float64) (float64, bool) { return math.Log(x), x > 0 })
	d["log10"] = unaryFloat("log10", func(x float64) (float64, bool) { return math.Log10(x), x > 0 })

	for name, f := range map[string]func(float64) float64{
		"exp": math.Exp, "sin": math.Sin, "cos": math.Cos, "tan": math.Tan, "asin": math.Asin, "acos": math.Acos,
		"atan": math.Atan, "sinh": math.Sinh, "cosh": math.Cosh, "tanh": math.Tanh, "cbrt": math.Cbrt,
	} {
		d[name] = unaryFloat(name, always(f))
	}

	d["abs"] = udfDef{n: 1, fn: udfAbs}
	d["sign"] = udfDef{n: 1, fn: udfSign}
	d["ceil"] = udfDef{n: 1, fn: roundWith(math.Ceil)}
	d["floor"] = udfDef{n: 1, fn: roundWith(math.Floor)}
	d["round"] = udfDef{n: -1, fn: udfRound(math.Round)}
	d["trunc"] = udfDef{n: -1, fn: udfRound(math.Trunc)}
	d["pow"] = udfDef{n: argc2, fn: binaryFloat(math.Pow)}
	d["atan2"] = udfDef{n: argc2, fn: binaryFloat(math.Atan2)}
	d["log"] = udfDef{n: -1, fn: udfLog}
	d["div"] = udfDef{n: argc2, fn: udfDiv}
	d["mod"] = udfDef{n: argc2, fn: udfMod}
	d["intdiv"] = udfDef{n: argc2, fn: udfIntDiv}
	d["sdiv"] = udfDef{n: argc2, fn: udfSafeDiv}
	d["ieee_div"] = udfDef{n: argc2, fn: binaryFloat(func(a, b float64) float64 { return a / b })}
	d["sadd"] = udfDef{
		n: argc2,
		fn: safeArith(
			func(a, b int64) (int64, bool) { return a + b, !addOverflow(a, b) },
			func(a, b float64) float64 { return a + b },
		),
	}
	d["ssub"] = udfDef{
		n: argc2,
		fn: safeArith(
			func(a, b int64) (int64, bool) { return a - b, !addOverflow(a, -b) && b != math.MinInt64 },
			func(a, b float64) float64 { return a - b },
		),
	}
	d["smul"] = udfDef{n: argc2, fn: safeArith(mulChecked, func(a, b float64) float64 { return a * b })}
	d["sneg"] = udfDef{n: 1, fn: udfSafeNeg}
	d["greatest"] = udfDef{n: -1, fn: extreme(1)}
	d["least"] = udfDef{n: -1, fn: extreme(-1)}
	d["is_nan"] = udfDef{n: 1, fn: floatPred(math.IsNaN)}
	d["is_inf"] = udfDef{n: 1, fn: floatPred(func(f float64) bool { return math.IsInf(f, 0) })}
	d["rand"] = udfDef{
		n:    0,
		rand: true,
		fn:   randomValue,
	}
}

func addOverflow(a, b int64) bool {
	s := a + b

	return (a > 0 && b > 0 && s < 0) || (a < 0 && b < 0 && s >= 0)
}

func mulChecked(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}

	p := a * b

	return p, p/b == a && (a != -1 || b != math.MinInt64) && (b != -1 || a != math.MinInt64)
}

func floatPred(f func(float64) bool) udfFn {
	return func(a []driver.Value) (driver.Value, error) {
		if a[0] == nil {
			return nil, nil
		}

		x, err := toFloat(a[0])
		if err != nil {
			return nil, err
		}

		return boolVal(f(x)), nil
	}
}

func udfAbs(a []driver.Value) (driver.Value, error) {
	switch t := a[0].(type) {
	case nil:
		return nil, nil //nolint:nilnil // NULL in, NULL out
	case int64:
		if t == math.MinInt64 {
			return nil, errBQ("int64 overflow: ABS(%d)", t)
		}

		if t < 0 {
			return -t, nil
		}

		return t, nil
	}

	x, err := toFloat(a[0])

	return math.Abs(x), err
}

func udfSign(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	x, err := toFloat(a[0])
	if err != nil {
		return nil, err
	}

	s := 0.0

	switch {
	case x > 0:
		s = 1
	case x < 0:
		s = -1
	}

	if isFloatVal(a[0]) {
		return s, nil
	}

	return int64(s), nil
}

func roundWith(f func(float64) float64) udfFn {
	return func(a []driver.Value) (driver.Value, error) {
		if a[0] == nil {
			return nil, nil
		}

		x, err := toFloat(a[0])
		if err != nil {
			return nil, err
		}

		return f(x), nil
	}
}

func udfRound(f func(float64) float64) udfFn {
	return func(a []driver.Value) (driver.Value, error) {
		if anyNull(a) {
			return nil, nil
		}

		x, err := toFloat(a[0])
		if err != nil {
			return nil, err
		}

		digits := int64(0)
		if len(a) > 1 {
			if digits, err = toInt(a[1]); err != nil {
				return nil, err
			}
		}

		scale := math.Pow10(int(digits))

		return f(x*scale) / scale, nil
	}
}

func binaryFloat(f func(a, b float64) float64) udfFn {
	return func(a []driver.Value) (driver.Value, error) {
		if anyNull(a) {
			return nil, nil
		}

		x, err := toFloat(a[0])
		if err != nil {
			return nil, err
		}

		y, err := toFloat(a[1])
		if err != nil {
			return nil, err
		}

		return f(x, y), nil
	}
}

func udfLog(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	x, err := toFloat(a[0])
	if err != nil {
		return nil, err
	}

	if x <= 0 {
		return nil, errBQ("an invalid argument to LOG: %v", x)
	}

	if len(a) == 1 {
		return math.Log(x), nil
	}

	b, err := toFloat(a[1])
	if err != nil {
		return nil, err
	}

	if b <= 0 || b == 1 {
		return nil, errBQ("an invalid base for LOG: %v", b)
	}

	return math.Log(x) / math.Log(b), nil
}

func udfDiv(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	x, err := toFloat(a[0])
	if err != nil {
		return nil, err
	}

	y, err := toFloat(a[1])
	if err != nil {
		return nil, err
	}

	if y == 0 {
		return nil, errBQ("division by zero: %v / %v", fmtNum(a[0]), fmtNum(a[1]))
	}

	return x / y, nil
}

func fmtNum(v driver.Value) string { return toStr(v) }

func udfSafeDiv(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	x, err := toFloat(a[0])
	if err != nil {
		return nil, err
	}

	y, err := toFloat(a[1])
	if err != nil {
		return nil, err
	}

	if y == 0 {
		return nil, nil //nolint:nilnil // SAFE_DIVIDE by zero is NULL
	}

	return x / y, nil
}

func udfMod(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	if !isFloatVal(a[0]) && !isFloatVal(a[1]) {
		x, err := toInt(a[0])
		if err != nil {
			return nil, err
		}

		y, err := toInt(a[1])
		if err != nil {
			return nil, err
		}

		if y == 0 {
			return nil, errBQ("division by zero: MOD(%d, %d)", x, y)
		}

		if y == -1 {
			return int64(0), nil
		}

		return x % y, nil
	}

	x, _ := toFloat(a[0])
	y, _ := toFloat(a[1])

	if y == 0 {
		return nil, errBQ("division by zero: MOD(%v, %v)", x, y)
	}

	return math.Mod(x, y), nil
}

func udfIntDiv(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	x, err := toInt(a[0])
	if err != nil {
		return nil, err
	}

	y, err := toInt(a[1])
	if err != nil {
		return nil, err
	}

	if y == 0 {
		return nil, errBQ("division by zero: DIV(%d, %d)", x, y)
	}

	if x == math.MinInt64 && y == -1 {
		return nil, errBQ("int64 overflow: DIV(%d, %d)", x, y)
	}

	return x / y, nil
}

func safeArith(ints func(a, b int64) (int64, bool), floats func(a, b float64) float64) udfFn {
	return func(a []driver.Value) (driver.Value, error) {
		if anyNull(a) {
			return nil, nil
		}

		if !isFloatVal(a[0]) && !isFloatVal(a[1]) {
			x, err := toInt(a[0])
			if err != nil {
				return nil, err
			}

			y, err := toInt(a[1])
			if err != nil {
				return nil, err
			}

			if r, ok := ints(x, y); ok {
				return r, nil
			}

			return nil, nil
		}

		x, _ := toFloat(a[0])
		y, _ := toFloat(a[1])

		return floats(x, y), nil
	}
}

func udfSafeNeg(a []driver.Value) (driver.Value, error) {
	switch t := a[0].(type) {
	case nil:
		return nil, nil //nolint:nilnil // NULL in, NULL out
	case int64:
		if t == math.MinInt64 {
			return nil, nil //nolint:nilnil // overflow is NULL
		}

		return -t, nil
	}

	x, err := toFloat(a[0])

	return -x, err
}

func extreme(dir int) udfFn {
	return func(a []driver.Value) (driver.Value, error) {
		if anyNull(a) {
			return nil, nil
		}

		best := a[0]

		for _, v := range a[1:] {
			if compareValues(v, best)*dir > 0 {
				best = v
			}
		}

		return best, nil
	}
}

func compareValues(a, b driver.Value) int {
	if as, ok := a.(string); ok {
		return strings.Compare(as, toStr(b))
	}

	x, _ := toFloat(a)
	y, _ := toFloat(b)

	switch {
	case x < y:
		return -1
	case x > y:
		return 1
	}

	return 0
}

func randomValue([]driver.Value) (driver.Value, error) {
	return rand.Float64(), nil //nolint:gosec // SQL RAND() is not security sensitive
}
