package iot

import (
	"crypto/rand"
	"encoding/binary"
	"math"
)

func floorF(f float64) float64 { return math.Floor(f) }

func math1(f func(float64) float64) funcDef {
	return funcDef{impl: func(c *sqlCtx, args []any) any {
		x, ok := toDecimal(arg(args, 0))
		if !ok {
			return sqlUndefined{}
		}

		return c.num(f(x))
	}}
}

func intRounder(f func(float64) float64) funcDef {
	return funcDef{impl: func(_ *sqlCtx, args []any) any {
		x, ok := toDecimal(arg(args, 0))
		if !ok {
			return sqlUndefined{}
		}

		if i, fok := floatToInt(f(x)); fok {
			return i
		}

		return sqlUndefined{}
	}}
}

func math2(f func(a, b float64) float64) funcDef {
	return funcDef{impl: func(c *sqlCtx, args []any) any {
		a, aok := toDecimal(arg(args, 0))
		b, bok := toDecimal(arg(args, 1))

		if !aok || !bok {
			return sqlUndefined{}
		}

		return c.num(f(a, b))
	}}
}

func bitFunc(f func(a, b int64) int64) funcDef {
	return funcDef{impl: func(_ *sqlCtx, args []any) any {
		a, aok := toIntConv(arg(args, 0))
		b, bok := toIntConv(arg(args, 1))

		if !aok || !bok {
			return sqlUndefined{}
		}

		return f(a, b)
	}}
}

func mathFuncs() map[string]funcDef {
	return map[string]funcDef{
		"abs":       {impl: absFunc},
		"acos":      math1(math.Acos),
		"asin":      math1(math.Asin),
		"atan":      math1(math.Atan),
		"atan2":     math2(math.Atan2),
		"cos":       math1(math.Cos),
		"cosh":      math1(math.Cosh),
		"sin":       math1(math.Sin),
		"sinh":      math1(math.Sinh),
		"tan":       math1(math.Tan),
		"tanh":      math1(math.Tanh),
		"exp":       math1(math.Exp),
		"ln":        math1(math.Log),
		"log":       math1(math.Log10),
		"sqrt":      math1(math.Sqrt),
		"power":     math2(math.Pow),
		"ceil":      intRounder(math.Ceil),
		"floor":     intRounder(math.Floor),
		"round":     intRounder(math.Round),
		"sign":      {impl: signFunc},
		"mod":       {impl: modFunc},
		"remainder": {impl: modFunc},
		"trunc":     {impl: truncFunc},
		"rand":      {impl: func(*sqlCtx, []any) any { return randFloat() }},
		"nanvl":     {impl: nanvlFunc},
		"bitand":    bitFunc(func(a, b int64) int64 { return a & b }),
		"bitor":     bitFunc(func(a, b int64) int64 { return a | b }),
		"bitxor":    bitFunc(func(a, b int64) int64 { return a ^ b }),
		"bitnot": {impl: func(_ *sqlCtx, args []any) any {
			if a, ok := toIntConv(arg(args, 0)); ok {
				return ^a
			}

			return sqlUndefined{}
		}},
	}
}

func absFunc(c *sqlCtx, args []any) any {
	if i, ok := arg(args, 0).(int64); ok {
		if i < 0 {
			return -i
		}

		return i
	}

	if f, ok := toDecimal(arg(args, 0)); ok {
		return c.num(math.Abs(f))
	}

	return sqlUndefined{}
}

func signFunc(_ *sqlCtx, args []any) any {
	f, ok := toDecimal(arg(args, 0))
	if !ok {
		return sqlUndefined{}
	}

	switch {
	case f > 0:
		return int64(1)
	case f < 0:
		return int64(-1)
	}

	return int64(0)
}

func modFunc(c *sqlCtx, args []any) any {
	return arithOp(c, "%", arg(args, 0), arg(args, 1))
}

const maxTruncPlaces = 34

func truncFunc(c *sqlCtx, args []any) any {
	x, xok := toDecimal(arg(args, 0))
	n, nok := toIntConv(arg(args, 1))

	if !xok || !nok {
		return sqlUndefined{}
	}

	n = min(max(n, 0), maxTruncPlaces)
	p := math.Pow10(int(n))

	return c.num(math.Trunc(x*p) / p)
}

func nanvlFunc(_ *sqlCtx, args []any) any {
	a := arg(args, 0)

	if a == nil || isUndef(a) {
		return arg(args, 1)
	}

	if f, ok := a.(float64); ok && math.IsNaN(f) {
		return arg(args, 1)
	}

	return a
}

const (
	randMantissaShift = 11
	randMantissaBits  = 53
)

// randFloat returns a uniform value in [0, 1).
func randFloat() float64 {
	var b [8]byte

	if _, err := rand.Read(b[:]); err != nil {
		return 0
	}

	return float64(binary.BigEndian.Uint64(b[:])>>randMantissaShift) / (1 << randMantissaBits)
}
