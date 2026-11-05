package sparql

import (
	"math"
	"math/big"
	"strconv"
	"strings"
)

type numKind uint8

const (
	nInt numKind = iota
	nDec
	nFloat
	nDouble
)

type numVal struct {
	r *big.Rat
	f float64
	i int64
	k numKind
}

func isIntegerType(dt string) bool {
	local, ok := strings.CutPrefix(dt, NSXSD)
	if !ok {
		return false
	}

	switch local {
	case "integer", "int", "long", "short", "byte", "nonNegativeInteger", "positiveInteger", "negativeInteger",
		"nonPositiveInteger", "unsignedInt", "unsignedLong", "unsignedShort", "unsignedByte":
		return true
	default:
		return false
	}
}

func asNumeric(t Term) (numVal, bool) {
	if t.Kind != KindLiteral || t.Lang != "" {
		return numVal{}, false
	}

	v := strings.TrimSpace(t.Value)

	switch {
	case isIntegerType(t.Datatype):
		i, err := strconv.ParseInt(strings.TrimPrefix(v, "+"), 10, 64)
		if err != nil {
			return numVal{}, false
		}

		return numVal{k: nInt, i: i}, true
	case t.Datatype == XSDDecimal:
		r, ok := new(big.Rat).SetString(v)
		if !ok {
			return numVal{}, false
		}

		return numVal{k: nDec, r: r}, true
	case t.Datatype == XSDDouble || t.Datatype == XSDFloat:
		f, ok := parseFloatLex(v)
		if !ok {
			return numVal{}, false
		}

		k := nDouble
		if t.Datatype == XSDFloat {
			k = nFloat
		}

		return numVal{k: k, f: f}, true
	default:
		return numVal{}, false
	}
}

func parseFloatLex(v string) (float64, bool) {
	switch v {
	case "INF", "+INF":
		return math.Inf(1), true
	case "-INF":
		return math.Inf(-1), true
	case "NaN":
		return math.NaN(), true
	}

	f, err := strconv.ParseFloat(v, 64)
	if err != nil && !strings.Contains(err.Error(), "range") {
		return 0, false
	}

	return f, true
}

func (n numVal) float() float64 {
	switch n.k {
	case nInt:
		return float64(n.i)
	case nDec:
		f, _ := n.r.Float64()

		return f
	default:
		return n.f
	}
}

func (n numVal) rat() *big.Rat {
	if n.k == nInt {
		return new(big.Rat).SetInt64(n.i)
	}

	return n.r
}

func (n numVal) term() Term {
	switch n.k {
	case nInt:
		return Typed(strconv.FormatInt(n.i, 10), XSDInteger)
	case nDec:
		return Typed(formatDecimal(n.r), XSDDecimal)
	case nFloat:
		return Typed(formatDouble(n.f), XSDFloat)
	default:
		return Typed(formatDouble(n.f), XSDDouble)
	}
}

const decimalDigits = 18

func formatDecimal(r *big.Rat) string {
	if r.IsInt() {
		return r.Num().String() + ".0"
	}

	s := r.FloatString(decimalDigits)
	s = strings.TrimRight(s, "0")

	if strings.HasSuffix(s, ".") {
		s += "0"
	}

	return s
}

func formatDouble(f float64) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "INF"
	case math.IsInf(f, -1):
		return "-INF"
	}

	s := strconv.FormatFloat(f, 'E', -1, 64)
	mant, exp, _ := strings.Cut(s, "E")

	if !strings.Contains(mant, ".") {
		mant += ".0"
	}

	e, _ := strconv.Atoi(exp)

	return mant + "E" + strconv.Itoa(e)
}

func promote(a, b numVal) numKind { return max(a.k, b.k) }

func numCompare(a, b numVal) (int, bool) {
	switch k := promote(a, b); k {
	case nInt:
		switch {
		case a.i < b.i:
			return -1, true
		case a.i > b.i:
			return 1, true
		}

		return 0, true
	case nDec:
		return a.rat().Cmp(b.rat()), true
	default:
		x, y := a.float(), b.float()

		switch {
		case math.IsNaN(x) || math.IsNaN(y):
			return 0, false
		case x < y:
			return -1, true
		case x > y:
			return 1, true
		}

		return 0, true
	}
}

func numArith(op string, a, b numVal) (numVal, error) {
	k := promote(a, b)

	if k >= nFloat {
		x, y := a.float(), b.float()

		var f float64

		switch op {
		case "+":
			f = x + y
		case "-":
			f = x - y
		case "*":
			f = x * y
		default:
			f = x / y
		}

		return numVal{k: k, f: f}, nil
	}

	if op == "/" {
		if b.rat().Sign() == 0 {
			return numVal{}, errDivZero
		}

		return numVal{k: nDec, r: new(big.Rat).Quo(a.rat(), b.rat())}, nil
	}

	if k == nInt {
		switch op {
		case "+":
			return numVal{k: nInt, i: a.i + b.i}, nil
		case "-":
			return numVal{k: nInt, i: a.i - b.i}, nil
		default:
			return numVal{k: nInt, i: a.i * b.i}, nil
		}
	}

	x, y := a.rat(), b.rat()
	r := new(big.Rat)

	switch op {
	case "+":
		r.Add(x, y)
	case "-":
		r.Sub(x, y)
	default:
		r.Mul(x, y)
	}

	return numVal{k: nDec, r: r}, nil
}
