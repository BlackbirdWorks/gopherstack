package bqsql

import (
	"fmt"
	"math"
	"math/big"
)

const (
	mathPrec      = 640
	expLimit      = 200
	expSeriesCap  = 400
	powExactLimit = 256
	powBitLimit   = 4096
	halvings      = 16
	logSeedIters  = 8
	halfDivisor   = 2
)

func (d Decimal) rat() *big.Rat {
	return new(big.Rat).SetFrac(d.unscaled(), pow10(decScale(d.kind)))
}

func (d Decimal) bigFloat() *big.Float {
	return new(big.Float).SetPrec(mathPrec).SetRat(d.rat())
}

func decimalFromFloat(k Kind, f *big.Float, expr string, args ...any) (Decimal, error) {
	r, _ := f.Rat(nil)
	n := divRound(new(big.Int).Mul(r.Num(), pow10(decScale(k))), r.Denom())

	return newDecimal(k, n, expr, args...)
}

func decErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrDecimal, fmt.Sprintf(format, args...))
}

// Sqrt returns the square root rounded half away from zero; negative input is an error.
func (d Decimal) Sqrt() (Decimal, error) {
	if d.Sign() < 0 {
		return Decimal{}, decErr("an invalid argument to SQRT: %s", d.String())
	}

	scaled := new(big.Int).Mul(d.unscaled(), pow10(decScale(d.kind)))
	r := new(big.Int).Sqrt(scaled)
	rem := new(big.Int).Sub(scaled, new(big.Int).Mul(r, r))

	if rem.Cmp(r) > 0 {
		r.Add(r, big.NewInt(1))
	}

	return newDecimal(d.kind, r, "SQRT(%s)", d.String())
}

func expFloat(x *big.Float) *big.Float {
	div := new(big.Float).SetPrec(mathPrec).SetInt(new(big.Int).Lsh(big.NewInt(1), halvings))
	r := new(big.Float).SetPrec(mathPrec).Quo(x, div)
	sum := new(big.Float).SetPrec(mathPrec).SetInt64(1)
	term := new(big.Float).SetPrec(mathPrec).SetInt64(1)
	eps := new(big.Float).SetPrec(mathPrec).SetMantExp(big.NewFloat(1), -mathPrec)

	for i := 1; i < expSeriesCap; i++ {
		term.Mul(term, r)
		term.Quo(term, new(big.Float).SetPrec(mathPrec).SetInt64(int64(i)))
		sum.Add(sum, term)

		if new(big.Float).Abs(term).Cmp(eps) < 0 {
			break
		}
	}

	for range halvings {
		sum.Mul(sum, sum)
	}

	return sum
}

// Exp returns e^d; huge results overflow and tiny ones underflow to zero.
func (d Decimal) Exp() (Decimal, error) {
	f := d.Float64()
	switch {
	case f > expLimit:
		return Decimal{}, decOverflow(d.kind, "EXP(%s)", d.String())
	case f < -expLimit:
		return Decimal{n: new(big.Int), kind: decKind(d.kind)}, nil
	}

	return decimalFromFloat(d.kind, expFloat(d.bigFloat()), "EXP(%s)", d.String())
}

func lnFloat(x *big.Float) *big.Float {
	seed, _ := x.Float64()
	y := new(big.Float).SetPrec(mathPrec).SetFloat64(math.Log(seed))
	two := new(big.Float).SetPrec(mathPrec).SetInt64(halfDivisor)

	for range logSeedIters {
		e := expFloat(y)
		num := new(big.Float).SetPrec(mathPrec).Sub(x, e)
		den := new(big.Float).SetPrec(mathPrec).Add(x, e)
		y.Add(y, new(big.Float).SetPrec(mathPrec).Mul(two, num.Quo(num, den)))
	}

	return y
}

// Ln returns the natural logarithm; non-positive input is an error.
func (d Decimal) Ln() (Decimal, error) {
	if d.Sign() <= 0 {
		return Decimal{}, decErr("an invalid argument to LN: %s", d.String())
	}

	return decimalFromFloat(d.kind, lnFloat(d.bigFloat()), "LN(%s)", d.String())
}

// Log10 returns the base-10 logarithm; non-positive input is an error.
func (d Decimal) Log10() (Decimal, error) {
	if d.Sign() <= 0 {
		return Decimal{}, decErr("an invalid argument to LOG10: %s", d.String())
	}

	ten := new(big.Float).SetPrec(mathPrec).SetInt64(decimalBase)
	q := new(big.Float).SetPrec(mathPrec).Quo(lnFloat(d.bigFloat()), lnFloat(ten))

	return decimalFromFloat(d.kind, q, "LOG10(%s)", d.String())
}

// Log returns the logarithm of d in the given base; d, base <= 0 and base = 1 are errors.
func (d Decimal) Log(base Decimal) (Decimal, error) {
	one := unitDecimal(base.kind)
	if d.Sign() <= 0 || base.Sign() <= 0 || base.Cmp(one) == 0 {
		return Decimal{}, decErr("an invalid argument to LOG: %s, %s", d.String(), base.String())
	}

	k := KindNumeric
	if d.kind == KindBigNumeric || base.kind == KindBigNumeric {
		k = KindBigNumeric
	}

	q := new(big.Float).SetPrec(mathPrec).Quo(lnFloat(d.bigFloat()), lnFloat(base.bigFloat()))

	return decimalFromFloat(k, q, "LOG(%s, %s)", d.String(), base.String())
}

func (d Decimal) isInteger() bool {
	return new(big.Int).Rem(d.unscaled(), pow10(decScale(d.kind))).Sign() == 0
}

func ratPow(x *big.Rat, n int64) (*big.Rat, bool) {
	neg := n < 0
	if neg {
		n = -n
	}

	res := new(big.Rat).SetInt64(1)
	base := new(big.Rat).Set(x)

	for ; n > 0; n >>= 1 {
		if n&1 == 1 {
			res.Mul(res, base)
		}

		if res.Num().BitLen() > powBitLimit || res.Denom().BitLen() > powBitLimit {
			return nil, false
		}

		base.Mul(base, base)

		if n > 1 && (base.Num().BitLen() > powBitLimit || base.Denom().BitLen() > powBitLimit) {
			return nil, false
		}
	}

	if neg {
		res.Inv(res)
	}

	return res, true
}

// Pow returns d^e. Underflow yields zero; a negative base with fractional exponent and 0^negative are errors.
func (d Decimal) Pow(e Decimal) (Decimal, error) {
	k := KindNumeric
	if d.kind == KindBigNumeric || e.kind == KindBigNumeric {
		k = KindBigNumeric
	}

	expr := fmt.Sprintf("POW(%s, %s)", d.String(), e.String())

	switch {
	case d.Sign() == 0 && e.Sign() < 0, d.Sign() < 0 && !e.isInteger():
		return Decimal{}, decErr("an invalid argument to POW: %s, %s", d.String(), e.String())
	case d.Sign() == 0:
		return powZeroBase(k, e)
	}

	if r, ok := d.powExact(e); ok {
		v := divRound(new(big.Int).Mul(r.Num(), pow10(decScale(k))), r.Denom())

		return newDecimal(k, v, "%s", expr)
	}

	return d.powSeries(e, k, expr)
}

// powExact computes d^e as an exact rational for small integer exponents.
func (d Decimal) powExact(e Decimal) (*big.Rat, bool) {
	if !e.isInteger() {
		return nil, false
	}

	n := new(big.Int).Div(e.unscaled(), pow10(decScale(e.kind)))
	if !n.IsInt64() || n.CmpAbs(big.NewInt(powExactLimit)) > 0 {
		return nil, false
	}

	return ratPow(d.rat(), n.Int64())
}

func (d Decimal) powSeries(e Decimal, k Kind, expr string) (Decimal, error) {
	ax := new(big.Float).SetPrec(mathPrec).Abs(d.bigFloat())
	prod := new(big.Float).SetPrec(mathPrec).Mul(e.bigFloat(), lnFloat(ax))

	f, _ := prod.Float64()
	if f > expLimit {
		return Decimal{}, decOverflow(k, "%s", expr)
	}

	if f < -expLimit {
		return Decimal{n: new(big.Int), kind: k}, nil
	}

	res := expFloat(prod)
	if d.Sign() < 0 && !e.isEven() {
		res.Neg(res)
	}

	return decimalFromFloat(k, res, "%s", expr)
}

func (d Decimal) isEven() bool {
	whole := new(big.Int).Div(d.unscaled(), pow10(decScale(d.kind)))

	return whole.Bit(0) == 0
}

func powZeroBase(k Kind, e Decimal) (Decimal, error) {
	if e.Sign() == 0 {
		return unitDecimal(k), nil
	}

	return Decimal{n: new(big.Int), kind: k}, nil
}

// IntDiv returns the integer quotient truncated toward zero; a zero divisor is an error.
func (d Decimal) IntDiv(o Decimal) (Decimal, error) {
	k, x, y, err := widest(d, o)
	if err != nil {
		return Decimal{}, err
	}

	if y.Sign() == 0 {
		return Decimal{}, decErr("division by zero: DIV(%s, %s)", d.String(), o.String())
	}

	q := new(big.Int).Quo(x, y)

	return newDecimal(k, q.Mul(q, pow10(decScale(k))), "DIV(%s, %s)", d.String(), o.String())
}
