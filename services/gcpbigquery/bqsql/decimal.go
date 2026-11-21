package bqsql

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
)

// ErrDecimal is wrapped by every NUMERIC/BIGNUMERIC arithmetic and conversion failure.
var ErrDecimal = errors.New("decimal error")

const (
	numericScale     = 9
	bigNumericScale  = 38
	numericDigits    = 38
	maxDecimalText   = 512
	maxDecimalExp    = 1024
	maxDecimalDigits = 160
	bigNumericBits   = 255
	paramNumericInt  = 29
	paramBigInt      = 38
)

// RoundMode selects how ROUND breaks ties.
type RoundMode uint8

// Rounding modes of ROUND(x, n, mode).
const (
	RoundHalfAwayFromZero RoundMode = iota
	RoundHalfEven
)

// Decimal is an immutable exact NUMERIC (scale 9) or BIGNUMERIC (scale 38) value held as a scaled integer.
// The zero value is NUMERIC 0.
type Decimal struct {
	n    *big.Int
	kind Kind
}

func decScale(k Kind) int {
	if k == KindBigNumeric {
		return bigNumericScale
	}

	return numericScale
}

func pow10(n int) *big.Int {
	return new(big.Int).Exp(big.NewInt(decimalBase), big.NewInt(int64(n)), nil)
}

func decKind(k Kind) Kind {
	if k == KindBigNumeric {
		return KindBigNumeric
	}

	return KindNumeric
}

func (d Decimal) unscaled() *big.Int {
	if d.n == nil {
		return new(big.Int)
	}

	return d.n
}

func decOverflow(k Kind, expr string, args ...any) error {
	name := "numeric"
	if k == KindBigNumeric {
		name = litBignumeric
	}

	return fmt.Errorf("%w: %s overflow: %s", ErrDecimal, name, fmt.Sprintf(expr, args...))
}

func inRange(k Kind, n *big.Int) bool {
	if k == KindBigNumeric {
		lim := new(big.Int).Lsh(big.NewInt(1), bigNumericBits)
		if n.Sign() < 0 {
			return n.Cmp(new(big.Int).Neg(lim)) >= 0
		}

		return n.Cmp(lim) < 0
	}

	return new(big.Int).Abs(n).Cmp(pow10(numericDigits)) < 0
}

func newDecimal(k Kind, n *big.Int, expr string, args ...any) (Decimal, error) {
	k = decKind(k)
	if !inRange(k, n) {
		return Decimal{}, decOverflow(k, expr, args...)
	}

	return Decimal{n: n, kind: k}, nil
}

// divRound returns num/den rounded half away from zero; den must be non-zero.
func divRound(num, den *big.Int) *big.Int {
	q, r := new(big.Int).QuoRem(new(big.Int).Abs(num), new(big.Int).Abs(den), new(big.Int))
	if r.Lsh(r, 1).Cmp(new(big.Int).Abs(den)) >= 0 {
		q.Add(q, big.NewInt(1))
	}

	if num.Sign()*den.Sign() < 0 {
		q.Neg(q)
	}

	return q
}

// ParseDecimal parses a decimal literal or string (sign, fraction, exponent) and rounds half away from zero.
func ParseDecimal(k Kind, s string) (Decimal, error) {
	k = decKind(k)
	bad := fmt.Errorf("%w: invalid %s value %q", ErrDecimal, k.RESTName(), clip(s))

	s = strings.TrimSpace(s)
	if s == "" || len(s) > maxDecimalText {
		return Decimal{}, bad
	}

	neg := false

	switch s[0] {
	case '-':
		neg = true
		s = s[1:]
	case '+':
		s = s[1:]
	}

	digits, exp10, ok := splitDecimal(s)
	if !ok {
		return Decimal{}, bad
	}

	return scaleDigits(k, digits, exp10, neg)
}

func clip(s string) string {
	const limit = 64
	if len(s) > limit {
		return s[:limit] + "..."
	}

	return s
}

// splitDecimal returns the significant digits (no leading zeros) and the power of ten that scales them.
func splitDecimal(s string) (string, int, bool) {
	mant, expText, hasExp := strings.Cut(strings.ToLower(s), "e")
	intPart, frac, _ := strings.Cut(mant, ".")

	if intPart == "" && frac == "" || !allDigits(intPart) || !allDigits(frac) {
		return "", 0, false
	}

	exp := 0

	if hasExp {
		e, ok := parseExp(expText)
		if !ok {
			return "", 0, false
		}

		exp = e
	}

	return strings.TrimLeft(intPart+frac, "0"), exp - len(frac), true
}

func allDigits(s string) bool {
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}

	return true
}

func parseExp(s string) (int, bool) {
	sign := 1

	if s != "" && (s[0] == '-' || s[0] == '+') {
		if s[0] == '-' {
			sign = -1
		}

		s = s[1:]
	}

	if s == "" || len(s) > 5 || !allDigits(s) {
		return 0, false
	}

	e := 0
	for i := range len(s) {
		e = e*decimalBase + int(s[i]-'0')
	}

	if e > maxDecimalExp {
		return 0, false
	}

	return sign * e, true
}

func scaleDigits(k Kind, digits string, exp10 int, neg bool) (Decimal, error) {
	if digits == "" {
		return Decimal{n: new(big.Int), kind: k}, nil
	}

	n, ok := new(big.Int).SetString(digits, decimalBase)
	if !ok {
		return Decimal{}, fmt.Errorf("%w: invalid %s value %q", ErrDecimal, k.RESTName(), clip(digits))
	}

	shift := exp10 + decScale(k)

	switch {
	case shift >= 0:
		if len(digits)+shift > maxDecimalDigits {
			return Decimal{}, decOverflow(k, "%s", clip(digits))
		}

		n.Mul(n, pow10(shift))
	case -shift > len(digits)+1:
		n.SetInt64(0)
	default:
		n = divRound(n, pow10(-shift))
	}

	if neg {
		n.Neg(n)
	}

	return newDecimal(k, n, "%s", clip(digits))
}

// DecimalFromInt converts an INT64 exactly.
func DecimalFromInt(k Kind, v int64) (Decimal, error) {
	n := new(big.Int).Mul(big.NewInt(v), pow10(decScale(k)))

	return newDecimal(k, n, "%d", v)
}

// DecimalFromFloat converts a FLOAT64 exactly then rounds half away from zero; NaN and infinities are errors.
func DecimalFromFloat(k Kind, f float64) (Decimal, error) {
	k = decKind(k)
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return Decimal{}, fmt.Errorf("%w: cannot convert %v to %s", ErrDecimal, f, k.RESTName())
	}

	r := new(big.Rat).SetFloat64(f)
	n := divRound(new(big.Int).Mul(r.Num(), pow10(decScale(k))), r.Denom())

	return newDecimal(k, n, "%v", f)
}

// Kind reports NUMERIC or BIGNUMERIC.
func (d Decimal) Kind() Kind { return decKind(d.kind) }

// Sign returns -1, 0 or +1.
func (d Decimal) Sign() int { return d.unscaled().Sign() }

// String renders the value as BigQuery prints it: no exponent, no trailing zeros, never "-0".
func (d Decimal) String() string {
	n := d.unscaled()
	if n.Sign() == 0 {
		return "0"
	}

	sc := decScale(d.kind)
	s := new(big.Int).Abs(n).String()

	if len(s) <= sc {
		s = strings.Repeat("0", sc-len(s)+1) + s
	}

	out := trimDecimal(s[:len(s)-sc] + "." + s[len(s)-sc:])
	if n.Sign() < 0 {
		return "-" + out
	}

	return out
}

// Float64 returns the nearest FLOAT64.
func (d Decimal) Float64() float64 {
	f, _ := new(big.Rat).SetFrac(d.unscaled(), pow10(decScale(d.kind))).Float64()

	return f
}

// Int64 rounds half away from zero to an INT64 and fails when out of range.
func (d Decimal) Int64() (int64, error) {
	n := divRound(d.unscaled(), pow10(decScale(d.kind)))
	if !n.IsInt64() {
		return 0, fmt.Errorf("%w: INT64 out of range: %s", ErrDecimal, d.String())
	}

	return n.Int64(), nil
}

// As converts to the target kind, rounding half away from zero when narrowing.
func (d Decimal) As(k Kind) (Decimal, error) {
	k = decKind(k)
	from, to := decScale(d.kind), decScale(k)

	switch {
	case from == to:
		return newDecimal(k, d.unscaled(), "%s", d.String())
	case to > from:
		return newDecimal(k, new(big.Int).Mul(d.unscaled(), pow10(to-from)), "%s", d.String())
	}

	return newDecimal(k, divRound(d.unscaled(), pow10(from-to)), "%s", d.String())
}

func widest(a, b Decimal) (Kind, *big.Int, *big.Int, error) {
	k := KindNumeric
	if a.kind == KindBigNumeric || b.kind == KindBigNumeric {
		k = KindBigNumeric
	}

	x, err := a.As(k)
	if err != nil {
		return k, nil, nil, err
	}

	y, err := b.As(k)
	if err != nil {
		return k, nil, nil, err
	}

	return k, x.unscaled(), y.unscaled(), nil
}

// Cmp compares two values of any decimal kind.
func (d Decimal) Cmp(o Decimal) int {
	_, x, y, err := widest(d, o)
	if err != nil {
		return d.unscaled().Sign() - o.unscaled().Sign()
	}

	return x.Cmp(y)
}

// Add returns d + o.
func (d Decimal) Add(o Decimal) (Decimal, error) {
	k, x, y, err := widest(d, o)
	if err != nil {
		return Decimal{}, err
	}

	return newDecimal(k, new(big.Int).Add(x, y), "%s + %s", d.String(), o.String())
}

// Sub returns d - o.
func (d Decimal) Sub(o Decimal) (Decimal, error) {
	k, x, y, err := widest(d, o)
	if err != nil {
		return Decimal{}, err
	}

	return newDecimal(k, new(big.Int).Sub(x, y), "%s - %s", d.String(), o.String())
}

// Mul returns d * o rounded to the result scale.
func (d Decimal) Mul(o Decimal) (Decimal, error) {
	k, x, y, err := widest(d, o)
	if err != nil {
		return Decimal{}, err
	}

	p := divRound(new(big.Int).Mul(x, y), pow10(decScale(k)))

	return newDecimal(k, p, "%s * %s", d.String(), o.String())
}

// Quo returns d / o rounded to the result scale; a zero divisor is an error.
func (d Decimal) Quo(o Decimal) (Decimal, error) {
	k, x, y, err := widest(d, o)
	if err != nil {
		return Decimal{}, err
	}

	if y.Sign() == 0 {
		return Decimal{}, fmt.Errorf("%w: division by zero: %s / %s", ErrDecimal, d.String(), o.String())
	}

	q := divRound(new(big.Int).Mul(x, pow10(decScale(k))), y)

	return newDecimal(k, q, "%s / %s", d.String(), o.String())
}

// Mod returns the remainder of d / o with the sign of d; a zero divisor is an error.
func (d Decimal) Mod(o Decimal) (Decimal, error) {
	k, x, y, err := widest(d, o)
	if err != nil {
		return Decimal{}, err
	}

	if y.Sign() == 0 {
		return Decimal{}, fmt.Errorf("%w: division by zero: MOD(%s, %s)", ErrDecimal, d.String(), o.String())
	}

	return newDecimal(k, new(big.Int).Rem(x, y), "MOD(%s, %s)", d.String(), o.String())
}

// Neg returns -d.
func (d Decimal) Neg() (Decimal, error) {
	return newDecimal(d.kind, new(big.Int).Neg(d.unscaled()), "-%s", d.String())
}

// Abs returns |d|.
func (d Decimal) Abs() (Decimal, error) {
	return newDecimal(d.kind, new(big.Int).Abs(d.unscaled()), "ABS(%s)", d.String())
}

// Round rounds to the given number of fractional digits (negative rounds integer digits).
func (d Decimal) Round(digits int, mode RoundMode) (Decimal, error) {
	drop := decScale(d.kind) - digits
	if drop <= 0 {
		return d, nil
	}

	if drop > maxDecimalDigits {
		return Decimal{n: new(big.Int), kind: decKind(d.kind)}, nil
	}

	unit := pow10(drop)
	n := d.unscaled()

	var q *big.Int

	if mode == RoundHalfEven {
		q = divRoundEven(n, unit)
	} else {
		q = divRound(n, unit)
	}

	return newDecimal(d.kind, q.Mul(q, unit), "ROUND(%s, %d)", d.String(), digits)
}

func divRoundEven(num, den *big.Int) *big.Int {
	q, r := new(big.Int).QuoRem(new(big.Int).Abs(num), den, new(big.Int))
	switch r.Lsh(r, 1).Cmp(den) {
	case 1:
		q.Add(q, big.NewInt(1))
	case 0:
		if q.Bit(0) == 1 {
			q.Add(q, big.NewInt(1))
		}
	}

	if num.Sign() < 0 {
		q.Neg(q)
	}

	return q
}

// Trunc drops fractional digits beyond the given count toward zero.
func (d Decimal) Trunc(digits int) Decimal {
	drop := decScale(d.kind) - digits
	if drop <= 0 {
		return d
	}

	if drop > maxDecimalDigits {
		return Decimal{n: new(big.Int), kind: decKind(d.kind)}
	}

	unit := pow10(drop)
	q := new(big.Int).Quo(d.unscaled(), unit)

	return Decimal{n: q.Mul(q, unit), kind: decKind(d.kind)}
}

// Floor returns the largest integer value not greater than d.
func (d Decimal) Floor() (Decimal, error) {
	t := d.Trunc(0)
	if d.Sign() < 0 && t.Cmp(d) != 0 {
		return t.Sub(unitDecimal(t.kind))
	}

	return t, nil
}

// Ceil returns the smallest integer value not less than d.
func (d Decimal) Ceil() (Decimal, error) {
	t := d.Trunc(0)
	if d.Sign() > 0 && t.Cmp(d) != 0 {
		return t.Add(unitDecimal(t.kind))
	}

	return t, nil
}

func unitDecimal(k Kind) Decimal {
	return Decimal{n: pow10(decScale(k)), kind: decKind(k)}
}

// Constrain applies NUMERIC(p, s) or BIGNUMERIC(p, s): round to s digits, fail when more than p digits remain.
func (d Decimal) Constrain(p, s int) (Decimal, error) {
	k := decKind(d.kind)
	if err := checkDecimalParams(k, p, s); err != nil {
		return Decimal{}, err
	}

	r, err := d.Round(s, RoundHalfAwayFromZero)
	if err != nil {
		return Decimal{}, err
	}

	lim := pow10(p - s + decScale(k))
	if new(big.Int).Abs(r.unscaled()).Cmp(lim) >= 0 {
		return Decimal{}, fmt.Errorf("%w: %s out of range for %s(%d, %d)", ErrDecimal, d.String(), k.RESTName(), p, s)
	}

	return r, nil
}

const (
	sqlTextPrefix = '#'
	sqlTextBytes  = 32
	sqlTextLen    = 1 + 2*sqlTextBytes
	narrowShift   = bigNumericScale - numericScale
)

// SQLText encodes d for storage in SQLite: '#' plus 64 hex digits of (value*10^38 + 2^255),
// so byte order equals numeric order and equal values share one encoding for either kind.
func (d Decimal) SQLText() string {
	v := new(big.Int).Set(d.unscaled())
	if decKind(d.kind) == KindNumeric {
		v.Mul(v, pow10(narrowShift))
	}

	v.Add(v, new(big.Int).Lsh(big.NewInt(1), bigNumericBits))

	buf := make([]byte, sqlTextLen)
	buf[0] = sqlTextPrefix
	hex.Encode(buf[1:], v.FillBytes(make([]byte, sqlTextBytes)))

	return string(buf)
}

// IsSQLText reports whether s is an encoding produced by SQLText.
func IsSQLText(s string) bool {
	if len(s) != sqlTextLen || s[0] != sqlTextPrefix {
		return false
	}

	for i := 1; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' && c < 'a' || c > 'f' {
			return false
		}
	}

	return true
}

// DecodeSQLText reverses SQLText into a value of kind k, rounding when a wider value is narrowed.
func DecodeSQLText(s string, k Kind) (Decimal, error) {
	if !IsSQLText(s) {
		return Decimal{}, fmt.Errorf("%w: malformed %s value", ErrDecimal, decKind(k).RESTName())
	}

	raw, err := hex.DecodeString(s[1:])
	if err != nil {
		return Decimal{}, fmt.Errorf("%w: malformed %s value", ErrDecimal, decKind(k).RESTName())
	}

	v := new(big.Int).SetBytes(raw)
	v.Sub(v, new(big.Int).Lsh(big.NewInt(1), bigNumericBits))

	if decKind(k) == KindNumeric {
		v = divRound(v, pow10(narrowShift))
	}

	return newDecimal(k, v, "%s", s)
}

// ParseNumericText parses the PARSE_NUMERIC and PARSE_BIGNUMERIC string form: commas before the point, one sign
// before or after the number, whitespace around the sign and the number, optional exponent.
func ParseNumericText(k Kind, s string) (Decimal, error) {
	bad := fmt.Errorf("%w: invalid %s value %q", ErrDecimal, decKind(k).RESTName(), clip(s))
	if len(s) > maxDecimalText {
		return Decimal{}, bad
	}

	sign, body := splitSign(s)
	if body == "" || body[0] == '+' || body[0] == '-' || strings.ContainsAny(body, numericBlanks) {
		return Decimal{}, bad
	}

	whole, rest := body, ""
	if i := strings.IndexAny(body, ".eE"); i >= 0 {
		whole, rest = body[:i], body[i:]
	}

	if strings.HasPrefix(whole, ",") || strings.Contains(rest, ",") {
		return Decimal{}, bad
	}

	d, err := ParseDecimal(k, strings.ReplaceAll(whole, ",", "")+rest)
	if err != nil {
		return Decimal{}, bad
	}

	if sign == '-' {
		return d.Neg()
	}

	return d, nil
}

const numericBlanks = " \t\r\n"

// splitSign removes one leading or trailing sign and the whitespace around it.
func splitSign(s string) (byte, string) {
	s = strings.Trim(s, numericBlanks)
	if s == "" {
		return 0, s
	}

	if s[0] == '+' || s[0] == '-' {
		return s[0], strings.Trim(s[1:], numericBlanks)
	}

	last := s[len(s)-1]
	if len(s) > 1 && (last == '+' || last == '-') && s[len(s)-2] != 'e' && s[len(s)-2] != 'E' {
		return last, strings.Trim(s[:len(s)-1], numericBlanks)
	}

	return 0, s
}

func decodeScaled(s string) (*big.Int, error) {
	if !IsSQLText(s) {
		return nil, fmt.Errorf("%w: malformed decimal value", ErrDecimal)
	}

	raw, err := hex.DecodeString(s[1:])
	if err != nil {
		return nil, fmt.Errorf("%w: malformed decimal value", ErrDecimal)
	}

	v := new(big.Int).SetBytes(raw)

	return v.Sub(v, new(big.Int).Lsh(big.NewInt(1), bigNumericBits)), nil
}

func decimalFromScaled(k Kind, v *big.Int, expr string, args ...any) (Decimal, error) {
	if decKind(k) == KindNumeric {
		v = divRound(v, pow10(narrowShift))
	}

	return newDecimal(k, v, expr, args...)
}

func checkDecimalParams(k Kind, p, s int) error {
	maxScale, maxInt := numericScale, paramNumericInt

	if decKind(k) == KindBigNumeric {
		maxScale, maxInt = bigNumericScale, paramBigInt
	}

	if s < 0 || s > maxScale || p < max(1, s) || p > s+maxInt {
		return fmt.Errorf("%w: invalid %s(%d, %d)", ErrDecimal, decKind(k).RESTName(), p, s)
	}

	return nil
}
