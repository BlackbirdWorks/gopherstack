// Package dynamonum implements DynamoDB's exact decimal Number type: up to 38
// significant digits, magnitude 1E-130..9.9999999999999999999999999999999999999E+125.
package dynamonum

import (
	"errors"
	"math/big"
	"strconv"
)

const (
	// MaxDigits is the maximum number of significant digits.
	MaxDigits = 38
	maxExp    = 125
	minExp    = -130
	fastDigit = 18
	base10    = 10
)

var (
	// ErrInvalid is returned for text that is not a decimal number.
	ErrInvalid = errors.New("not a valid number")
	// ErrOverflow is returned when the magnitude exceeds the supported range.
	ErrOverflow = errors.New( //nolint:staticcheck // verbatim AWS message
		"Number overflow. Attempting to store a number with magnitude larger than supported range")
	// ErrUnderflow is returned when a non-zero magnitude is below the supported range.
	ErrUnderflow = errors.New( //nolint:staticcheck // verbatim AWS message
		"Number underflow. Attempting to store a number with magnitude smaller than supported range")
	// ErrTooManyDigits is returned when a value needs more than 38 significant digits.
	ErrTooManyDigits = errors.New( //nolint:staticcheck // verbatim AWS message
		"Attempting to store more than 38 significant digits in a Number")
)

// Num is a normalised decimal: value = (+/-) d[:n] * 10^scale, with no leading or
// trailing zero digit. Non-zero integers of at most 18 digits are held only in
// small (the fast path); the digit fields are then unset. The zero value is zero.
// Equal values have equal representations, so Num is comparable with ==.
type Num struct {
	small int64
	d     [MaxDigits]byte
	scale int16
	n     uint8
	neg   bool
}

// IsZero reports whether the value is zero.
func (x Num) IsZero() bool { return x.n == 0 && x.small == 0 }

// wide returns the digit form of x.
func (x Num) wide() Num {
	if x.small == 0 {
		return x
	}

	return fromInt64Wide(x.small)
}

// compact converts an integer of at most fastDigit digits to the small form.
func (x Num) compact() Num {
	if x.n == 0 {
		return Num{}
	}

	if v, ok := x.int64Fast(); ok {
		return Num{small: v}
	}

	return x
}

// mag is the decimal exponent of the most significant digit.
func (x Num) mag() int { return int(x.scale) + int(x.n) - 1 }

// Parse parses and range-checks a DynamoDB number string.
func Parse(s string) (Num, error) {
	if plainInt(s) {
		return parsePlainInt(s), nil
	}

	return parseGeneral(s)
}

// mantissa accumulates significant digits; trailing zeros stay pending (zeros).
type mantissa struct {
	cnt     int
	zeros   int
	frac    int
	buf     [MaxDigits]byte
	leading bool
	extra   bool
}

func (m *mantissa) add(c byte) {
	if m.leading && c == '0' {
		return
	}

	m.leading = false

	if c == '0' {
		m.zeros++

		return
	}

	if m.cnt+m.zeros >= MaxDigits {
		m.extra = true

		return
	}

	for ; m.zeros > 0; m.zeros-- {
		m.buf[m.cnt] = '0'
		m.cnt++
	}

	m.buf[m.cnt] = c
	m.cnt++
}

// scan consumes [digits][.digits] from s and returns the index where it stopped.
func (m *mantissa) scan(s string, i int) (int, bool) {
	pointSeen, anyDigit := false, false

	for ; i < len(s); i++ {
		c := s[i]

		switch {
		case c == '.':
			if pointSeen {
				return i, false
			}

			pointSeen = true
		case c >= '0' && c <= '9':
			anyDigit = true

			if pointSeen {
				m.frac++
			}

			m.add(c)
		default:
			return i, anyDigit
		}
	}

	return i, anyDigit
}

func parseGeneral(s string) (Num, error) {
	var x Num

	i := 0
	if i < len(s) && (s[i] == '-' || s[i] == '+') {
		x.neg = s[i] == '-'
		i++
	}

	m := mantissa{leading: true}

	i, ok := m.scan(s, i)
	if !ok {
		return Num{}, ErrInvalid
	}

	exp := 0

	if i < len(s) {
		if s[i] != 'e' && s[i] != 'E' {
			return Num{}, ErrInvalid
		}

		e, err := parseExp(s[i+1:])
		if err != nil {
			return Num{}, err
		}

		exp = e
	}

	if m.extra {
		return Num{}, ErrTooManyDigits
	}

	if m.cnt == 0 {
		return Num{}, nil
	}

	copy(x.d[:], m.buf[:m.cnt])
	x.n = uint8(m.cnt) //nolint:gosec // cnt <= MaxDigits
	x.scale = clampScale(exp - m.frac + m.zeros)

	if err := x.check(); err != nil {
		return Num{}, err
	}

	return x.compact(), nil
}

// parsePlainInt handles plainInt text (<= 38 digits, within range by construction).
func parsePlainInt(s string) Num {
	var x Num

	if s == "0" {
		return x
	}

	neg := s[0] == '-'
	if neg {
		s = s[1:]
	}

	if len(s) <= fastDigit {
		var v int64

		for i := range len(s) {
			v = v*base10 + int64(s[i]-'0')
		}

		if neg {
			v = -v
		}

		return Num{small: v}
	}

	x.neg = neg
	end := len(s)
	for s[end-1] == '0' {
		end--
	}

	copy(x.d[:], s[:end])
	x.n = uint8(end)              //nolint:gosec // end <= MaxDigits
	x.scale = int16(len(s) - end) //nolint:gosec // < MaxDigits

	return x
}

func clampScale(v int) int16 {
	const lim = 1 << 14
	if v > lim {
		v = lim
	}

	if v < -lim {
		v = -lim
	}

	return int16(v)
}

func parseExp(s string) (int, error) {
	if s == "" {
		return 0, ErrInvalid
	}

	neg := false
	if s[0] == '-' || s[0] == '+' {
		neg = s[0] == '-'
		s = s[1:]
	}

	if s == "" {
		return 0, ErrInvalid
	}

	v := 0

	for i := range len(s) {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, ErrInvalid
		}

		if v < 1<<20 {
			v = v*base10 + int(c-'0')
		}
	}

	if neg {
		v = -v
	}

	return v, nil
}

func (x Num) check() error {
	if x.n == 0 {
		return nil
	}

	if m := x.mag(); m > maxExp {
		return ErrOverflow
	} else if m < minExp {
		return ErrUnderflow
	}

	return nil
}

// FromInt64 converts an integer.
func FromInt64(v int64) Num {
	if v > -smallLimit && v < smallLimit {
		return Num{small: v}
	}

	return fromInt64Wide(v)
}

const smallLimit = 1_000_000_000_000_000_000

func fromInt64Wide(v int64) Num {
	var x Num

	if v == 0 {
		return x
	}

	u := uint64(v) //nolint:gosec // sign handled below
	if v < 0 {
		x.neg = true
		u = -u
	}

	var tmp [20]byte

	n := 0

	for u > 0 {
		tmp[n] = byte(u%base10) + '0'
		u /= base10
		n++
	}

	scale := 0

	for k := 0; k < n && tmp[k] == '0'; k++ {
		scale++
	}

	x.n = uint8(n - scale) //nolint:gosec // n <= 19
	for k := range int(x.n) {
		x.d[k] = tmp[n-1-k]
	}

	x.scale = int16(scale)

	return x
}

// FromFloat converts a float64 via its shortest decimal form; false if not finite or out of range.
func FromFloat(f float64) (Num, bool) {
	x, err := Parse(strconv.FormatFloat(f, 'e', -1, 64))

	return x, err == nil
}

// Cmp returns -1, 0 or +1.
func (x Num) Cmp(y Num) int {
	if x.small != 0 && y.small != 0 {
		switch {
		case x.small < y.small:
			return -1
		case x.small > y.small:
			return 1
		}

		return 0
	}

	x, y = x.wide(), y.wide()

	switch {
	case x.n == 0 && y.n == 0:
		return 0
	case x.n == 0:
		if y.neg {
			return 1
		}

		return -1
	case y.n == 0:
		if x.neg {
			return -1
		}

		return 1
	case x.neg != y.neg:
		if x.neg {
			return -1
		}

		return 1
	}

	c := x.cmpMag(y)
	if x.neg {
		return -c
	}

	return c
}

func (x Num) cmpMag(y Num) int {
	if mx, my := x.mag(), y.mag(); mx != my {
		if mx < my {
			return -1
		}

		return 1
	}

	m := min(x.n, y.n)
	for i := range m {
		if x.d[i] != y.d[i] {
			if x.d[i] < y.d[i] {
				return -1
			}

			return 1
		}
	}

	switch {
	case x.n < y.n:
		return -1
	case x.n > y.n:
		return 1
	}

	return 0
}

// String renders canonical plain decimal text (no exponent, no redundant zeros).
func (x Num) String() string {
	if x.small != 0 {
		return strconv.FormatInt(x.small, base10)
	}

	if x.n == 0 {
		return "0"
	}

	var b []byte

	if x.neg {
		b = append(b, '-')
	}

	n, sc := int(x.n), int(x.scale)

	switch {
	case sc >= 0:
		b = append(b, x.d[:n]...)
		for range sc {
			b = append(b, '0')
		}
	case -sc >= n:
		b = append(b, "0."...)
		for range -sc - n {
			b = append(b, '0')
		}

		b = append(b, x.d[:n]...)
	default:
		b = append(b, x.d[:n+sc]...)
		b = append(b, '.')
		b = append(b, x.d[n+sc:n]...)
	}

	return string(b)
}

// Canonical returns s with redundant zeros, exponent and sign normalised, as DynamoDB
// stores numbers. Invalid text and text with meaningless leading zeros ("007") is
// returned unchanged so validation can reject it.
func Canonical(s string) string {
	if plainInt(s) {
		return s
	}

	body := s
	if body != "" && (body[0] == '-' || body[0] == '+') {
		body = body[1:]
	}

	if len(body) > 1 && body[0] == '0' && body[1] >= '0' && body[1] <= '9' {
		return s
	}

	x, err := Parse(s)
	if err != nil {
		return s
	}

	if c := x.String(); c != s {
		return c
	}

	return s
}

// plainInt reports whether s is already a canonical integer ("0", "-12", "340").
func plainInt(s string) bool {
	if s == "0" {
		return true
	}

	i := 0
	if s != "" && s[0] == '-' {
		i = 1
	}

	if i >= len(s) || s[i] < '1' || s[i] > '9' || len(s)-i > MaxDigits {
		return false
	}

	for ; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}

	return true
}

func (x Num) int64Fast() (int64, bool) {
	if x.small != 0 {
		return x.small, true
	}

	if x.scale < 0 || int(x.n)+int(x.scale) > fastDigit {
		return 0, false
	}

	var v int64

	for i := range int(x.n) {
		v = v*base10 + int64(x.d[i]-'0')
	}

	for range x.scale {
		v *= base10
	}

	if x.neg {
		v = -v
	}

	return v, true
}

func (x Num) toBig() (*big.Int, int) {
	x = x.wide()
	z := new(big.Int)
	z.SetString(string(x.d[:x.n]), base10)

	if x.neg {
		z.Neg(z)
	}

	return z, int(x.scale)
}

// Add returns x+y, or an error when the result leaves the supported range or precision.
func Add(x, y Num) (Num, error) {
	if a, ok := x.int64Fast(); ok {
		if b, ok2 := y.int64Fast(); ok2 {
			return FromInt64(a + b), nil
		}
	}

	return addBig(x, y)
}

// Sub returns x-y.
func Sub(x, y Num) (Num, error) {
	if y.small != 0 {
		y.small = -y.small
	} else if y.n != 0 {
		y.neg = !y.neg
	}

	return Add(x, y)
}

func addBig(x, y Num) (Num, error) {
	if x.IsZero() {
		return y, nil
	}

	if y.IsZero() {
		return x, nil
	}

	a, sa := x.toBig()
	b, sb := y.toBig()
	s := min(sa, sb)

	a.Mul(a, pow10(sa-s))
	b.Mul(b, pow10(sb-s))
	a.Add(a, b)

	return fromBig(a, s)
}

func pow10(n int) *big.Int {
	return new(big.Int).Exp(big.NewInt(base10), big.NewInt(int64(n)), nil)
}

func fromBig(z *big.Int, scale int) (Num, error) {
	var x Num

	if z.Sign() == 0 {
		return x, nil
	}

	x.neg = z.Sign() < 0
	str := new(big.Int).Abs(z).String()

	end := len(str)
	for end > 1 && str[end-1] == '0' {
		end--
		scale++
	}

	if end > MaxDigits {
		return Num{}, ErrTooManyDigits
	}

	copy(x.d[:], str[:end])
	x.n = uint8(end) //nolint:gosec // end <= MaxDigits
	x.scale = clampScale(scale)

	if err := x.check(); err != nil {
		return Num{}, err
	}

	return x.compact(), nil
}
