package bqsql

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
)

const (
	fmtDecimalTag     = "\x01"
	defaultFormatPrec = 6
	sciLowExp         = -4
	minExpDigits      = 2
	maxFormatPrec     = 1000
)

type decFormatSpec struct {
	width int
	prec  int
	minus bool
	plus  bool
	space bool
	zero  bool
	sharp bool
}

func parseDecimalFlags(flags string) decFormatSpec {
	sp := decFormatSpec{prec: defaultFormatPrec}

	i := 0
	for ; i < len(flags) && strings.IndexByte("-+ #0", flags[i]) >= 0; i++ {
		switch flags[i] {
		case '-':
			sp.minus = true
		case '+':
			sp.plus = true
		case ' ':
			sp.space = true
		case '#':
			sp.sharp = true
		case '0':
			sp.zero = true
		}
	}

	w, rest, hasDot := strings.Cut(flags[i:], ".")
	sp.width, _ = strconv.Atoi(w)

	if hasDot {
		sp.prec, _ = strconv.Atoi(rest)
	}

	return sp
}

// formatDecimal prints one FORMAT argument tagged by dec_fmt; %f, %e and %g are computed on exact digits.
func formatDecimal(spec byte, flags, tagged string) (string, error) {
	kind := KindNumeric
	if strings.HasPrefix(tagged, "B") {
		kind = KindBigNumeric
	}

	d, err := ParseDecimal(kind, tagged[1:])
	if err != nil {
		return "", decFail(err)
	}

	sp := parseDecimalFlags(flags)

	switch spec {
	case 's':
		return fmt.Sprintf("%"+flags+"s", d.String()), nil
	case 't', 'T':
		txt := d.String()
		if !strings.Contains(txt, ".") {
			txt += ".0"
		}

		if spec == 'T' {
			txt = kind.RESTName() + ` "` + txt + `"`
		}

		return fmt.Sprintf("%"+flags+"s", txt), nil
	case 'f', 'F':
		body, ferr := fixedBody(d, min(sp.prec, maxFormatPrec), sp.sharp)

		return padDecimal(d.Sign() < 0, body, sp), ferr
	case 'e', 'E':
		return padDecimal(d.Sign() < 0, sciBody(d, sp.prec, sp.sharp, spec == 'E'), sp), nil
	case 'g', 'G':
		return padDecimal(d.Sign() < 0, generalBody(d, sp, spec == 'G'), sp), nil
	}

	return "", errBQ("Invalid format specifier %%%c for %s", spec, kind.RESTName())
}

func padDecimal(neg bool, body string, sp decFormatSpec) string {
	sign := ""

	switch {
	case neg:
		sign = "-"
	case sp.plus:
		sign = "+"
	case sp.space:
		sign = " "
	}

	pad := max(min(sp.width, maxFormatPrec)-len(sign)-len(body), 0)

	switch {
	case sp.minus:
		return sign + body + strings.Repeat(" ", pad)
	case sp.zero:
		return sign + strings.Repeat("0", pad) + body
	}

	return strings.Repeat(" ", pad) + sign + body
}

// fixedBody renders |d| in decimal notation with exactly prec fractional digits, rounding half away from zero.
func fixedBody(d Decimal, prec int, sharp bool) (string, error) {
	r, err := d.Round(prec, RoundHalfAwayFromZero)
	if err != nil {
		return "", decFail(err)
	}

	abs, _ := r.Abs()
	whole, frac, _ := strings.Cut(abs.String(), ".")
	frac += strings.Repeat("0", max(prec-len(frac), 0))

	switch {
	case prec > 0:
		return whole + "." + frac, nil
	case sharp:
		return whole + ".", nil
	}

	return whole, nil
}

// sciDigits returns prec+1 significant digits of |d| rounded half away from zero and the decimal exponent.
func sciDigits(d Decimal, prec int) (string, int) {
	n := new(big.Int).Abs(d.unscaled())
	want := prec + 1

	if n.Sign() == 0 {
		return strings.Repeat("0", want), 0
	}

	sig := n.String()
	exp := len(sig) - 1 - decScale(d.kind)

	if len(sig) > want {
		q := divRound(n, pow10(len(sig)-want)).String()
		if len(q) > want {
			q = q[:want]
			exp++
		}

		return q, exp
	}

	return sig + strings.Repeat("0", want-len(sig)), exp
}

func sciBody(d Decimal, prec int, sharp, upper bool) string {
	digits, exp := sciDigits(d, prec)

	return sciText(digits, exp, sharp, upper)
}

func sciText(digits string, exp int, sharp, upper bool) string {
	body := digits[:1]
	if len(digits) > 1 || sharp {
		body += "." + digits[1:]
	}

	mark, sign := "e", "+"
	if upper {
		mark = "E"
	}

	if exp < 0 {
		sign, exp = "-", -exp
	}

	e := strconv.Itoa(exp)
	if len(e) < minExpDigits {
		e = strings.Repeat("0", minExpDigits-len(e)) + e
	}

	return body + mark + sign + e
}

func generalBody(d Decimal, sp decFormatSpec, upper bool) string {
	p := max(sp.prec, 1)
	digits, exp := sciDigits(d, p-1)

	if exp < sciLowExp || exp >= p {
		if !sp.sharp {
			digits = strings.TrimRight(digits, "0")
			if digits == "" {
				digits = "0"
			}
		}

		return sciText(digits, exp, sp.sharp, upper)
	}

	body, _ := fixedBody(d, p-1-exp, sp.sharp)
	if sp.sharp || !strings.Contains(body, ".") {
		return body
	}

	return strings.TrimSuffix(strings.TrimRight(body, "0"), ".")
}
