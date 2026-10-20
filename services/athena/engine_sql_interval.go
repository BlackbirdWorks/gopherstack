package athena

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var (
	dayTimeInterval  = regexp.MustCompile(`^([+-]?)(\d+) (\d{1,2}):(\d{1,2}):(\d{1,2})$`)
	yearMonthInteval = regexp.MustCompile(`^([+-]?)(\d+)-(\d{1,2})$`)
)

const (
	secondsPerMinute = 60
	secondsPerHour   = 3600
	secondsPerDay    = 86400
)

func singularUnit(w string) string { return strings.TrimSuffix(strings.ToLower(w), "s") }

func isIntervalUnit(w string) bool {
	switch singularUnit(w) {
	case unitSecond, unitMinute, unitHour, unitDay, unitWeek, unitMonth, unitQuarter, unitYear:
		return true
	default:
		return false
	}
}

// intervalAmount converts an INTERVAL literal into a date_add unit and count.
func intervalAmount(lit, from, to string) (string, int64, error) {
	lit = strings.TrimSpace(lit)

	switch {
	case to == "":
		n, err := strconv.ParseInt(lit, 10, 64)
		if err != nil {
			return "", 0, badInterval("Invalid INTERVAL value '%s'", lit)
		}

		return from, n, nil
	case from == unitYear && to == unitMonth:
		return yearMonthAmount(lit)
	case from == unitDay && to == unitSecond:
		return dayTimeAmount(lit)
	default:
		return "", 0, badInterval("INTERVAL %s TO %s is not supported", strings.ToUpper(from), strings.ToUpper(to))
	}
}

func badInterval(format string, args ...any) error {
	return &syntaxError{"line 1:1: " + fmt.Sprintf(format, args...)}
}

func signed(sign string, n int64) int64 {
	if sign == "-" {
		return -n
	}

	return n
}

func yearMonthAmount(lit string) (string, int64, error) {
	m := yearMonthInteval.FindStringSubmatch(lit)
	if m == nil {
		return "", 0, badInterval("Invalid INTERVAL YEAR TO MONTH value '%s'", lit)
	}

	y, _ := strconv.ParseInt(m[2], 10, 64)
	mo, _ := strconv.ParseInt(m[3], 10, 64)

	return unitMonth, signed(m[1], y*monthsPerYear+mo), nil
}

func dayTimeAmount(lit string) (string, int64, error) {
	m := dayTimeInterval.FindStringSubmatch(lit)
	if m == nil {
		return "", 0, badInterval("Invalid INTERVAL DAY TO SECOND value '%s'", lit)
	}

	d, _ := strconv.ParseInt(m[2], 10, 64)
	h, _ := strconv.ParseInt(m[3], 10, 64)
	mi, _ := strconv.ParseInt(m[4], 10, 64)
	s, _ := strconv.ParseInt(m[5], 10, 64)

	return unitSecond, signed(m[1], d*secondsPerDay+h*secondsPerHour+mi*secondsPerMinute+s), nil
}

// intervalSpec scans `'n' UNIT [TO UNIT]` after the INTERVAL word at toks[i].
func intervalSpec(toks []token, i int) (int, string, string, int) {
	lit := nextSig(toks, i)
	end := lit

	u := nextSig(toks, end)
	if u < 0 || toks[u].kind != tkWord || !isIntervalUnit(toks[u].text) {
		return lit, "", "", end
	}

	from, to := singularUnit(toks[u].text), ""
	end = u

	if t := nextSig(toks, end); t >= 0 && toks[t].isWord("to") {
		if u2 := nextSig(toks, t); u2 >= 0 && toks[u2].kind == tkWord && isIntervalUnit(toks[u2].text) {
			to, end = singularUnit(toks[u2].text), u2
		}
	}

	return lit, from, to, end
}

// interval rewrites `operand (+|-) INTERVAL 'n' UNIT [TO UNIT]` at toks[i] onto
// athena_date_add; the operator and left operand are already in out.
func (x *translator) interval(toks []token, i int, out []token) ([]token, int) {
	lit, from, to, end := intervalSpec(toks, i)

	opIdx := lastSig(out)
	if from == "" || opIdx < 0 || (!out[opIdx].isPunct("+") && !out[opIdx].isPunct("-")) {
		x.err = &syntaxError{"line 1:1: INTERVAL is only supported as the right operand of + or -"}

		return out, end
	}

	unit, n, err := intervalAmount(stringLiteral(toks[lit]), from, to)
	if err != nil {
		x.err = err

		return out, end
	}

	if out[opIdx].isPunct("-") {
		n = -n
	}

	left := out[:opIdx]

	start := primaryStart(left)
	if start < 0 {
		x.err = &syntaxError{"line 1:1: INTERVAL arithmetic needs a date or timestamp operand"}

		return out, end
	}

	operand := strings.TrimSpace(joinTokens(left[start:]))
	res := token{kind: tkWord, text: fmt.Sprintf("athena_date_add('%s', %d, %s)", unit, n, operand)}

	return append(left[:start:start], res), end
}
