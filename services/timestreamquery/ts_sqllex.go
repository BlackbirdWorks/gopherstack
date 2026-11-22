package timestreamquery

import (
	"errors"
	"strings"
	"unicode"
)

type tokKind uint8

const (
	tkEOF tokKind = iota
	tkIdent
	tkQuoted
	tkString
	tkNumber
	tkDuration
	tkOp
	tkVar
)

const (
	maxSQLTokens = 100000
	maxSQLDepth  = 200
)

var errSQLSyntax = errors.New("unsupported sql syntax")

type token struct {
	text       string
	start, end int
	kind       tokKind
}

func (t token) is(kw string) bool { return t.kind == tkIdent && strings.EqualFold(t.text, kw) }

func (t token) isOp(op string) bool { return t.kind == tkOp && t.text == op }

func isIdentStart(c byte) bool { return c == '_' || unicode.IsLetter(rune(c)) }

func isIdentPart(c byte) bool { return isIdentStart(c) || (c >= '0' && c <= '9') }

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// skipComment returns the index after a `--` or `/* */` comment starting at q[i], or i when none starts there.
const commentDelimLen = 2

// skipComment returns the index after a `--` or `/* */` comment starting at q[i], or i when none starts there.
func skipComment(q string, i int) int {
	switch {
	case strings.HasPrefix(q[i:], "--"):
		if nl := strings.IndexByte(q[i:], '\n'); nl >= 0 {
			return i + nl
		}

		return len(q)
	case strings.HasPrefix(q[i:], "/*"):
		if end := strings.Index(q[i+commentDelimLen:], "*/"); end >= 0 {
			return i + commentDelimLen + end + commentDelimLen
		}

		return len(q)
	}

	return i
}

// stripSQLComments replaces comments outside quoted literals and identifiers with a single space.
func stripSQLComments(q string) string {
	if !strings.Contains(q, "--") && !strings.Contains(q, "/*") {
		return q
	}

	var sb strings.Builder

	for i := 0; i < len(q); {
		if q[i] == '\'' || q[i] == '"' {
			end := closingQuote(q, i)
			sb.WriteString(q[i:end])

			i = end

			continue
		}

		if j := skipComment(q, i); j != i {
			sb.WriteByte(' ')

			i = j

			continue
		}

		sb.WriteByte(q[i])

		i++
	}

	return sb.String()
}

func unquoteBody(q string, start, end int) string {
	quote := q[start]

	return strings.ReplaceAll(q[start+1:end-1], string([]byte{quote, quote}), string([]byte{quote}))
}

func skipDigits(q string, i int) int {
	for i < len(q) && isDigit(q[i]) {
		i++
	}

	return i
}

func skipExponent(q string, j int) int {
	if j >= len(q) || (q[j] != 'e' && q[j] != 'E') {
		return j
	}

	k := j + 1
	if k < len(q) && (q[k] == '+' || q[k] == '-') {
		k++
	}

	if k < len(q) && isDigit(q[k]) {
		return skipDigits(q, k)
	}

	return j
}

func lexNumber(q string, i int) int {
	j := skipDigits(q, i)

	if j < len(q) && q[j] == '.' {
		j = skipDigits(q, j+1)
	}

	return skipExponent(q, j)
}

func isDurationUnit(w string) bool {
	switch strings.ToLower(w) {
	case "ns", "us", "ms", "s", "m", "h", "d", unitNanosecond, "microsecond", unitMillisecond, unitSecond, unitMinute,
		unitHour, unitDay, "nanoseconds", "microseconds", "milliseconds", "seconds", "minutes", "hours", "days":
		return true
	}

	return isYearMonthUnit(w)
}

func isYearMonthUnit(w string) bool {
	switch strings.ToLower(w) {
	case "y", "year", "years", "month", "months":
		return true
	}

	return false
}

func lexOp(q string, i int) string {
	for _, op := range []string{"<>", "!=", "<=", ">=", "||", "->", "::", "=>"} {
		if strings.HasPrefix(q[i:], op) {
			return op
		}
	}

	return q[i : i+1]
}

func lexQuotedTok(q string, i int) (token, int, error) {
	c := q[i]

	end := closingQuote(q, i)
	if end > len(q) || q[end-1] != c || end-i < commentDelimLen {
		return token{}, 0, errSQLSyntax
	}

	k := tkString
	if c == '"' {
		k = tkQuoted
	}

	return token{kind: k, text: unquoteBody(q, i, end), start: i, end: end}, end, nil
}

func lexNumberTok(q string, i int) (token, int, error) {
	j := lexNumber(q, i)
	k := tkNumber

	if j < len(q) && isIdentStart(q[j]) {
		w := j
		for w < len(q) && isIdentPart(q[w]) {
			w++
		}

		if !isDurationUnit(q[j:w]) {
			return token{}, 0, errSQLSyntax
		}

		j, k = w, tkDuration
	}

	return token{kind: k, text: q[i:j], start: i, end: j}, j, nil
}

func lexIdentTok(q string, i int) (token, int) {
	j := i
	for j < len(q) && isIdentPart(q[j]) {
		j++
	}

	if strings.EqualFold(q[i:j], "measure_value") && strings.HasPrefix(q[j:], "::") {
		k := j + commentDelimLen
		for k < len(q) && isIdentPart(q[k]) {
			k++
		}

		j = k
	}

	return token{kind: tkIdent, text: q[i:j], start: i, end: j}, j
}

func lexOne(q string, i int) (token, int, error) {
	c := q[i]

	switch {
	case c == '\'' || c == '"':
		return lexQuotedTok(q, i)
	case isDigit(c) || (c == '.' && i+1 < len(q) && isDigit(q[i+1])):
		return lexNumberTok(q, i)
	case isIdentStart(c):
		t, j := lexIdentTok(q, i)

		return t, j, nil
	case c == '@':
		j := i + 1
		for j < len(q) && isIdentPart(q[j]) {
			j++
		}

		return token{kind: tkVar, text: q[i:j], start: i, end: j}, j, nil
	}

	op := lexOp(q, i)

	return token{kind: tkOp, text: op, start: i, end: i + len(op)}, i + len(op), nil
}

func isSpaceByte(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

// tokenizeSQL splits q into tokens, dropping comments; measure_value::type is one identifier.
func tokenizeSQL(q string) ([]token, error) {
	var toks []token

	for i := 0; i < len(q); {
		if len(toks) > maxSQLTokens {
			return nil, errSQLSyntax
		}

		if j := skipComment(q, i); j != i {
			i = j

			continue
		}

		if isSpaceByte(q[i]) {
			i++

			continue
		}

		t, next, err := lexOne(q, i)
		if err != nil {
			return nil, err
		}

		toks = append(toks, t)
		i = next
	}

	return append(toks, token{kind: tkEOF, start: len(q), end: len(q)}), nil
}
