package athena

import (
	"fmt"
	"strings"
)

func isTypedLiteralWord(w string) bool {
	switch strings.ToLower(w) {
	case typeDate, typeTimestamp, "time":
		return true
	default:
		return false
	}
}

func singularUnit(w string) string { return strings.TrimSuffix(strings.ToLower(w), "s") }

func isIntervalUnit(w string) bool {
	switch singularUnit(w) {
	case unitSecond, unitMinute, unitHour, unitDay, unitWeek, unitMonth, unitQuarter, unitYear:
		return true
	default:
		return false
	}
}

func badInterval(format string, args ...any) error {
	return &syntaxError{"line 1:1: " + fmt.Sprintf(format, args...)}
}

// intervalLiteralAt reports the string token index when toks[i] starts `INTERVAL ['+'|'-'] 'text'`.
func intervalLiteralAt(toks []token, i int) (int, string, bool) {
	if !toks[i].isWord(kwInterval) {
		return 0, "", false
	}

	j := nextSig(toks, i)
	sign := ""

	if j >= 0 && (toks[j].isPunct("-") || toks[j].isPunct("+")) {
		sign = toks[j].text
		j = nextSig(toks, j)
	}

	if j < 0 || toks[j].kind != tkString {
		return 0, "", false
	}

	return j, sign, true
}

// usesIntervals reports whether the statement can produce interval values.
func usesIntervals(toks []token) bool {
	for i, t := range toks {
		if t.kind != tkWord {
			continue
		}

		if _, _, ok := intervalLiteralAt(toks, i); ok {
			return true
		}

		if p := prevSig(toks, i); t.isWord(kwInterval) && p >= 0 && toks[p].isWord("as") {
			return true
		}

		if n := nextSig(toks, i); n >= 0 && toks[n].isPunct("(") &&
			(t.isWord("parse_duration") || t.isWord("to_milliseconds")) {
			return true
		}
	}

	return false
}

// intervalSpec scans `UNIT [TO UNIT]` after the literal string at toks[lit].
func intervalSpec(toks []token, lit int) (string, string, int) {
	u := nextSig(toks, lit)
	if u < 0 || toks[u].kind != tkWord || !isIntervalUnit(toks[u].text) {
		return "", "", lit
	}

	from, to, end := singularUnit(toks[u].text), "", u

	if t := nextSig(toks, end); t >= 0 && toks[t].isWord("to") {
		if u2 := nextSig(toks, t); u2 >= 0 && toks[u2].kind == tkWord && isIntervalUnit(toks[u2].text) {
			to, end = singularUnit(toks[u2].text), u2
		}
	}

	return from, to, end
}

// rewriteIntervals turns INTERVAL literals into interval values and routes + - * / through
// athena_iv_op so interval and temporal operands combine at run time; uses=false leaves toks alone.
func rewriteIntervals(toks []token) ([]token, bool, error) {
	if !usesIntervals(toks) {
		return toks, false, nil
	}

	toks, err := intervalLiterals(toks)
	if err != nil {
		return nil, false, err
	}

	for _, ops := range []string{"*/", "+-"} {
		toks = routeOperators(toks, ops)
	}

	return toks, true, nil
}

func intervalLiterals(toks []token) ([]token, error) {
	out := make([]token, 0, len(toks))

	for i := 0; i < len(toks); i++ {
		lit, sign, ok := intervalLiteralAt(toks, i)
		if !ok {
			out = append(out, toks[i])

			continue
		}

		from, to, end := intervalSpec(toks, lit)
		if from == "" {
			return nil, badInterval("INTERVAL requires a unit after '%s'", stringLiteral(toks[lit]))
		}

		kind, n, err := parseIntervalLiteral(sign+stringLiteral(toks[lit]), from, to)
		if err != nil {
			return nil, err
		}

		fn := "athena_iv_dt"
		if kind == ivKindYM {
			fn = "athena_iv_ym"
		}

		out = append(out, token{kind: tkWord, text: fmt.Sprintf("%s(%d)", fn, n)})
		i = end
	}

	return out, nil
}

func isOperandEnd(t token) bool {
	switch t.kind {
	case tkNumber, tkString, tkQuoted:
		return true
	case tkPunct:
		return t.text == ")" || t.text == "]"
	case tkWord:
		return !isOperandKeyword(strings.ToLower(t.text))
	default:
		return false
	}
}

func isBinaryOperator(toks []token, i int) bool {
	p := prevSig(toks, i)

	return p >= 0 && isOperandEnd(toks[p])
}

const ivWrapTokens = 8

func routeOperators(toks []token, ops string) []token {
	for i := 0; i < len(toks); i++ {
		t := toks[i]
		if t.kind != tkPunct || !strings.Contains(ops, t.text) || !isBinaryOperator(toks, i) {
			continue
		}

		l, r := operandStart(toks, prevSig(toks, i)), operandEnd(toks, i)
		if l < 0 || r < 0 || bothNumbers(toks, l, i, r) {
			continue
		}

		wrapped := make([]token, 0, r-l+ivWrapTokens)
		wrapped = append(
			wrapped,
			token{kind: tkWord, text: "athena_iv_op"},
			token{kind: tkPunct, text: "("},
			token{
				kind: tkString,
				text: "'" + t.text + "'",
			},
			token{kind: tkPunct, text: ","},
			token{kind: tkSpace, text: " "},
		)
		wrapped = append(wrapped, trimSpaceTokens(toks[l:i])...)
		wrapped = append(wrapped, token{kind: tkPunct, text: ","}, token{kind: tkSpace, text: " "})
		wrapped = append(wrapped, trimSpaceTokens(toks[i+1:r+1])...)
		wrapped = append(wrapped, token{kind: tkPunct, text: ")"})

		rest := append([]token(nil), toks[r+1:]...)
		toks = append(append(toks[:l:l], wrapped...), rest...)
		i = l + len(wrapped) - 1
	}

	return toks
}

func bothNumbers(toks []token, l, op, r int) bool {
	left, right := significant(toks[l:op]), significant(toks[op+1:r+1])

	return len(left) == 1 && len(right) == 1 && left[0].kind == tkNumber && right[0].kind == tkNumber
}

func isUnarySign(toks []token, i int) bool {
	if !toks[i].isPunct("-") && !toks[i].isPunct("+") {
		return false
	}

	p := prevSig(toks, i)

	return p < 0 || !isOperandEnd(toks[p])
}

// operandStart finds where the primary expression ending at toks[end] begins, including a unary sign.
func operandStart(toks []token, end int) int {
	if end < 0 {
		return -1
	}

	s := primaryBegin(toks, end)
	if s < 0 {
		return -1
	}

	if p := prevSig(toks, s); p >= 0 && isUnarySign(toks, p) {
		return p
	}

	return s
}

func primaryBegin(toks []token, end int) int {
	t := toks[end]

	switch {
	case t.isPunct(")"):
		return callBegin(toks, end)
	case t.isPunct("]"):
		open := matchBackBracket(toks, end)
		if open < 0 {
			return -1
		}

		return primaryBegin(toks, prevSig(toks, open))
	case t.kind == tkString:
		return literalBegin(toks, end)
	case t.isWord("end"):
		return matchCaseBack(toks, end)
	case t.kind == tkWord && !isOperandKeyword(strings.ToLower(t.text)), t.kind == tkQuoted, t.kind == tkNumber:
		return chainStart(toks, end)
	default:
		return -1
	}
}

func callBegin(toks []token, closeIdx int) int {
	open := matchBackParen(toks, closeIdx)
	if open < 0 {
		return -1
	}

	if p := prevSig(toks, open); p >= 0 && toks[p].kind == tkWord &&
		!isOperandKeyword(strings.ToLower(toks[p].text)) {
		return chainStart(toks[:p+1], p)
	}

	return open
}

func literalBegin(toks []token, str int) int {
	if p := prevSig(toks, str); p >= 0 && toks[p].kind == tkWord && isTypedLiteralWord(toks[p].text) {
		return p
	}

	return str
}

func matchBackBracket(toks []token, closeIdx int) int {
	depth := 0

	for i := closeIdx; i >= 0; i-- {
		switch {
		case toks[i].isPunct("]"):
			depth++
		case toks[i].isPunct("["):
			depth--
			if depth == 0 {
				return i
			}
		}
	}

	return -1
}

func matchCaseBack(toks []token, end int) int {
	depth := 0

	for i := end; i >= 0; i-- {
		switch {
		case toks[i].isWord("end"):
			depth++
		case toks[i].isWord("case"):
			depth--
			if depth == 0 {
				return i
			}
		}
	}

	return -1
}

func matchCaseForward(toks []token, start int) int {
	depth := 0

	for i := start; i < len(toks); i++ {
		switch {
		case toks[i].isWord("case"):
			depth++
		case toks[i].isWord("end"):
			depth--
			if depth == 0 {
				return i
			}
		}
	}

	return -1
}

// operandEnd returns the index of the last token of the primary expression right of toks[op].
func operandEnd(toks []token, op int) int {
	j := nextSig(toks, op)
	for j >= 0 && (toks[j].isPunct("-") || toks[j].isPunct("+")) {
		j = nextSig(toks, j)
	}

	if j < 0 {
		return -1
	}

	end := baseEnd(toks, j)

	for end >= 0 {
		n := nextSig(toks, end)

		switch {
		case n >= 0 && toks[n].isPunct("["):
			end = matchBracket(toks, n)
		case n >= 0 && toks[n].isPunct(".") && nextSig(toks, n) >= 0 && isStructChainToken(toks[nextSig(toks, n)]):
			end = chainTail(toks, nextSig(toks, n))
		default:
			return end
		}
	}

	return -1
}

func chainTail(toks []token, name int) int {
	if n := nextSig(toks, name); n >= 0 && toks[n].isPunct("(") {
		return matchParen(toks, n)
	}

	return name
}

func baseEnd(toks []token, j int) int {
	t := toks[j]

	switch {
	case t.isPunct("("):
		return matchParen(toks, j)
	case t.kind == tkNumber, t.kind == tkString:
		return j
	case t.isWord("case"):
		return matchCaseForward(toks, j)
	case t.kind == tkWord && isOperandKeyword(strings.ToLower(t.text)):
		return -1
	case t.kind == tkWord, t.kind == tkQuoted:
		return nameEnd(toks, j)
	default:
		return -1
	}
}

func nameEnd(toks []token, j int) int {
	n := nextSig(toks, j)
	if n < 0 {
		return j
	}

	switch {
	case toks[n].isPunct("("):
		end := matchParen(toks, n)
		if o := nextSig(toks, end); end >= 0 && o >= 0 && toks[o].isWord("over") {
			if q := nextSig(toks, o); q >= 0 && toks[q].isPunct("(") {
				return matchParen(toks, q)
			}
		}

		return end
	case toks[n].kind == tkString && isTypedLiteralWord(toks[j].text):
		return n
	default:
		return j
	}
}
