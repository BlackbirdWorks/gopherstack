package athena

import "strings"

// temporalOutputs reports, per top-level output column, whether the expression yields a date or
// timestamp (interval arithmetic, date_add, date_trunc, CAST AS date/timestamp); nil when unanalysable.
func temporalOutputs(query string) []bool {
	toks, err := tokenize(query)
	if err != nil {
		return nil
	}

	sig := significant(toks)

	sel := outermostSelect(sig)
	if sel < 0 {
		return nil
	}

	items := selectItems(sig, sel)
	out := make([]bool, len(items))

	for k, item := range items {
		if len(item) > 0 && item[len(item)-1].isPunct("*") {
			return nil
		}

		expr, _ := splitAliasSig(item)
		out[k] = isTemporalExpr(stripParens(expr))
	}

	return out
}

func isTemporalExpr(expr []token) bool {
	if len(expr) == 0 {
		return false
	}

	if isTemporalCall(expr) || isTemporalCast(expr) {
		return true
	}

	return hasIntervalArithmetic(expr)
}

func isTemporalCall(expr []token) bool {
	if expr[0].kind != tkWord || len(expr) < argsThree || !expr[1].isPunct("(") || matchParen(expr, 1) != len(expr)-1 {
		return false
	}

	switch strings.ToLower(expr[0].text) {
	case "date_add", "date_trunc":
		return true
	default:
		return false
	}
}

func isTemporalCast(expr []token) bool {
	if len(expr) < argsThree || !expr[0].isWord("cast") || !expr[1].isPunct("(") || matchParen(expr, 1) != len(expr)-1 {
		return false
	}

	asIdx := lastTopLevelWord(expr[2:len(expr)-1], kwAs)
	if asIdx < 0 {
		return false
	}

	typ := strings.ToLower(joinTokens(expr[2+asIdx+1 : len(expr)-1]))

	typ = strings.TrimSpace(typ)

	return strings.HasPrefix(typ, typeDate) || strings.HasPrefix(typ, typeTimestamp)
}

func isTypedTemporalLiteral(expr []token, i int) bool {
	return (expr[i].isWord(typeDate) || expr[i].isWord(typeTimestamp)) && i+1 < len(expr) && expr[i+1].kind == tkString
}

// hasIntervalArithmetic is true for `x +|- INTERVAL ...` or `DATE '..' +|- x` at the top level.
func hasIntervalArithmetic(expr []token) bool {
	depth, interval, plusMinus := 0, false, false

	for i, t := range expr {
		switch {
		case t.isPunct("("):
			depth++
		case t.isPunct(")"):
			depth--
		case depth == 0 && (t.isWord(kwInterval) || isTypedTemporalLiteral(expr, i)):
			interval = true
		case depth == 0 && (t.isPunct("+") || t.isPunct("-")):
			plusMinus = true
		}
	}

	return interval && plusMinus
}

// temporalType names the Athena type of a date or timestamp string; "" when it is neither.
func temporalType(s string) string {
	switch {
	case len(s) == len(dateLayout) && isDateOnly(s):
		if _, ok := parseTS(s); ok {
			return typeDate
		}
	case len(s) > len(dateLayout):
		if _, ok := parseTS(s); ok {
			return typeTimestamp
		}
	}

	return ""
}
