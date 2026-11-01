package athena

import (
	"context"
	"strings"
)

const travelSentinel = "\x00travel:"

// travelClause recognises FOR TIMESTAMP|VERSION AS OF <expr> after the table
// reference ending at end and returns "<kind>:<expr text>" and the last consumed token.
func (x *translator) travelClause(toks []token, end int) (string, int) {
	n := nextSig(toks, end)
	if n < 0 || !toks[n].isWord("for") {
		return "", end
	}

	k := nextSig(toks, n)
	if k < 0 || (!toks[k].isWord("timestamp") && !toks[k].isWord("version")) {
		return "", end
	}

	a := nextSig(toks, k)
	if a < 0 || !toks[a].isWord("as") {
		return "", end
	}

	o := nextSig(toks, a)
	if o < 0 || !toks[o].isWord("of") {
		return "", end
	}

	first := nextSig(toks, o)
	if first < 0 {
		x.err = &syntaxError{"line 1:1: mismatched input '<EOF>': expecting a value after AS OF"}

		return "", end
	}

	last := travelExprEnd(toks, first)
	if last < 0 {
		x.err = &syntaxError{"line 1:1: invalid expression after FOR " + strings.ToUpper(toks[k].text) + " AS OF"}

		return "", end
	}

	kind := travelTimestamp
	if toks[k].isWord("version") {
		kind = travelVersion
	}

	return kind + ":" + strings.TrimSpace(joinTokens(toks[first:last+1])), last
}

// travelExprEnd returns the index of the last token of the AS OF operand: a
// literal, TIMESTAMP 'literal', a niladic function, or a parenthesised expression.
func travelExprEnd(toks []token, first int) int {
	t := toks[first]

	switch {
	case t.isPunct("("):
		return matchParen(toks, first)
	case t.kind == tkString || t.kind == tkNumber:
		return first
	case t.isWord("timestamp"):
		if n := nextSig(toks, first); n >= 0 && toks[n].kind == tkString {
			return n
		}
	case t.kind == tkWord:
		if n := nextSig(toks, first); n >= 0 && toks[n].isPunct("(") {
			return matchParen(toks, n)
		}

		return first
	}

	return -1
}

func splitTravel(parts []string) ([]string, travelSpec) {
	if len(parts) == 0 || !strings.HasPrefix(parts[len(parts)-1], travelSentinel) {
		return parts, travelSpec{}
	}

	spec := strings.TrimPrefix(parts[len(parts)-1], travelSentinel)
	kind, expr, _ := strings.Cut(spec, ":")

	return parts[:len(parts)-1], travelSpec{kind: kind, value: evalTravelExpr(expr)}
}

// evalTravelExpr evaluates the AS OF operand to its text; unevaluable input is returned as written.
func evalTravelExpr(expr string) string {
	toks, err := tokenize(expr)
	if err == nil {
		if sig := significant(toks); len(sig) == 1 && sig[0].kind == tkString {
			return stringLiteral(sig[0])
		}
	}

	sqlText, _, terr := translateSQL("SELECT "+expr, func([]string) resolvedRef { return resolvedRef{} })
	if terr != nil {
		return expr
	}

	plan := &enginePlan{sql: sqlText}

	res, _, out := plan.run(context.Background(), defaultEngineLimits())
	if out.failed || res == nil || len(res.rows) == 0 || len(res.rows[0]) == 0 {
		return expr
	}

	return res.rows[0][0]
}
