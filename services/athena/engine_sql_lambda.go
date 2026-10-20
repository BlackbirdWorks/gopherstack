package athena

import (
	"fmt"
	"strings"
)

// lambda is one parsed `params -> body` argument.
const (
	fnTransform = "transform"
	fnAnyMatch  = "any_match"
	fnAllMatch  = "all_match"
	fnNoneMatch = "none_match"
)

type lambda struct {
	params []string
	body   []token
}

func lambdaArity(lw string) int {
	switch lw {
	case fnTransform, fnFilter, fnAnyMatch, fnAllMatch, fnNoneMatch:
		return 1
	case "map_filter", "transform_values", "transform_keys", "zip_with", "reduce":
		return argsTwo
	default:
		return 0
	}
}

// splitArgs splits call arguments on commas outside parentheses and brackets.
func splitArgs(toks []token) [][]token {
	var (
		parts [][]token
		cur   []token
		depth int
	)

	for _, t := range toks {
		switch {
		case t.isPunct("("), t.isPunct("["):
			depth++
		case t.isPunct(")"), t.isPunct("]"):
			depth--
		}

		if depth == 0 && t.isPunct(",") {
			parts = append(parts, cur)
			cur = nil

			continue
		}

		cur = append(cur, t)
	}

	return append(parts, cur)
}

func arrowIndex(toks []token) int {
	depth := 0

	for i := 0; i+1 < len(toks); i++ {
		switch {
		case toks[i].isPunct("("):
			depth++
		case toks[i].isPunct(")"):
			depth--
		case depth == 0 && toks[i].isPunct("-") && toks[i+1].isPunct(">"):
			return i
		}
	}

	return -1
}

func lambdaParams(toks []token) ([]string, bool) {
	sig := significant(toks)
	if len(sig) == 1 && sig[0].kind == tkWord {
		return []string{sig[0].text}, true
	}

	if len(sig) < argsThree || !sig[0].isPunct("(") || !sig[len(sig)-1].isPunct(")") {
		return nil, false
	}

	var names []string

	for _, part := range splitCommas(sig[1 : len(sig)-1]) {
		if len(part) != 1 || part[0].kind != tkWord {
			return nil, false
		}

		names = append(names, part[0].text)
	}

	return names, true
}

func parseLambda(arg []token) (lambda, bool) {
	a := arrowIndex(arg)
	if a < 0 {
		return lambda{}, false
	}

	params, ok := lambdaParams(arg[:a])

	return lambda{params: params, body: arg[a+argsTwo:]}, ok
}

func hasLambda(args [][]token) bool {
	for _, a := range args {
		if arrowIndex(a) >= 0 {
			return true
		}
	}

	return false
}

func (x *translator) sqlOf(toks []token) string {
	return strings.TrimSpace(joinTokens(x.translate(toks)))
}

func guardNull(operands []string, expr string) string {
	conds := make([]string, len(operands))
	for i, o := range operands {
		conds[i] = "(" + o + ") IS NULL"
	}

	return "CASE WHEN " + strings.Join(conds, " OR ") + " THEN NULL ELSE " + expr + " END"
}

func arrayElems(arr, param string) string {
	return `(SELECT athena_elem(value, type) AS ` + quoteIdent(param) +
		` FROM json_each(athena_unmark(` + arr + `)) ORDER BY key)`
}

func mapElems(m string, p lambda) string {
	return `(SELECT key AS ` + quoteIdent(p.params[0]) + `, athena_elem(value, type) AS ` + quoteIdent(p.params[1]) +
		` FROM json_each(athena_unmark(` + m + `)) ORDER BY key)`
}

func zipElems(a, b string, p lambda) string {
	return `(SELECT athena_elem(l.value, l.type) AS ` + quoteIdent(p.params[0]) +
		`, athena_elem(r.value, r.type) AS ` + quoteIdent(p.params[1]) +
		` FROM json_each(athena_index_list(` + a + `, ` + b + `)) n` +
		` LEFT JOIN json_each(athena_unmark(` + a + `)) l ON l.key = n.value` +
		` LEFT JOIN json_each(athena_unmark(` + b + `)) r ON r.key = n.value ORDER BY n.value)`
}

func matchAgg(lw string) string {
	hit := `sum("_r" IS NOT NULL AND "_r") > 0`
	miss := `sum("_r" IS NOT NULL AND NOT "_r") > 0`
	unknown := `sum("_r" IS NULL) > 0`

	switch lw {
	case fnAnyMatch:
		return `CASE WHEN ` + hit + ` THEN 1 WHEN ` + unknown + ` THEN NULL ELSE 0 END`
	case fnAllMatch:
		return `CASE WHEN ` + miss + ` THEN 0 WHEN ` + unknown + ` THEN NULL ELSE 1 END`
	default:
		return `CASE WHEN ` + hit + ` THEN 0 WHEN ` + unknown + ` THEN NULL ELSE 1 END`
	}
}

func (x *translator) lambdaErr(lw string) ([]token, int) {
	x.err = &syntaxError{fmt.Sprintf("line 1:1: Unexpected parameters for function %s", lw)}

	return nil, 0
}

// lambdaCall rewrites array/map functions taking lambda arguments onto
// correlated json_each subqueries; end=0 leaves the call to normal handling.
func (x *translator) lambdaCall(toks []token, lw string, open int) ([]token, int) {
	want := lambdaArity(lw)
	if want == 0 {
		return nil, 0
	}

	closeIdx := matchParen(toks, open)
	if closeIdx < 0 {
		return nil, 0
	}

	args := splitArgs(toks[open+1 : closeIdx])
	if !hasLambda(args) {
		return nil, 0
	}

	sql, ok := x.lambdaSQL(lw, args)
	if !ok {
		return x.lambdaErr(lw)
	}

	return []token{{kind: tkWord, text: sql}}, closeIdx
}

func (x *translator) lambdaArg(arg []token, arity int) (lambda, bool) {
	l, ok := parseLambda(arg)

	return l, ok && len(l.params) == arity
}

func (x *translator) lambdaSQL(lw string, args [][]token) (string, bool) {
	switch lw {
	case fnTransform, fnFilter, fnAnyMatch, fnAllMatch, fnNoneMatch:
		return x.arraySQL(lw, args)
	case "reduce":
		return x.reduceSQL(args)
	case "zip_with":
		return x.zipSQL(args)
	default:
		return x.mapSQL(lw, args)
	}
}

func (x *translator) arraySQL(lw string, args [][]token) (string, bool) {
	if len(args) != argsTwo {
		return "", false
	}

	l, ok := x.lambdaArg(args[1], 1)
	if !ok {
		return "", false
	}

	arr, body := x.sqlOf(args[0]), x.sqlOf(l.body)
	elems := arrayElems(arr, l.params[0])

	var inner string

	switch lw {
	case fnTransform:
		inner = `(SELECT array_agg(` + body + `) FROM ` + elems + `)`
	case fnFilter:
		inner = `(SELECT array_agg(` + quoteIdent(l.params[0]) + `) FROM ` + elems + ` WHERE ` + body + `)`
	default:
		inner = `(SELECT ` + matchAgg(lw) + ` FROM (SELECT ` + body + ` AS "_r" FROM ` + elems + `))`
	}

	return guardNull([]string{arr}, inner), true
}

func (x *translator) mapSQL(lw string, args [][]token) (string, bool) {
	if len(args) != argsTwo {
		return "", false
	}

	l, ok := x.lambdaArg(args[1], argsTwo)
	if !ok {
		return "", false
	}

	m, body := x.sqlOf(args[0]), x.sqlOf(l.body)
	elems := mapElems(m, l)
	k, v := quoteIdent(l.params[0]), quoteIdent(l.params[1])

	var inner string

	switch lw {
	case "map_filter":
		inner = `(SELECT map_agg(` + k + `, ` + v + `) FROM ` + elems + ` WHERE ` + body + `)`
	case "transform_values":
		inner = `(SELECT map_agg(` + k + `, ` + body + `) FROM ` + elems + `)`
	default:
		inner = `(SELECT map_agg(` + body + `, ` + v + `) FROM ` + elems + `)`
	}

	return guardNull([]string{m}, inner), true
}

func (x *translator) zipSQL(args [][]token) (string, bool) {
	if len(args) != argsThree {
		return "", false
	}

	l, ok := x.lambdaArg(args[2], argsTwo)
	if !ok {
		return "", false
	}

	a, b, body := x.sqlOf(args[0]), x.sqlOf(args[1]), x.sqlOf(l.body)
	inner := `(SELECT array_agg(` + body + `) FROM ` + zipElems(a, b, l) + `)`

	return guardNull([]string{a, b}, inner), true
}

func (x *translator) reduceSQL(args [][]token) (string, bool) {
	const reduceArgs = 4

	if len(args) != reduceArgs {
		return "", false
	}

	step, sok := x.lambdaArg(args[2], argsTwo)
	fin, fok := x.lambdaArg(args[3], 1)

	if !sok || !fok {
		return "", false
	}

	arr, init := x.sqlOf(args[0]), x.sqlOf(args[1])
	s, e := quoteIdent(step.params[0]), quoteIdent(step.params[1])

	inner := `(WITH RECURSIVE "_e"("_i", ` + e + `) AS (SELECT key, athena_elem(value, type) ` +
		`FROM json_each(athena_unmark(` + arr + `))), "_r"("_i", ` + s + `) AS (SELECT -1, ` + init +
		` UNION ALL SELECT "_e"."_i", ` + x.sqlOf(step.body) + ` FROM "_r" JOIN "_e" ON "_e"."_i" = "_r"."_i" + 1) ` +
		`SELECT ` + x.sqlOf(fin.body) + ` FROM (SELECT ` + s + ` AS ` + quoteIdent(fin.params[0]) +
		` FROM "_r" ORDER BY "_i" DESC LIMIT 1))`

	return guardNull([]string{arr}, inner), true
}
