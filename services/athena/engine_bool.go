package athena

import "strings"

// SQLite has no boolean type, so a comparison or logical expression comes
// back as 0/1 with no declared type. boolOutputs classifies the outermost
// SELECT list statically so those columns render true/false.

var boolFuncs = map[string]bool{
	"regexp_like": true, "starts_with": true, "contains": true, "is_nan": true, "is_finite": true,
	"is_infinite": true, "any_match": true, "all_match": true, "none_match": true,
	"json_array_contains": true, "bool_and": true, "bool_or": true, "every": true,
	"arrays_overlap": true,
}

var boolPassFuncs = map[string]bool{"coalesce": true, "try": true, "nullif": true, "greatest": true, "least": true}

var selectEnders = map[string]bool{
	kwFrom: true, kwWhere: true, kwGroup: true, kwHaving: true, "order": true, kwLimit: true, kwOffset: true,
	"union": true, kwIntersect: true, kwExcept: true, kwFetch: true, "window": true,
}

type boolScope struct {
	yes map[string]bool
	no  map[string]bool
}

// boolOutputs reports, per top-level output column, whether it is boolean;
// nil means the shape is not analysable (SELECT *, no SELECT).
func boolOutputs(query string) []bool {
	toks, err := tokenize(query)
	if err != nil {
		return nil
	}

	sig := significant(toks)
	sc := &boolScope{yes: map[string]bool{}, no: map[string]bool{}}

	const passes = 3

	for range passes {
		for i, t := range sig {
			if t.isWord("select") && !prevIsDotTok(sig, i) {
				sc.scanSelect(sig, i, true)
			}
		}
	}

	return sc.scanSelect(sig, outermostSelect(sig), false)
}

func prevIsDotTok(sig []token, i int) bool { return i > 0 && sig[i-1].isPunct(".") }

// outermostSelect returns the first SELECT outside any parenthesis (CTE bodies
// and subqueries sit deeper), or the first SELECT at all; -1 if none.
func outermostSelect(sig []token) int {
	depth, first := 0, -1

	for i, t := range sig {
		switch {
		case t.isPunct("("):
			depth++
		case t.isPunct(")"):
			depth--
		case t.isWord("select"):
			if depth == 0 {
				return i
			}

			if first < 0 {
				first = i
			}
		}
	}

	return first
}

// scanSelect classifies the select items starting at sel. With learn set it
// records alias classifications instead of returning the output vector.
func (sc *boolScope) scanSelect(sig []token, sel int, learn bool) []bool {
	if sel < 0 {
		return nil
	}

	i := sel + 1
	if i < len(sig) && (sig[i].isWord("distinct") || sig[i].isWord("all")) {
		i++
	}

	var (
		items [][]token
		cur   []token
		depth int
		cases int
	)

loop:
	for ; i < len(sig); i++ {
		t := sig[i]

		switch {
		case t.isPunct("("):
			depth++
		case t.isPunct(")"):
			depth--
			if depth < 0 {
				break loop
			}
		case t.isWord("case"):
			cases++
		case t.isWord("end") && cases > 0:
			cases--
		case depth == 0 && cases == 0 && t.kind == tkWord && selectEnders[strings.ToLower(t.text)]:
			break loop
		case depth == 0 && cases == 0 && t.isPunct(","):
			items = append(items, cur)
			cur = nil

			continue
		}

		cur = append(cur, t)
	}

	items = append(items, cur)
	out := make([]bool, len(items))

	for k, item := range items {
		if len(item) > 0 && item[len(item)-1].isPunct("*") {
			return nil
		}

		expr, alias := splitAliasSig(item)
		out[k] = sc.isBool(expr)

		if learn {
			sc.record(expr, alias, out[k])
		}
	}

	if learn {
		return nil
	}

	return out
}

func (sc *boolScope) record(expr []token, alias string, isB bool) {
	name := alias
	if name == "" && len(expr) > 0 {
		last := expr[len(expr)-1]
		if last.kind == tkWord || last.kind == tkQuoted {
			name = last.ident()
		}
	}

	if name == "" {
		return
	}

	if isB {
		sc.yes[name] = true
	} else if alias != "" || len(expr) != 1 {
		sc.no[name] = true
	}
}

// splitAliasSig splits `expr [AS] alias` on already-significant tokens.
func splitAliasSig(item []token) ([]token, string) {
	n := len(item)
	if n >= argsThree && item[n-2].isWord("as") {
		return item[:n-2], item[n-1].ident()
	}

	if n >= argsTwo && (item[n-1].kind == tkWord || item[n-1].kind == tkQuoted) &&
		!isOperandKeyword(strings.ToLower(item[n-1].text)) && !isClauseWord(item[n-1]) &&
		(item[n-2].kind == tkWord || item[n-2].kind == tkQuoted || item[n-2].isPunct(")") ||
			item[n-2].kind == tkNumber || item[n-2].kind == tkString) && !item[n-2].isPunct(".") {
		return item[:n-1], item[n-1].ident()
	}

	return item, ""
}

func isClauseWord(t token) bool {
	switch strings.ToLower(t.text) {
	case "end", "null", "true", "false", "and", "or", "not", "is", "in", "like", "between":
		return true
	}

	return false
}

func (sc *boolScope) isBool(expr []token) bool {
	for len(expr) >= argsTwo && expr[0].isPunct("(") && matchParen(expr, 0) == len(expr)-1 {
		if len(expr) > argsTwo && expr[1].isWord("select") {
			return false
		}

		expr = expr[1 : len(expr)-1]
	}

	if len(expr) == 0 {
		return false
	}

	if len(expr) == 1 {
		return sc.isBoolAtom(expr[0])
	}

	if sc.hasBoolOperator(expr) {
		return true
	}

	if expr[0].isWord("case") && expr[len(expr)-1].isWord("end") && matchCaseEnd(expr) {
		return sc.caseIsBool(expr)
	}

	return sc.isBoolCall(expr) || sc.isBoolRef(expr)
}

func (sc *boolScope) isBoolAtom(t token) bool {
	switch {
	case t.isWord(strTrue), t.isWord(strFalse):
		return true
	case t.kind == tkWord || t.kind == tkQuoted:
		n := t.ident()

		return sc.yes[n] && !sc.no[n]
	}

	return false
}

func (sc *boolScope) isBoolRef(expr []token) bool {
	if len(expr) == argsThree && expr[1].isPunct(".") {
		return sc.isBoolAtom(expr[2])
	}

	return false
}

func (sc *boolScope) hasBoolOperator(expr []token) bool {
	depth, cases := 0, 0

	for i, t := range expr {
		switch {
		case t.isPunct("("):
			depth++
		case t.isPunct(")"):
			depth--
		case t.isWord("case"):
			cases++
		case t.isWord("end") && cases > 0:
			cases--
		case depth == 0 && cases == 0 && t.kind == tkPunct && strings.Contains("=<>!", t.text) && !arrowAt(expr, i):
			return true
		case depth == 0 && cases == 0 && t.kind == tkWord && isBoolWord(strings.ToLower(t.text)) && !prevIsDotTok(expr, i):
			return true
		}
	}

	return false
}

func arrowAt(expr []token, i int) bool {
	return expr[i].isPunct(">") && i > 0 && expr[i-1].isPunct("-")
}

func isBoolWord(lw string) bool {
	switch lw {
	case "and", "or", "not", "is", "in", "between", "like", "exists", "ilike":
		return true
	}

	return false
}

func matchCaseEnd(expr []token) bool {
	cases := 0

	for i, t := range expr {
		switch {
		case t.isWord("case"):
			cases++
		case t.isWord("end"):
			cases--
			if cases == 0 {
				return i == len(expr)-1
			}
		}
	}

	return false
}

func (sc *boolScope) caseIsBool(expr []token) bool {
	depth, cases := 0, 0
	start := -1

	flush := func(end int) bool {
		return start >= 0 && !(end-start == 1 && expr[start].isWord("null")) && sc.isBool(expr[start:end])
	}

	for i, t := range expr {
		switch {
		case t.isPunct("("):
			depth++
		case t.isPunct(")"):
			depth--
		case t.isWord("case"):
			cases++
		case t.isWord("end"):
			cases--
			if cases == 0 && flush(i) {
				return true
			}
		case depth == 0 && cases == 1 && (t.isWord("when") || t.isWord("else")):
			if flush(i) {
				return true
			}

			start = -1
			if t.isWord("else") {
				start = i + 1
			}
		case depth == 0 && cases == 1 && t.isWord("then"):
			start = i + 1
		}
	}

	return false
}

func (sc *boolScope) isBoolCall(expr []token) bool {
	if len(expr) < argsThree || expr[0].kind != tkWord || !expr[1].isPunct("(") || matchParen(expr, 1) != len(expr)-1 {
		return false
	}

	name := strings.ToLower(expr[0].text)
	args := splitCommasSig(expr[2 : len(expr)-1])

	switch {
	case boolFuncs[name]:
		return true
	case name == "cast" || name == "try_cast":
		return len(args) == 1 && endsAsBoolean(args[0])
	case name == "if":
		return len(args) >= argsTwo && (sc.isBool(args[1]) || len(args) > argsTwo && sc.isBool(args[2]))
	case boolPassFuncs[name]:
		for _, a := range args {
			if sc.isBool(a) {
				return true
			}
		}
	}

	return false
}

func endsAsBoolean(toks []token) bool {
	n := len(toks)

	return n >= argsTwo && toks[n-1].isWord(typeBoolean) && toks[n-2].isWord("as")
}

func splitCommasSig(toks []token) [][]token {
	var (
		parts [][]token
		cur   []token
		depth int
	)

	for _, t := range toks {
		switch {
		case t.isPunct("("):
			depth++
		case t.isPunct(")"):
			depth--
		case depth == 0 && t.isPunct(","):
			parts = append(parts, cur)
			cur = nil

			continue
		}

		cur = append(cur, t)
	}

	return append(parts, cur)
}
