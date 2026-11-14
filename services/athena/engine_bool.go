package athena

import (
	"slices"
	"strings"
)

// SQLite has no boolean type, so a comparison or logical expression comes
// back as 0/1 with no declared type. boolOutputs classifies the outermost
// SELECT list statically so those columns render true/false.

const (
	kwNull    = "null"
	kwCase    = "case"
	kwEnd     = "end"
	kwAnd     = "and"
	kwOr      = "or"
	kwOrder   = "order"
	kwUnion   = "union"
	kwWindow  = "window"
	kwBetween = "between"
	kwIs      = "is"
	kwIn      = "in"
	kwLike    = "like"
	kwNot     = "not"
	kwThen    = "then"
	kwElse    = "else"
	kwWhen    = "when"
	kwSelect  = "select"
	kwAs      = "as"
)

func isBoolFunc(n string) bool {
	switch n {
	case "regexp_like", "starts_with", "contains", "is_nan", "is_finite", "is_infinite", "any_match", "all_match",
		"none_match", "json_array_contains", "bool_and", "bool_or", "every", "arrays_overlap":
		return true
	}

	return false
}

func isBoolPassFunc(n string) bool {
	switch n {
	case "coalesce", "try", "nullif", "greatest", "least":
		return true
	}

	return false
}

func isSelectEnder(lw string) bool {
	switch lw {
	case kwFrom, kwWhere, kwGroup, kwHaving, kwOrder, kwLimit, kwOffset, kwUnion, kwIntersect, kwExcept, kwFetch,
		kwWindow:
		return true
	}

	return false
}

func isBoolWord(lw string) bool {
	switch lw {
	case kwAnd, kwOr, kwNot, kwIs, kwIn, kwBetween, kwLike, "exists", "ilike":
		return true
	}

	return false
}

func isCmpPunct(t token) bool {
	return t.isPunct("=") || t.isPunct("<") || t.isPunct(">") || t.isPunct("!")
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
			if t.isWord(kwSelect) && !prevIsDotTok(sig, i) {
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
		case t.isWord(kwSelect):
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

// selectItems splits the select list starting after the SELECT at sel.
func selectItems(sig []token, sel int) [][]token {
	i := sel + 1
	if i < len(sig) && (sig[i].isWord("distinct") || sig[i].isWord("all")) {
		i++
	}

	items := make([][]token, 0, 1)
	cur := make([]token, 0, len(sig)-i)
	depth, cases := 0, 0

	for ; i < len(sig); i++ {
		t := sig[i]

		if t.isPunct(")") && depth == 0 {
			break
		}

		if depth == 0 && cases == 0 && t.kind == tkWord && isSelectEnder(strings.ToLower(t.text)) {
			break
		}

		if depth == 0 && cases == 0 && t.isPunct(",") {
			items, cur = append(items, cur), nil

			continue
		}

		depth, cases = trackNesting(t, depth, cases)
		cur = append(cur, t)
	}

	return append(items, cur)
}

func trackNesting(t token, depth, cases int) (int, int) {
	switch {
	case t.isPunct("("):
		depth++
	case t.isPunct(")"):
		depth--
	case t.isWord(kwCase):
		cases++
	case t.isWord(kwEnd) && cases > 0:
		cases--
	}

	return depth, cases
}

// scanSelect classifies the select items starting at sel. With learn set it
// records alias classifications instead of returning the output vector.
func (sc *boolScope) scanSelect(sig []token, sel int, learn bool) []bool {
	if sel < 0 {
		return nil
	}

	items := selectItems(sig, sel)
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

	switch {
	case name == "":
	case isB:
		sc.yes[name] = true
	case alias != "" || len(expr) != 1:
		sc.no[name] = true
	}
}

func isNameTok(t token) bool { return t.kind == tkWord || t.kind == tkQuoted }

// splitAliasSig splits `expr [AS] alias` on already-significant tokens.
func splitAliasSig(item []token) ([]token, string) {
	n := len(item)
	if n >= argsThree && item[n-2].isWord(kwAs) {
		return item[:n-2], item[n-1].ident()
	}

	if n >= argsTwo && isNameTok(item[n-1]) && !isOperandKeyword(strings.ToLower(item[n-1].text)) &&
		!isClauseWord(item[n-1]) && !item[n-2].isPunct(".") &&
		(isNameTok(item[n-2]) || item[n-2].isPunct(")") || item[n-2].kind == tkNumber || item[n-2].kind == tkString) {
		return item[:n-1], item[n-1].ident()
	}

	return item, ""
}

func isClauseWord(t token) bool {
	switch strings.ToLower(t.text) {
	case kwEnd, kwNull, strTrue, strFalse, kwAnd, kwOr, kwNot, kwIs, kwIn, kwLike, kwBetween:
		return true
	}

	return false
}

func stripParens(expr []token) []token {
	for len(expr) >= argsTwo && expr[0].isPunct("(") && matchParen(expr, 0) == len(expr)-1 {
		if len(expr) > argsTwo && expr[1].isWord(kwSelect) {
			return nil
		}

		expr = expr[1 : len(expr)-1]
	}

	return expr
}

func (sc *boolScope) isBool(expr []token) bool {
	expr = stripParens(expr)

	switch {
	case len(expr) == 0:
		return false
	case len(expr) == 1:
		return sc.isBoolAtom(expr[0])
	case hasBoolOperator(expr):
		return true
	case expr[0].isWord(kwCase) && expr[len(expr)-1].isWord(kwEnd) && matchCaseEnd(expr):
		return sc.caseIsBool(expr)
	}

	return sc.isBoolCall(expr) || sc.isBoolRef(expr)
}

func (sc *boolScope) isBoolAtom(t token) bool {
	if t.isWord(strTrue) || t.isWord(strFalse) {
		return true
	}

	if isNameTok(t) {
		n := t.ident()

		return sc.yes[n] && !sc.no[n]
	}

	return false
}

func (sc *boolScope) isBoolRef(expr []token) bool {
	return len(expr) == argsThree && expr[1].isPunct(".") && sc.isBoolAtom(expr[2])
}

func hasBoolOperator(expr []token) bool {
	depth, cases := 0, 0

	for i, t := range expr {
		if depth == 0 && cases == 0 && isBoolOperatorAt(expr, i) {
			return true
		}

		depth, cases = trackNesting(t, depth, cases)
	}

	return false
}

func isBoolOperatorAt(expr []token, i int) bool {
	t := expr[i]

	switch {
	case isCmpPunct(t):
		isArrow := t.isPunct(">") && i > 0 && expr[i-1].isPunct("-")

		return !isArrow
	case t.kind == tkWord:
		return isBoolWord(strings.ToLower(t.text)) && !prevIsDotTok(expr, i)
	}

	return false
}

func matchCaseEnd(expr []token) bool {
	cases := 0

	for i, t := range expr {
		switch {
		case t.isWord(kwCase):
			cases++
		case t.isWord(kwEnd):
			cases--
			if cases == 0 {
				return i == len(expr)-1
			}
		}
	}

	return false
}

// caseBranches returns the result expressions (THEN and ELSE) of a CASE block.
func caseBranches(expr []token) [][]token {
	var (
		out          [][]token
		depth, cases int
		start        = -1
	)

	flush := func(end int) {
		if start >= 0 && (end-start != 1 || !expr[start].isWord(kwNull)) {
			out = append(out, expr[start:end])
		}

		start = -1
	}

	for i, t := range expr {
		top := depth == 0 && cases == 1

		switch {
		case top && (t.isWord(kwWhen) || t.isWord(kwElse)):
			flush(i)

			if t.isWord(kwElse) {
				start = i + 1
			}
		case top && t.isWord(kwThen):
			start = i + 1
		case t.isWord(kwEnd) && cases == 1 && depth == 0:
			flush(i)
		}

		depth, cases = trackNesting(t, depth, cases)
	}

	return out
}

func (sc *boolScope) caseIsBool(expr []token) bool {
	return slices.ContainsFunc(caseBranches(expr), sc.isBool)
}

func (sc *boolScope) isBoolCall(expr []token) bool {
	if len(expr) < argsThree || expr[0].kind != tkWord || !expr[1].isPunct("(") ||
		matchParen(expr, 1) != len(expr)-1 {
		return false
	}

	name := strings.ToLower(expr[0].text)
	args := splitCommasSig(expr[2 : len(expr)-1])

	switch {
	case isBoolFunc(name):
		return true
	case name == "cast" || name == "try_cast":
		return len(args) == 1 && endsAsBoolean(args[0])
	case name == "if":
		return len(args) >= argsTwo && (sc.isBool(args[1]) || len(args) > argsTwo && sc.isBool(args[2]))
	case isBoolPassFunc(name):
		return slices.ContainsFunc(args, sc.isBool)
	}

	return false
}

func endsAsBoolean(toks []token) bool {
	n := len(toks)

	return n >= argsTwo && toks[n-1].isWord(typeBoolean) && toks[n-2].isWord(kwAs)
}

func splitCommasSig(toks []token) [][]token {
	var (
		parts [][]token
		cur   = make([]token, 0, len(toks))
		depth int
	)

	for _, t := range toks {
		switch {
		case t.isPunct("("):
			depth++
		case t.isPunct(")"):
			depth--
		case depth == 0 && t.isPunct(","):
			parts, cur = append(parts, cur), nil

			continue
		}

		cur = append(cur, t)
	}

	return append(parts, cur)
}
