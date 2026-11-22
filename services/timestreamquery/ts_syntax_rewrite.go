package timestreamquery

import (
	"regexp"
	"strconv"
	"strings"
)

var (
	rePosition = regexp.MustCompile(`(?i)\bposition\s*\(`)
	reExtract  = regexp.MustCompile(
		`(?i)\bextract\s*\(\s*(dow|doy|yow|quarter|week|day_of_year|day_of_month|day_of_week)\s+from\s`)
)

func extractFunction(field string) string {
	switch strings.ToLower(field) {
	case "dow", "day_of_week":
		return "day_of_week"
	case "doy", "day_of_year":
		return "day_of_year"
	case "yow":
		return "year_of_week"
	case unitDayOfMonth:
		return unitDay
	}

	return strings.ToLower(field)
}

// rewriteFunctionSyntax rewrites position(x IN y) and the extract fields the base engine lacks into plain calls.
func rewriteFunctionSyntax(q string) string {
	lq := strings.ToLower(q)
	if !strings.Contains(lq, "position") && !strings.Contains(lq, "extract") {
		return q
	}

	q = rewriteCalls(q, rePosition, func(_ []string, inner string) (string, bool) {
		parts := splitTopWord(inner, "in")
		if len(parts) != argsTwo {
			return "", false
		}

		return "strpos(" + parts[1] + ", " + parts[0] + ")", true
	})

	return rewriteCalls(q, reExtract, func(g []string, inner string) (string, bool) {
		return extractFunction(g[1]) + "(" + inner + ")", true
	})
}

// rewriteCalls replaces each match of re (which ends just before the call's arguments or at its opening parenthesis)
// with the text replace returns for the argument text.
func rewriteCalls(q string, re *regexp.Regexp, replace func(groups []string, inner string) (string, bool)) string {
	mask := quoteMask(q)

	var (
		sb  strings.Builder
		pos int
	)

	for _, loc := range re.FindAllStringSubmatchIndex(q, -1) {
		if loc[0] < pos || mask[loc[0]] {
			continue
		}

		open := loc[0] + strings.Index(q[loc[0]:loc[1]], "(")

		closeAt := closeParen(q, open)
		if closeAt < 0 {
			continue
		}

		out, ok := replace(submatches(q, loc), q[max(open+1, loc[1]):closeAt])
		if !ok {
			continue
		}

		sb.WriteString(q[pos:loc[0]])
		sb.WriteString(out)

		pos = closeAt + 1
	}

	sb.WriteString(q[pos:])

	return sb.String()
}

func submatches(q string, loc []int) []string {
	groups := make([]string, 0, len(loc)/argsTwo)

	for i := 0; i+1 < len(loc); i += argsTwo {
		if loc[i] >= 0 {
			groups = append(groups, q[loc[i]:loc[i+1]])
		} else {
			groups = append(groups, "")
		}
	}

	return groups
}

// splitTopWord splits s around the first top-level occurrence of the keyword word.
func splitTopWord(s, word string) []string {
	mask := quoteMask(s)
	depth := 0

	for i := range len(s) {
		switch {
		case mask[i]:
		case s[i] == '(' || s[i] == '[':
			depth++
		case s[i] == ')' || s[i] == ']':
			depth--
		case depth == 0 && isWordAt(s, i, word):
			return []string{strings.TrimSpace(s[:i]), strings.TrimSpace(s[i+len(word):])}
		}
	}

	return []string{s}
}

func isWordAt(s string, i int, word string) bool {
	if i+len(word) > len(s) || !strings.EqualFold(s[i:i+len(word)], word) {
		return false
	}

	before := i == 0 || !isIdentPart(s[i-1])
	after := i+len(word) == len(s) || !isIdentPart(s[i+len(word)])

	return before && after
}

func isQuantifier(t token) bool { return t.is("all") || t.is("any") || t.is("some") }

func operandBoundary(t token) bool {
	if t.kind == tkOp {
		return t.text == "," || isCmpOp(t.text)
	}

	if t.kind != tkIdent {
		return false
	}

	switch strings.ToLower(t.text) {
	case "select", "distinct", "where", "and", "or", "not", "on", kwHaving, "when", "then", "else", "case", "by",
		kwLimit, "using":
		return true
	}

	return false
}

// operandStart returns the offset where the expression ending before toks[opIdx] begins, or -1.
func operandStart(toks []token, opIdx int) int {
	depth := 0
	start := -1

	for j := opIdx - 1; j >= 0; j-- {
		t := toks[j]

		switch {
		case t.isOp(")"):
			depth++
		case t.isOp("("):
			if depth == 0 {
				return start
			}

			depth--
		case depth == 0 && operandBoundary(t):
			return start
		}

		start = t.start
	}

	return start
}

// rewriteQuantified rewrites `x op ALL|ANY|SOME (query)` into a scalar subquery over the query's rows.
func rewriteQuantified(q string) string {
	lq := strings.ToLower(q)
	if !strings.Contains(lq, "all") && !strings.Contains(lq, "any") && !strings.Contains(lq, "some") {
		return q
	}

	toks, err := tokenizeSQL(q)
	if err != nil {
		return q
	}

	var (
		sb      strings.Builder
		cur     int
		counter int
	)

	for i := 1; i+2 < len(toks); i++ {
		repl, operand, end, ok := quantifiedAt(q, toks, i, counter)
		if !ok || operand < cur {
			continue
		}

		sb.WriteString(q[cur:operand])
		sb.WriteString(repl)

		counter++
		cur = end

		for i+1 < len(toks) && toks[i+1].start < cur {
			i++
		}
	}

	sb.WriteString(q[cur:])

	return sb.String()
}

// quantifiedAt rewrites the quantified comparison whose operator is toks[i]; operand and end are its source span.
func quantifiedAt(q string, toks []token, i, counter int) (string, int, int, bool) {
	t := toks[i]
	if t.kind != tkOp || !isCmpOp(t.text) || !isQuantifier(toks[i+1]) || !toks[i+argsTwo].isOp("(") {
		return "", 0, 0, false
	}

	operand := operandStart(toks, i)
	open := toks[i+argsTwo].start
	closeAt := closeParen(q, open)

	if operand < 0 || closeAt < 0 {
		return "", 0, 0, false
	}

	aliased, ok := aliasFirstColumn(rewriteQuantified(q[open+1 : closeAt]))
	if !ok {
		return "", 0, 0, false
	}

	repl := quantifiedSQL(
		strings.TrimSpace(q[operand:t.start]),
		t.text,
		strings.ToLower(toks[i+1].text),
		aliased,
		counter,
	)

	return repl, operand, closeAt + 1, true
}

// aliasFirstColumn names the single output column of a quantified subquery "_v".
func aliasFirstColumn(sub string) (string, bool) {
	toks, err := tokenizeSQL(sub)
	if err != nil || len(toks) < argsTwo {
		return "", false
	}

	if toks[0].is("values") {
		return "SELECT column1 AS \"_v\" FROM (" + sub + ")", true
	}

	if !toks[0].is("select") {
		return "", false
	}

	return aliasSelectItem(sub, toks)
}

func aliasSelectItem(sub string, toks []token) (string, bool) {
	i := 1
	if toks[i].is("distinct") || toks[i].is("all") {
		i++
	}

	depth := 0
	itemStart := toks[i].start

	for j := i; j < len(toks); j++ {
		t := toks[j]

		switch {
		case t.isOp("("):
			depth++
		case t.isOp(")"):
			depth--
		case depth == 0 && endsSelectItem(t):
			return aliasItem(sub, toks, i, j, itemStart), !t.isOp(",")
		}
	}

	return "", false
}

func endsSelectItem(t token) bool {
	return t.is("from") || t.isOp(",") || t.kind == tkEOF || t.is("union") || t.is(kwWhere)
}

func aliasItem(sub string, toks []token, from, to, itemStart int) string {
	end := toks[to].start
	if to > from+1 && toks[to-2].is("as") {
		return sub[:toks[to-2].start] + `AS "_v"` + sub[toks[to-1].end:]
	}

	return sub[:itemStart] + strings.TrimRight(sub[itemStart:end], " \t\r\n") + ` AS "_v" ` + sub[end:]
}

func quantifiedSQL(operand, op, mode, sub string, n int) string {
	aliased, ok := aliasFirstColumn(sub)
	if !ok {
		return "(" + operand + ") " + op + " " + strings.ToUpper(mode) + " (" + sub + ")"
	}

	rel := `(` + aliased + `) AS "_q` + strconv.Itoa(n) + `"`
	cmp := "(" + operand + ") " + op + ` "_v"`

	all := `SELECT CASE WHEN count(*) = 0 THEN 1 ` +
		`WHEN max(CASE WHEN NOT (` + cmp + `) THEN 1 ELSE 0 END) = 1 THEN 0 ` +
		`WHEN max(CASE WHEN (` + cmp + `) IS NULL THEN 1 ELSE 0 END) = 1 THEN NULL ELSE 1 END FROM ` + rel

	some := `SELECT CASE WHEN max(CASE WHEN (` + cmp + `) THEN 1 ELSE 0 END) = 1 THEN 1 ` +
		`WHEN count(*) = 0 THEN 0 ` +
		`WHEN max(CASE WHEN (` + cmp + `) IS NULL THEN 1 ELSE 0 END) = 1 THEN NULL ELSE 0 END FROM ` + rel

	if mode == "all" {
		return "((" + all + ") = 1)"
	}

	return "((" + some + ") = 1)"
}
