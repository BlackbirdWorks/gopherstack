package timestreamquery

import (
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode"
)

var reCallHead = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)\s*\(`)

func isBooleanKeyword(w string) bool {
	switch w {
	case "AND", "OR", "NOT", "IS", "LIKE", "BETWEEN", "IN", "EXISTS",
		"=", "<>", "!=", "<", ">", "<=", ">=":
		return true
	}

	return false
}

// isBooleanFunc reports Trino/Timestream functions whose result type is BOOLEAN.
func isBooleanFunc(name string) bool {
	switch strings.ToUpper(name) {
	case "CONTAINS", "REGEXP_LIKE", "STARTS_WITH", "ENDS_WITH", "IS_NAN", "IS_FINITE", "IS_INFINITE",
		"ANY_MATCH", "ALL_MATCH", "NONE_MATCH", "JSON_ARRAY_CONTAINS", "BOOL_AND", "BOOL_OR", "EVERY",
		"ARRAYS_OVERLAP", "IS_JSON_SCALAR":
		return true
	}

	return false
}

type selectList struct {
	items []string
	depth int
}

// scanSelectLists returns the projection items of every SELECT in q with their paren depth.
func scanSelectLists(q string) []selectList {
	var out []selectList

	depth := 0
	n := len(q)

	for i := 0; i < n; i++ {
		switch c := q[i]; {
		case c == '\'' || c == '"':
			i = skipQuoted(q, i)
		case c == '(':
			depth++
		case c == ')':
			depth--
		case qWordStart(q, i) && qWordAt(q, i) == "SELECT":
			items := readProjection(q, i+len("SELECT"))
			out = append(out, selectList{items: items, depth: depth})
			i += len("SELECT") - 1
		}
	}

	return out
}

func qWordStart(q string, i int) bool {
	return (i == 0 || !isWordByte(q[i-1])) && isWordByte(q[i])
}

func isWordByte(b byte) bool {
	return b == '_' || unicode.IsLetter(rune(b)) || unicode.IsDigit(rune(b))
}

func qWordAt(q string, i int) string {
	j := i
	for j < len(q) && isWordByte(q[j]) {
		j++
	}

	return strings.ToUpper(q[i:j])
}

func skipQuoted(q string, i int) int {
	quote := q[i]

	for j := i + 1; j < len(q); j++ {
		if q[j] == quote {
			if j+1 < len(q) && q[j+1] == quote {
				j++

				continue
			}

			return j
		}
	}

	return len(q) - 1
}

func readProjection(q string, from int) []string {
	depth := 0
	start := from
	end := len(q)

	var items []string

scan:
	for i := from; i < len(q); i++ {
		switch c := q[i]; {
		case c == '\'' || c == '"':
			i = skipQuoted(q, i)
		case c == '(':
			depth++
		case c == ')':
			if depth == 0 {
				end = i

				break scan
			}

			depth--
		case c == ',' && depth == 0:
			items = append(items, q[start:i])
			start = i + 1
		case depth == 0 && qWordStart(q, i) && qWordAt(q, i) == "FROM":
			end = i

			break scan
		}
	}

	items = append(items, q[start:end])

	for i, it := range items {
		items[i] = strings.TrimSpace(it)
	}

	items[0] = trimQuantifier(items[0])

	return items
}

func trimQuantifier(item string) string {
	for _, p := range []string{"DISTINCT ", "ALL "} {
		if strings.HasPrefix(strings.ToUpper(item), p) {
			return strings.TrimSpace(item[len(p):])
		}
	}

	return item
}

// topLevelTokens returns the upper-cased words and comparison operators at paren depth 0.
func topLevelTokens(e string) []string {
	var toks []string

	depth := 0

	for i := 0; i < len(e); i++ {
		switch c := e[i]; {
		case c == '\'' || c == '"':
			i = skipQuoted(e, i)
			if depth == 0 {
				toks = append(toks, "<literal>")
			}
		case c == '(':
			depth++
		case c == ')':
			depth--
		case depth != 0:
		case qWordStart(e, i):
			w := qWordAt(e, i)
			toks = append(toks, w)
			i += len(w) - 1
		case c == '<' || c == '>' || c == '=' || c == '!':
			op := readOperator(e, i)
			toks = append(toks, op)
			i += len(op) - 1
		}
	}

	return toks
}

func readOperator(e string, i int) string {
	j := i + 1
	for j < len(e) && (e[j] == '=' || e[j] == '>') {
		j++
	}

	return e[i:j]
}

func stripOuterParens(e string) string {
	for {
		e = strings.TrimSpace(e)
		if len(e) < 2 || e[0] != '(' || matchParen(e, 0) != len(e)-1 {
			return e
		}

		e = e[1 : len(e)-1]
	}
}

func matchParen(e string, open int) int {
	depth := 0

	for i := open; i < len(e); i++ {
		switch e[i] {
		case '\'', '"':
			i = skipQuoted(e, i)
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}

	return -1
}

// stripAlias drops a trailing "AS alias" from a projection item.
func stripAlias(item string) (string, string) {
	depth := 0
	last := -1

	for i := 0; i < len(item); i++ {
		switch c := item[i]; {
		case c == '\'' || c == '"':
			i = skipQuoted(item, i)
		case c == '(':
			depth++
		case c == ')':
			depth--
		case depth == 0 && qWordStart(item, i) && qWordAt(item, i) == "AS":
			last = i
		}
	}

	if last < 0 {
		return item, ""
	}

	return strings.TrimSpace(item[:last]), stripQuotes(strings.TrimSpace(item[last+2:]))
}

// isBooleanExpr reports whether a SQL expression always evaluates to BOOLEAN.
func isBooleanExpr(expr string) bool {
	e := stripOuterParens(expr)
	if e == "" {
		return false
	}

	if m := reCallHead.FindStringSubmatch(e); m != nil && !isBooleanKeyword(strings.ToUpper(m[1])) {
		open := len(m[0]) - 1
		if matchParen(e, open) == len(e)-1 {
			return isBooleanFunc(m[1])
		}
	}

	toks := topLevelTokens(e)
	if len(toks) == 0 {
		return false
	}

	if toks[0] == "CASE" || toks[0] == "CAST" || toks[0] == "TRY_CAST" {
		return false
	}

	if len(toks) == 1 {
		return toks[0] == "TRUE" || toks[0] == "FALSE" || toks[0] == "NOT" || toks[0] == "EXISTS"
	}

	return slices.ContainsFunc(toks, isBooleanKeyword)
}

func booleanAliases(lists []selectList) map[string]bool {
	out := map[string]bool{}

	for _, l := range lists {
		for _, it := range l.items {
			expr, alias := stripAlias(it)
			if alias != "" && isBooleanExpr(expr) {
				out[strings.ToLower(alias)] = true
			}
		}
	}

	return out
}

func outermostSelect(lists []selectList) selectList {
	top := lists[0]
	for _, l := range lists {
		if l.depth < top.depth {
			top = l
		}
	}

	return top
}

// booleanColumnPlan decides which result columns of query are BOOLEAN.
// Plain references to an alias that names a boolean expression in any SELECT inherit the type.
func booleanColumnPlan(query string, names []string) []bool {
	lists := scanSelectLists(query)
	plan := make([]bool, len(names))

	if len(lists) == 0 {
		return plan
	}

	boolAlias := booleanAliases(lists)
	top := outermostSelect(lists)

	positional := len(top.items) == len(names)
	for _, it := range top.items {
		if strings.Contains(it, "*") && !strings.Contains(it, "(") {
			positional = false
		}
	}

	for i, n := range names {
		byAlias := boolAlias[strings.ToLower(n)]
		if !positional {
			plan[i] = byAlias

			continue
		}

		expr, _ := stripAlias(top.items[i])
		plan[i] = isBooleanExpr(expr) || (isPlainColumnRef(expr) && byAlias)
	}

	return plan
}

// applyBooleanTypes retypes engine 0/1 results of boolean expressions as BOOLEAN true/false.
func applyBooleanTypes(rs *resultSet, query string) {
	names := make([]string, len(rs.cols))
	for i, c := range rs.cols {
		names[i] = c.Name
	}

	plan := booleanColumnPlan(query, names)

	for i, isBool := range plan {
		if !isBool || (rs.cols[i].Type.ScalarType != ScalarTypeBigint && len(rs.rows) != 0) {
			continue
		}

		rs.cols[i].Type.ScalarType = ScalarTypeBoolean

		for r := range rs.rows {
			d := &rs.rows[r].Data[i]
			if d.ScalarValue == nil {
				continue
			}

			b := *d.ScalarValue != "0"
			s := strconv.FormatBool(b)
			d.ScalarValue = &s
		}
	}
}
