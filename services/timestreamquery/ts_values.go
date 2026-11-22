package timestreamquery

import (
	"strconv"
	"strings"
)

func endsValues(t token) bool {
	if t.kind == tkEOF {
		return true
	}

	if t.kind != tkIdent {
		return false
	}

	switch strings.ToLower(t.text) {
	case "from", "order", kwLimit, "offset", "union", "intersect", "except", "fetch", kwWhere, "group", kwHaving, "as":
		return true
	}

	return false
}

// normalizeQuery strips comments and the trailing semicolon, then rewrites bare VALUES rows.
func normalizeQuery(q string) string {
	q = strings.TrimSpace(strings.TrimRight(strings.TrimSpace(stripSQLComments(q)), ";"))

	return rewriteQuantified(rewriteFunctionSyntax(normalizeValues(q)))
}

// normalizeValues rewrites `VALUES 1, 2` to `VALUES (1), (2)` and `VALUES ((a, b))` to `VALUES (a, b)`.
func normalizeValues(q string) string {
	if !strings.Contains(strings.ToLower(q), "values") {
		return q
	}

	toks, err := tokenizeSQL(q)
	if err != nil {
		return q
	}

	var (
		sb  strings.Builder
		cur int
	)

	for i := 0; i < len(toks); i++ {
		if !toks[i].is("values") {
			continue
		}

		end, items := valuesItems(q, toks, i+1)
		if len(items) == 0 {
			continue
		}

		sb.WriteString(q[cur:toks[i].end])
		sb.WriteString(" ")
		sb.WriteString(strings.Join(items, ", "))

		cur = end
		i = skipTokensTo(toks, i, end)
	}

	sb.WriteString(q[cur:])

	return sb.String()
}

func skipTokensTo(toks []token, from, end int) int {
	i := from

	for i+1 < len(toks) && toks[i+1].start < end {
		i++
	}

	return i
}

// valuesItems splits the row list that starts at toks[from]; end is the offset just past the last row.
func valuesItems(q string, toks []token, from int) (int, []string) {
	var (
		items []string
		depth int
		start = -1
		end   = -1
	)

	flush := func(stop int) {
		if start >= 0 && stop > start {
			items = append(items, normalizeValuesRow(strings.TrimSpace(q[start:stop])))
		}

		start = -1
	}

	for i := from; i < len(toks); i++ {
		t := toks[i]

		if depth == 0 && endsValues(t) {
			flush(toks[i].start)

			return finishValues(q, items, toks[i].start, end)
		}

		switch {
		case t.isOp("("):
			depth++
		case t.isOp(")"):
			depth--
			if depth < 0 {
				flush(t.start)

				return finishValues(q, items, t.start, end)
			}
		case t.isOp(",") && depth == 0:
			flush(t.start)

			continue
		}

		if start < 0 {
			start = t.start
		}

		end = t.end
	}

	return len(q), nil
}

func finishValues(q string, items []string, stop, end int) (int, []string) {
	if len(items) == 0 {
		return len(q), nil
	}

	if end < 0 || end > stop {
		end = stop
	}

	return end, items
}

func normalizeValuesRow(item string) string {
	if group, ok := parenGroup(item); ok {
		if inner, isGroup := parenGroup(strings.TrimSpace(group)); isGroup && hasTopComma(inner) {
			return "(" + inner + ")"
		}

		return item
	}

	return "(" + item + ")"
}

// parenGroup returns the text inside s when s is exactly one parenthesised group.
func parenGroup(s string) (string, bool) {
	if len(s) < argsTwo || s[0] != '(' || closeParen(s, 0) != len(s)-1 {
		return "", false
	}

	return s[1 : len(s)-1], true
}

func hasTopComma(s string) bool { return len(splitTop(s)) > 1 }

// rewriteValuesAliases turns `(VALUES ...) AS t(a, b)` into a SELECT that names the columns SQLite numbers itself.
func rewriteValuesAliases(q string) string {
	if !strings.Contains(strings.ToLower(q), "values") {
		return q
	}

	toks, err := tokenizeSQL(q)
	if err != nil {
		return q
	}

	var (
		sb  strings.Builder
		cur int
	)

	for i := 1; i+1 < len(toks); i++ {
		if !toks[i].is("values") || !toks[i-1].isOp("(") || toks[i-1].start < cur {
			continue
		}

		closeAt := closeParen(q, toks[i-1].start)
		if closeAt < 0 {
			continue
		}

		alias, names, end := valuesAlias(q, toks, closeAt)
		if alias == "" {
			continue
		}

		cols := make([]string, len(names))
		for k, n := range names {
			cols[k] = "column" + strconv.Itoa(k+1) + " AS " + n
		}

		sb.WriteString(q[cur:toks[i-1].start])
		sb.WriteString(
			"(SELECT " + strings.Join(cols, ", ") + " FROM " + q[toks[i-1].start:closeAt+1] + ") AS " + alias,
		)

		cur = end
	}

	sb.WriteString(q[cur:])

	return sb.String()
}

// valuesAlias reads `[AS] name (c1, c2)` following the parenthesis at closeAt.
func valuesAlias(q string, toks []token, closeAt int) (string, []string, int) {
	k := 0
	for k < len(toks) && toks[k].start <= closeAt {
		k++
	}

	if k < len(toks) && toks[k].is("as") {
		k++
	}

	if k+1 >= len(toks) || (toks[k].kind != tkIdent && toks[k].kind != tkQuoted) || !toks[k+1].isOp("(") {
		return "", nil, 0
	}

	alias := q[toks[k].start:toks[k].end]

	var names []string

	for j := k + argsTwo; j < len(toks); j++ {
		switch {
		case toks[j].isOp(")"):
			return alias, names, toks[j].end
		case toks[j].isOp(","):
		case toks[j].kind == tkIdent || toks[j].kind == tkQuoted:
			names = append(names, q[toks[j].start:toks[j].end])
		default:
			return "", nil, 0
		}
	}

	return "", nil, 0
}
