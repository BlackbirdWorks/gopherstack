package timestreamquery

import (
	"slices"
	"strings"
	"unicode"
)

// SelectColumn is a PrepareQuery output column (types.SelectColumn).
type SelectColumn struct {
	DatabaseName string
	TableName    string
	ColumnInfo
	Aliased bool
}

func inferSelectColumns(queryString string, cols []ColumnInfo) []SelectColumn {
	out := make([]SelectColumn, len(cols))
	for i, c := range cols {
		out[i] = SelectColumn{ColumnInfo: c}
	}

	q := strings.TrimSpace(queryString)
	upper := strings.ToUpper(q)
	if !strings.HasPrefix(upper, "SELECT") {
		return out
	}

	fromIdx := indexKeyword(upper, "FROM")
	if fromIdx < 0 {
		return out
	}

	projection := strings.TrimSpace(q[len("SELECT"):fromIdx])
	if projection == "*" {
		return out
	}

	db, table := singleFromTable(q[fromIdx+len("FROM"):])
	parts := splitProjection(projection)

	idx := 0
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		if idx >= len(out) {
			break
		}

		expr, aliased := splitAlias(part)
		out[idx].Aliased = aliased

		if isPlainColumnRef(expr) {
			out[idx].DatabaseName = db
			out[idx].TableName = table
		}

		idx++
	}

	return out
}

func splitAlias(item string) (string, bool) {
	upper := strings.ToUpper(item)
	if i := strings.LastIndex(upper, " AS "); i >= 0 {
		return strings.TrimSpace(item[:i]), true
	}

	return item, false
}

func isPlainColumnRef(expr string) bool {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return false
	}

	idents := splitQualified(expr)
	if len(idents) == 0 || len(idents) > 2 {
		return false
	}

	return !slices.Contains(idents, "")
}

// splitQualified splits dotted identifiers, honouring double-quoted segments.
// It returns nil when expr contains anything other than identifiers and dots.
func splitQualified(expr string) []string {
	var (
		parts   []string
		cur     strings.Builder
		inQuote bool
	)

	for _, r := range expr {
		switch {
		case r == '"':
			inQuote = !inQuote
		case inQuote:
			cur.WriteRune(r)
		case r == '.':
			parts = append(parts, cur.String())
			cur.Reset()
		case unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_':
			cur.WriteRune(r)
		default:
			return nil
		}
	}

	if inQuote {
		return nil
	}

	return append(parts, cur.String())
}

func singleFromTable(fromRest string) (string, string) {
	rest := strings.TrimSpace(fromRest)
	upper := strings.ToUpper(rest)

	for _, kw := range []string{"JOIN", "UNION"} {
		if indexKeyword(upper, kw) >= 0 {
			return "", ""
		}
	}

	end := len(rest)
	for _, kw := range []string{"WHERE", "GROUP", "ORDER", "HAVING", "LIMIT", "OFFSET", "WINDOW"} {
		if i := indexKeyword(upper, kw); i >= 0 && i < end {
			end = i
		}
	}

	clause := strings.TrimSpace(rest[:end])
	if strings.ContainsAny(clause, ",(") {
		return "", ""
	}

	ref := clause
	if f := strings.Fields(clause); len(f) > 0 {
		ref = f[0]
	}

	idents := splitQualified(ref)
	if len(idents) != 2 || idents[0] == "" || idents[1] == "" {
		return "", ""
	}

	return idents[0], idents[1]
}
