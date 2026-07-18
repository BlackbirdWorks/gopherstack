package athena

import (
	"errors"
	"strings"
)

// errUnbalancedValues is returned when a VALUES clause has unbalanced parentheses.
var errUnbalancedValues = errors.New("malformed VALUES clause")

// columnNames returns the ordered names of the given columns.
func columnNames(cols []Column) []string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.Name
	}

	return out
}

// resolveDatabaseRef splits a (possibly catalog-qualified) database reference into
// its catalog and database components, defaulting the catalog from ctx.
func resolveDatabaseRef(ref string, ctx QueryExecutionContext) (string, string) {
	ref = unquoteIdent(ref)

	catalog := ""
	database := ref

	if cat, db, ok := strings.Cut(ref, "."); ok {
		catalog = cat
		database = db
	}

	if catalog == "" {
		catalog = ctx.Catalog
	}

	if catalog == "" {
		catalog = awsDataCatalog
	}

	return catalog, unquoteIdent(database)
}

// unquoteIdent strips surrounding backticks, double quotes, and a trailing
// semicolon from an identifier token.
func unquoteIdent(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimSuffix(s, ";")
	s = strings.Trim(s, "`\"")

	return strings.TrimSpace(s)
}

// stripLeadingKeyword removes the first matching (case-insensitive) prefix from
// query and returns the remainder trimmed, plus whether a prefix matched.
func stripLeadingKeyword(query string, prefixes []string) (string, bool) {
	upper := strings.ToUpper(strings.TrimSpace(query))
	trimmed := strings.TrimSpace(query)

	for _, p := range prefixes {
		if strings.HasPrefix(upper, p) {
			return strings.TrimSpace(trimmed[len(p):]), true
		}
	}

	return trimmed, false
}

// stripIfClause removes a leading "IF NOT EXISTS" or "IF EXISTS" from rest,
// returning the remainder and whether the clause was present.
func stripIfClause(rest string) (string, bool) {
	upper := strings.ToUpper(rest)

	switch {
	case strings.HasPrefix(upper, "IF NOT EXISTS"):
		return strings.TrimSpace(rest[len("IF NOT EXISTS"):]), true
	case strings.HasPrefix(upper, "IF EXISTS"):
		return strings.TrimSpace(rest[len("IF EXISTS"):]), true
	default:
		return rest, false
	}
}

// parseDatabaseTarget extracts the database name and IF-clause flag from a
// CREATE/DROP DATABASE|SCHEMA statement.
func parseDatabaseTarget(query string, prefixes []string) (string, bool) {
	rest, _ := stripLeadingKeyword(query, prefixes)
	rest, ifClause := stripIfClause(rest)

	return unquoteIdent(firstToken(rest)), ifClause
}

// parseDropTable extracts the table name and IF EXISTS flag from a DROP
// TABLE/VIEW statement.
func parseDropTable(query string) (string, bool) {
	rest, _ := stripLeadingKeyword(query, []string{"DROP TABLE", "DROP VIEW"})
	rest, ifExists := stripIfClause(rest)

	return unquoteIdent(firstToken(rest)), ifExists
}

// parseCreateTable extracts the table name, column definitions, and IF NOT
// EXISTS flag from a CREATE [EXTERNAL] TABLE (...) statement.
func parseCreateTable(query string) (string, []Column, bool, bool) {
	rest, _ := stripLeadingKeyword(query, []string{"CREATE EXTERNAL TABLE", "CREATE TABLE"})
	rest, ifNotExists := stripIfClause(rest)

	open := strings.Index(rest, "(")
	if open < 0 {
		return "", nil, ifNotExists, false
	}

	name := unquoteIdent(strings.TrimSpace(rest[:open]))

	inner, ok := extractParenGroup(rest[open:])
	if !ok {
		return "", nil, ifNotExists, false
	}

	cols := parseColumnDefs(inner)

	return name, cols, ifNotExists, true
}

// parseColumnDefs parses a comma-separated column definition list ("col type,
// ...") into Columns, ignoring table-level clauses.
func parseColumnDefs(inner string) []Column {
	var cols []Column

	for _, def := range splitTopLevel(inner) {
		def = strings.TrimSpace(def)
		if def == "" {
			continue
		}

		name, typ, _ := strings.Cut(def, " ")
		name = unquoteIdent(name)

		if name == "" {
			continue
		}

		cols = append(cols, Column{Name: name, Type: strings.TrimSpace(typ)})
	}

	return cols
}

// parseCTAS extracts the table name and the SELECT sub-statement from a CREATE
// TABLE ... AS SELECT statement.
func parseCTAS(query string) (string, string, bool) {
	rest, _ := stripLeadingKeyword(query, []string{"CREATE EXTERNAL TABLE", "CREATE TABLE"})
	rest, _ = stripIfClause(rest)

	asIdx := indexTopLevelAs(rest)
	if asIdx < 0 {
		return "", "", false
	}

	header := strings.TrimSpace(rest[:asIdx])
	selectSQL := strings.TrimSpace(rest[asIdx+len(" AS "):])

	// The table name is the first token of the header; any WITH(...) properties
	// follow it and are ignored.
	name := unquoteIdent(firstToken(header))

	return name, selectSQL, true
}

// indexTopLevelAs returns the byte index of the first " AS " token that is not
// nested inside parentheses, or -1.
func indexTopLevelAs(s string) int {
	upper := strings.ToUpper(s)
	depth := 0

	for i := 0; i+4 <= len(upper); i++ {
		switch upper[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}

		if depth == 0 && upper[i:i+4] == " AS " {
			return i
		}
	}

	return -1
}

// parseInsert parses an INSERT INTO statement, returning the target table, an
// optional explicit column list, the raw VALUES payload (when present), and the
// SELECT sub-statement (when present).
func parseInsert(query string) (string, []string, string, string, bool) {
	rest, ok := stripLeadingKeyword(query, []string{"INSERT INTO"})
	if !ok {
		return "", nil, "", "", false
	}

	// Target table: up to the first '(' or whitespace.
	tEnd := strings.IndexAny(rest, " \t\n(")
	if tEnd < 0 {
		return "", nil, "", "", false
	}

	target := unquoteIdent(rest[:tEnd])
	rest = strings.TrimSpace(rest[tEnd:])

	var cols []string

	if strings.HasPrefix(rest, "(") {
		inner, gok := extractParenGroup(rest)
		if !gok {
			return "", nil, "", "", false
		}

		for _, c := range splitTopLevel(inner) {
			cols = append(cols, unquoteIdent(c))
		}

		rest = strings.TrimSpace(rest[strings.Index(rest, ")")+1:])
	}

	upper := strings.ToUpper(rest)

	switch {
	case strings.HasPrefix(upper, "VALUES"):
		return target, cols, strings.TrimSpace(rest[len("VALUES"):]), "", true
	case strings.HasPrefix(upper, "SELECT"), strings.HasPrefix(upper, "WITH"):
		return target, cols, "", rest, true
	default:
		return "", nil, "", "", false
	}
}

// parseValuesTuples parses a VALUES payload — "(a, b), (c, d)" — into a slice of
// positional string rows.
func parseValuesTuples(payload string) ([][]string, error) {
	var tuples [][]string

	i := 0
	for i < len(payload) {
		// Advance to the next '('.
		for i < len(payload) && payload[i] != '(' {
			i++
		}

		if i >= len(payload) {
			break
		}

		inner, ok := extractParenGroup(payload[i:])
		if !ok {
			return nil, errUnbalancedValues
		}

		vals := splitTopLevel(inner)
		row := make([]string, len(vals))

		for j, v := range vals {
			row[j] = unquoteLiteral(v)
		}

		tuples = append(tuples, row)

		// Skip past the closing ')'.
		i += strings.Index(payload[i:], ")") + 1
	}

	if len(tuples) == 0 {
		return nil, errUnbalancedValues
	}

	return tuples, nil
}

// unquoteLiteral trims surrounding single/double quotes from a SQL literal.
func unquoteLiteral(s string) string {
	const minQuotedLen = 2 // an opening and a closing quote

	s = strings.TrimSpace(s)
	if len(s) >= minQuotedLen {
		if (s[0] == '\'' && s[len(s)-1] == '\'') || (s[0] == '"' && s[len(s)-1] == '"') {
			return s[1 : len(s)-1]
		}
	}

	return s
}

// extractParenGroup returns the content between the leading '(' of s and its
// matching ')', respecting nesting. s must start at or before the opening paren.
func extractParenGroup(s string) (string, bool) {
	open := strings.Index(s, "(")
	if open < 0 {
		return "", false
	}

	depth := 0

	for i := open; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return s[open+1 : i], true
			}
		}
	}

	return "", false
}

// splitTopLevel splits s on commas that are not nested inside parentheses or
// angle brackets (so types like decimal(10,2) and array<int,int> stay intact).
func splitTopLevel(s string) []string {
	var (
		parts []string
		buf   strings.Builder
		depth int
	)

	for i := range len(s) {
		switch s[i] {
		case '(', '<':
			depth++
		case ')', '>':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(buf.String()))
				buf.Reset()

				continue
			}
		}

		buf.WriteByte(s[i])
	}

	if strings.TrimSpace(buf.String()) != "" {
		parts = append(parts, strings.TrimSpace(buf.String()))
	}

	return parts
}
