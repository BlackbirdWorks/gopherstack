package athena

import (
	"fmt"
	"strconv"
	"strings"
)

// sqlResult stores the computed result of a SELECT query.
type sqlResult struct {
	columns []sqlColumn
	rows    [][]string // each element is a row of string-encoded values
}

type sqlColumn struct {
	name string
	typ  string
}

// sqlResultPage is a paginated slice of a sqlResult.
type sqlResultPage struct {
	NextToken string
	Columns   []sqlColumn
	Rows      [][]string
}

// InsertRows loads test data into a table for SQL execution.
// Rows are maps of column-name → value. All values are stored as-is and
// converted to strings when returned by GetQueryResults.
func (b *InMemoryBackend) InsertRows(catalog, database, table string, rows []map[string]any) {
	b.mu.Lock("InsertRows")
	defer b.mu.Unlock()

	key := catalog + "/" + database + "/" + table
	b.tableData[key] = append(b.tableData[key], rows...)
}

// executeSQL parses a basic SELECT query and returns its result set.
// Supported syntax:
//
//	SELECT * | col1[, col2, ...]
//	FROM [catalog.][database.]table
//	[WHERE col = 'val' | col = num]
//	[LIMIT n]
//
// Queries without a recognisable FROM clause, or referencing an unknown table,
// return an empty result set (no columns, no rows).
func (b *InMemoryBackend) executeSQL(query string, ctx QueryExecutionContext) *sqlResult {
	q := strings.TrimSpace(query)
	upper := strings.ToUpper(q)

	if !strings.HasPrefix(upper, "SELECT") {
		return &sqlResult{}
	}

	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx < 0 {
		return &sqlResult{}
	}

	selectClause := strings.TrimSpace(q[len("SELECT"):fromIdx])
	rest := strings.TrimSpace(q[fromIdx+len(" FROM "):])

	tableRef, whereClause, limit := parseFromClause(rest)

	catalog, database, table := resolveTableRef(tableRef, ctx)
	key := catalog + "/" + database + "/" + table

	b.mu.RLock("executeSQL")
	rawRows := b.tableData[key]
	meta := b.tables[catalog+"/"+database]
	b.mu.RUnlock()

	if rawRows == nil {
		return &sqlResult{}
	}

	colMeta := deriveColMeta(meta, table, rawRows)
	selected := resolveSelectedColumns(selectClause, colMeta)
	pred := parsePredicate(whereClause)

	return applyPredicateAndProject(rawRows, selected, pred, limit)
}

// parseFromClause parses the FROM ... [WHERE ...] [LIMIT ...] tail of a SELECT.
func parseFromClause(rest string) (string, string, int) {
	var tableRef, whereClause string
	var limit int

	upper := strings.ToUpper(rest)
	whereIdx := strings.Index(upper, " WHERE ")
	limitIdx := strings.Index(upper, " LIMIT ")

	switch {
	case whereIdx >= 0 && (limitIdx < 0 || whereIdx < limitIdx):
		tableRef = strings.TrimSpace(rest[:whereIdx])
		restAfterWhere := strings.TrimSpace(rest[whereIdx+len(" WHERE "):])
		upperAfterWhere := strings.ToUpper(restAfterWhere)
		li := strings.Index(upperAfterWhere, " LIMIT ")
		if li >= 0 {
			whereClause = strings.TrimSpace(restAfterWhere[:li])
			limit = parseLimit(strings.TrimSpace(restAfterWhere[li+len(" LIMIT "):]))
		} else {
			whereClause = restAfterWhere
		}
	case limitIdx >= 0:
		tableRef = strings.TrimSpace(rest[:limitIdx])
		limit = parseLimit(strings.TrimSpace(rest[limitIdx+len(" LIMIT "):]))
	default:
		tableRef = rest
	}

	return tableRef, whereClause, limit
}

// deriveColMeta returns column metadata for a table, falling back to first-row keys.
func deriveColMeta(meta map[string]*TableMetadata, table string, rawRows []map[string]any) []Column {
	var colMeta []Column

	if meta != nil {
		if tm, ok := meta[table]; ok {
			colMeta = tm.Columns
		}
	}

	if len(colMeta) == 0 && len(rawRows) > 0 {
		for k := range rawRows[0] {
			colMeta = append(colMeta, Column{Name: k, Type: columnTypeString})
		}
		sortColumns(colMeta)
	}

	return colMeta
}

// resolveSelectedColumns expands the SELECT clause into a list of sqlColumns.
func resolveSelectedColumns(selectClause string, colMeta []Column) []sqlColumn {
	var selected []sqlColumn

	if strings.TrimSpace(selectClause) == "*" {
		for _, c := range colMeta {
			selected = append(selected, sqlColumn{name: c.Name, typ: c.Type})
		}
	} else {
		for _, raw := range splitColumns(selectClause) {
			name := strings.TrimSpace(raw)
			typ := columnType(name, colMeta)
			selected = append(selected, sqlColumn{name: name, typ: typ})
		}
	}

	return selected
}

// applyPredicateAndProject filters rows by pred and projects the selected columns.
func applyPredicateAndProject(rawRows []map[string]any, selected []sqlColumn, pred predicate, limit int) *sqlResult {
	var result [][]string

	for _, row := range rawRows {
		if pred != nil && !pred(row) {
			continue
		}

		projected := make([]string, 0, len(selected))
		for _, col := range selected {
			projected = append(projected, anyToString(row[col.name]))
		}

		result = append(result, projected)

		if limit > 0 && len(result) >= limit {
			break
		}
	}

	return &sqlResult{columns: selected, rows: result}
}

// GetQueryResults returns a paginated result page for a query execution.
// nextToken is a decimal-encoded row offset; empty string means start from 0.
// maxResults = 0 means "use default page size (1000)".
func (b *InMemoryBackend) GetQueryResults(id, nextToken string, maxResults int) (*sqlResultPage, error) {
	b.mu.RLock("GetQueryResults-lookup")
	res, ok := b.queryResults[id]
	b.mu.RUnlock()

	if !ok {
		// execution exists but has no stored result → empty
		return &sqlResultPage{}, nil
	}

	if maxResults <= 0 || maxResults > athenaMaxQueryResultsPageSize {
		maxResults = athenaMaxQueryResultsPageSize
	}

	offset := 0

	if nextToken != "" {
		n, err := strconv.Atoi(nextToken)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("%w: invalid NextToken", ErrValidation)
		}

		offset = n
	}

	if offset >= len(res.rows) {
		return &sqlResultPage{Columns: res.columns}, nil
	}

	end := min(offset+maxResults, len(res.rows))

	page := &sqlResultPage{
		Columns: res.columns,
		Rows:    res.rows[offset:end],
	}

	if end < len(res.rows) {
		page.NextToken = strconv.Itoa(end)
	}

	return page, nil
}

// --- helpers ---

const (
	tableRefPartsFull = 3 // catalog.database.table
	tableRefPartsTwo  = 2 // database.table
)

func resolveTableRef(ref string, ctx QueryExecutionContext) (string, string, string) {
	var catalog, database, table string
	parts := strings.Split(ref, ".")
	switch len(parts) {
	case tableRefPartsFull:
		catalog = parts[0]
		database = parts[1]
		table = parts[2]
	case tableRefPartsTwo:
		database = parts[0]
		table = parts[1]
	default:
		table = parts[0]
	}

	if catalog == "" {
		catalog = ctx.Catalog
	}

	if catalog == "" {
		catalog = awsDataCatalog
	}

	if database == "" {
		database = ctx.Database
	}

	if database == "" {
		database = "default"
	}

	return catalog, database, table
}

func parseLimit(s string) int {
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil || n <= 0 {
		return 0
	}

	return n
}

func splitColumns(s string) []string {
	return strings.Split(s, ",")
}

func columnType(name string, cols []Column) string {
	for _, c := range cols {
		if strings.EqualFold(c.Name, name) {
			return c.Type
		}
	}

	return columnTypeString
}

// predicate is a row-level filter function.
type predicate func(row map[string]any) bool

// parsePredicate parses a simple equality predicate: col = 'val' | col = num.
// Returns nil when the clause is empty or unparseable (meaning no filter).
func parsePredicate(clause string) predicate {
	if clause == "" {
		return nil
	}

	before, after, found := strings.Cut(clause, "=")
	if !found {
		return nil
	}

	col := strings.TrimSpace(before)
	val := strings.TrimSpace(after)
	val = strings.Trim(val, "'\"")

	return func(row map[string]any) bool {
		return anyToString(row[col]) == val
	}
}

// anyToString converts an arbitrary value to its string representation.
func anyToString(v any) string {
	if v == nil {
		return ""
	}

	switch t := v.(type) {
	case string:
		return t
	case int:
		return strconv.Itoa(t)
	case int32:
		return strconv.Itoa(int(t))
	case int64:
		return strconv.FormatInt(t, 10)
	case float32:
		return strconv.FormatFloat(float64(t), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		if t {
			return "true"
		}

		return "false"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// sortColumns sorts Column slice by name for deterministic output.
func sortColumns(cols []Column) {
	for i := 1; i < len(cols); i++ {
		for j := i; j > 0 && cols[j].Name < cols[j-1].Name; j-- {
			cols[j], cols[j-1] = cols[j-1], cols[j]
		}
	}
}
