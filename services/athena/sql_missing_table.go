package athena

import (
	"regexp"
	"strings"
)

var simpleTableRef = regexp.MustCompile(`^"?[A-Za-z_][\w$-]*"?(\."?[A-Za-z_][\w$-]*"?){0,2}$`)

// missingSelectTable fails a plain `SELECT ... FROM ref` whose table exists in
// neither the catalog nor the loaded row data, as Athena's TABLE_NOT_FOUND.
// Joins, subqueries and information_schema references are not inspected.
func (b *InMemoryBackend) missingSelectTable(query string, ctx QueryExecutionContext) statementOutcome {
	upper := strings.ToUpper(query)
	if hasKeyword(upper, "WITH") {
		return stmtOK()
	}

	fromIdx := strings.Index(upper, " FROM ")
	if fromIdx < 0 {
		return stmtOK()
	}

	ref, _, _ := parseFromClause(strings.TrimSpace(query[fromIdx+len(" FROM "):]))
	ref = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(ref), ";"))

	if !simpleTableRef.MatchString(ref) || strings.Contains(strings.ToLower(ref), "information_schema") {
		return stmtOK()
	}

	catalog, database, table := resolveTableRef(strings.ReplaceAll(ref, `"`, ""), ctx)

	b.mu.RLock("missingSelectTable")
	defer b.mu.RUnlock()

	if _, ok := b.tableData[catalog+"/"+database+"/"+table]; ok ||
		b.tables.Has(tableMetadataKey(catalog, database, table)) {
		return stmtOK()
	}

	if b.isGlueBacked(catalog) {
		if _, err := b.glueSource.GetTable(database, table); err == nil {
			return stmtOK()
		}
	}

	return stmtFail(athenaErrTypeEntityMiss,
		"TABLE_NOT_FOUND: line 1:15: Table '%s.%s.%s' does not exist",
		strings.ToLower(catalog), database, table)
}
