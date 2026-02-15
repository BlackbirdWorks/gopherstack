package dynamodb

// Export internal functions for testing in the dynamodb_test package.
// This allows us to satisfy the testpackage linter while still unit testing
// the package's internal logic.

// EvaluateExpression is now exported in expressions.go

func CompareValues(lhs any, op string, rhs any) bool {
	return compareValues(lhs, op, rhs)
}

func UnwrapAttributeValue(v any) any {
	return unwrapAttributeValue(v)
}

// ExtractFunctionArgs is no longer supported

func FindExclusiveStartIndex(items []map[string]any, startKey map[string]any, keySchema []KeySchemaElement) int {
	return findExclusiveStartIndex(items, startKey, keySchema)
}

func CompareAny(v1, v2 any, typ string) int {
	return compareAny(v1, v2, typ)
}

func ToString(val any) string {
	return toString(val)
}

func ApplyGSIProjection(
	item map[string]any,
	proj Projection,
	tableKeySchema []KeySchemaElement,
	indexKeySchema []KeySchemaElement,
) map[string]any {
	return applyGSIProjection(item, proj, tableKeySchema, indexKeySchema)
}

func ParseStr(v any) string {
	return parseStr(v)
}

func (db *InMemoryDB) ExtractKeySchema(table *Table, indexName string) ([]KeySchemaElement, *Projection, error) {
	return db.extractKeySchema(table, indexName)
}

func (db *InMemoryDB) SortCandidates(
	candidates []map[string]any,
	skDef KeySchemaElement,
	table *Table,
	scanIndexForward *bool,
) {
	db.sortCandidates(candidates, skDef, table, scanIndexForward)
}

func (t *Table) PKIndex() map[string]int {
	return t.pkIndex
}

func (t *Table) PKSKIndex() map[string]map[string]int {
	return t.pkskIndex
}

func (t *Table) InitializeIndexes() {
	t.initializeIndexes()
}

func (t *Table) RebuildIndexes() {
	t.rebuildIndexes()
}
