package dynamodb

import (
	"context"

	"github.com/blackbirdworks/gopherstack/dynamodb/models"
	"github.com/blackbirdworks/gopherstack/pkgs/dynamoattr"
)

// Export internal functions for testing in the dynamodb_test package.
// This allows us to satisfy the testpackage linter while still unit testing
// the package's internal logic.

// EvaluateExpression is now exported in expressions.go

func CompareValues(lhs any, op string, rhs any) bool {
	return dynamoattr.CompareValues(lhs, op, rhs)
}

func UnwrapAttributeValue(v any) any {
	return dynamoattr.UnwrapAttributeValue(v)
}

// ExtractFunctionArgs is no longer supported

func FindExclusiveStartIndex(
	items []map[string]any,
	startKey map[string]any,
	keySchema []models.KeySchemaElement,
) int {
	return findExclusiveStartIndex(items, startKey, keySchema)
}

func CompareAny(v1, v2 any, typ string) int {
	return compareAny(v1, v2, typ)
}

func ToString(val any) string {
	return dynamoattr.ToString(val)
}

func ApplyGSIProjection(
	item map[string]any,
	proj models.Projection,
	tableKeySchema []models.KeySchemaElement,
	indexKeySchema []models.KeySchemaElement,
) map[string]any {
	return applyGSIProjection(item, proj, tableKeySchema, indexKeySchema)
}

func ParseStr(v any) string {
	return dynamoattr.ToString(v)
}

func (db *InMemoryDB) ExtractKeySchema(
	table *Table,
	indexName string,
) ([]models.KeySchemaElement, *models.Projection, error) {
	return db.extractKeySchema(table, indexName)
}

func (db *InMemoryDB) SortCandidates(
	candidates []map[string]any,
	skDef models.KeySchemaElement,
	table *Table,
	scanIndexForward bool,
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

func (j *Janitor) SweepTTL(ctx context.Context) {
	j.sweepTTL(ctx)
}

// StreamShardID exposes the canonical shard ID for stream tests.
const StreamShardID = streamShardID
