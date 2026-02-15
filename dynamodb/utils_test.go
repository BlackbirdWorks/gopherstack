package dynamodb_test

import (
	"Gopherstack/dynamodb"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractKeySchema(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()
	tableName := "SchemaTable"
	db.CreateTable([]byte(`{
		"TableName": "` + tableName + `",
		"KeySchema": [
			{"AttributeName": "pk", "KeyType": "HASH"},
			{"AttributeName": "sk", "KeyType": "RANGE"}
		],
		"AttributeDefinitions": [
			{"AttributeName": "pk", "AttributeType": "S"},
			{"AttributeName": "sk", "AttributeType": "N"}
		],
		"ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
		"GlobalSecondaryIndexes": [
			{
				"IndexName": "GSI1",
				"KeySchema": [
					{"AttributeName": "gsiPK", "KeyType": "HASH"},
					{"AttributeName": "gsiSK", "KeyType": "RANGE"}
				],
				"Projection": {"ProjectionType": "ALL"},
				"ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}
			}
		]
	}`))

	table := db.Tables[tableName]

	// Test Main Table Schema
	schema, idx, err := db.ExtractKeySchema(table, "")
	require.NoError(t, err)
	assert.Nil(t, idx)
	assert.Len(t, schema, 2)
	assert.Equal(t, "pk", schema[0].AttributeName)
	assert.Equal(t, "sk", schema[1].AttributeName)

	// Test GSI Schema
	schema, idx, err = db.ExtractKeySchema(table, "GSI1")
	require.NoError(t, err)
	assert.NotNil(t, idx)
	// idx is *Projection, so check its properties if needed, but not IndexName
	assert.Equal(t, "ALL", idx.ProjectionType)
	assert.Len(t, schema, 2)
	assert.Equal(t, "gsiPK", schema[0].AttributeName)
	assert.Equal(t, "gsiSK", schema[1].AttributeName)

	// Test Invalid Index
	_, _, err = db.ExtractKeySchema(table, "InvalidIndex")
	require.Error(t, err)
	// The error message is "Requested resource not found: Index: InvalidIndex not found"
	// It does not contain "ProcessFail" unless NewResourceNotFoundException adds it?
	// Let's check for "not found" which is safe.
	assert.Contains(t, err.Error(), "not found")
}

func TestSortCandidates(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()
	table := &dynamodb.Table{
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			{AttributeName: "sk_n", AttributeType: "N"},
			{AttributeName: "sk_s", AttributeType: "S"},
		},
	}

	// Test Number Sorting
	candidates := []map[string]any{
		{"sk_n": map[string]any{"N": "10"}},
		{"sk_n": map[string]any{"N": "2"}},
		{"sk_n": map[string]any{"N": "30"}},
	}
	skDef := dynamodb.KeySchemaElement{AttributeName: "sk_n", KeyType: "RANGE"}
	forward := true

	db.SortCandidates(candidates, skDef, table, &forward)

	assert.Equal(t, "2", candidates[0]["sk_n"].(map[string]any)["N"])
	assert.Equal(t, "10", candidates[1]["sk_n"].(map[string]any)["N"])
	assert.Equal(t, "30", candidates[2]["sk_n"].(map[string]any)["N"])

	// Test String Sorting
	candidatesRequest := []map[string]any{
		{"sk_s": map[string]any{"S": "c"}},
		{"sk_s": map[string]any{"S": "a"}},
		{"sk_s": map[string]any{"S": "b"}},
	}
	skDefS := dynamodb.KeySchemaElement{AttributeName: "sk_s", KeyType: "RANGE"}

	db.SortCandidates(candidatesRequest, skDefS, table, &forward)

	assert.Equal(t, "a", candidatesRequest[0]["sk_s"].(map[string]any)["S"])
	assert.Equal(t, "b", candidatesRequest[1]["sk_s"].(map[string]any)["S"])
	assert.Equal(t, "c", candidatesRequest[2]["sk_s"].(map[string]any)["S"])

	// Test Reverse Sorting
	forward = false
	db.SortCandidates(candidatesRequest, skDefS, table, &forward)
	assert.Equal(t, "c", candidatesRequest[0]["sk_s"].(map[string]any)["S"])
}

func TestFindExclusiveStartIndex(t *testing.T) {
	t.Parallel()
	// db := NewInMemoryDB() // Not needed for standalone function
	items := []map[string]any{
		{"pk": map[string]any{"S": "1"}},
		{"pk": map[string]any{"S": "2"}},
		{"pk": map[string]any{"S": "3"}},
	}

	keySchema := []dynamodb.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}}

	// Found "2" (index 1). Function returns index + 1 = 2.
	startKey := map[string]any{"pk": map[string]any{"S": "2"}}
	idx := dynamodb.FindExclusiveStartIndex(items, startKey, keySchema)
	assert.Equal(t, 2, idx)

	// Not Found
	startKey = map[string]any{"pk": map[string]any{"S": "4"}}
	idx = dynamodb.FindExclusiveStartIndex(items, startKey, keySchema)
	assert.Equal(t, 0, idx) // Should return 0 if not found (start from beginning?)
	// verify implementation behavior for not found: returns 0.
}

func TestCompareAny(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		v1   any
		v2   any
		typ  string
		want int
	}{
		{"Number <", "10", "20", "N", -1},
		{"Number >", "20", "10", "N", 1},
		{"Number =", "10", "10", "N", 0},
		{"String <", "a", "b", "S", -1},
		{"String >", "b", "a", "S", 1},
		{"String =", "a", "a", "S", 0},
		{"Nil v1", nil, "a", "S", 0},
		{"Nil v2", "a", nil, "S", 0},
		// v1 and v2 as ints/floats for "N" type (parseNumber handles it)
		{"Float <", 10.5, 20.5, "N", -1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := dynamodb.CompareAny(tc.v1, tc.v2, tc.typ)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestToString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		val  any
		want string
	}{
		{"str", "str"},
		{123, "123"},
		{true, "true"},
		{false, "false"},
		{nil, ""},
		{[]string{"a", "b"}, `["a","b"]`},
	}

	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			t.Parallel()
			got := dynamodb.ToString(tc.val)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestApplyGSIProjection(t *testing.T) {
	t.Parallel()
	item := map[string]any{
		"pk":    map[string]any{"S": "p"},
		"sk":    map[string]any{"N": "1"},
		"other": map[string]any{"S": "o"},
		"gsiPK": map[string]any{"S": "gp"},
		"gsiSK": map[string]any{"N": "2"},
	}

	// KEYS_ONLY
	projKeys := dynamodb.Projection{ProjectionType: "KEYS_ONLY"}
	keySchema := []dynamodb.KeySchemaElement{
		{AttributeName: "gsiPK", KeyType: "HASH"},
		{AttributeName: "gsiSK", KeyType: "RANGE"},
	}
	tableKeySchema := []dynamodb.KeySchemaElement{
		{AttributeName: "pk", KeyType: "HASH"},
		{AttributeName: "sk", KeyType: "RANGE"},
	}

	filtered := dynamodb.ApplyGSIProjection(item, projKeys, tableKeySchema, keySchema)
	assert.Len(t, filtered, 4) // pk, sk, gsiPK, gsiSK
	assert.Contains(t, filtered, "pk")
	assert.Contains(t, filtered, "sk")
	assert.Contains(t, filtered, "gsiPK")
	assert.Contains(t, filtered, "gsiSK")
	assert.NotContains(t, filtered, "other")

	// INCLUDE
	projInc := dynamodb.Projection{
		ProjectionType:   "INCLUDE",
		NonKeyAttributes: []string{"other"},
	}
	filteredInc := dynamodb.ApplyGSIProjection(item, projInc, tableKeySchema, keySchema)
	assert.Len(t, filteredInc, 5)
	assert.Contains(t, filteredInc, "other")

	// ALL
	projAll := dynamodb.Projection{ProjectionType: "ALL"}
	filteredAll := dynamodb.ApplyGSIProjection(item, projAll, tableKeySchema, keySchema)
	assert.Equal(t, item, filteredAll)
}

func TestRebuildIndexes(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()
	tableName := "RebuildTable"

	// Create table manually to avoid triggering implicit rebuilds (though CreateTable calls it)
	// We want to test the function itself.
	db.CreateTable([]byte(`{
		"TableName": "` + tableName + `",
		"KeySchema": [
			{"AttributeName": "pk", "KeyType": "HASH"},
			{"AttributeName": "sk", "KeyType": "RANGE"}
		],
		"AttributeDefinitions": [
			{"AttributeName": "pk", "AttributeType": "S"},
			{"AttributeName": "sk", "AttributeType": "N"}
		],
		"ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}
	}`))

	table := db.Tables[tableName]

	// Corrupt indexes manually
	table.InitializeIndexes() // Clears indexes
	assert.Empty(t, table.PKIndex())
	assert.Empty(t, table.PKSKIndex())

	// Add items directly to storage
	item1 := map[string]any{"pk": map[string]any{"S": "p1"}, "sk": map[string]any{"N": "1"}}
	item2 := map[string]any{"pk": map[string]any{"S": "p1"}, "sk": map[string]any{"N": "2"}}
	table.Items = append(table.Items, item1, item2)

	// Rebuild
	table.RebuildIndexes()

	// Verify
	assert.NotNil(t, table.PKSKIndex()["p1"])
	assert.Equal(t, 0, table.PKSKIndex()["p1"]["1"]) // Index of item1
	assert.Equal(t, 1, table.PKSKIndex()["p1"]["2"]) // Index of item2
}

func TestSortCandidates_InferredType(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()
	// Table WITHOUT AttributeDefinitions for SK
	table := &dynamodb.Table{
		AttributeDefinitions: []dynamodb.AttributeDefinition{},
	}

	candidates := []map[string]any{
		{"sk": map[string]any{"N": "10"}},
		{"sk": map[string]any{"N": "2"}},
		{"sk": map[string]any{"N": "30"}},
	}
	skDef := dynamodb.KeySchemaElement{AttributeName: "sk", KeyType: "RANGE"}
	forward := true

	db.SortCandidates(candidates, skDef, table, &forward)

	assert.Equal(t, "2", candidates[0]["sk"].(map[string]any)["N"])
	assert.Equal(t, "10", candidates[1]["sk"].(map[string]any)["N"])
	assert.Equal(t, "30", candidates[2]["sk"].(map[string]any)["N"])
}

func TestParseStr(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "test", dynamodb.ParseStr("test"))
	assert.Equal(t, "123", dynamodb.ParseStr(123))
	assert.Equal(t, "true", dynamodb.ParseStr(true))
	assert.Empty(t, dynamodb.ParseStr(nil))
}

func TestCalculateItemSize_Complex(t *testing.T) {
	t.Parallel()
	item := map[string]any{
		"pk": map[string]any{"S": strings.Repeat("a", 1000)},
		"list": map[string]any{"L": []any{
			map[string]any{"S": "val1"},
			map[string]any{"N": "123"},
		}},
	}
	size, err := dynamodb.CalculateItemSize(item)
	require.NoError(t, err)
	assert.Greater(t, size, 1000)
}
