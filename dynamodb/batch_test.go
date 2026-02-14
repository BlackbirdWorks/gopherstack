package dynamodb_test

import (
	"Gopherstack/dynamodb"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchWriteItem(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()

	// Setup tables
	for _, name := range []string{"Table1", "Table2"} {
		_, err := db.CreateTable([]byte(`{
			"TableName": "` + name + `",
			"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
			"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
			"ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
		}`))
		require.NoError(t, err)
	}

	// Batch Write: Put items into Table1 and Table2
	input := `{
		"RequestItems": {
			"Table1": [
				{"PutRequest": {"Item": {"pk": {"S": "item1"}, "val": {"S": "v1"}}}},
				{"PutRequest": {"Item": {"pk": {"S": "item2"}, "val": {"S": "v2"}}}}
			],
			"Table2": [
				{"PutRequest": {"Item": {"pk": {"S": "item3"}, "val": {"S": "v3"}}}}
			]
		}
	}`

	_, err := db.BatchWriteItem([]byte(input))
	require.NoError(t, err)

	// Verify items
	verifyItem(t, db, "Table1", "item1", true)
	verifyItem(t, db, "Table1", "item2", true)
	verifyItem(t, db, "Table2", "item3", true)

	// Batch Write: Delete items from Table1
	inputDelete := `{
		"RequestItems": {
			"Table1": [
				{"DeleteRequest": {"Key": {"pk": {"S": "item1"}}}}
			]
		}
	}`
	_, err = db.BatchWriteItem([]byte(inputDelete))
	require.NoError(t, err)

	verifyItem(t, db, "Table1", "item1", false)
	verifyItem(t, db, "Table1", "item2", true)
}

func TestBatchGetItem(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()

	// Setup table and data
	tableName := "BatchGetTable"
	_, err := db.CreateTable([]byte(`{
		"TableName": "` + tableName + `",
		"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
		"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
		"ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
	}`))
	require.NoError(t, err)

	db.PutItem([]byte(`{"TableName": "` + tableName + `", "Item": {"pk": {"S": "item1"}, "val": {"S": "v1"}}}`))
	db.PutItem([]byte(`{"TableName": "` + tableName + `", "Item": {"pk": {"S": "item2"}, "val": {"S": "v2"}}}`))
	db.PutItem([]byte(`{"TableName": "` + tableName + `", "Item": {"pk": {"S": "item3"}, "val": {"S": "v3"}}}`))

	// Batch Get
	input := `{
		"RequestItems": {
			"` + tableName + `": {
				"Keys": [
					{"pk": {"S": "item1"}},
					{"pk": {"S": "item3"}}
				]
			}
		}
	}`

	res, err := db.BatchGetItem([]byte(input))
	require.NoError(t, err)

	out, ok := res.(dynamodb.BatchGetItemOutput)
	require.True(t, ok)

	items, ok := out.Responses[tableName]
	require.True(t, ok)
	assert.Len(t, items, 2)

	// Check content
	found1 := false
	found3 := false
	for _, item := range items {
		pk := item["pk"].(map[string]any)["S"]
		if pk == "item1" {
			found1 = true
		}
		if pk == "item3" {
			found3 = true
		}
	}
	assert.True(t, found1, "item1 not found")
	assert.True(t, found3, "item3 not found")
}

func TestBatchWriteItem_ValidationErrors(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()
	// No tables created

	// Table not found
	input := `{
		"RequestItems": {
			"MissingTable": [
				{"PutRequest": {"Item": {"pk": {"S": "item1"}}}}
			]
		}
	}`
	_, err := db.BatchWriteItem([]byte(input))
	require.Error(t, err)
	require.ErrorContains(t, err, "not found")

	// Invalid RequestItems (empty)
	inputEmpty := `{"RequestItems": {}}`
	_, err = db.BatchWriteItem([]byte(inputEmpty))
	require.Error(t, err)
	require.ErrorContains(t, err, "cannot be empty")

	// Invalid Batch Size (> 25)
	// Construct large batch
	var hugeBatchBuilder strings.Builder
	hugeBatchBuilder.WriteString(`{"RequestItems": {"T": [`)
	for range 26 {
		hugeBatchBuilder.WriteString(`{"PutRequest": {"Item": {"pk": {"S": "i"}}}},`)
	}
	hugeBatch := hugeBatchBuilder.String()
	hugeBatch = hugeBatch[:len(hugeBatch)-1] + `]}}`

	// Create table T so table check passes, fail on size
	db.CreateTable([]byte(`{
		"TableName": "T",
		"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
		"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
		"ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}
	}`))

	_, err = db.BatchWriteItem([]byte(hugeBatch))
	require.Error(t, err)
	require.ErrorContains(t, err, "limit exceeded")
}

func verifyItem(t *testing.T, db *dynamodb.InMemoryDB, tableName, pk string, shouldExist bool) {
	t.Helper()
	res, err := db.GetItem([]byte(`{
		"TableName": "` + tableName + `",
		"Key": {"pk": {"S": "` + pk + `"}}
	}`))
	require.NoError(t, err)
	outItem := res.(dynamodb.GetItemOutput).Item
	if shouldExist {
		assert.NotNil(t, outItem, "Item %s should exist in %s", pk, tableName)
	} else {
		assert.Nil(t, outItem, "Item %s should NOT exist in %s", pk, tableName)
	}
}
