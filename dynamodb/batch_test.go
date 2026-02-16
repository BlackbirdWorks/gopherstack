package dynamodb_test

import (
	"Gopherstack/dynamodb"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchWriteItem(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()

	// Setup tables
	for _, name := range []string{"Table1", "Table2"} {
		ct := dynamodb.CreateTableInput{
			TableName: name,
			KeySchema: []dynamodb.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
			AttributeDefinitions: []dynamodb.AttributeDefinition{
				{AttributeName: "pk", AttributeType: "S"},
			},
		}
		_, err := db.CreateTable(dynamodb.ToSDKCreateTableInput(&ct))
		require.NoError(t, err)
	}

	// Batch Write: Put items into Table1 and Table2
	input := dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]dynamodb.WriteRequest{
			"Table1": {
				{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]any{
							"pk":  map[string]any{"S": "item1"},
							"val": map[string]any{"S": "v1"},
						},
					},
				},
				{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]any{
							"pk":  map[string]any{"S": "item2"},
							"val": map[string]any{"S": "v2"},
						},
					},
				},
				{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]any{
							"pk":  map[string]any{"S": "item3"},
							"val": map[string]any{"S": "v3"},
						},
					},
				},
			},
			"Table2": {
				{
					PutRequest: &dynamodb.PutRequest{
						Item: map[string]any{"pk": map[string]any{"S": "item3"}, "val": map[string]any{"S": "v3"}},
					},
				},
			},
		},
	}

	sdkInput, _ := dynamodb.ToSDKBatchWriteItemInput(&input)
	_, err := db.BatchWriteItem(sdkInput)
	require.NoError(t, err)

	// Verify items
	verifyItem(t, db, "Table1", "item1", true)
	verifyItem(t, db, "Table1", "item2", true)
	verifyItem(t, db, "Table2", "item3", true)

	// Batch Write: Delete items from Table1
	inputDelete := dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]dynamodb.WriteRequest{
			"Table1": {
				{DeleteRequest: &dynamodb.DeleteRequest{Key: map[string]any{"pk": map[string]any{"S": "item1"}}}},
			},
		},
	}
	sdkInputDelete, _ := dynamodb.ToSDKBatchWriteItemInput(&inputDelete)
	_, err = db.BatchWriteItem(sdkInputDelete)
	require.NoError(t, err)

	verifyItem(t, db, "Table1", "item1", false)
	verifyItem(t, db, "Table1", "item2", true)
}

func TestBatchGetItem(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()

	// Setup table and data
	tableName := "BatchGetTable"
	ct := dynamodb.CreateTableInput{
		TableName: tableName,
		KeySchema: []dynamodb.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			{AttributeName: "pk", AttributeType: "S"},
		},
	}
	_, err := db.CreateTable(dynamodb.ToSDKCreateTableInput(&ct))
	require.NoError(t, err)

	put1 := dynamodb.PutItemInput{
		TableName: tableName,
		Item:      map[string]any{"pk": map[string]any{"S": "item1"}, "val": map[string]any{"S": "v1"}},
	}
	sdkPut1, _ := dynamodb.ToSDKPutItemInput(&put1)
	_, _ = db.PutItem(sdkPut1)

	put2 := dynamodb.PutItemInput{
		TableName: tableName,
		Item:      map[string]any{"pk": map[string]any{"S": "item2"}, "val": map[string]any{"S": "v2"}},
	}
	sdkPut2, _ := dynamodb.ToSDKPutItemInput(&put2)
	_, _ = db.PutItem(sdkPut2)

	put3 := dynamodb.PutItemInput{
		TableName: tableName,
		Item:      map[string]any{"pk": map[string]any{"S": "item3"}, "val": map[string]any{"S": "v3"}},
	}
	sdkPut3, _ := dynamodb.ToSDKPutItemInput(&put3)
	_, _ = db.PutItem(sdkPut3)

	// Batch Get
	input := dynamodb.BatchGetItemInput{
		RequestItems: map[string]dynamodb.KeysAndAttributes{
			tableName: {
				Keys: []map[string]any{
					{"pk": map[string]any{"S": "item1"}},
					{"pk": map[string]any{"S": "item3"}},
				},
			},
		},
	}

	sdkInput, _ := dynamodb.ToSDKBatchGetItemInput(&input)
	res, err := db.BatchGetItem(sdkInput)
	require.NoError(t, err)

	require.NotNil(t, res)

	items, ok := res.Responses[tableName]
	require.True(t, ok)
	assert.Len(t, items, 2)

	// Check content
	found1 := false
	found3 := false
	for _, item := range items {
		// item is map[string]types.AttributeValue
		pkVal, valOk := item["pk"]
		require.True(t, valOk)
		// Assuming it is string
		if s, sOk := pkVal.(*types.AttributeValueMemberS); sOk {
			pk := s.Value
			if pk == "item1" {
				found1 = true
			}
			if pk == "item3" {
				found3 = true
			}
		}
	}
	assert.True(t, found1, "item1 not found")
	assert.True(t, found3, "item3 not found")
}

func TestBatchWriteItem_ValidationErrors(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()

	// Table not found
	input := dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]dynamodb.WriteRequest{
			"MissingTable": {
				{PutRequest: &dynamodb.PutRequest{Item: map[string]any{"pk": map[string]any{"S": "item1"}}}},
			},
		},
	}
	sdkInput, _ := dynamodb.ToSDKBatchWriteItemInput(&input)
	_, err := db.BatchWriteItem(sdkInput)
	require.Error(t, err)
	require.ErrorContains(t, err, "not found")

	// Invalid RequestItems (empty)
	inputEmpty := dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]dynamodb.WriteRequest{},
	}
	sdkInputEmpty, _ := dynamodb.ToSDKBatchWriteItemInput(&inputEmpty)
	_, err = db.BatchWriteItem(sdkInputEmpty)
	require.Error(t, err)
	require.ErrorContains(t, err, "cannot be empty")

	// Invalid Batch Size (> 25)
	requests := make([]dynamodb.WriteRequest, 26)
	for i := range 26 {
		requests[i] = dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{Item: map[string]any{"pk": map[string]any{"S": "i"}}},
		}
	}
	hugeBatch := dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]dynamodb.WriteRequest{
			"T": requests,
		},
	}

	// Create table T so table check passes, fail on size
	ct := dynamodb.CreateTableInput{
		TableName: "T",
		KeySchema: []dynamodb.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			{AttributeName: "pk", AttributeType: "S"},
		},
	}
	_, _ = db.CreateTable(dynamodb.ToSDKCreateTableInput(&ct))

	sdkHugeBatch, _ := dynamodb.ToSDKBatchWriteItemInput(&hugeBatch)
	_, err = db.BatchWriteItem(sdkHugeBatch)
	require.Error(t, err)
	require.ErrorContains(t, err, "limit exceeded")
}

func verifyItem(t *testing.T, db *dynamodb.InMemoryDB, tableName, pk string, shouldExist bool) {
	t.Helper()
	input := dynamodb.GetItemInput{
		TableName: tableName,
		Key:       map[string]any{"pk": map[string]any{"S": pk}},
	}
	sdkInput, _ := dynamodb.ToSDKGetItemInput(&input)

	res, err := db.GetItem(sdkInput)
	require.NoError(t, err)

	outItem := res.Item
	if shouldExist {
		assert.NotEmpty(t, outItem, "Item %s should exist in %s", pk, tableName)
	} else {
		assert.Empty(t, outItem, "Item %s should NOT exist in %s", pk, tableName)
	}
}
