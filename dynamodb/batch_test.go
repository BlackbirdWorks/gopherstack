package dynamodb_test

import (
	"Gopherstack/dynamodb"
	"Gopherstack/dynamodb/models"
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
		ct := models.CreateTableInput{
			TableName: name,
			KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
			AttributeDefinitions: []models.AttributeDefinition{
				{AttributeName: "pk", AttributeType: "S"},
			},
		}
		_, err := db.CreateTable(models.ToSDKCreateTableInput(&ct))
		require.NoError(t, err)
	}

	// Batch Write: Put items into Table1 and Table2
	input := models.BatchWriteItemInput{
		RequestItems: map[string][]models.WriteRequest{
			"Table1": {
				{
					PutRequest: &models.PutRequest{
						Item: map[string]any{
							"pk":  map[string]any{"S": "item1"},
							"val": map[string]any{"S": "v1"},
						},
					},
				},
				{
					PutRequest: &models.PutRequest{
						Item: map[string]any{
							"pk":  map[string]any{"S": "item2"},
							"val": map[string]any{"S": "v2"},
						},
					},
				},
				{
					PutRequest: &models.PutRequest{
						Item: map[string]any{
							"pk":  map[string]any{"S": "item3"},
							"val": map[string]any{"S": "v3"},
						},
					},
				},
			},
			"Table2": {
				{
					PutRequest: &models.PutRequest{
						Item: map[string]any{"pk": map[string]any{"S": "item3"}, "val": map[string]any{"S": "v3"}},
					},
				},
			},
		},
	}

	sdkInput, _ := models.ToSDKBatchWriteItemInput(&input)
	_, err := db.BatchWriteItem(sdkInput)
	require.NoError(t, err)

	// Verify items
	verifyItem(t, db, "Table1", "item1", true)
	verifyItem(t, db, "Table1", "item2", true)
	verifyItem(t, db, "Table2", "item3", true)

	// Batch Write: Delete items from Table1
	inputDelete := models.BatchWriteItemInput{
		RequestItems: map[string][]models.WriteRequest{
			"Table1": {
				{DeleteRequest: &models.DeleteRequest{Key: map[string]any{"pk": map[string]any{"S": "item1"}}}},
			},
		},
	}
	sdkInputDelete, _ := models.ToSDKBatchWriteItemInput(&inputDelete)
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
	ct := models.CreateTableInput{
		TableName: tableName,
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "pk", AttributeType: "S"},
		},
	}
	_, err := db.CreateTable(models.ToSDKCreateTableInput(&ct))
	require.NoError(t, err)

	put1 := models.PutItemInput{
		TableName: tableName,
		Item:      map[string]any{"pk": map[string]any{"S": "item1"}, "val": map[string]any{"S": "v1"}},
	}
	sdkPut1, _ := models.ToSDKPutItemInput(&put1)
	_, _ = db.PutItem(sdkPut1)

	put2 := models.PutItemInput{
		TableName: tableName,
		Item:      map[string]any{"pk": map[string]any{"S": "item2"}, "val": map[string]any{"S": "v2"}},
	}
	sdkPut2, _ := models.ToSDKPutItemInput(&put2)
	_, _ = db.PutItem(sdkPut2)

	put3 := models.PutItemInput{
		TableName: tableName,
		Item:      map[string]any{"pk": map[string]any{"S": "item3"}, "val": map[string]any{"S": "v3"}},
	}
	sdkPut3, _ := models.ToSDKPutItemInput(&put3)
	_, _ = db.PutItem(sdkPut3)

	// Batch Get
	input := models.BatchGetItemInput{
		RequestItems: map[string]models.KeysAndAttributes{
			tableName: {
				Keys: []map[string]any{
					{"pk": map[string]any{"S": "item1"}},
					{"pk": map[string]any{"S": "item3"}},
				},
			},
		},
	}

	sdkInput, _ := models.ToSDKBatchGetItemInput(&input)
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
	input := models.BatchWriteItemInput{
		RequestItems: map[string][]models.WriteRequest{
			"MissingTable": {
				{PutRequest: &models.PutRequest{Item: map[string]any{"pk": map[string]any{"S": "item1"}}}},
			},
		},
	}
	sdkInput, _ := models.ToSDKBatchWriteItemInput(&input)
	_, err := db.BatchWriteItem(sdkInput)
	require.Error(t, err)
	require.ErrorContains(t, err, "not found")

	// Invalid RequestItems (empty)
	inputEmpty := models.BatchWriteItemInput{
		RequestItems: map[string][]models.WriteRequest{},
	}
	sdkInputEmpty, _ := models.ToSDKBatchWriteItemInput(&inputEmpty)
	_, err = db.BatchWriteItem(sdkInputEmpty)
	require.Error(t, err)
	require.ErrorContains(t, err, "cannot be empty")

	// Invalid Batch Size (> 25)
	requests := make([]models.WriteRequest, 26)
	for i := range 26 {
		requests[i] = models.WriteRequest{
			PutRequest: &models.PutRequest{Item: map[string]any{"pk": map[string]any{"S": "i"}}},
		}
	}
	hugeBatch := models.BatchWriteItemInput{
		RequestItems: map[string][]models.WriteRequest{
			"T": requests,
		},
	}

	// Create table T so table check passes, fail on size
	ct := models.CreateTableInput{
		TableName: "T",
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "pk", AttributeType: "S"},
		},
	}
	_, _ = db.CreateTable(models.ToSDKCreateTableInput(&ct))

	sdkHugeBatch, _ := models.ToSDKBatchWriteItemInput(&hugeBatch)
	_, err = db.BatchWriteItem(sdkHugeBatch)
	require.Error(t, err)
	require.ErrorContains(t, err, "limit exceeded")
}

func verifyItem(t *testing.T, db *dynamodb.InMemoryDB, tableName, pk string, shouldExist bool) {
	t.Helper()
	input := models.GetItemInput{
		TableName: tableName,
		Key:       map[string]any{"pk": map[string]any{"S": pk}},
	}
	sdkInput, _ := models.ToSDKGetItemInput(&input)

	res, err := db.GetItem(sdkInput)
	require.NoError(t, err)

	outItem := res.Item
	if shouldExist {
		assert.NotEmpty(t, outItem, "Item %s should exist in %s", pk, tableName)
	} else {
		assert.Empty(t, outItem, "Item %s should NOT exist in %s", pk, tableName)
	}
}
