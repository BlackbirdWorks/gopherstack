package dynamodb_test

import (
	"github.com/blackbirdworks/gopherstack/dynamodb"
	"github.com/blackbirdworks/gopherstack/dynamodb/models"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateItem_UPDATED_NEW_OnNewItem(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()

	// Setup table
	ctInput := models.CreateTableInput{
		TableName: "TestTable",
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "pk", AttributeType: "S"},
		},
	}
	_, err := db.CreateTable(context.Background(), models.ToSDKCreateTableInput(&ctInput))
	require.NoError(t, err)

	// Update a NEW item (doesn't exist yet) with UPDATED_NEW
	updateInput := models.UpdateItemInput{
		TableName:        "TestTable",
		Key:              map[string]any{"pk": map[string]any{"S": "newitem"}},
		UpdateExpression: "SET attr1 = :v1, attr2 = :v2",
		ExpressionAttributeValues: map[string]any{
			":v1": map[string]any{"S": "value1"},
			":v2": map[string]any{"S": "value2"},
		},
		ReturnValues: "UPDATED_NEW",
	}
	sdkUpdate, _ := models.ToSDKUpdateItemInput(&updateInput)

	res, err := db.UpdateItem(context.Background(), sdkUpdate)
	require.NoError(t, err)

	// When updating a NEW item with UPDATED_NEW, should return all attributes
	// that were updated (including the key)
	assert.NotNil(t, res.Attributes, "Attributes should not be nil for UPDATED_NEW on new item")

	if res.Attributes != nil {
		wireAttrs := models.FromSDKItem(res.Attributes)
		t.Logf("Returned attributes: %+v", wireAttrs)

		// Should NOT contain pk (key attribute isn't "updated" in DDB sense)
		assert.NotContains(t, wireAttrs, "pk", "Should NOT contain primary key")

		// Should contain attr1 and attr2 (updated attributes)
		assert.Contains(t, wireAttrs, "attr1", "Should contain updated attribute attr1")
		assert.Contains(t, wireAttrs, "attr2", "Should contain updated attribute attr2")
	}
}

func TestUpdateItem_UPDATED_NEW_OnExistingItem(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()

	// Setup table
	ctInput := models.CreateTableInput{
		TableName: "TestTable",
		KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "pk", AttributeType: "S"},
		},
	}
	_, err := db.CreateTable(context.Background(), models.ToSDKCreateTableInput(&ctInput))
	require.NoError(t, err)

	// Create an initial item
	putInput := models.PutItemInput{
		TableName: "TestTable",
		Item: map[string]any{
			"pk":    map[string]any{"S": "existingitem"},
			"attr1": map[string]any{"S": "original1"},
			"attr2": map[string]any{"S": "original2"},
			"attr3": map[string]any{"S": "unchanged"},
		},
	}
	sdkPut, _ := models.ToSDKPutItemInput(&putInput)
	_, err = db.PutItem(context.Background(), sdkPut)
	require.NoError(t, err)

	// Update ONLY attr1 with UPDATED_NEW
	updateInput := models.UpdateItemInput{
		TableName:        "TestTable",
		Key:              map[string]any{"pk": map[string]any{"S": "existingitem"}},
		UpdateExpression: "SET attr1 = :v1",
		ExpressionAttributeValues: map[string]any{
			":v1": map[string]any{"S": "updated1"},
		},
		ReturnValues: "UPDATED_NEW",
	}
	sdkUpdate, _ := models.ToSDKUpdateItemInput(&updateInput)

	res, err := db.UpdateItem(context.Background(), sdkUpdate)
	require.NoError(t, err)

	// When updating an existing item with UPDATED_NEW, should return ONLY
	// the attributes that were actually updated
	assert.NotNil(t, res.Attributes, "Attributes should not be nil for UPDATED_NEW")

	if res.Attributes != nil {
		wireAttrs := models.FromSDKItem(res.Attributes)
		t.Logf("Returned attributes: %+v", wireAttrs)

		// Should contain ONLY attr1 (the updated attribute)
		assert.Contains(t, wireAttrs, "attr1", "Should contain updated attribute attr1")
		assert.Equal(
			t,
			"updated1",
			wireAttrs["attr1"].(map[string]any)["S"],
			"attr1 should have new value",
		)

		// Should NOT contain pk, attr2, or attr3 (not updated)
		assert.NotContains(t, wireAttrs, "pk", "Should NOT contain pk (not updated)")
		assert.NotContains(t, wireAttrs, "attr2", "Should NOT contain attr2 (not updated)")
		assert.NotContains(t, wireAttrs, "attr3", "Should NOT contain attr3 (not updated)")
	}
}
