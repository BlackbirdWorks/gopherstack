package dynamodb_test

import (
	"Gopherstack/dynamodb"
	"Gopherstack/dynamodb/models"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUpdateItem_VersioningPattern tests the common versioning pattern with UPDATED_NEW
func TestUpdateItem_VersioningPattern(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()

	// Setup table
	ctInput := models.CreateTableInput{
		TableName:            "TestTable",
		KeySchema:            []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		AttributeDefinitions: []models.AttributeDefinition{{AttributeName: "pk", AttributeType: "S"}},
	}
	_, err := db.CreateTable(models.ToSDKCreateTableInput(&ctInput))
	require.NoError(t, err)

	// Test 1: First update on NEW item with if_not_exists pattern and UPDATED_NEW
	t.Log("=== Test 1: First update on NEW item ===")
	updateInput1 := models.UpdateItemInput{
		TableName: "TestTable",
		Key:       map[string]any{"pk": map[string]any{"S": "item1"}},
		// Simulates: SET version = if_not_exists(version, :zero) + :inc, data = :data
		// For simplicity in unit test, we'll just set version and data
		UpdateExpression:         "SET version = :v, #data = :data",
		ExpressionAttributeNames: map[string]string{"#data": "data"},
		ExpressionAttributeValues: map[string]any{
			":v":    map[string]any{"N": "1"},
			":data": map[string]any{"S": "first version"},
		},
		ReturnValues: "UPDATED_NEW",
	}
	sdkUpdate1, _ := models.ToSDKUpdateItemInput(&updateInput1)

	res1, err := db.UpdateItem(sdkUpdate1)
	require.NoError(t, err)
	require.NotNil(t, res1.Attributes, "UPDATED_NEW should return attributes for new item")

	wireAttrs1 := models.FromSDKItem(res1.Attributes)
	t.Logf("Returned attributes: %+v", wireAttrs1)

	// For NEW item, should return all attributes that were set
	assert.Contains(t, wireAttrs1, "pk", "Should contain primary key")
	assert.Contains(t, wireAttrs1, "version", "Should contain version")
	assert.Contains(t, wireAttrs1, "data", "Should contain data")

	assert.Equal(t, "item1", wireAttrs1["pk"].(map[string]any)["S"])
	assert.Equal(t, "1", wireAttrs1["version"].(map[string]any)["N"])
	assert.Equal(t, "first version", wireAttrs1["data"].(map[string]any)["S"])

	// Test 2: Second update on EXISTING item with UPDATED_NEW
	t.Log("=== Test 2: Second update on EXISTING item ===")
	updateInput2 := models.UpdateItemInput{
		TableName: "TestTable",
		Key:       map[string]any{"pk": map[string]any{"S": "item1"}},
		// Simulates: SET version = version + :inc, data = :data
		UpdateExpression:         "SET version = :v, #data = :data",
		ExpressionAttributeNames: map[string]string{"#data": "data"},
		ExpressionAttributeValues: map[string]any{
			":v":    map[string]any{"N": "2"},
			":data": map[string]any{"S": "second version"},
		},
		ReturnValues: "UPDATED_NEW",
	}
	sdkUpdate2, _ := models.ToSDKUpdateItemInput(&updateInput2)

	res2, err := db.UpdateItem(sdkUpdate2)
	require.NoError(t, err)
	require.NotNil(t, res2.Attributes)

	wireAttrs2 := models.FromSDKItem(res2.Attributes)
	t.Logf("Returned attributes: %+v", wireAttrs2)

	// For EXISTING item, should return ONLY updated attributes
	assert.Contains(t, wireAttrs2, "version", "Should contain version")
	assert.Contains(t, wireAttrs2, "data", "Should contain data")
	assert.NotContains(t, wireAttrs2, "pk", "Should NOT contain pk (not updated)")

	assert.Equal(t, "2", wireAttrs2["version"].(map[string]any)["N"])
	assert.Equal(t, "second version", wireAttrs2["data"].(map[string]any)["S"])

	// Test 3: Upsert from existing - add new attribute
	t.Log("=== Test 3: Upsert - add new attribute to existing item ===")
	updateInput3 := models.UpdateItemInput{
		TableName:        "TestTable",
		Key:              map[string]any{"pk": map[string]any{"S": "item1"}},
		UpdateExpression: "SET version = :v, details = :details",
		ExpressionAttributeValues: map[string]any{
			":v": map[string]any{"N": "3"},
			":details": map[string]any{"M": map[string]any{
				"author": map[string]any{"S": "test"},
			}},
		},
		ReturnValues: "UPDATED_NEW",
	}
	sdkUpdate3, _ := models.ToSDKUpdateItemInput(&updateInput3)

	res3, err := db.UpdateItem(sdkUpdate3)
	require.NoError(t, err)
	require.NotNil(t, res3.Attributes)

	wireAttrs3 := models.FromSDKItem(res3.Attributes)
	t.Logf("Returned attributes: %+v", wireAttrs3)

	// Should return only updated attributes (version and new details)
	assert.Contains(t, wireAttrs3, "version", "Should contain version")
	assert.Contains(t, wireAttrs3, "details", "Should contain details")
	assert.NotContains(t, wireAttrs3, "pk", "Should NOT contain pk")
	assert.NotContains(t, wireAttrs3, "data", "Should NOT contain data (not updated)")

	assert.Equal(t, "3", wireAttrs3["version"].(map[string]any)["N"])
}

// TestUpdateItem_BlankToUpsert tests starting from blank state then upserting
func TestUpdateItem_BlankToUpsert(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()

	// Setup table
	ctInput := models.CreateTableInput{
		TableName:            "TestTable",
		KeySchema:            []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
		AttributeDefinitions: []models.AttributeDefinition{{AttributeName: "pk", AttributeType: "S"}},
	}
	_, err := db.CreateTable(models.ToSDKCreateTableInput(&ctInput))
	require.NoError(t, err)

	// Start from completely blank state - no items in table
	t.Log("=== Starting from blank table ===")

	// Test 1: Create first item with UPDATED_NEW
	t.Log("=== Test 1: First upsert on blank table ===")
	updateInput1 := models.UpdateItemInput{
		TableName:        "TestTable",
		Key:              map[string]any{"pk": map[string]any{"S": "newdoc"}},
		UpdateExpression: "SET version = :v, content = :content, created = :ts",
		ExpressionAttributeValues: map[string]any{
			":v":       map[string]any{"N": "1"},
			":content": map[string]any{"S": "initial content"},
			":ts":      map[string]any{"N": "1234567890"},
		},
		ReturnValues: "UPDATED_NEW",
	}
	sdkUpdate1, _ := models.ToSDKUpdateItemInput(&updateInput1)

	res1, err := db.UpdateItem(sdkUpdate1)
	require.NoError(t, err)
	require.NotNil(t, res1.Attributes, "UPDATED_NEW on new item should return attributes")

	wireAttrs1 := models.FromSDKItem(res1.Attributes)
	t.Logf("First upsert returned: %+v", wireAttrs1)

	// Should include key and all set attributes
	assert.Contains(t, wireAttrs1, "pk")
	assert.Contains(t, wireAttrs1, "version")
	assert.Contains(t, wireAttrs1, "content")
	assert.Contains(t, wireAttrs1, "created")

	// Test 2: Update existing item - change content and version
	t.Log("=== Test 2: Upsert on existing data ===")
	updateInput2 := models.UpdateItemInput{
		TableName:        "TestTable",
		Key:              map[string]any{"pk": map[string]any{"S": "newdoc"}},
		UpdateExpression: "SET version = :v, content = :content, modified = :ts",
		ExpressionAttributeValues: map[string]any{
			":v":       map[string]any{"N": "2"},
			":content": map[string]any{"S": "updated content"},
			":ts":      map[string]any{"N": "1234567999"},
		},
		ReturnValues: "UPDATED_NEW",
	}
	sdkUpdate2, _ := models.ToSDKUpdateItemInput(&updateInput2)

	res2, err := db.UpdateItem(sdkUpdate2)
	require.NoError(t, err)
	require.NotNil(t, res2.Attributes)

	wireAttrs2 := models.FromSDKItem(res2.Attributes)
	t.Logf("Second upsert returned: %+v", wireAttrs2)

	// Should include only updated attributes, not pk or created
	assert.Contains(t, wireAttrs2, "version")
	assert.Contains(t, wireAttrs2, "content")
	assert.Contains(t, wireAttrs2, "modified")
	assert.NotContains(t, wireAttrs2, "pk", "Should NOT contain pk")
	assert.NotContains(t, wireAttrs2, "created", "Should NOT contain created (not updated)")

	assert.Equal(t, "2", wireAttrs2["version"].(map[string]any)["N"])
	assert.Equal(t, "updated content", wireAttrs2["content"].(map[string]any)["S"])
}
