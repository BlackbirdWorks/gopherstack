package dynamodb_test

import (
	"Gopherstack/dynamodb"
	"Gopherstack/dynamodb/models"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUpdateItem_AllReturnValues verifies all ReturnValues options work correctly.
func TestUpdateItem_AllReturnValues(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment is an optimization, not a bug
		name         string
		returnValue  string
		expectAttrs  bool
		expectedKeys []string // keys that should be present
		excludedKeys []string // keys that should NOT be present
	}{
		{
			name:         "NONE returns nothing",
			returnValue:  "NONE",
			expectAttrs:  false,
			expectedKeys: nil,
			excludedKeys: nil,
		},
		{
			name:         "ALL_OLD returns all old attributes",
			returnValue:  "ALL_OLD",
			expectAttrs:  true,
			expectedKeys: []string{"pk", "attr1", "attr2"},
			excludedKeys: nil,
		},
		{
			name:         "ALL_NEW returns all new attributes",
			returnValue:  "ALL_NEW",
			expectAttrs:  true,
			expectedKeys: []string{"pk", "attr1", "attr2"},
			excludedKeys: nil,
		},
		{
			name:         "UPDATED_OLD returns only updated attributes (old values)",
			returnValue:  "UPDATED_OLD",
			expectAttrs:  true,
			expectedKeys: []string{"attr1"},       // only attr1 was updated
			excludedKeys: []string{"pk", "attr2"}, // pk and attr2 not updated
		},
		{
			name:         "UPDATED_NEW returns only updated attributes (new values)",
			returnValue:  "UPDATED_NEW",
			expectAttrs:  true,
			expectedKeys: []string{"attr1"},       // only attr1 was updated
			excludedKeys: []string{"pk", "attr2"}, // pk and attr2 not updated
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

			// Create initial item
			putInput := models.PutItemInput{
				TableName: "TestTable",
				Item: map[string]any{
					"pk":    map[string]any{"S": "testitem"},
					"attr1": map[string]any{"S": "original1"},
					"attr2": map[string]any{"S": "unchanged"},
				},
			}
			sdkPut, _ := models.ToSDKPutItemInput(&putInput)
			_, err = db.PutItem(sdkPut)
			require.NoError(t, err)

			// Update only attr1
			updateInput := models.UpdateItemInput{
				TableName:        "TestTable",
				Key:              map[string]any{"pk": map[string]any{"S": "testitem"}},
				UpdateExpression: "SET attr1 = :v1",
				ExpressionAttributeValues: map[string]any{
					":v1": map[string]any{"S": "updated1"},
				},
				ReturnValues: tt.returnValue,
			}
			sdkUpdate, _ := models.ToSDKUpdateItemInput(&updateInput)

			res, err := db.UpdateItem(sdkUpdate)
			require.NoError(t, err)

			if tt.expectAttrs {
				require.NotNil(t, res.Attributes, "Expected attributes to be returned")
				wireAttrs := models.FromSDKItem(res.Attributes)

				for _, key := range tt.expectedKeys {
					assert.Contains(t, wireAttrs, key, "Expected key %s to be present", key)
				}

				for _, key := range tt.excludedKeys {
					assert.NotContains(t, wireAttrs, key, "Expected key %s to NOT be present", key)
				}

				// Verify values are correct
				switch tt.returnValue {
				case "UPDATED_OLD":
					// Should have old value of attr1
					assert.Equal(t, "original1", wireAttrs["attr1"].(map[string]any)["S"])
				case "UPDATED_NEW":
					// Should have new value of attr1
					assert.Equal(t, "updated1", wireAttrs["attr1"].(map[string]any)["S"])
				case "ALL_OLD":
					// Should have old values
					assert.Equal(t, "original1", wireAttrs["attr1"].(map[string]any)["S"])
					assert.Equal(t, "unchanged", wireAttrs["attr2"].(map[string]any)["S"])
				case "ALL_NEW":
					// Should have new values
					assert.Equal(t, "updated1", wireAttrs["attr1"].(map[string]any)["S"])
					assert.Equal(t, "unchanged", wireAttrs["attr2"].(map[string]any)["S"])
				}
			} else {
				assert.Nil(t, res.Attributes, "Expected no attributes to be returned")
			}
		})
	}
}

// TestUpdateItem_NewItemReturnValues tests ReturnValues for creating a new item.
func TestUpdateItem_NewItemReturnValues(t *testing.T) {
	t.Parallel()

	tests := []struct { //nolint:govet // fieldalignment is an optimization, not a bug
		name         string
		returnValue  string
		expectAttrs  bool
		expectedKeys []string
	}{
		{
			name:         "NONE returns nothing",
			returnValue:  "NONE",
			expectAttrs:  false,
			expectedKeys: nil,
		},
		{
			name:         "ALL_OLD returns nothing (item didn't exist)",
			returnValue:  "ALL_OLD",
			expectAttrs:  false,
			expectedKeys: nil,
		},
		{
			name:         "ALL_NEW returns all attributes",
			returnValue:  "ALL_NEW",
			expectAttrs:  true,
			expectedKeys: []string{"pk", "attr1", "attr2"},
		},
		{
			name:         "UPDATED_OLD returns nothing (item didn't exist)",
			returnValue:  "UPDATED_OLD",
			expectAttrs:  false,
			expectedKeys: nil,
		},
		{
			name:         "UPDATED_NEW returns all attributes (all are new)",
			returnValue:  "UPDATED_NEW",
			expectAttrs:  true,
			expectedKeys: []string{"pk", "attr1", "attr2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

			// Update a NEW item (doesn't exist yet)
			updateInput := models.UpdateItemInput{
				TableName:        "TestTable",
				Key:              map[string]any{"pk": map[string]any{"S": "newitem"}},
				UpdateExpression: "SET attr1 = :v1, attr2 = :v2",
				ExpressionAttributeValues: map[string]any{
					":v1": map[string]any{"S": "value1"},
					":v2": map[string]any{"S": "value2"},
				},
				ReturnValues: tt.returnValue,
			}
			sdkUpdate, _ := models.ToSDKUpdateItemInput(&updateInput)

			res, err := db.UpdateItem(sdkUpdate)
			require.NoError(t, err)

			if tt.expectAttrs {
				require.NotNil(t, res.Attributes, "Expected attributes to be returned for %s", tt.returnValue)
				wireAttrs := models.FromSDKItem(res.Attributes)

				for _, key := range tt.expectedKeys {
					assert.Contains(t, wireAttrs, key, "Expected key %s to be present", key)
				}
			} else {
				assert.Nil(t, res.Attributes, "Expected no attributes to be returned for %s", tt.returnValue)
			}
		})
	}
}
