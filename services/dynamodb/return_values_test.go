package dynamodb_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateItem_UPDATED_NEW(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup           func(t *testing.T, db *dynamodb.InMemoryDB)
		updateInput     models.UpdateItemInput
		name            string
		wantAttr1Value  string
		wantAttrPresent []string
		wantAttrAbsent  []string
	}{
		{
			name: "new item returns all updated attributes but not key",
			updateInput: models.UpdateItemInput{
				TableName:        "TestTable",
				Key:              map[string]any{"pk": map[string]any{"S": "newitem"}},
				UpdateExpression: "SET attr1 = :v1, attr2 = :v2",
				ExpressionAttributeValues: map[string]any{
					":v1": map[string]any{"S": "value1"},
					":v2": map[string]any{"S": "value2"},
				},
				ReturnValues: "UPDATED_NEW",
			},
			wantAttrPresent: []string{"attr1", "attr2"},
			wantAttrAbsent:  []string{"pk"},
		},
		{
			name: "existing item returns only the updated attributes",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
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
				_, err := db.PutItem(t.Context(), sdkPut)
				require.NoError(t, err)
			},
			updateInput: models.UpdateItemInput{
				TableName:        "TestTable",
				Key:              map[string]any{"pk": map[string]any{"S": "existingitem"}},
				UpdateExpression: "SET attr1 = :v1",
				ExpressionAttributeValues: map[string]any{
					":v1": map[string]any{"S": "updated1"},
				},
				ReturnValues: "UPDATED_NEW",
			},
			wantAttrPresent: []string{"attr1"},
			wantAttrAbsent:  []string{"pk", "attr2", "attr3"},
			wantAttr1Value:  "updated1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()

			ctInput := models.CreateTableInput{
				TableName: "TestTable",
				KeySchema: []models.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
				AttributeDefinitions: []models.AttributeDefinition{
					{AttributeName: "pk", AttributeType: "S"},
				},
			}
			_, err := db.CreateTable(t.Context(), models.ToSDKCreateTableInput(&ctInput))
			require.NoError(t, err)

			if tt.setup != nil {
				tt.setup(t, db)
			}

			sdkUpdate, _ := models.ToSDKUpdateItemInput(&tt.updateInput)
			res, err := db.UpdateItem(t.Context(), sdkUpdate)
			require.NoError(t, err)

			require.NotNil(t, res.Attributes, "Attributes should not be nil for UPDATED_NEW")

			wireAttrs := models.FromSDKItem(res.Attributes)
			t.Logf("Returned attributes: %+v", wireAttrs)

			for _, key := range tt.wantAttrPresent {
				assert.Contains(t, wireAttrs, key, "should contain updated attribute %q", key)
			}
			for _, key := range tt.wantAttrAbsent {
				assert.NotContains(t, wireAttrs, key, "should NOT contain attribute %q", key)
			}
			if tt.wantAttr1Value != "" {
				assert.Equal(
					t,
					tt.wantAttr1Value,
					wireAttrs["attr1"].(map[string]any)["S"],
					"attr1 should have new value",
				)
			}
		})
	}
}
