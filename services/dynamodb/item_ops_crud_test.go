package dynamodb_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteItem_SwapWithLast(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		initialItems []map[string]any
		deleteKey    string
		expectedIdx  string // The key of the item expected at the deleted index after swap
		remaining    []string
	}{
		{
			name: "DeleteOnlyItem",
			initialItems: []map[string]any{
				{"id": map[string]any{"S": "A"}, "val": map[string]any{"S": "1"}},
			},
			deleteKey:   "A",
			expectedIdx: "", // No item left
			remaining:   []string{},
		},
		{
			name: "DeleteLastItem",
			initialItems: []map[string]any{
				{"id": map[string]any{"S": "A"}, "val": map[string]any{"S": "1"}},
				{"id": map[string]any{"S": "B"}, "val": map[string]any{"S": "2"}},
			},
			deleteKey:   "B",
			expectedIdx: "", // No swap, just pop
			remaining:   []string{"A"},
		},
		{
			name: "DeleteFirstWithSwap",
			initialItems: []map[string]any{
				{"id": map[string]any{"S": "A"}, "val": map[string]any{"S": "1"}},
				{"id": map[string]any{"S": "B"}, "val": map[string]any{"S": "2"}},
				{"id": map[string]any{"S": "C"}, "val": map[string]any{"S": "3"}},
			},
			deleteKey:   "A",
			expectedIdx: "C", // C should move to index 0
			remaining:   []string{"B", "C"},
		},
		{
			name: "DeleteMiddleWithSwap",
			initialItems: []map[string]any{
				{"id": map[string]any{"S": "A"}, "val": map[string]any{"S": "1"}},
				{"id": map[string]any{"S": "B"}, "val": map[string]any{"S": "2"}},
				{"id": map[string]any{"S": "C"}, "val": map[string]any{"S": "3"}},
			},
			deleteKey:   "B",
			expectedIdx: "C", // C should move to index 1
			remaining:   []string{"A", "C"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()
			tableName := "TestTable"
			createTableHelper(t, db, tableName, "id")

			// Setup items
			for _, item := range tt.initialItems {
				sdkItem, _ := models.ToSDKItem(item)
				input := models.PutItemInput{
					TableName: tableName,
					Item:      item,
				}
				sdkInput, _ := models.ToSDKPutItemInput(&input)
				_, err := db.PutItem(context.Background(), sdkInput)
				require.NoError(t, err)
				_ = sdkItem
			}

			// Perform deletion
			deleteInput := models.DeleteItemInput{
				TableName: tableName,
				Key:       map[string]any{"id": map[string]any{"S": tt.deleteKey}},
			}
			sdkDel, _ := models.ToSDKDeleteItemInput(&deleteInput)
			_, err := db.DeleteItem(t.Context(), sdkDel)
			require.NoError(t, err)

			// Verify remaining items via GetItem (Index Integrity)
			for _, key := range tt.remaining {
				getInput := models.GetItemInput{
					TableName: tableName,
					Key:       map[string]any{"id": map[string]any{"S": key}},
				}
				sdkGet, _ := models.ToSDKGetItemInput(&getInput)
				resp, getErr := db.GetItem(t.Context(), sdkGet)
				require.NoError(t, getErr)
				assert.NotEmpty(t, resp.Item, "Should be able to find item %s after deletion", key)
			}

			// Verify deleted item is gone
			getInput := models.GetItemInput{
				TableName: tableName,
				Key:       map[string]any{"id": map[string]any{"S": tt.deleteKey}},
			}
			sdkGet, _ := models.ToSDKGetItemInput(&getInput)
			resp, getErr2 := db.GetItem(t.Context(), sdkGet)
			require.NoError(t, getErr2)
			assert.Empty(t, resp.Item, "Item %s should be deleted", tt.deleteKey)
		})
	}
}

func TestMissingSK_KeyOperations(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) *dynamodb.InMemoryDB {
		t.Helper()
		db := dynamodb.NewInMemoryDB()
		createTableHelper(t, db, "PKSKTable", "pk", "sk")
		putInput := models.PutItemInput{
			TableName: "PKSKTable",
			Item: map[string]any{
				"pk":  map[string]any{"S": "p1"},
				"sk":  map[string]any{"S": "s1"},
				"val": map[string]any{"S": "v1"},
			},
		}
		sdkPut, _ := models.ToSDKPutItemInput(&putInput)
		_, err := db.PutItem(context.Background(), sdkPut)
		require.NoError(t, err)

		return db
	}

	t.Run("GetItem missing SK returns error", func(t *testing.T) {
		t.Parallel()
		db := setup(t)
		getInput := models.GetItemInput{
			TableName: "PKSKTable",
			Key:       map[string]any{"pk": map[string]any{"S": "p1"}},
		}
		sdkGet, _ := models.ToSDKGetItemInput(&getInput)
		_, err := db.GetItem(context.Background(), sdkGet)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Missing key element: sk")
	})

	t.Run("DeleteItem missing SK returns error", func(t *testing.T) {
		t.Parallel()
		db := setup(t)
		delInput := models.DeleteItemInput{
			TableName: "PKSKTable",
			Key:       map[string]any{"pk": map[string]any{"S": "p1"}},
		}
		sdkDel, _ := models.ToSDKDeleteItemInput(&delInput)
		_, err := db.DeleteItem(context.Background(), sdkDel)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Missing key element: sk")
	})

	t.Run("UpdateItem missing SK returns error", func(t *testing.T) {
		t.Parallel()
		db := setup(t)
		updateInput := models.UpdateItemInput{
			TableName:        "PKSKTable",
			Key:              map[string]any{"pk": map[string]any{"S": "p1"}},
			UpdateExpression: "SET val = :v",
			ExpressionAttributeValues: map[string]any{
				":v": map[string]any{"S": "updated"},
			},
		}
		sdkUpd, _ := models.ToSDKUpdateItemInput(&updateInput)
		_, err := db.UpdateItem(context.Background(), sdkUpd)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Missing key element: sk")
	})

	t.Run("GetItem with SK succeeds", func(t *testing.T) {
		t.Parallel()
		db := setup(t)
		getInput := models.GetItemInput{
			TableName: "PKSKTable",
			Key: map[string]any{
				"pk": map[string]any{"S": "p1"},
				"sk": map[string]any{"S": "s1"},
			},
		}
		sdkGet, _ := models.ToSDKGetItemInput(&getInput)
		resp, err := db.GetItem(context.Background(), sdkGet)
		require.NoError(t, err)
		assert.NotEmpty(t, resp.Item)
	})
}
