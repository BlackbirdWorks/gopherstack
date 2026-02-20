package dynamodb_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/dynamodb"
	"github.com/blackbirdworks/gopherstack/dynamodb/models"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateItem_ComplexPaths(t *testing.T) {
	t.Parallel()

	tableName := "UpdateComplexTable"

	tests := []struct {
		setup      func(*testing.T, *dynamodb.InMemoryDB)
		verifyFunc func(t *testing.T, db *dynamodb.InMemoryDB)
		name       string
		input      string
		wantErr    bool
	}{
		{
			name: "SET Nested Map Field",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				putInput := models.PutItemInput{
					TableName: tableName,
					Item: map[string]any{
						"pk": map[string]any{"S": "nested-map"},
						"info": map[string]any{
							"M": map[string]any{
								"author": map[string]any{"S": "me"},
								"year":   map[string]any{"N": "2020"},
							},
						},
					},
				}
				sdkPut, _ := models.ToSDKPutItemInput(&putInput)
				_, err := db.PutItem(context.Background(), sdkPut)
				require.NoError(t, err)
			},
			input: `{
				"TableName": "` + tableName + `",
				"Key": {"pk": {"S": "nested-map"}},
				"UpdateExpression": "SET info.year = :y",
				"ExpressionAttributeValues": {":y": {"N": "2025"}}
			}`,
			verifyFunc: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				item := getItem(t, db, tableName, "nested-map")
				info := item["info"].(map[string]any)["M"].(map[string]any)
				assert.Equal(t, "2025", info["year"].(map[string]any)["N"])
			},
		},
		{
			name: "SET List Element by Index",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				putInput := models.PutItemInput{
					TableName: tableName,
					Item: map[string]any{
						"pk": map[string]any{"S": "list-update"},
						"tags": map[string]any{
							"L": []any{
								map[string]any{"S": "a"},
								map[string]any{"S": "b"},
								map[string]any{"S": "c"},
							},
						},
					},
				}
				sdkPut, _ := models.ToSDKPutItemInput(&putInput)
				_, err := db.PutItem(context.Background(), sdkPut)
				require.NoError(t, err)
			},
			input: `{
				"TableName": "` + tableName + `",
				"Key": {"pk": {"S": "list-update"}},
				"UpdateExpression": "SET tags[1] = :val",
				"ExpressionAttributeValues": {":val": {"S": "updated-b"}}
			}`,
			verifyFunc: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				item := getItem(t, db, tableName, "list-update")
				tags := item["tags"].(map[string]any)["L"].([]any)
				assert.Equal(t, "updated-b", tags[1].(map[string]any)["S"])
			},
		},
		{
			name: "SET Nested List in Map",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				putInput := models.PutItemInput{
					TableName: tableName,
					Item: map[string]any{
						"pk": map[string]any{"S": "nested-list"},
						"data": map[string]any{"M": map[string]any{
							"scores": map[string]any{
								"L": []any{map[string]any{"N": "10"}, map[string]any{"N": "20"}},
							},
						}},
					},
				}
				sdkPut, _ := models.ToSDKPutItemInput(&putInput)
				_, err := db.PutItem(context.Background(), sdkPut)
				require.NoError(t, err)
			},
			input: `{
				"TableName": "` + tableName + `",
				"Key": {"pk": {"S": "nested-list"}},
				"UpdateExpression": "SET data.scores[0] = :val",
				"ExpressionAttributeValues": {":val": {"N": "99"}}
			}`,
			verifyFunc: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				item := getItem(t, db, tableName, "nested-list")
				data := item["data"].(map[string]any)["M"].(map[string]any)
				scores := data["scores"].(map[string]any)["L"].([]any)
				assert.Equal(t, "99", scores[0].(map[string]any)["N"])
			},
		},
		{
			name: "REMOVE List Element (Shift)",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				putInput := models.PutItemInput{
					TableName: tableName,
					Item: map[string]any{
						"pk": map[string]any{"S": "remove-list"},
						"tags": map[string]any{
							"L": []any{
								map[string]any{"S": "a"},
								map[string]any{"S": "b"},
								map[string]any{"S": "c"},
							},
						},
					},
				}
				sdkPut, _ := models.ToSDKPutItemInput(&putInput)
				_, err := db.PutItem(context.Background(), sdkPut)
				require.NoError(t, err)
			},
			input: `{
				"TableName": "` + tableName + `",
				"Key": {"pk": {"S": "remove-list"}},
				"UpdateExpression": "REMOVE tags[1]"
			}`,
			verifyFunc: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				item := getItem(t, db, tableName, "remove-list")
				tags := item["tags"].(map[string]any)["L"].([]any)
				// Should be [a, c] now
				assert.Len(t, tags, 2)
				assert.Equal(t, "a", tags[0].(map[string]any)["S"])
				assert.Equal(t, "c", tags[1].(map[string]any)["S"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()
			createTableHelper(t, db, tableName, "pk")

			if tc.setup != nil {
				tc.setup(t, db)
			}

			updateInput := mustUnmarshal[models.UpdateItemInput](t, tc.input)
			sdkUpdate, _ := models.ToSDKUpdateItemInput(&updateInput)
			_, err := db.UpdateItem(context.Background(), sdkUpdate)
			if tc.wantErr {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)

			if tc.verifyFunc != nil {
				tc.verifyFunc(t, db)
			}
		})
	}
}
