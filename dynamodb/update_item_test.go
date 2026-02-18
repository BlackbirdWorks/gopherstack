package dynamodb_test

import (
	"Gopherstack/dynamodb"
	"Gopherstack/dynamodb/models"
	"context"
	"testing"

	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateItem(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *dynamodb.InMemoryDB)
		verifyFunc func(t *testing.T, db *dynamodb.InMemoryDB, out *dynamodb_sdk.UpdateItemOutput)
		name       string
		input      string
		wantErr    bool
	}{
		{
			name: "New Item Creation (SET)",
			input: `{
				"TableName": "UpdateTestTable",
				"Key": {"pk": {"S": "new-item"}},
				"UpdateExpression": "SET #v = :val",
				"ExpressionAttributeNames": {"#v": "value"},
				"ExpressionAttributeValues": {":val": {"S": "test"}}
			}`,
			verifyFunc: func(t *testing.T, db *dynamodb.InMemoryDB, _ *dynamodb_sdk.UpdateItemOutput) {
				t.Helper()
				item := getItem(t, db, "UpdateTestTable", "new-item")
				require.NotNil(t, item)
				assert.Equal(t, "test", item["value"].(map[string]any)["S"])
			},
		},
		{
			name: "Update Existing Item (SET)",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				putInput := models.PutItemInput{
					TableName: "UpdateTestTable",
					Item: map[string]any{
						"pk":    map[string]any{"S": "update-item"},
						"count": map[string]any{"N": "1"},
					},
				}
				sdkPut, _ := models.ToSDKPutItemInput(&putInput)
				_, err := db.PutItem(context.Background(), sdkPut)
				require.NoError(t, err)
			},
			input: `{
				"TableName": "UpdateTestTable",
				"Key": {"pk": {"S": "update-item"}},
				"UpdateExpression": "SET count = :val",
				"ExpressionAttributeValues": {":val": {"N": "5"}},
				"ReturnValues": "ALL_NEW"
			}`,
			verifyFunc: func(t *testing.T, _ *dynamodb.InMemoryDB, out *dynamodb_sdk.UpdateItemOutput) {
				t.Helper()
				assert.NotNil(t, out.Attributes)
				wireAttrs := models.FromSDKItem(out.Attributes)
				assert.Equal(t, "5", wireAttrs["count"].(map[string]any)["N"])
			},
		},
		{
			name: "REMOVE Attribute",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				putInput := models.PutItemInput{
					TableName: "UpdateTestTable",
					Item: map[string]any{
						"pk":    map[string]any{"S": "remove-item"},
						"extra": map[string]any{"S": "delete-me"},
					},
				}
				sdkPut, _ := models.ToSDKPutItemInput(&putInput)
				_, err := db.PutItem(context.Background(), sdkPut)
				require.NoError(t, err)
			},
			input: `{
				"TableName": "UpdateTestTable",
				"Key": {"pk": {"S": "remove-item"}},
				"UpdateExpression": "REMOVE extra"
			}`,
			verifyFunc: func(t *testing.T, db *dynamodb.InMemoryDB, _ *dynamodb_sdk.UpdateItemOutput) {
				t.Helper()
				item := getItem(t, db, "UpdateTestTable", "remove-item")
				_, exists := item["extra"]
				assert.False(t, exists, "Attribute should be removed")
			},
		},
		{
			name: "ADD Number",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				putInput := models.PutItemInput{
					TableName: "UpdateTestTable",
					Item: map[string]any{
						"pk":      map[string]any{"S": "add-item"},
						"counter": map[string]any{"N": "10"},
					},
				}
				sdkPut, _ := models.ToSDKPutItemInput(&putInput)
				_, err := db.PutItem(context.Background(), sdkPut)
				require.NoError(t, err)
			},
			input: `{
				"TableName": "UpdateTestTable",
				"Key": {"pk": {"S": "add-item"}},
				"UpdateExpression": "ADD counter :incr",
				"ExpressionAttributeValues": {":incr": {"N": "5"}}
			}`,
			verifyFunc: func(t *testing.T, db *dynamodb.InMemoryDB, _ *dynamodb_sdk.UpdateItemOutput) {
				t.Helper()
				item := getItem(t, db, "UpdateTestTable", "add-item")
				assert.Equal(t, "15", item["counter"].(map[string]any)["N"])
			},
		},
		{
			name: "Condition Check Failed",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				putInput := models.PutItemInput{
					TableName: "UpdateTestTable",
					Item: map[string]any{
						"pk":  map[string]any{"S": "condition-item"},
						"ver": map[string]any{"N": "1"},
					},
				}
				sdkPut, _ := models.ToSDKPutItemInput(&putInput)
				_, err := db.PutItem(context.Background(), sdkPut)
				require.NoError(t, err)
			},
			input: `{
				"TableName": "UpdateTestTable",
				"Key": {"pk": {"S": "condition-item"}},
				"UpdateExpression": "SET ver = :v",
				"ConditionExpression": "ver = :old_ver",
				"ExpressionAttributeValues": {
					":v": {"N": "2"},
					":old_ver": {"N": "99"}
				}
			}`,
			wantErr: true,
		},
		{
			name: "ReturnValues ALL_OLD",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				putInput := models.PutItemInput{
					TableName: "UpdateTestTable",
					Item: map[string]any{
						"pk":   map[string]any{"S": "old-val-item"},
						"data": map[string]any{"S": "original"},
					},
				}
				sdkPut, _ := models.ToSDKPutItemInput(&putInput)
				_, err := db.PutItem(context.Background(), sdkPut)
				require.NoError(t, err)
			},
			input: `{
				"TableName": "UpdateTestTable",
				"Key": {"pk": {"S": "old-val-item"}},
				"UpdateExpression": "SET data = :new",
				"ExpressionAttributeValues": {":new": {"S": "updated"}},
				"ReturnValues": "ALL_OLD"
			}`,
			verifyFunc: func(t *testing.T, _ *dynamodb.InMemoryDB, out *dynamodb_sdk.UpdateItemOutput) {
				t.Helper()
				assert.NotNil(t, out.Attributes)
				wireAttrs := models.FromSDKItem(out.Attributes)
				assert.Equal(t, "original", wireAttrs["data"].(map[string]any)["S"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()

			// Setup table
			ctInput := models.CreateTableInput{
				TableName: "UpdateTestTable",
				KeySchema: []models.KeySchemaElement{
					{AttributeName: "pk", KeyType: "HASH"},
				},
				AttributeDefinitions: []models.AttributeDefinition{
					{AttributeName: "pk", AttributeType: "S"},
				},
			}
			_, err := db.CreateTable(context.Background(), models.ToSDKCreateTableInput(&ctInput))
			require.NoError(t, err)

			if tc.setup != nil {
				tc.setup(t, db)
			}

			updateInput := mustUnmarshal[models.UpdateItemInput](t, tc.input)
			sdkUpdate, _ := models.ToSDKUpdateItemInput(&updateInput)

			res, err := db.UpdateItem(context.Background(), sdkUpdate)
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			out := res

			if tc.verifyFunc != nil {
				tc.verifyFunc(t, db, out)
			}
		})
	}
}

func getItem(t *testing.T, db *dynamodb.InMemoryDB, tableName, pk string) map[string]any {
	t.Helper()
	input := models.GetItemInput{
		TableName: tableName,
		Key:       map[string]any{"pk": map[string]any{"S": pk}},
	}
	sdkInput, _ := models.ToSDKGetItemInput(&input)

	res, err := db.GetItem(context.Background(), sdkInput)
	require.NoError(t, err)
	// res.Item is map[string]types.AttributeValue
	if res.Item == nil {
		return nil
	}

	return models.FromSDKItem(res.Item)
}
