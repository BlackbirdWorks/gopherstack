package dynamodb_test

import (
	"Gopherstack/dynamodb"
	"testing"

	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateItem(t *testing.T) {
	t.Parallel()

	tableName := "UpdateTestTable"

	tests := []struct {
		setup      func(*dynamodb.InMemoryDB)
		verifyFunc func(t *testing.T, db *dynamodb.InMemoryDB, out *dynamodb_sdk.UpdateItemOutput)
		name       string
		input      string
		wantErr    bool
	}{
		{
			name: "New Item Creation (SET)",
			input: `{
				"TableName": "` + tableName + `",
				"Key": {"pk": {"S": "new-item"}},
				"UpdateExpression": "SET #v = :val",
				"ExpressionAttributeNames": {"#v": "value"},
				"ExpressionAttributeValues": {":val": {"S": "test"}}
			}`,
			verifyFunc: func(t *testing.T, db *dynamodb.InMemoryDB, _ *dynamodb_sdk.UpdateItemOutput) {
				t.Helper()
				item := getItem(t, db, tableName, "new-item")
				require.NotNil(t, item)
				assert.Equal(t, "test", item["value"].(map[string]any)["S"])
			},
		},
		{
			name: "Update Existing Item (SET)",
			setup: func(db *dynamodb.InMemoryDB) {
				putInput := dynamodb.PutItemInput{
					TableName: tableName,
					Item: map[string]any{
						"pk":    map[string]any{"S": "update-item"},
						"count": map[string]any{"N": "1"},
					},
				}
				sdkPut, _ := dynamodb.ToSDKPutItemInput(&putInput)
				_, _ = db.PutItem(sdkPut)
			},
			input: `{
				"TableName": "` + tableName + `",
				"Key": {"pk": {"S": "update-item"}},
				"UpdateExpression": "SET count = :val",
				"ExpressionAttributeValues": {":val": {"N": "5"}},
				"ReturnValues": "ALL_NEW"
			}`,
			verifyFunc: func(t *testing.T, _ *dynamodb.InMemoryDB, out *dynamodb_sdk.UpdateItemOutput) {
				t.Helper()
				assert.NotNil(t, out.Attributes)
				// Convert to wire format to check value easily
				wireAttrs := dynamodb.FromSDKItem(out.Attributes)
				assert.Equal(t, "5", wireAttrs["count"].(map[string]any)["N"])
			},
		},
		{
			name: "REMOVE Attribute",
			setup: func(db *dynamodb.InMemoryDB) {
				putInput := dynamodb.PutItemInput{
					TableName: tableName,
					Item: map[string]any{
						"pk":    map[string]any{"S": "remove-item"},
						"extra": map[string]any{"S": "delete-me"},
					},
				}
				sdkPut, _ := dynamodb.ToSDKPutItemInput(&putInput)
				_, _ = db.PutItem(sdkPut)
			},
			input: `{
				"TableName": "` + tableName + `",
				"Key": {"pk": {"S": "remove-item"}},
				"UpdateExpression": "REMOVE extra"
			}`,
			verifyFunc: func(t *testing.T, db *dynamodb.InMemoryDB, _ *dynamodb_sdk.UpdateItemOutput) {
				t.Helper()
				item := getItem(t, db, tableName, "remove-item")
				_, exists := item["extra"]
				assert.False(t, exists, "Attribute should be removed")
			},
		},
		{
			name: "ADD Number",
			setup: func(db *dynamodb.InMemoryDB) {
				putInput := dynamodb.PutItemInput{
					TableName: tableName,
					Item: map[string]any{
						"pk":      map[string]any{"S": "add-item"},
						"counter": map[string]any{"N": "10"},
					},
				}
				sdkPut, _ := dynamodb.ToSDKPutItemInput(&putInput)
				_, _ = db.PutItem(sdkPut)
			},
			input: `{
				"TableName": "` + tableName + `",
				"Key": {"pk": {"S": "add-item"}},
				"UpdateExpression": "ADD counter :incr",
				"ExpressionAttributeValues": {":incr": {"N": "5"}}
			}`,
			verifyFunc: func(t *testing.T, db *dynamodb.InMemoryDB, _ *dynamodb_sdk.UpdateItemOutput) {
				t.Helper()
				item := getItem(t, db, tableName, "add-item")
				assert.Equal(t, "15", item["counter"].(map[string]any)["N"])
			},
		},
		{
			name: "Condition Check Failed",
			setup: func(db *dynamodb.InMemoryDB) {
				putInput := dynamodb.PutItemInput{
					TableName: tableName,
					Item: map[string]any{
						"pk":  map[string]any{"S": "condition-item"},
						"ver": map[string]any{"N": "1"},
					},
				}
				sdkPut, _ := dynamodb.ToSDKPutItemInput(&putInput)
				_, _ = db.PutItem(sdkPut)
			},
			input: `{
				"TableName": "` + tableName + `",
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
			setup: func(db *dynamodb.InMemoryDB) {
				putInput := dynamodb.PutItemInput{
					TableName: tableName,
					Item: map[string]any{
						"pk":   map[string]any{"S": "old-val-item"},
						"data": map[string]any{"S": "original"},
					},
				}
				sdkPut, _ := dynamodb.ToSDKPutItemInput(&putInput)
				_, _ = db.PutItem(sdkPut)
			},
			input: `{
				"TableName": "` + tableName + `",
				"Key": {"pk": {"S": "old-val-item"}},
				"UpdateExpression": "SET data = :new",
				"ExpressionAttributeValues": {":new": {"S": "updated"}},
				"ReturnValues": "ALL_OLD"
			}`,
			verifyFunc: func(t *testing.T, _ *dynamodb.InMemoryDB, out *dynamodb_sdk.UpdateItemOutput) {
				t.Helper()
				assert.NotNil(t, out.Attributes)
				wireAttrs := dynamodb.FromSDKItem(out.Attributes)
				assert.Equal(t, "original", wireAttrs["data"].(map[string]any)["S"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()

			// Setup table
			ctInput := dynamodb.CreateTableInput{
				TableName:            tableName,
				KeySchema:            []dynamodb.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
				AttributeDefinitions: []dynamodb.AttributeDefinition{{AttributeName: "pk", AttributeType: "S"}},
			}
			_, err := db.CreateTable(dynamodb.ToSDKCreateTableInput(&ctInput))
			require.NoError(t, err)

			if tc.setup != nil {
				tc.setup(db)
			}

			updateInput := mustUnmarshal[dynamodb.UpdateItemInput](t, tc.input)
			sdkUpdate, _ := dynamodb.ToSDKUpdateItemInput(&updateInput)

			res, err := db.UpdateItem(sdkUpdate)
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
	input := dynamodb.GetItemInput{
		TableName: tableName,
		Key:       map[string]any{"pk": map[string]any{"S": pk}},
	}
	sdkInput, _ := dynamodb.ToSDKGetItemInput(&input)

	res, err := db.GetItem(sdkInput)
	require.NoError(t, err)
	// res.Item is map[string]types.AttributeValue
	if res.Item == nil {
		return nil
	}

	return dynamodb.FromSDKItem(res.Item)
}
