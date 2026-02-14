package dynamodb_test

import (
	"Gopherstack/dynamodb"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateItem(t *testing.T) {
	t.Parallel()

	tableName := "UpdateTestTable"

	tests := []struct {
		setup      func(*dynamodb.InMemoryDB)
		verifyFunc func(t *testing.T, db *dynamodb.InMemoryDB, out dynamodb.UpdateItemOutput)
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
			verifyFunc: func(t *testing.T, db *dynamodb.InMemoryDB, _ dynamodb.UpdateItemOutput) {
				t.Helper()
				item := getItem(t, db, tableName, "new-item")
				require.NotNil(t, item)
				assert.Equal(t, "test", item["value"].(map[string]any)["S"])
			},
		},
		{
			name: "Update Existing Item (SET)",
			setup: func(db *dynamodb.InMemoryDB) {
				_, _ = db.PutItem([]byte(`{
					"TableName": "` + tableName + `",
					"Item": {"pk": {"S": "update-item"}, "count": {"N": "1"}}
				}`))
			},
			input: `{
				"TableName": "` + tableName + `",
				"Key": {"pk": {"S": "update-item"}},
				"UpdateExpression": "SET count = :val",
				"ExpressionAttributeValues": {":val": {"N": "5"}},
				"ReturnValues": "ALL_NEW"
			}`,
			verifyFunc: func(t *testing.T, _ *dynamodb.InMemoryDB, out dynamodb.UpdateItemOutput) {
				t.Helper()
				assert.NotNil(t, out.Attributes)
				assert.Equal(t, "5", out.Attributes["count"].(map[string]any)["N"])
			},
		},
		{
			name: "REMOVE Attribute",
			setup: func(db *dynamodb.InMemoryDB) {
				_, _ = db.PutItem([]byte(`{
					"TableName": "` + tableName + `",
					"Item": {"pk": {"S": "remove-item"}, "extra": {"S": "delete-me"}}
				}`))
			},
			input: `{
				"TableName": "` + tableName + `",
				"Key": {"pk": {"S": "remove-item"}},
				"UpdateExpression": "REMOVE extra"
			}`,
			verifyFunc: func(t *testing.T, db *dynamodb.InMemoryDB, _ dynamodb.UpdateItemOutput) {
				t.Helper()
				item := getItem(t, db, tableName, "remove-item")
				_, exists := item["extra"]
				assert.False(t, exists, "Attribute should be removed")
			},
		},
		{
			name: "ADD Number",
			setup: func(db *dynamodb.InMemoryDB) {
				_, _ = db.PutItem([]byte(`{
					"TableName": "` + tableName + `",
					"Item": {"pk": {"S": "add-item"}, "counter": {"N": "10"}}
				}`))
			},
			input: `{
				"TableName": "` + tableName + `",
				"Key": {"pk": {"S": "add-item"}},
				"UpdateExpression": "ADD counter :incr",
				"ExpressionAttributeValues": {":incr": {"N": "5"}}
			}`,
			verifyFunc: func(t *testing.T, db *dynamodb.InMemoryDB, _ dynamodb.UpdateItemOutput) {
				t.Helper()
				item := getItem(t, db, tableName, "add-item")
				assert.Equal(t, "15", item["counter"].(map[string]any)["N"])
			},
		},
		{
			name: "Condition Check Failed",
			setup: func(db *dynamodb.InMemoryDB) {
				_, _ = db.PutItem([]byte(`{
					"TableName": "` + tableName + `",
					"Item": {"pk": {"S": "condition-item"}, "ver": {"N": "1"}}
				}`))
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
				_, _ = db.PutItem([]byte(`{
					"TableName": "` + tableName + `",
					"Item": {"pk": {"S": "old-val-item"}, "data": {"S": "original"}}
				}`))
			},
			input: `{
				"TableName": "` + tableName + `",
				"Key": {"pk": {"S": "old-val-item"}},
				"UpdateExpression": "SET data = :new",
				"ExpressionAttributeValues": {":new": {"S": "updated"}},
				"ReturnValues": "ALL_OLD"
			}`,
			verifyFunc: func(t *testing.T, _ *dynamodb.InMemoryDB, out dynamodb.UpdateItemOutput) {
				t.Helper()
				assert.NotNil(t, out.Attributes)
				assert.Equal(t, "original", out.Attributes["data"].(map[string]any)["S"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()

			// Setup table
			_, err := db.CreateTable([]byte(`{
				"TableName": "` + tableName + `",
				"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
				"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
				"ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
			}`))
			require.NoError(t, err)

			if tc.setup != nil {
				tc.setup(db)
			}

			res, err := db.UpdateItem([]byte(tc.input))
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			out, ok := res.(dynamodb.UpdateItemOutput)
			require.True(t, ok)

			if tc.verifyFunc != nil {
				tc.verifyFunc(t, db, out)
			}
		})
	}
}

func getItem(t *testing.T, db *dynamodb.InMemoryDB, tableName, pk string) map[string]any {
	t.Helper()
	res, err := db.GetItem([]byte(`{
		"TableName": "` + tableName + `",
		"Key": {"pk": {"S": "` + pk + `"}}
	}`))
	require.NoError(t, err)
	out := res.(dynamodb.GetItemOutput)

	return out.Item
}
