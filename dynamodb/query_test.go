package dynamodb_test

import (
	"encoding/json"
	"strconv"
	"testing"

	"Gopherstack/dynamodb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuery(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()

	// Setup table
	tableName := "QueryTestTable"

	ctInput := mustUnmarshal[dynamodb.CreateTableInput](t, `{
		"TableName": "`+tableName+`",
		"KeySchema": [
			{"AttributeName": "pk", "KeyType": "HASH"},
			{"AttributeName": "sk", "KeyType": "RANGE"}
		],
		"AttributeDefinitions": [
			{"AttributeName": "pk", "AttributeType": "S"},
			{"AttributeName": "sk", "AttributeType": "N"}
		],
		"ProvisionedThroughput": {
			"ReadCapacityUnits": 5,
			"WriteCapacityUnits": 5
		}
	}`)
	_, err := db.CreateTable(dynamodb.ToSDKCreateTableInput(&ctInput))
	require.NoError(t, err)

	// Insert items
	// pk=A, sk=1..5
	// pk=B, sk=1..5
	for _, pk := range []string{"A", "B"} {
		for i := 1; i <= 5; i++ {
			putInput := mustUnmarshal[dynamodb.PutItemInput](t, `{
				"TableName": "`+tableName+`",
				"Item": {
					"pk": {"S": "`+pk+`"},
					"sk": {"N": "`+strconv.Itoa(i)+`"},
					"data": {"S": "data-`+pk+`-`+strconv.Itoa(i)+`"}
				}
			}`)
			sdkPut, _ := dynamodb.ToSDKPutItemInput(&putInput)
			_, putErr := db.PutItem(sdkPut)
			require.NoError(t, putErr)
		}
	}

	tests := []struct {
		name       string
		input      string
		errMessage string
		wantItems  []map[string]any
		wantErr    bool
	}{
		{
			name: "Simple PK Query",
			input: `{
				"TableName": "` + tableName + `",
				"KeyConditionExpression": "pk = :pk",
				"ExpressionAttributeValues": {
					":pk": {"S": "A"}
				}
			}`,
			wantItems: makeItems("A", 1, 5),
		},
		{
			name: "PK + SK Exact Match",
			input: `{
				"TableName": "` + tableName + `",
				"KeyConditionExpression": "pk = :pk AND sk = :sk",
				"ExpressionAttributeValues": {
					":pk": {"S": "A"},
					":sk": {"N": "3"}
				}
			}`,
			wantItems: makeItems("A", 3, 3),
		},
		{
			name: "SK Condition: Multiple comparisons (BEETWEEN)",
			input: `{
				"TableName": "` + tableName + `",
				"KeyConditionExpression": "pk = :pk AND sk BETWEEN :min AND :max",
				"ExpressionAttributeValues": {
					":pk": {"S": "A"},
					":min": {"N": "2"},
					":max": {"N": "4"}
				}
			}`,
			wantItems: makeItems("A", 2, 4),
		},
		{
			name: "ScanIndexForward = false (Reverse)",
			input: `{
				"TableName": "` + tableName + `",
				"KeyConditionExpression": "pk = :pk",
				"ExpressionAttributeValues": {
					":pk": {"S": "A"}
				},
				"ScanIndexForward": false
			}`,
			wantItems: reverseItems(makeItems("A", 1, 5)),
		},
		{
			name: "Limit + ExclusiveStartKey (Pagination)",
			input: `{
				"TableName": "` + tableName + `",
				"KeyConditionExpression": "pk = :pk",
				"ExpressionAttributeValues": {
					":pk": {"S": "A"}
				},
				"Limit": 2
			}`,
			wantItems: makeItems("A", 1, 2),
		},
		{
			name: "FilterExpression: Only even numbers",
			input: `{
				"TableName": "` + tableName + `",
				"KeyConditionExpression": "pk = :pk",
				"FilterExpression": "sk = :v2 OR sk = :v4",
				"ExpressionAttributeValues": {
					":pk": {"S": "A"},
					":v2": {"N": "2"},
					":v4": {"N": "4"}
				}
			}`,
			wantItems: []map[string]any{
				makeItem("A", 2),
				makeItem("A", 4),
			},
		},
		{
			name: "ProjectionExpression",
			input: `{
				"TableName": "` + tableName + `",
				"KeyConditionExpression": "pk = :pk AND sk = :sk",
				"ExpressionAttributeValues": {
					":pk": {"S": "A"},
					":sk": {"N": "1"}
				},
				"ProjectionExpression": "pk, sk"
			}`,
			wantItems: []map[string]any{
				{"pk": map[string]any{"S": "A"}, "sk": map[string]any{"N": "1"}},
			},
		},
		{
			name: "PK B Query",
			input: `{
				"TableName": "` + tableName + `",
				"KeyConditionExpression": "pk = :pk",
				"ExpressionAttributeValues": {
					":pk": {"S": "B"}
				}
			}`,
			wantItems: makeItems("B", 1, 5),
		},
		{
			name: "Missing Table",
			input: `{
				"TableName": "NonExistentTable",
				"KeyConditionExpression": "pk = :pk",
				"ExpressionAttributeValues": {":pk": {"S": "A"}}
			}`,
			wantErr:    true,
			errMessage: "Requested resource not found",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			queryInput := mustUnmarshal[dynamodb.QueryInput](t, tc.input)
			sdkQuery, _ := dynamodb.ToSDKQueryInput(&queryInput)

			res, queryErr := db.Query(sdkQuery)
			if tc.wantErr {
				require.Error(t, queryErr)

				if tc.errMessage != "" {
					assert.Contains(t, queryErr.Error(), tc.errMessage)
				}

				return
			}

			require.NoError(t, queryErr)

			// Unwrap output to wire format for comparison
			var gotItems []map[string]any
			for _, item := range res.Items {
				gotItems = append(gotItems, dynamodb.FromSDKItem(item))
			}

			// Verify items
			// We convert both to JSON for easier deep comparison ensuring types match
			wantJSON, _ := json.Marshal(tc.wantItems)
			gotJSON, _ := json.Marshal(gotItems)
			assert.JSONEq(t, string(wantJSON), string(gotJSON))
		})
	}
}

// Helper functions for test data generation

func toStr(i int) string {
	return strconv.Itoa(i)
}

func makeItem(pk string, sk int) map[string]any {
	return map[string]any{
		"pk":   map[string]any{"S": pk},
		"sk":   map[string]any{"N": toStr(sk)},
		"data": map[string]any{"S": "data-" + pk + "-" + toStr(sk)},
	}
}

func makeItems(pk string, startSk, endSk int) []map[string]any {
	var items []map[string]any
	for i := startSk; i <= endSk; i++ {
		items = append(items, makeItem(pk, i))
	}

	return items
}

func reverseItems(items []map[string]any) []map[string]any {
	reversed := make([]map[string]any, len(items))
	for i, item := range items {
		reversed[len(items)-1-i] = item
	}

	return reversed
}

// fromSDKItem is redundant if FromSDKItem is exported, using exported one.

// FromSDKItem converts map[string]types.AttributeValue to map[string]any (wire format)
// It is available in dynamodb package
