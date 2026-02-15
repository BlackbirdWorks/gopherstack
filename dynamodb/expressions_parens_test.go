package dynamodb_test

import (
	"encoding/json"
	"testing"

	"Gopherstack/dynamodb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuery_KeyCondition_WithParenthesesAndBeginsWith(t *testing.T) {
	t.Parallel()

	tests := []struct {
		attrNames              map[string]string
		attrValues             map[string]any
		name                   string
		keyConditionExpression string
		expectedCount          int
	}{
		{
			name:                   "Parentheses with begins_with",
			keyConditionExpression: "(#0 = :0) AND (begins_with (#1, :1))",
			attrNames: map[string]string{
				"#0": "pk",
				"#1": "sk",
			},
			attrValues: map[string]any{
				":0": map[string]any{"S": "p1"},
				":1": map[string]any{"S": "prefix_"},
			},
			expectedCount: 2,
		},
		{
			name:                   "Nested parentheses",
			keyConditionExpression: "((#0 = :0)) AND ((begins_with (#1, :1)))",
			attrNames: map[string]string{
				"#0": "pk",
				"#1": "sk",
			},
			attrValues: map[string]any{
				":0": map[string]any{"S": "p1"},
				":1": map[string]any{"S": "prefix_"},
			},
			expectedCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()
			tableName := "TestTable_Parens"

			// Create table
			_, err := db.CreateTable([]byte(`{
				"TableName": "` + tableName + `",
				"KeySchema": [
					{"AttributeName": "pk", "KeyType": "HASH"},
					{"AttributeName": "sk", "KeyType": "RANGE"}
				],
				"AttributeDefinitions": [
					{"AttributeName": "pk", "AttributeType": "S"},
					{"AttributeName": "sk", "AttributeType": "S"}
				],
				"ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}
			}`))
			require.NoError(t, err)

			// Put items
			items := []struct {
				pk string
				sk string
			}{
				{"p1", "prefix_match"},
				{"p1", "prefix_other"},
				{"p1", "no_match"},
				{"p2", "prefix_match"},
			}

			for _, item := range items {
				_, putErr := db.PutItem([]byte(`{
					"TableName": "` + tableName + `",
					"Item": {
						"pk": {"S": "` + item.pk + `"},
						"sk": {"S": "` + item.sk + `"}
					}
				}`))
				require.NoError(t, putErr)
			}

			queryInput := struct {
				ExpressionAttributeNames  map[string]string `json:"ExpressionAttributeNames"`
				ExpressionAttributeValues map[string]any    `json:"ExpressionAttributeValues"`
				TableName                 string            `json:"TableName"`
				KeyConditionExpression    string            `json:"KeyConditionExpression"`
			}{
				TableName:                 tableName,
				KeyConditionExpression:    tt.keyConditionExpression,
				ExpressionAttributeNames:  tt.attrNames,
				ExpressionAttributeValues: tt.attrValues,
			}

			inputJSON, err := json.Marshal(queryInput)
			require.NoError(t, err)

			result, err := db.Query(inputJSON)
			require.NoError(t, err)

			qOut, ok := result.(dynamodb.QueryOutput)
			require.True(t, ok, "Result should be QueryOutput")

			assert.Len(t, qOut.Items, tt.expectedCount)
		})
	}
}
