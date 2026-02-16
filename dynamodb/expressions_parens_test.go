package dynamodb_test

import (
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
			ct := dynamodb.CreateTableInput{
				TableName: tableName,
				KeySchema: []dynamodb.KeySchemaElement{
					{AttributeName: "pk", KeyType: "HASH"},
					{AttributeName: "sk", KeyType: "RANGE"},
				},
				AttributeDefinitions: []dynamodb.AttributeDefinition{
					{AttributeName: "pk", AttributeType: "S"},
					{AttributeName: "sk", AttributeType: "S"},
				},
			}
			_, err := db.CreateTable(dynamodb.ToSDKCreateTableInput(&ct))
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
				put := dynamodb.PutItemInput{
					TableName: tableName,
					Item: map[string]any{
						"pk": map[string]any{"S": item.pk},
						"sk": map[string]any{"S": item.sk},
					},
				}
				sdkPut, _ := dynamodb.ToSDKPutItemInput(&put)
				_, putErr := db.PutItem(sdkPut)
				require.NoError(t, putErr)
			}

			queryInput := dynamodb.QueryInput{
				TableName:                 tableName,
				KeyConditionExpression:    tt.keyConditionExpression,
				ExpressionAttributeNames:  tt.attrNames,
				ExpressionAttributeValues: tt.attrValues,
			}

			sdkQuery, _ := dynamodb.ToSDKQueryInput(&queryInput)
			result, err := db.Query(sdkQuery)
			require.NoError(t, err)
			require.NotNil(t, result)

			assert.Len(t, result.Items, tt.expectedCount)
		})
	}
}
