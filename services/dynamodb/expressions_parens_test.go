package dynamodb_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"

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
			ct := models.CreateTableInput{
				TableName: tableName,
				KeySchema: []models.KeySchemaElement{
					{AttributeName: "pk", KeyType: "HASH"},
					{AttributeName: "sk", KeyType: "RANGE"},
				},
				AttributeDefinitions: []models.AttributeDefinition{
					{AttributeName: "pk", AttributeType: "S"},
					{AttributeName: "sk", AttributeType: "S"},
				},
			}
			sdkCreate := models.ToSDKCreateTableInput(&ct)
			_, err := db.CreateTable(context.Background(), sdkCreate)
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
				put := models.PutItemInput{
					TableName: tableName,
					Item: map[string]any{
						"pk": map[string]any{"S": item.pk},
						"sk": map[string]any{"S": item.sk},
					},
				}
				sdkPut, _ := models.ToSDKPutItemInput(&put)
				_, putErr := db.PutItem(context.Background(), sdkPut)
				require.NoError(t, putErr)
			}

			queryInput := models.QueryInput{
				TableName:                 tableName,
				KeyConditionExpression:    tt.keyConditionExpression,
				ExpressionAttributeNames:  tt.attrNames,
				ExpressionAttributeValues: tt.attrValues,
			}

			sdkQuery, _ := models.ToSDKQueryInput(&queryInput)
			result, err := db.Query(context.Background(), sdkQuery)
			require.NoError(t, err)
			require.NotNil(t, result)

			assert.Len(t, result.Items, tt.expectedCount)
		})
	}
}
