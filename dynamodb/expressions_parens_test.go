package dynamodb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuery_KeyCondition_WithParenthesesAndBeginsWith(t *testing.T) {
	db := NewInMemoryDB()
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
		_, err := db.PutItem([]byte(`{
			"TableName": "` + tableName + `",
			"Item": {
				"pk": {"S": "` + item.pk + `"},
				"sk": {"S": "` + item.sk + `"}
			}
		}`))
		require.NoError(t, err)
	}

	// Query with literal expression from user request: (#0 = :0) AND (begins_with (#1, :1)
	// Note: The user provided specific placeholder names #0, :0, #1, :1
	queryJSON := `{
		"TableName": "` + tableName + `",
		"KeyConditionExpression": "(#0 = :0) AND (begins_with (#1, :1))",
		"ExpressionAttributeNames": {
			"#0": "pk",
			"#1": "sk"
		},
		"ExpressionAttributeValues": {
			":0": {"S": "p1"},
			":1": {"S": "prefix_"}
		}
	}`

	result, err := db.Query([]byte(queryJSON))
	require.NoError(t, err)

	var output QueryOutput
	_ = output // unused, just for type

	// We need to marshal/unmarshal to get the struct because Query returns interface{} (any)
	// The in-memory DB implementation returns the struct directly, but let's be safe and check type or marshal

	// Wait, the InMemoryDB.Query returns (any, error). In validation it returns QueryOutput.
	// Let's assert on the returned value.

	// The implementation returns ScanOutput or QueryOutput structs directly as `any`.
	qOut, ok := result.(QueryOutput)
	require.True(t, ok, "Result should be QueryOutput")

	assert.Equal(t, 2, len(qOut.Items), "Should return 2 items")

	// Verify items are correct
	foundKeys := make(map[string]bool)
	for _, item := range qOut.Items {
		pk := item["pk"].(map[string]any)["S"].(string)
		sk := item["sk"].(map[string]any)["S"].(string)
		foundKeys[pk+":"+sk] = true
	}

	assert.True(t, foundKeys["p1:prefix_match"])
	assert.True(t, foundKeys["p1:prefix_other"])
	assert.False(t, foundKeys["p1:no_match"])
}
