package dynamodb_test

import (
	"encoding/json"
	"testing"

	"Gopherstack/dynamodb"

	"github.com/stretchr/testify/require"
)

//nolint:ireturn // Test helper returning generic type T
func mustUnmarshal[T any](t *testing.T, jsonStr string) T {
	t.Helper()
	var val T
	err := json.Unmarshal([]byte(jsonStr), &val)
	require.NoError(t, err)

	return val
}

func createTableHelper(t *testing.T, db *dynamodb.InMemoryDB, name string, pk string, sk ...string) {
	t.Helper()
	keySchema := []dynamodb.KeySchemaElement{
		{AttributeName: pk, KeyType: dynamodb.KeyTypeHash},
	}
	attributeDefinitions := []dynamodb.AttributeDefinition{
		{AttributeName: pk, AttributeType: "S"},
	}

	if len(sk) > 0 {
		keySchema = append(keySchema, dynamodb.KeySchemaElement{
			AttributeName: sk[0], KeyType: dynamodb.KeyTypeRange,
		})
		attributeDefinitions = append(attributeDefinitions, dynamodb.AttributeDefinition{
			AttributeName: sk[0], AttributeType: "S"},
		)
	}

	createInput := dynamodb.CreateTableInput{
		TableName:            name,
		KeySchema:            keySchema,
		AttributeDefinitions: attributeDefinitions,
	}
	sdkInput := dynamodb.ToSDKCreateTableInput(&createInput)
	_, err := db.CreateTable(sdkInput)
	require.NoError(t, err)
}
