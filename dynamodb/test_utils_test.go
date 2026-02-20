package dynamodb_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/blackbirdworks/gopherstack/dynamodb/models"

	"github.com/blackbirdworks/gopherstack/dynamodb"

	"github.com/stretchr/testify/require"
)

//nolint:ireturn // Generic test helper returning type param is fine.
func mustUnmarshal[T any](t *testing.T, jsonStr string) T {
	t.Helper()
	var val T
	err := json.Unmarshal([]byte(jsonStr), &val)
	require.NoError(t, err)

	return val
}

func createTableHelper(
	t *testing.T,
	db *dynamodb.InMemoryDB,
	name string,
	pk string,
	sk ...string,
) {
	t.Helper()
	keySchema := []models.KeySchemaElement{
		{AttributeName: pk, KeyType: models.KeyTypeHash},
	}
	attributeDefinitions := []models.AttributeDefinition{
		{AttributeName: pk, AttributeType: "S"},
	}

	if len(sk) > 0 {
		keySchema = append(keySchema, models.KeySchemaElement{
			AttributeName: sk[0], KeyType: models.KeyTypeRange,
		})
		attributeDefinitions = append(attributeDefinitions, models.AttributeDefinition{
			AttributeName: sk[0], AttributeType: "S"},
		)
	}

	createInput := models.CreateTableInput{
		TableName:            name,
		KeySchema:            keySchema,
		AttributeDefinitions: attributeDefinitions,
	}
	sdkInput := models.ToSDKCreateTableInput(&createInput)
	_, err := db.CreateTable(context.Background(), sdkInput)
	require.NoError(t, err)
}
