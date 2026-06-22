package dynamodb_test

import (
	"encoding/json"
	"testing"

	sdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"

	"github.com/stretchr/testify/require"
)

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
		attributeDefinitions = append(
			attributeDefinitions, models.AttributeDefinition{
				AttributeName: sk[0], AttributeType: "S",
			},
		)
	}

	rcDefault := int64(5)
	wcDefault := int64(5)
	createInput := models.CreateTableInput{
		TableName:            name,
		KeySchema:            keySchema,
		AttributeDefinitions: attributeDefinitions,
	}
	sdkInput := models.ToSDKCreateTableInput(&createInput)
	// DynamoDB requires ProvisionedThroughput when BillingMode is PROVISIONED (the default).
	sdkInput.ProvisionedThroughput = &sdktypes.ProvisionedThroughput{
		ReadCapacityUnits:  &rcDefault,
		WriteCapacityUnits: &wcDefault,
	}
	_, err := db.CreateTable(t.Context(), sdkInput)
	require.NoError(t, err)
}
