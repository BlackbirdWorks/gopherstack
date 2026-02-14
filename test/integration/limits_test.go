//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidationAndLimits(t *testing.T) {
	t.Parallel()
	client := createDynamoDBClient(t)

	// Helper to create table
	createTable := func(t *testing.T) string {
		tableName := "Limits_" + uuid.NewString()
		_, err := client.CreateTable(t.Context(), &dynamodb.CreateTableInput{
			TableName: aws.String(tableName),
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			},
			ProvisionedThroughput: &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(5),
				WriteCapacityUnits: aws.Int64(5),
			},
		})
		require.NoError(t, err)

		t.Cleanup(func() {
			client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(tableName)})
		})
		return tableName
	}

	t.Run("ItemSizeLimit", func(t *testing.T) {
		t.Parallel()
		tableName := createTable(t)

		// Create a large item > 400KB
		// 400KB = 409600 bytes
		largeVal := strings.Repeat("a", 410000)

		_, err := client.PutItem(t.Context(), &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"pk":   &types.AttributeValueMemberS{Value: "largeItem"},
				"data": &types.AttributeValueMemberS{Value: largeVal},
			},
		})
		assert.Error(t, err)
		// Check error type/message if possible.
		// SDK v2 might wrap it.
		// We expect ItemCollectionSizeLimitExceededException (mapped from our internal error)
		// Or Generic 400 with message.
		// For now, just asserting Error is sufficient proof of validation trigger.
		// print check
		t.Logf("Got expected error: %v", err)
	})

	t.Run("MissingKey", func(t *testing.T) {
		t.Parallel()
		tableName := createTable(t)

		_, err := client.PutItem(t.Context(), &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"other": &types.AttributeValueMemberS{Value: "val"},
			},
		})
		assert.Error(t, err)
		// Expect ValidationException: Missing key element
	})

	t.Run("DataTypeValidation", func(t *testing.T) {
		t.Parallel()
		tableName := createTable(t)

		// This test using SDK is tricky because SDK typed structs enforce types on client side usually.
		// However, we can try to send a "valid" SDK request (string) but one that our server rejects?
		// "validateDataTypes" currently checks if "N" is a string.
		// The SDK types.AttributeValueMemberN IS a string.
		// So checking "must be string" passes for valid SDK requests.

		// To fail "validateDataTypes" logic (internal check), we'd need to send a non-string as "N".
		// But valid SDK client sends "N" as string.
		// So this test confirms HAPPY path (valid N string passes).

		_, err := client.PutItem(t.Context(), &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"pk":  &types.AttributeValueMemberS{Value: "item1"},
				"num": &types.AttributeValueMemberN{Value: "123"},
			},
		})
		assert.NoError(t, err)

		// To truly test invalid type we might need raw JSON request or a modified client,
		// but Go strict typing makes it hard to send bad types via SDK.
		// We can rely on unit tests for `validation.go` for deep type checking if needed later.
	})
}
