package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestIntegration_DDB_UpdateItem_UPDATED_NEW_NewItem(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createDynamoDBClient(t)

	tableName := "UpdatedNewTest-" + uuid.NewString()
	ctx := t.Context()

	// Create table
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	// Update a NEW item with UPDATED_NEW
	t.Log("=== Updating NEW item with UPDATED_NEW ===")
	out, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "newitem"},
		},
		UpdateExpression: aws.String("SET attr1 = :v1, attr2 = :v2"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":v1": &types.AttributeValueMemberS{Value: "value1"},
			":v2": &types.AttributeValueMemberS{Value: "value2"},
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	})
	require.NoError(t, err)
	require.NotNil(t, out.Attributes, "Attributes should not be nil for UPDATED_NEW on new item")

	t.Logf("Returned attributes: %+v", out.Attributes)

	// Should contain only updated attributes (attr1, attr2)
	AssertItem(t, out.Attributes, map[string]any{
		"attr1": "value1",
		"attr2": "value2",
	})

	// Clean up
	_, _ = client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String(tableName),
	})
}
