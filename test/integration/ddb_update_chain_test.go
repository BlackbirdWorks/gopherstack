package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestIntegration_DDB_UpdateItem_Chain(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	ctx := t.Context()
	client := createDynamoDBClient(t)

	tableName := "update-chain-test"

	_, createErr := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String("pk"),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String("pk"),
				KeyType:       types.KeyTypeHash,
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	require.NoError(t, createErr)

	t.Cleanup(func() {
		_, _ = client.DeleteTable(t.Context(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	})

	// 1. Put initial item
	_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "item-1"},
			"data": &types.AttributeValueMemberS{Value: "original"},
			"meta": &types.AttributeValueMemberM{
				Value: map[string]types.AttributeValue{
					"owner": &types.AttributeValueMemberS{Value: "alice"},
				},
			},
		},
	})
	require.NoError(t, err)

	// 2. Update item-1 and get ALL_NEW
	updateOut, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "item-1"},
		},
		UpdateExpression: aws.String("SET #d = :v, meta.version = :ver"),
		ExpressionAttributeNames: map[string]string{
			"#d": "data",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":v":   &types.AttributeValueMemberS{Value: "updated"},
			":ver": &types.AttributeValueMemberN{Value: "1"},
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	require.NoError(t, err)
	require.NotNil(t, updateOut.Attributes)

	// 3. Use returned attributes to create item-2
	// Attributes is map[string]types.AttributeValue, which is exactly what PutItem expects
	item2 := updateOut.Attributes
	item2["pk"] = &types.AttributeValueMemberS{Value: "item-2"}

	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item:      item2,
	})
	require.NoError(t, err)

	// 4. Verify both items exist and have same content (except PK)
	get1, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "item-1"},
		},
	})
	require.NoError(t, err)
	AssertItem(t, get1.Item, map[string]any{
		"pk":   "item-1",
		"data": "updated",
		"meta": map[string]any{
			"owner":   "alice",
			"version": "1",
		},
	})

	get2, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "item-2"},
		},
	})
	require.NoError(t, err)
	AssertItem(t, get2.Item, map[string]any{
		"pk":   "item-2",
		"data": "updated",
		"meta": map[string]any{
			"owner":   "alice",
			"version": "1",
		},
	})
}
