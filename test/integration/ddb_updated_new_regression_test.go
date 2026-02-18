package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestIntegration_DDB_UpdateItem_UpdatedNew_SameValue(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	ctx := t.Context()
	client := createDynamoDBClient(t)

	tableName := "updated-new-regression"

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
			"pk":  &types.AttributeValueMemberS{Value: "item-1"},
			"foo": &types.AttributeValueMemberN{Value: "1"},
			"bar": &types.AttributeValueMemberS{Value: "original"},
		},
	})
	require.NoError(t, err)

	// 2. Update item-1 setting foo to 1 (same value) and bar to "new"
	// ReturnValues: UPDATED_NEW
	updateOut, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "item-1"},
		},
		UpdateExpression: aws.String("SET foo = :one, bar = :new"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":one": &types.AttributeValueMemberN{Value: "1"},
			":new": &types.AttributeValueMemberS{Value: "new"},
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	})
	require.NoError(t, err)

	t.Logf("Returned attributes: %+v", updateOut.Attributes)

	// In DynamoDB, UPDATED_NEW should return both foo and bar because they were both in the SET clause,
	// even if foo's value didn't change.
	require.NotNil(t, updateOut.Attributes)
	_, hasFoo := updateOut.Attributes["foo"]
	_, hasBar := updateOut.Attributes["bar"]

	require.True(t, hasBar, "bar should be returned")
	require.True(t, hasFoo, "foo should be returned even if value is the same")

	// 3. Test with nested attributes
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "item-2"},
			"map": &types.AttributeValueMemberM{
				Value: map[string]types.AttributeValue{
					"sub": &types.AttributeValueMemberN{Value: "10"},
				},
			},
		},
	})
	require.NoError(t, err)

	updateOut2, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "item-2"},
		},
		UpdateExpression: aws.String("SET #m.#s = :val"),
		ExpressionAttributeNames: map[string]string{
			"#m": "map",
			"#s": "sub",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":val": &types.AttributeValueMemberN{Value: "10"},
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	})
	require.NoError(t, err)
	require.NotNil(t, updateOut2.Attributes)
	_, hasMap := updateOut2.Attributes["map"]
	require.True(t, hasMap, "map should be returned even if nested value is the same")

	// 4. Test ONLY updating one attribute to the same value
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "item-3"},
			"same": &types.AttributeValueMemberS{Value: "value"},
		},
	})
	require.NoError(t, err)

	updateOut3, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "item-3"},
		},
		UpdateExpression: aws.String("SET same = :val"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":val": &types.AttributeValueMemberS{Value: "value"},
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	})
	require.NoError(t, err)

	t.Logf("Returned attributes (only same): %+v", updateOut3.Attributes)
	require.NotNil(
		t,
		updateOut3.Attributes,
		"Attributes should not be nil for UPDATED_NEW even if same value",
	)
	_, hasSame := updateOut3.Attributes["same"]
	require.True(t, hasSame, "Attribute 'same' should be returned even if value is the same")

	// 5. Test UPDATED_NEW on new item (should NOT return pk)
	updateOut4, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "new-item"},
		},
		UpdateExpression: aws.String("SET attr = :val"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":val": &types.AttributeValueMemberS{Value: "value"},
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	})
	require.NoError(t, err)

	t.Logf("Returned attributes (new item): %+v", updateOut4.Attributes)
	require.NotNil(t, updateOut4.Attributes)
	_, hasPk := updateOut4.Attributes["pk"]
	require.False(t, hasPk, "pk should NOT be returned for UPDATED_NEW on new item")
	_, hasAttr := updateOut4.Attributes["attr"]
	require.True(t, hasAttr, "attr should be returned for UPDATED_NEW on new item")
}
