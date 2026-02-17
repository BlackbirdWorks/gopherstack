package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_DDB_VersionControl tests the DynamoDB version control pattern
// described in: https://aws.amazon.com/blogs/database/implementing-version-control-using-amazon-dynamodb/
// This pattern uses if_not_exists() to initialize a version counter and then increments it.
func TestIntegration_DDB_VersionControl(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	ctx := t.Context()
	client := createDynamoDBClient(t)

	tableName := "version-control-test"

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

	// Step 1: First update — should initialize version to 1
	out1, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "doc1"},
		},
		UpdateExpression: aws.String(
			"SET version = if_not_exists(version, :zero) + :inc, #data = :data",
		),
		ExpressionAttributeNames: map[string]string{"#data": "data"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":zero": &types.AttributeValueMemberN{Value: "0"},
			":inc":  &types.AttributeValueMemberN{Value: "1"},
			":data": &types.AttributeValueMemberS{Value: "first version"},
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	require.NoError(t, err, "Should create item with version 1")
	v1, ok := out1.Attributes["version"].(*types.AttributeValueMemberN)
	require.True(t, ok, "version should be a number")
	assert.Equal(t, "1", v1.Value)
	d1, ok := out1.Attributes["data"].(*types.AttributeValueMemberS)
	require.True(t, ok)
	assert.Equal(t, "first version", d1.Value)

	// Step 2: Second update — should increment version to 2
	out2, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "doc1"},
		},
		UpdateExpression:         aws.String("SET version = version + :inc, #data = :data"),
		ExpressionAttributeNames: map[string]string{"#data": "data"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":inc":  &types.AttributeValueMemberN{Value: "1"},
			":data": &types.AttributeValueMemberS{Value: "second version"},
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	require.NoError(t, err, "Should increment version to 2")
	v2, ok := out2.Attributes["version"].(*types.AttributeValueMemberN)
	require.True(t, ok)
	assert.Equal(t, "2", v2.Value)
	d2, ok := out2.Attributes["data"].(*types.AttributeValueMemberS)
	require.True(t, ok)
	assert.Equal(t, "second version", d2.Value)

	// Step 3: Update with condition — version must be 2
	out3, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "doc1"},
		},
		UpdateExpression:         aws.String("SET version = version + :inc, #data = :data"),
		ConditionExpression:      aws.String("version = :expected"),
		ExpressionAttributeNames: map[string]string{"#data": "data"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":inc":      &types.AttributeValueMemberN{Value: "1"},
			":data":     &types.AttributeValueMemberS{Value: "third version"},
			":expected": &types.AttributeValueMemberN{Value: "2"},
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	require.NoError(t, err, "Should update when version matches")
	v3, ok := out3.Attributes["version"].(*types.AttributeValueMemberN)
	require.True(t, ok)
	assert.Equal(t, "3", v3.Value)

	// Step 4: Update with wrong version — should fail (current is 3)
	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "doc1"},
		},
		UpdateExpression:         aws.String("SET version = version + :inc, #data = :data"),
		ConditionExpression:      aws.String("version = :expected"),
		ExpressionAttributeNames: map[string]string{"#data": "data"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":inc":      &types.AttributeValueMemberN{Value: "1"},
			":data":     &types.AttributeValueMemberS{Value: "should not update"},
			":expected": &types.AttributeValueMemberN{Value: "1"}, // Wrong version (current is 3)
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	require.Error(t, err, "Should fail when version doesn't match")
	assert.Contains(t, err.Error(), "ConditionalCheckFailed")

	// Step 5: Alternative pattern using ADD instead of SET
	out5, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "doc2"},
		},
		UpdateExpression:         aws.String("ADD version :inc SET #data = :data"),
		ExpressionAttributeNames: map[string]string{"#data": "data"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":inc":  &types.AttributeValueMemberN{Value: "1"},
			":data": &types.AttributeValueMemberS{Value: "using ADD"},
		},
		ReturnValues: types.ReturnValueAllNew,
	})
	require.NoError(t, err, "Should create item with version using ADD")
	v5, ok := out5.Attributes["version"].(*types.AttributeValueMemberN)
	require.True(t, ok)
	assert.Equal(t, "1", v5.Value)
}
