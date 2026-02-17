package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_DDB_VersioningFlowWithUPDATED_NEW tests the versioning pattern
// with UPDATED_NEW return value, starting from a blank state and then upserting.
func TestIntegration_DDB_VersioningFlowWithUPDATED_NEW(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	ctx := t.Context()
	client := createDynamoDBClient(t)

	tableName := "versioning-updated-new-" + uuid.NewString()

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
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, createErr)

	t.Cleanup(func() {
		_, _ = client.DeleteTable(t.Context(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	})

	// Test 1: First update on NEW item with UPDATED_NEW
	// Should return all updated attributes including the key and version
	t.Log("=== Test 1: First update on NEW item with UPDATED_NEW ===")
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
		ReturnValues: types.ReturnValueUpdatedNew,
	})
	require.NoError(t, err, "Should create item with version 1")
	require.NotNil(t, out1.Attributes, "UPDATED_NEW should return attributes for new item")

	t.Logf("Returned attributes: %+v", out1.Attributes)

	// For a NEW item, UPDATED_NEW should return all attributes that were set
	// including the key and all updated attributes
	assert.Contains(t, out1.Attributes, "pk", "Should contain primary key")
	assert.Contains(t, out1.Attributes, "version", "Should contain version")
	assert.Contains(t, out1.Attributes, "data", "Should contain data")

	// Verify values
	pk1, ok := out1.Attributes["pk"].(*types.AttributeValueMemberS)
	require.True(t, ok, "pk should be a string")
	assert.Equal(t, "doc1", pk1.Value)

	v1, ok := out1.Attributes["version"].(*types.AttributeValueMemberN)
	require.True(t, ok, "version should be a number")
	assert.Equal(t, "1", v1.Value)

	d1, ok := out1.Attributes["data"].(*types.AttributeValueMemberS)
	require.True(t, ok, "data should be a string")
	assert.Equal(t, "first version", d1.Value)

	// Test 2: Second update on EXISTING item with UPDATED_NEW
	// Should return only the attributes that were actually updated (not pk, not details)
	t.Log("=== Test 2: Second update on EXISTING item with UPDATED_NEW ===")
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
		ReturnValues: types.ReturnValueUpdatedNew,
	})
	require.NoError(t, err, "Should increment version to 2")
	require.NotNil(t, out2.Attributes, "UPDATED_NEW should return attributes")

	t.Logf("Returned attributes: %+v", out2.Attributes)

	// For an EXISTING item, UPDATED_NEW should return ONLY updated attributes
	assert.Contains(t, out2.Attributes, "version", "Should contain updated version")
	assert.Contains(t, out2.Attributes, "data", "Should contain updated data")
	assert.NotContains(t, out2.Attributes, "pk", "Should NOT contain pk (not updated)")

	// Verify values
	v2, ok := out2.Attributes["version"].(*types.AttributeValueMemberN)
	require.True(t, ok)
	assert.Equal(t, "2", v2.Value)

	d2, ok := out2.Attributes["data"].(*types.AttributeValueMemberS)
	require.True(t, ok)
	assert.Equal(t, "second version", d2.Value)

	// Test 3: Upsert pattern - update item that exists with additional attributes
	t.Log("=== Test 3: Upsert on EXISTING item adding new attribute ===")
	out3, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "doc1"},
		},
		UpdateExpression: aws.String("SET version = version + :inc, details = :details"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":inc": &types.AttributeValueMemberN{Value: "1"},
			":details": &types.AttributeValueMemberM{
				Value: map[string]types.AttributeValue{
					"author": &types.AttributeValueMemberS{Value: "test"},
				},
			},
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	})
	require.NoError(t, err)
	require.NotNil(t, out3.Attributes)

	t.Logf("Returned attributes: %+v", out3.Attributes)

	// Should return only the attributes that were updated
	assert.Contains(t, out3.Attributes, "version", "Should contain updated version")
	assert.Contains(t, out3.Attributes, "details", "Should contain newly added details")
	assert.NotContains(t, out3.Attributes, "pk", "Should NOT contain pk")
	assert.NotContains(t, out3.Attributes, "data", "Should NOT contain data (not updated)")

	v3, ok := out3.Attributes["version"].(*types.AttributeValueMemberN)
	require.True(t, ok)
	assert.Equal(t, "3", v3.Value)

	// Test 4: Start fresh with a different key using ADD pattern
	t.Log("=== Test 4: NEW item with ADD version pattern and UPDATED_NEW ===")
	out4, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
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
		ReturnValues: types.ReturnValueUpdatedNew,
	})
	require.NoError(t, err, "Should create item with version using ADD")
	require.NotNil(t, out4.Attributes)

	t.Logf("Returned attributes: %+v", out4.Attributes)

	// For a NEW item with ADD, all attributes should be returned
	assert.Contains(t, out4.Attributes, "pk", "Should contain primary key")
	assert.Contains(t, out4.Attributes, "version", "Should contain version")
	assert.Contains(t, out4.Attributes, "data", "Should contain data")

	v4, ok := out4.Attributes["version"].(*types.AttributeValueMemberN)
	require.True(t, ok)
	assert.Equal(t, "1", v4.Value)
}
