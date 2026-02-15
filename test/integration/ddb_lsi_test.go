//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalSecondaryIndex(t *testing.T) {
	t.Parallel()
	client := createDynamoDBClient(t)

	tableName := "LSI_Test_" + uuid.NewString()

	// Create Table with LSI
	_, err := client.CreateTable(t.Context(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
			{AttributeName: aws.String("lsi_sk"), AttributeType: types.ScalarAttributeTypeN},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{
			{
				IndexName: aws.String("LSI_1"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},      // Must match table PK
					{AttributeName: aws.String("lsi_sk"), KeyType: types.KeyTypeRange}, // Different SK
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			},
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
	time.Sleep(10 * time.Millisecond)

	// Insert Items
	// PK=A, SK=1, LSI_SK=50
	// PK=A, SK=2, LSI_SK=40
	// PK=A, SK=3, LSI_SK=30
	vars := []struct {
		sk    string
		lsiSk string
	}{
		{"1", "50"},
		{"2", "40"},
		{"3", "30"},
	}

	for _, v := range vars {
		_, err := client.PutItem(t.Context(), &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"pk":     &types.AttributeValueMemberS{Value: "A"},
				"sk":     &types.AttributeValueMemberN{Value: v.sk},
				"lsi_sk": &types.AttributeValueMemberN{Value: v.lsiSk},
				"data":   &types.AttributeValueMemberS{Value: "some data"},
			},
		})
		require.NoError(t, err)
	}

	// Query Table (By SK) -> Should be ordered 1, 2, 3
	outTable, err := client.Query(t.Context(), &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "A"},
		},
	})
	assert.NoError(t, err)
	assert.Len(t, outTable.Items, 3)
	assert.Equal(t, "1", outTable.Items[0]["sk"].(*types.AttributeValueMemberN).Value)
	assert.Equal(t, "3", outTable.Items[2]["sk"].(*types.AttributeValueMemberN).Value)

	// Query LSI (By LSI_SK) -> Should be ordered 30, 40, 50 (which corresponds to SK 3, 2, 1)
	outLSI, err := client.Query(t.Context(), &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		IndexName:              aws.String("LSI_1"),
		KeyConditionExpression: aws.String("pk = :pk"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":pk": &types.AttributeValueMemberS{Value: "A"},
		},
	})
	assert.NoError(t, err)
	assert.Len(t, outLSI.Items, 3)
	// Order should be by LSI_SK (30 < 40 < 50)
	assert.Equal(t, "30", outLSI.Items[0]["lsi_sk"].(*types.AttributeValueMemberN).Value)
	assert.Equal(t, "3", outLSI.Items[0]["sk"].(*types.AttributeValueMemberN).Value) // The item with lsi_sk=30 has sk=3

	assert.Equal(t, "50", outLSI.Items[2]["lsi_sk"].(*types.AttributeValueMemberN).Value)
	assert.Equal(t, "1", outLSI.Items[2]["sk"].(*types.AttributeValueMemberN).Value) // The item with lsi_sk=50 has sk=1
}
