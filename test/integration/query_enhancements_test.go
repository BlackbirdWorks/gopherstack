//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryEnhancements(t *testing.T) {
	t.Parallel()
	client := createDynamoDBClient(t)

	// Helper to create table with PK and SK
	createTable := func(t *testing.T) string {
		tableName := "QueryEnhance_" + uuid.NewString()
		_, err := client.CreateTable(t.Context(), &dynamodb.CreateTableInput{
			TableName: aws.String(tableName),
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
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
		return tableName
	}

	t.Run("SortKey_Ordering", func(t *testing.T) {
		t.Parallel()
		tableName := createTable(t)

		// Insert 5 items: pk=A, sk=1, 2, 3, 4, 5
		for i := 1; i <= 5; i++ {
			client.PutItem(t.Context(), &dynamodb.PutItemInput{
				TableName: aws.String(tableName),
				Item: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "A"},
					"sk": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", i)},
				},
			})
		}

		// Forward (Default)
		out, err := client.Query(t.Context(), &dynamodb.QueryInput{
			TableName:              aws.String(tableName),
			KeyConditionExpression: aws.String("pk = :pk"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "A"},
			},
		})
		assert.NoError(t, err)
		assert.Len(t, out.Items, 5)
		assert.Equal(t, "1", out.Items[0]["sk"].(*types.AttributeValueMemberN).Value)
		assert.Equal(t, "5", out.Items[4]["sk"].(*types.AttributeValueMemberN).Value)

		// Reverse (ScanIndexForward = false)
		outRev, err := client.Query(t.Context(), &dynamodb.QueryInput{
			TableName:              aws.String(tableName),
			KeyConditionExpression: aws.String("pk = :pk"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "A"},
			},
			ScanIndexForward: aws.Bool(false),
		})
		assert.NoError(t, err)
		assert.Len(t, outRev.Items, 5)
		assert.Equal(t, "5", outRev.Items[0]["sk"].(*types.AttributeValueMemberN).Value)
		assert.Equal(t, "1", outRev.Items[4]["sk"].(*types.AttributeValueMemberN).Value)
	})

	t.Run("SortKey_Conditions", func(t *testing.T) {
		t.Parallel()
		tableName := createTable(t)

		for i := 1; i <= 5; i++ {
			client.PutItem(t.Context(), &dynamodb.PutItemInput{
				TableName: aws.String(tableName),
				Item: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "B"},
					"sk": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", i)},
				},
			})
		}

		// SK > 3
		out, err := client.Query(t.Context(), &dynamodb.QueryInput{
			TableName:              aws.String(tableName),
			KeyConditionExpression: aws.String("pk = :pk AND sk > :v"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "B"},
				":v":  &types.AttributeValueMemberN{Value: "3"},
			},
		})
		assert.NoError(t, err)
		assert.Len(t, out.Items, 2) // 4, 5
		assert.Equal(t, "4", out.Items[0]["sk"].(*types.AttributeValueMemberN).Value)

		// SK <= 2
		out2, err := client.Query(t.Context(), &dynamodb.QueryInput{
			TableName:              aws.String(tableName),
			KeyConditionExpression: aws.String("pk = :pk AND sk <= :v"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "B"},
				":v":  &types.AttributeValueMemberN{Value: "2"},
			},
		})
		assert.NoError(t, err)
		assert.Len(t, out2.Items, 2) // 1, 2
	})

	t.Run("Pagination", func(t *testing.T) {
		t.Parallel()
		tableName := createTable(t)

		// Insert 10 items
		for i := 1; i <= 10; i++ {
			client.PutItem(t.Context(), &dynamodb.PutItemInput{
				TableName: aws.String(tableName),
				Item: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "C"},
					"sk": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", i)},
				},
			})
		}

		// First Page: Limit 3
		out1, err := client.Query(t.Context(), &dynamodb.QueryInput{
			TableName:              aws.String(tableName),
			KeyConditionExpression: aws.String("pk = :pk"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "C"},
			},
			Limit: aws.Int32(3),
		})
		assert.NoError(t, err)
		assert.Len(t, out1.Items, 3)
		assert.NotNil(t, out1.LastEvaluatedKey)
		assert.Equal(t, "3", out1.Items[2]["sk"].(*types.AttributeValueMemberN).Value)

		// Second Page: Start from LastEvaluatedKey
		out2, err := client.Query(t.Context(), &dynamodb.QueryInput{
			TableName:              aws.String(tableName),
			KeyConditionExpression: aws.String("pk = :pk"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":pk": &types.AttributeValueMemberS{Value: "C"},
			},
			Limit:             aws.Int32(3),
			ExclusiveStartKey: out1.LastEvaluatedKey,
		})
		assert.NoError(t, err)
		assert.Len(t, out2.Items, 3) // 4, 5, 6
		assert.Equal(t, "4", out2.Items[0]["sk"].(*types.AttributeValueMemberN).Value)
		assert.Equal(t, "6", out2.Items[2]["sk"].(*types.AttributeValueMemberN).Value)
	})
}
