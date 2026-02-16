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

func TestIntegration_DDB_Query(t *testing.T) {
	t.Parallel()
	client := createDynamoDBClient(t)

	type testCase struct {
		setup  func(t *testing.T, ctx context.Context, tableName string)
		input  func(tableName string) *dynamodb.QueryInput
		verify func(t *testing.T, out *dynamodb.QueryOutput)
		name   string
	}

	tests := []testCase{
		{
			name: "Query_PartitionKey_ExactMatch",
			setup: func(t *testing.T, ctx context.Context, tableName string) {
				t.Helper()
				// Seed data
				items := []map[string]types.AttributeValue{
					{
						"pk":  &types.AttributeValueMemberS{Value: "user1"},
						"sk":  &types.AttributeValueMemberS{Value: "meta"},
						"val": &types.AttributeValueMemberS{Value: "data1"},
					},
					{
						"pk":  &types.AttributeValueMemberS{Value: "user1"},
						"sk":  &types.AttributeValueMemberS{Value: "profile"},
						"val": &types.AttributeValueMemberS{Value: "data2"},
					},
					{
						"pk":  &types.AttributeValueMemberS{Value: "user2"}, // Different PK
						"sk":  &types.AttributeValueMemberS{Value: "meta"},
						"val": &types.AttributeValueMemberS{Value: "data3"},
					},
				}

				for _, item := range items {
					_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
						TableName: aws.String(tableName),
						Item:      item,
					})
					require.NoError(t, err)
				}
			},
			input: func(tableName string) *dynamodb.QueryInput {
				return &dynamodb.QueryInput{
					TableName:              aws.String(tableName),
					KeyConditionExpression: aws.String("pk = :pk"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":pk": &types.AttributeValueMemberS{Value: "user1"},
					},
				}
			},
			verify: func(t *testing.T, out *dynamodb.QueryOutput) {
				t.Helper()
				// Should find 2 items for user1
				assert.Equal(t, int32(2), out.Count)
				assert.Len(t, out.Items, 2)
			},
		},
		{
			name: "Query_NoMatch",
			setup: func(t *testing.T, ctx context.Context, tableName string) {
				t.Helper()
				// Empty table or data that doesn't match
				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(tableName),
					Item: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "exist"},
						"sk": &types.AttributeValueMemberS{Value: "1"},
					},
				})
				require.NoError(t, err)
			},
			input: func(tableName string) *dynamodb.QueryInput {
				return &dynamodb.QueryInput{
					TableName:              aws.String(tableName),
					KeyConditionExpression: aws.String("pk = :pk"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":pk": &types.AttributeValueMemberS{Value: "non-existent"},
					},
				}
			},
			verify: func(t *testing.T, out *dynamodb.QueryOutput) {
				t.Helper()
				assert.Equal(t, int32(0), out.Count)
				assert.Empty(t, out.Items)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tableName := "QueryTestTable-" + uuid.NewString()
			ctx := t.Context()

			// Create table with PK and SK
			_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
				TableName: aws.String(tableName),
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
					{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
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
				_, dErr := client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
					TableName: aws.String(tableName),
				})
				assert.NoError(t, dErr)
			})

			time.Sleep(50 * time.Millisecond)

			if tt.setup != nil {
				tt.setup(t, ctx, tableName)
			}

			out, err := client.Query(ctx, tt.input(tableName))
			require.NoError(t, err)

			if tt.verify != nil {
				tt.verify(t, out)
			}
		})
	}
}
