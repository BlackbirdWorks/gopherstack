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

func TestDDB_PutItem(t *testing.T) {
	t.Parallel()

	client := createDynamoDBClient(t)

	type testCase struct {
		name   string
		setup  func(t *testing.T, tableName string)
		input  func(tableName string) *dynamodb.PutItemInput
		verify func(t *testing.T, out *dynamodb.PutItemOutput)
	}

	tests := []testCase{
		{
			name: "ReturnConsumedCapacity",
			input: func(tableName string) *dynamodb.PutItemInput {
				return &dynamodb.PutItemInput{
					TableName: aws.String(tableName),
					Item: map[string]types.AttributeValue{
						"pk":  &types.AttributeValueMemberS{Value: "item1"},
						"val": &types.AttributeValueMemberS{Value: "test"},
					},
					ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
				}
			},
			verify: func(t *testing.T, out *dynamodb.PutItemOutput) {
				require.NotNil(t, out.ConsumedCapacity)
				assert.Equal(t, float64(1.0), *out.ConsumedCapacity.CapacityUnits)
			},
		},
		{
			name: "ReturnItemCollectionMetrics",
			input: func(tableName string) *dynamodb.PutItemInput {
				return &dynamodb.PutItemInput{
					TableName: aws.String(tableName),
					Item: map[string]types.AttributeValue{
						"pk":  &types.AttributeValueMemberS{Value: "item2"},
						"val": &types.AttributeValueMemberS{Value: "test"},
					},
					ReturnItemCollectionMetrics: types.ReturnItemCollectionMetricsSize,
				}
			},
			verify: func(t *testing.T, out *dynamodb.PutItemOutput) {
				require.NotNil(t, out.ItemCollectionMetrics)
				assert.NotNil(t, out.ItemCollectionMetrics.ItemCollectionKey)
			},
		},
		{
			name: "ReturnValues_ALL_OLD",
			setup: func(t *testing.T, tableName string) {
				_, err := client.PutItem(t.Context(), &dynamodb.PutItemInput{
					TableName: aws.String(tableName),
					Item: map[string]types.AttributeValue{
						"pk":  &types.AttributeValueMemberS{Value: "item3"},
						"val": &types.AttributeValueMemberS{Value: "original"},
					},
				})
				require.NoError(t, err)
			},
			input: func(tableName string) *dynamodb.PutItemInput {
				return &dynamodb.PutItemInput{
					TableName: aws.String(tableName),
					Item: map[string]types.AttributeValue{
						"pk":  &types.AttributeValueMemberS{Value: "item3"},
						"val": &types.AttributeValueMemberS{Value: "new"},
					},
					ReturnValues: types.ReturnValueAllOld,
				}
			},
			verify: func(t *testing.T, out *dynamodb.PutItemOutput) {
				require.NotNil(t, out.Attributes)
				val, ok := out.Attributes["val"].(*types.AttributeValueMemberS)
				require.True(t, ok)
				assert.Equal(t, "original", val.Value)
			},
		},
	}

	for _, tt := range tests {
		tt := tt // capture range variable
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create a unique table for each test case to ensure total isolation
			tableName := "PutItemTestTable-" + uuid.NewString()

			// Use t.Context() for lifecycle management
			ctx := t.Context()

			_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
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
				// Use a fresh context for cleanup as t.Context() might be cancelled
				_, err := client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
					TableName: aws.String(tableName),
				})
				assert.NoError(t, err)
			})

			// Wait for table to be ready (usually instant for in-memory, but good practice)
			time.Sleep(50 * time.Millisecond)

			if tt.setup != nil {
				tt.setup(t, tableName)
			}

			out, err := client.PutItem(ctx, tt.input(tableName))
			require.NoError(t, err)

			if tt.verify != nil {
				tt.verify(t, out)
			}
		})
	}
}
