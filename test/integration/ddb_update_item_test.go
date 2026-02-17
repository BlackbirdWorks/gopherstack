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

func TestIntegration_DDB_UpdateItem(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createDynamoDBClient(t)

	type testCase struct {
		setup  func(t *testing.T, ctx context.Context, tableName string)
		input  func(tableName string) *dynamodb.UpdateItemInput
		verify func(t *testing.T, out *dynamodb.UpdateItemOutput)
		name   string
	}

	tests := []testCase{
		{
			name: "UpdateItem_Set_NewItem",
			input: func(tableName string) *dynamodb.UpdateItemInput {
				return &dynamodb.UpdateItemInput{
					TableName: aws.String(tableName),
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "item1"},
					},
					UpdateExpression: aws.String("SET val = :v"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":v": &types.AttributeValueMemberS{Value: "new_value"},
					},
					ReturnValues: types.ReturnValueAllNew,
				}
			},
			verify: func(t *testing.T, out *dynamodb.UpdateItemOutput) {
				t.Helper()
				require.NotNil(t, out.Attributes)
				AssertItem(t, out.Attributes, map[string]any{"pk": "item1", "val": "new_value"})
			},
		},
		{
			name: "UpdateItem_Set_ExistingItem",
			setup: func(t *testing.T, ctx context.Context, tableName string) {
				t.Helper()
				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(tableName),
					Item: map[string]types.AttributeValue{
						"pk":  &types.AttributeValueMemberS{Value: "item2"},
						"val": &types.AttributeValueMemberS{Value: "original"},
					},
				})
				require.NoError(t, err)
			},
			input: func(tableName string) *dynamodb.UpdateItemInput {
				return &dynamodb.UpdateItemInput{
					TableName: aws.String(tableName),
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "item2"},
					},
					UpdateExpression: aws.String("SET val = :v"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":v": &types.AttributeValueMemberS{Value: "updated"},
					},
					ReturnValues: types.ReturnValueAllOld,
				}
			},
			verify: func(t *testing.T, out *dynamodb.UpdateItemOutput) {
				t.Helper()
				require.NotNil(t, out.Attributes)
				AssertItem(t, out.Attributes, map[string]any{"pk": "item2", "val": "original"})
			},
		},
		{
			name: "UpdateItem_ReturnValue_UpdatedNew",
			setup: func(t *testing.T, ctx context.Context, tableName string) {
				t.Helper()
				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(tableName),
					Item: map[string]types.AttributeValue{
						"pk":    &types.AttributeValueMemberS{Value: "item3"},
						"other": &types.AttributeValueMemberS{Value: "stay"},
						"val":   &types.AttributeValueMemberS{Value: "original"},
					},
				})
				require.NoError(t, err)
			},
			input: func(tableName string) *dynamodb.UpdateItemInput {
				return &dynamodb.UpdateItemInput{
					TableName: aws.String(tableName),
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "item3"},
					},
					UpdateExpression: aws.String("SET val = :v"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":v": &types.AttributeValueMemberS{Value: "updated"},
					},
					ReturnValues: types.ReturnValueUpdatedNew,
				}
			},
			verify: func(t *testing.T, out *dynamodb.UpdateItemOutput) {
				t.Helper()
				require.NotNil(t, out.Attributes)
				// Should only contain the updated field
				AssertItem(t, out.Attributes, map[string]any{"val": "updated"})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tableName := "UpdateItemTestTable-" + uuid.NewString()
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
				_, dErr := client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
					TableName: aws.String(tableName),
				})
				assert.NoError(t, dErr)
			})

			time.Sleep(50 * time.Millisecond)

			if tt.setup != nil {
				tt.setup(t, ctx, tableName)
			}

			out, err := client.UpdateItem(ctx, tt.input(tableName))
			require.NoError(t, err)

			if tt.verify != nil {
				tt.verify(t, out)
			}
		})
	}
}
