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

func TestUpdateItem(t *testing.T) {
	t.Parallel()
	client := createDynamoDBClient(t)

	type testCase struct {
		name   string
		setup  func(t *testing.T, ctx context.Context, tableName string)
		input  func(tableName string) *dynamodb.UpdateItemInput
		verify func(t *testing.T, out *dynamodb.UpdateItemOutput)
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
				require.NotNil(t, out.Attributes)
				val, ok := out.Attributes["val"].(*types.AttributeValueMemberS)
				require.True(t, ok)
				assert.Equal(t, "new_value", val.Value)
			},
		},
		{
			name: "UpdateItem_Set_ExistingItem",
			setup: func(t *testing.T, ctx context.Context, tableName string) {
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
				require.NotNil(t, out.Attributes)
				val, ok := out.Attributes["val"].(*types.AttributeValueMemberS)
				require.True(t, ok)
				assert.Equal(t, "original", val.Value)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
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
				_, err := client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
					TableName: aws.String(tableName),
				})
				assert.NoError(t, err)
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
