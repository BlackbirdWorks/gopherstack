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

func TestConditionsAndFilters(t *testing.T) {
	t.Parallel()
	client := createDynamoDBClient(t)

	type testCase struct {
		name      string
		setup     func(t *testing.T, ctx context.Context, tableName string)
		operation func(t *testing.T, ctx context.Context, tableName string) error
		verify    func(t *testing.T, ctx context.Context, tableName string, err error)
	}

	tests := []testCase{
		{
			name: "PutItem_Condition_AttributeNotExists_Success",
			setup: func(t *testing.T, ctx context.Context, tableName string) {
				// Empty table
			},
			operation: func(t *testing.T, ctx context.Context, tableName string) error {
				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(tableName),
					Item: map[string]types.AttributeValue{
						"pk":  &types.AttributeValueMemberS{Value: "item1"},
						"val": &types.AttributeValueMemberS{Value: "data"},
					},
					ConditionExpression: aws.String("attribute_not_exists(pk)"),
				})
				return err
			},
			verify: func(t *testing.T, ctx context.Context, tableName string, err error) {
				assert.NoError(t, err)
			},
		},
		{
			name: "PutItem_Condition_AttributeNotExists_Fail",
			setup: func(t *testing.T, ctx context.Context, tableName string) {
				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(tableName),
					Item: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "item1"},
					},
				})
				require.NoError(t, err)
			},
			operation: func(t *testing.T, ctx context.Context, tableName string) error {
				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(tableName),
					Item: map[string]types.AttributeValue{
						"pk":  &types.AttributeValueMemberS{Value: "item1"},
						"val": &types.AttributeValueMemberS{Value: "overwrite"},
					},
					ConditionExpression: aws.String("attribute_not_exists(pk)"),
				})
				return err
			},
			verify: func(t *testing.T, ctx context.Context, tableName string, err error) {
				assert.Error(t, err)
				var ccf *types.ConditionalCheckFailedException
				assert.ErrorAs(t, err, &ccf)
			},
		},
		{
			name: "UpdateItem_Condition_Equals_Success",
			setup: func(t *testing.T, ctx context.Context, tableName string) {
				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(tableName),
					Item: map[string]types.AttributeValue{
						"pk":     &types.AttributeValueMemberS{Value: "item1"},
						"status": &types.AttributeValueMemberS{Value: "PENDING"},
					},
				})
				require.NoError(t, err)
			},
			operation: func(t *testing.T, ctx context.Context, tableName string) error {
				_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
					TableName: aws.String(tableName),
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "item1"},
					},
					UpdateExpression:         aws.String("SET status = :newStatus"),
					ConditionExpression:      aws.String("#s = :currStatus"),
					ExpressionAttributeNames: map[string]string{"#s": "status"},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":currStatus": &types.AttributeValueMemberS{Value: "PENDING"},
						":newStatus":  &types.AttributeValueMemberS{Value: "DONE"},
					},
				})
				return err
			},
			verify: func(t *testing.T, ctx context.Context, tableName string, err error) {
				assert.NoError(t, err)
				// Verify update
				out, _ := client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName: aws.String(tableName),
					Key:       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "item1"}},
				})
				assert.Equal(t, "DONE", out.Item["status"].(*types.AttributeValueMemberS).Value)
			},
		},
		{
			name: "Scan_FilterExpression",
			setup: func(t *testing.T, ctx context.Context, tableName string) {
				// 3 items: 2 matching filter, 1 not
				items := []string{"MATCH_1", "MATCH_2", "SKIP_1"}
				for _, id := range items {
					_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
						TableName: aws.String(tableName),
						Item: map[string]types.AttributeValue{
							"pk":   &types.AttributeValueMemberS{Value: id},
							"type": &types.AttributeValueMemberS{Value: "MATCH_TYPE"}, // Simplify: check prefix "MATCH" in pk
						},
					})
					require.NoError(t, err)
				}
			},
			operation: func(t *testing.T, ctx context.Context, tableName string) error {
				// We wrap verification in operation here for simplicity of test structure
				// Usually operation just returns error, verify checks result.
				// But Scan returns items.
				// Let's modify verify signature? No, let's use context or verify inside operation?
				// Actually, let's just assert inside verify by re-running scan or passing result?
				// The test structure returns only error.
				// Let's ignore this constraint and do check in verify.
				return nil
			},
			verify: func(t *testing.T, ctx context.Context, tableName string, err error) {
				out, err := client.Scan(ctx, &dynamodb.ScanInput{
					TableName:        aws.String(tableName),
					FilterExpression: aws.String("begins_with(pk, :prefix)"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":prefix": &types.AttributeValueMemberS{Value: "MATCH"},
					},
				})
				assert.NoError(t, err)
				assert.Len(t, out.Items, 2)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tableName := "ConditionTable-" + uuid.NewString()
			ctx := t.Context()

			// Create Table
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
				client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(tableName)})
			})
			time.Sleep(50 * time.Millisecond)

			if tt.setup != nil {
				tt.setup(t, ctx, tableName)
			}

			err = tt.operation(t, ctx, tableName)
			tt.verify(t, ctx, tableName, err)
		})
	}
}
