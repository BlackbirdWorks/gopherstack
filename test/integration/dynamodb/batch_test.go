//go:build integration

package dynamodb_test

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

func TestBatchOperations(t *testing.T) {
	t.Parallel()
	client := createDynamoDBClient(t)

	type testCase struct {
		name   string
		setup  func(t *testing.T, ctx context.Context, table1, table2 string)
		input  func(table1, table2 string) (interface{}, string) // Returns input struct and type "Write" or "Get"
		verify func(t *testing.T, out interface{})
	}

	tests := []testCase{
		{
			name: "BatchWriteItem_PutAndDelete",
			setup: func(t *testing.T, ctx context.Context, table1, table2 string) {
				// Seed table2 with an item to delete
				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(table2),
					Item: map[string]types.AttributeValue{
						"pk":  &types.AttributeValueMemberS{Value: "to_delete"},
						"val": &types.AttributeValueMemberS{Value: "data"},
					},
				})
				require.NoError(t, err)
			},
			input: func(table1, table2 string) (interface{}, string) {
				return &dynamodb.BatchWriteItemInput{
					RequestItems: map[string][]types.WriteRequest{
						table1: {
							{
								PutRequest: &types.PutRequest{
									Item: map[string]types.AttributeValue{
										"pk":  &types.AttributeValueMemberS{Value: "item1"},
										"val": &types.AttributeValueMemberS{Value: "new_data"},
									},
								},
							},
						},
						table2: {
							{
								DeleteRequest: &types.DeleteRequest{
									Key: map[string]types.AttributeValue{
										"pk": &types.AttributeValueMemberS{Value: "to_delete"},
									},
								},
							},
						},
					},
				}, "Write"
			},
			verify: func(t *testing.T, out interface{}) {
				output := out.(*dynamodb.BatchWriteItemOutput)
				assert.Empty(t, output.UnprocessedItems)
			},
		},
		{
			name: "BatchGetItem_MultipleTables",
			setup: func(t *testing.T, ctx context.Context, table1, table2 string) {
				// Seed both tables
				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(table1),
					Item: map[string]types.AttributeValue{
						"pk":  &types.AttributeValueMemberS{Value: "item1"},
						"val": &types.AttributeValueMemberS{Value: "t1_data"},
					},
				})
				require.NoError(t, err)

				_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(table2),
					Item: map[string]types.AttributeValue{
						"pk":  &types.AttributeValueMemberS{Value: "item2"},
						"val": &types.AttributeValueMemberS{Value: "t2_data"},
					},
				})
				require.NoError(t, err)
			},
			input: func(table1, table2 string) (interface{}, string) {
				return &dynamodb.BatchGetItemInput{
					RequestItems: map[string]types.KeysAndAttributes{
						table1: {
							Keys: []map[string]types.AttributeValue{
								{"pk": &types.AttributeValueMemberS{Value: "item1"}},
							},
						},
						table2: {
							Keys: []map[string]types.AttributeValue{
								{"pk": &types.AttributeValueMemberS{Value: "item2"}},
							},
						},
					},
				}, "Get"
			},
			verify: func(t *testing.T, out interface{}) {
				output := out.(*dynamodb.BatchGetItemOutput)
				assert.Empty(t, output.UnprocessedKeys)
				assert.Len(t, output.Responses, 2)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			table1 := "BatchTable1-" + uuid.NewString()
			table2 := "BatchTable2-" + uuid.NewString()
			ctx := t.Context()

			// Create two tables
			for _, tbl := range []string{table1, table2} {
				_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
					TableName: aws.String(tbl),
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
						TableName: aws.String(tbl),
					})
					assert.NoError(t, err)
				})
			}

			time.Sleep(50 * time.Millisecond)

			if tt.setup != nil {
				tt.setup(t, ctx, table1, table2)
			}

			inputStruct, opType := tt.input(table1, table2)
			if opType == "Write" {
				out, err := client.BatchWriteItem(ctx, inputStruct.(*dynamodb.BatchWriteItemInput))
				require.NoError(t, err)
				tt.verify(t, out)

				// Extra verification for Write: Check items
				// Check table1 has item1
				outGet, err := client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName: aws.String(table1),
					Key:       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "item1"}},
				})
				require.NoError(t, err)
				if tt.name == "BatchWriteItem_PutAndDelete" {
					assert.NotNil(t, outGet.Item)
				}

				// Check table2 has deleted to_delete
				outGet2, err := client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName: aws.String(table2),
					Key:       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "to_delete"}},
				})
				require.NoError(t, err)
				if tt.name == "BatchWriteItem_PutAndDelete" {
					assert.Nil(t, outGet2.Item)
				}

			} else {
				out, err := client.BatchGetItem(ctx, inputStruct.(*dynamodb.BatchGetItemInput))
				require.NoError(t, err)
				tt.verify(t, out)
			}
		})
	}
}
