package dynamodb_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/dynamodb"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_dynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchDeletePerformance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		numItems      int
		itemsPerBatch int
		deleteBatches int
		maxDuration   time.Duration
	}{
		{
			name:          "deletes 2500 items from 10000 within time limit",
			numItems:      10000,
			itemsPerBatch: 25,
			deleteBatches: 100,
			// 15s instead of 5s to account for race detector overhead and slow CI runners.
			maxDuration: 15 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			tableName := "LargeTable"

			// Setup table
			_, err := db.CreateTable(t.Context(), &sdk_dynamodb.CreateTableInput{
				TableName: aws.String(tableName),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
				},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
				},
			})
			require.NoError(t, err)

			// Pre-populate items
			numBatches := tt.numItems / tt.itemsPerBatch
			for b := range numBatches {
				requests := make([]types.WriteRequest, tt.itemsPerBatch)
				for j := range tt.itemsPerBatch {
					requests[j] = types.WriteRequest{
						PutRequest: &types.PutRequest{
							Item: map[string]types.AttributeValue{
								"id": &types.AttributeValueMemberS{
									Value: fmt.Sprintf("item-%d", b*tt.itemsPerBatch+j),
								},
								"data": &types.AttributeValueMemberS{Value: "some-bloated-data-to-make-it-real"},
							},
						},
					}
				}
				_, populateErr := db.BatchWriteItem(t.Context(), &sdk_dynamodb.BatchWriteItemInput{
					RequestItems: map[string][]types.WriteRequest{
						tableName: requests,
					},
				})
				require.NoError(t, populateErr)
			}

			// Measure time for batch deletes
			start := time.Now()
			for i := range tt.deleteBatches {
				requests := make([]types.WriteRequest, tt.itemsPerBatch)
				for j := range tt.itemsPerBatch {
					requests[j] = types.WriteRequest{
						DeleteRequest: &types.DeleteRequest{
							Key: map[string]types.AttributeValue{
								"id": &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%d", i*tt.itemsPerBatch+j)},
							},
						},
					}
				}
				_, deleteErr := db.BatchWriteItem(t.Context(), &sdk_dynamodb.BatchWriteItemInput{
					RequestItems: map[string][]types.WriteRequest{
						tableName: requests,
					},
				})
				require.NoError(t, deleteErr)
			}
			duration := time.Since(start)

			t.Logf("Deleted %d items from %d in %v", tt.deleteBatches*tt.itemsPerBatch, tt.numItems, duration)
			assert.LessOrEqual(t, duration, tt.maxDuration, "Batch delete is too slow!")
		})
	}
}
