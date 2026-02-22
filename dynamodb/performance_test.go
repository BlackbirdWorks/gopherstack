package dynamodb_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/dynamodb"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_dynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

func TestBatchDeletePerformance(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()
	ctx := context.Background()
	tableName := "LargeTable"

	// Setup table
	_, err := db.CreateTable(ctx, &sdk_dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
		},
	})
	require.NoError(t, err)

	// Pre-populate with 10,000 items
	const numItems = 10000
	const itemsPerBatch = 25
	numBatches := numItems / itemsPerBatch

	for b := range numBatches {
		requests := make([]types.WriteRequest, itemsPerBatch)
		for j := range itemsPerBatch {
			requests[j] = types.WriteRequest{
				PutRequest: &types.PutRequest{
					Item: map[string]types.AttributeValue{
						"id":   &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%d", b*itemsPerBatch+j)},
						"data": &types.AttributeValueMemberS{Value: "some-bloated-data-to-make-it-real"},
					},
				},
			}
		}
		var populateErr error
		_, populateErr = db.BatchWriteItem(ctx, &sdk_dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				tableName: requests,
			},
		})
		require.NoError(t, populateErr)
	}

	// Measure time for 100 batch deletes (25 items each = 2500 items)
	start := time.Now()
	for i := range 100 {
		requests := make([]types.WriteRequest, 25)
		for j := range 25 {
			requests[j] = types.WriteRequest{
				DeleteRequest: &types.DeleteRequest{
					Key: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%d", i*25+j)},
					},
				},
			}
		}
		var deleteErr error
		_, deleteErr = db.BatchWriteItem(ctx, &sdk_dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]types.WriteRequest{
				tableName: requests,
			},
		})
		require.NoError(t, deleteErr)
	}
	duration := time.Since(start)

	t.Logf("Deleted 2500 items from 10000 in %v", duration)

	// If it takes more than 15 seconds, it's definitely too slow (e.g. O(N^2) behavior).
	// We use 15s instead of 5s to account for race detector overhead and slow CI runners.
	const maxDuration = 15 * time.Second
	if duration > maxDuration {
		t.Errorf("Batch delete is too slow! Took %v, expected less than %v", duration, maxDuration)
	}
}
