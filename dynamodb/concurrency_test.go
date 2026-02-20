package dynamodb_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/dynamodb"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_dynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchConcurrency(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()
	ctx := context.Background()
	tableName := "ConcurrencyTable"

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

	const numGoroutines = 10
	const numIterations = 50
	var wg sync.WaitGroup

	// Start concurrent writers
	for g := range numGoroutines {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := range numIterations {
				input := &sdk_dynamodb.BatchWriteItemInput{
					RequestItems: map[string][]types.WriteRequest{
						tableName: {
							{
								PutRequest: &types.PutRequest{
									Item: map[string]types.AttributeValue{
										"id": &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%d-%d", gid, i)},
									},
								},
							},
						},
					},
				}
				var loopErr error
				_, loopErr = db.BatchWriteItem(ctx, input)
				assert.NoError(t, loopErr)
				time.Sleep(1 * time.Millisecond)
			}
		}(g)
	}

	// Start concurrent readers
	for g := range numGoroutines {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := range numIterations {
				input := &sdk_dynamodb.BatchGetItemInput{
					RequestItems: map[string]types.KeysAndAttributes{
						tableName: {
							Keys: []map[string]types.AttributeValue{
								{"id": &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%d-%d", gid, i/2)}},
							},
						},
					},
				}
				var loopErr error
				_, loopErr = db.BatchGetItem(ctx, input)
				// We don't necessarily expect to find the item yet, but we expect no race/panic
				assert.NoError(t, loopErr)
				time.Sleep(1 * time.Millisecond)
			}
		}(g)
	}

	wg.Wait()
}
