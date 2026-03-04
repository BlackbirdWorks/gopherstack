package dynamodb_test

import (
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

	tests := []struct {
		name          string
		tableName     string
		numGoroutines int
		numIterations int
	}{
		{
			name:          "concurrent_batch_writes_and_reads",
			tableName:     "ConcurrencyTable",
			numGoroutines: 10,
			numIterations: 50,
		},
		{
			name:          "low_concurrency_batch_operations",
			tableName:     "LowConcurrencyTable",
			numGoroutines: 2,
			numIterations: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			ctx := t.Context()

			_, err := db.CreateTable(ctx, &sdk_dynamodb.CreateTableInput{
				TableName: aws.String(tt.tableName),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
				},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
				},
			})
			require.NoError(t, err)

			var wg sync.WaitGroup

			for g := range tt.numGoroutines {
				wg.Add(1)
				go func(gid int) {
					defer wg.Done()
					for i := range tt.numIterations {
						input := &sdk_dynamodb.BatchWriteItemInput{
							RequestItems: map[string][]types.WriteRequest{
								tt.tableName: {
									{
										PutRequest: &types.PutRequest{
											Item: map[string]types.AttributeValue{
												"id": &types.AttributeValueMemberS{
													Value: fmt.Sprintf("item-%d-%d", gid, i),
												},
											},
										},
									},
								},
							},
						}
						_, writeErr := db.BatchWriteItem(ctx, input)
						assert.NoError(t, writeErr)
						time.Sleep(1 * time.Millisecond)
					}
				}(g)
			}

			for g := range tt.numGoroutines {
				wg.Add(1)
				go func(gid int) {
					defer wg.Done()
					for i := range tt.numIterations {
						input := &sdk_dynamodb.BatchGetItemInput{
							RequestItems: map[string]types.KeysAndAttributes{
								tt.tableName: {
									Keys: []map[string]types.AttributeValue{
										{
											"id": &types.AttributeValueMemberS{
												Value: fmt.Sprintf("item-%d-%d", gid, i/2),
											},
										},
									},
								},
							},
						}
						_, readErr := db.BatchGetItem(ctx, input)
						assert.NoError(t, readErr)
						time.Sleep(1 * time.Millisecond)
					}
				}(g)
			}

			wg.Wait()
		})
	}
}
