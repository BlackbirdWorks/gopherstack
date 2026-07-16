package dynamodb_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_dynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"
)

// TestDescribeTableConvoyDeadlock verifies that DescribeTable does not cause an RWMutex
// convoy/deadlock by holding the global db.mu.RLock while waiting for table.mu.RLock.
// If the bug exists, this test will quickly hang or time out.
func TestDescribeTableConvoyDeadlock(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	const tableName = "ConvoyTestTable"

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

	var wg sync.WaitGroup
	const iterations = 500

	// Writer: repeatedly takes table.mu.Lock() via PutItem
	wg.Go(func() {
		for i := range iterations {
			if ctx.Err() != nil {
				return
			}
			_, _ = db.PutItem(ctx, &sdk_dynamodb.PutItemInput{
				TableName: aws.String(tableName),
				Item: map[string]types.AttributeValue{
					"id": &types.AttributeValueMemberS{Value: fmt.Sprintf("val-%d", i)},
				},
			})
		}
	})

	// Convoys: repeatedly calls DescribeTable which takes db.mu.RLock() then table.mu.RLock()
	wg.Go(func() {
		for range iterations {
			if ctx.Err() != nil {
				return
			}
			_, _ = db.DescribeTable(ctx, &sdk_dynamodb.DescribeTableInput{
				TableName: aws.String(tableName),
			})
		}
	})

	// db.mu Writers: repeatedly calls CreateTable and DeleteTable which requires db.mu.Lock()
	wg.Go(func() {
		for i := range iterations / 10 {
			if ctx.Err() != nil {
				return
			}
			tempName := fmt.Sprintf("TempTable-%d", i)
			_, _ = db.CreateTable(ctx, &sdk_dynamodb.CreateTableInput{
				TableName: aws.String(tempName),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
				},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
				},
			})
			_, _ = db.DeleteTable(ctx, &sdk_dynamodb.DeleteTableInput{
				TableName: aws.String(tempName),
			})
		}
	})

	// GetItem calls: requires db.mu.RLock() (to get the table) and then table.mu.RLock().
	// This simulates the client timeouts we saw when GetItem was blocked by the convoy.
	wg.Go(func() {
		for range iterations {
			if ctx.Err() != nil {
				return
			}
			_, _ = db.GetItem(ctx, &sdk_dynamodb.GetItemInput{
				TableName: aws.String(tableName),
				Key: map[string]types.AttributeValue{
					"id": &types.AttributeValueMemberS{Value: "val-0"},
				},
			})
		}
	})

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success! No deadlocks.
	case <-ctx.Done():
		t.Fatal("Test timed out - potential deadlock detected")
	}
}
