package dynamodb_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_dynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"golang.org/x/sync/errgroup"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func TestTransactWriteItemsConvoyDeadlock(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()

	tableName := "transact-deadlock-table"
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	_, err := db.CreateTable(ctx, &sdk_dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
		},
	})
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	iterations := 1000

	var wg errgroup.Group

	// Thread 1: TransactWriteItems
	wg.Go(func() error {
		for i := range iterations {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			_, _ = db.TransactWriteItems(ctx, &sdk_dynamodb.TransactWriteItemsInput{
				TransactItems: []types.TransactWriteItem{
					{
						Put: &types.Put{
							TableName: aws.String(tableName),
							Item: map[string]types.AttributeValue{
								"id": &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%d", i)},
							},
						},
					},
				},
			})
		}

		return nil
	})

	// Thread 2: DeleteTable / CreateTable (needs db.mu.Lock)
	wg.Go(func() error {
		for i := range iterations / 10 {
			if ctx.Err() != nil {
				return ctx.Err()
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

		return nil
	})

	err = wg.Wait()
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected error: %v", err)
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatal("test deadlocked! TransactWriteItems and DeleteTable/CreateTable lock inversion detected.")
	}
}
