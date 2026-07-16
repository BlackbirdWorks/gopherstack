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

func TestPutItemConvoyDeadlock(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	tableName := "putitem-deadlock-table"
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

	// Thread 1: PutItem
	wg.Go(func() error {
		for i := range iterations {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			_, _ = db.PutItem(ctx, &sdk_dynamodb.PutItemInput{
				TableName: aws.String(tableName),
				Item: map[string]types.AttributeValue{
					"id": &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%d", i)},
				},
			})
			t.Logf("PutItem for %s completed", tableName)
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
			_, createErr := db.CreateTable(ctx, &sdk_dynamodb.CreateTableInput{
				TableName: aws.String(tempName),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
				},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
				},
			})
			if createErr != nil {
				t.Logf("CreateTable %s error: %v", tableName, createErr)
			}
			_, _ = db.DeleteTable(ctx, &sdk_dynamodb.DeleteTableInput{
				TableName: aws.String(tempName),
			})
		}

		return nil
	})

	err = wg.Wait()
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Unexpected error: %v", err)
	}

	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("Deadlock detected: context deadline exceeded before all operations finished")
	}
}
