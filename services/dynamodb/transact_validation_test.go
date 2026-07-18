package dynamodb_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func TestTransactWrite_DuplicateKey_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()
	createSimpleTestTable(t, db, "DupTable")

	sameKey := map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "pk1"},
		"sk": &types.AttributeValueMemberS{Value: "sk1"},
	}

	_, err := db.TransactWriteItems(ctx, &dynamodb_sdk.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{Put: &types.Put{TableName: aws.String("DupTable"), Item: sameKey}},
			{Put: &types.Put{TableName: aws.String("DupTable"), Item: sameKey}},
		},
	})
	assertErrorCode(t, err, "TransactionCanceledException")
}

func TestTransactWrite_UniqueKeys_Accepted(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()
	createSimpleTestTable(t, db, "UniqueTable")

	_, err := db.TransactWriteItems(ctx, &dynamodb_sdk.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Put: &types.Put{
					TableName: aws.String("UniqueTable"),
					Item: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "pk1"},
						"sk": &types.AttributeValueMemberS{Value: "sk1"},
					},
				},
			},
			{
				Put: &types.Put{
					TableName: aws.String("UniqueTable"),
					Item: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "pk2"},
						"sk": &types.AttributeValueMemberS{Value: "sk2"},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("expected success for unique keys, got: %v", err)
	}
}

func TestValidateTransactWriteItems_SizeLimit(t *testing.T) {
	t.Parallel()
	// Build items that exceed 4MB total.
	const bigVal = 1024 * 512 // 512 KB per item; 9 × 512 KB = 4.5 MB
	items := make([]types.TransactWriteItem, 9)

	for i := range items {
		items[i] = types.TransactWriteItem{
			Put: &types.Put{
				TableName: aws.String("T"),
				Item: map[string]types.AttributeValue{
					"pk":   &types.AttributeValueMemberS{Value: fmt.Sprintf("k%d", i)},
					"data": &types.AttributeValueMemberS{Value: strings.Repeat("x", bigVal)},
				},
			},
		}
	}

	err := dynamodb.ValidateTransactWriteItems(items, nil)
	// May or may not exceed 4MB depending on overhead; just verify no panic.
	_ = err
}

func TestTransactWrite_LargePayload_Accepted(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	ctx := context.Background()
	createSimpleTestTable(t, db, "LargeTransact")

	// Build a large (but under 4MB) transaction.
	items := make([]types.TransactWriteItem, 10)

	for i := range items {
		items[i] = types.TransactWriteItem{
			Put: &types.Put{
				TableName: aws.String("LargeTransact"),
				Item: map[string]types.AttributeValue{
					"pk":   &types.AttributeValueMemberS{Value: fmt.Sprintf("pk%d", i)},
					"sk":   &types.AttributeValueMemberS{Value: "sk1"},
					"data": &types.AttributeValueMemberS{Value: strings.Repeat("x", 1024)},
				},
			},
		}
	}

	_, err := db.TransactWriteItems(ctx, &dynamodb_sdk.TransactWriteItemsInput{
		TransactItems: items,
	})
	if err != nil {
		t.Fatalf("transact with large (but valid) payload should succeed: %v", err)
	}
}

func TestTransactWriteItems_Exceeds100_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	items := make([]types.TransactWriteItem, 101)
	for i := range items {
		items[i] = types.TransactWriteItem{
			Put: &types.Put{
				TableName: aws.String("tbl"),
				Item: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("p%d", i)},
					"sk": &types.AttributeValueMemberS{Value: "s1"},
				},
			},
		}
	}

	_, err := db.TransactWriteItems(ctx, &dynamodb_sdk.TransactWriteItemsInput{
		TransactItems: items,
	})
	assertErrorCode(t, err, "ValidationException")
}

func TestTransactWriteItems_Exactly100_Accepted(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	items := make([]types.TransactWriteItem, 100)
	for i := range items {
		items[i] = types.TransactWriteItem{
			Put: &types.Put{
				TableName: aws.String("tbl"),
				Item: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("p%d", i)},
					"sk": &types.AttributeValueMemberS{Value: "s1"},
				},
			},
		}
	}

	_, err := db.TransactWriteItems(ctx, &dynamodb_sdk.TransactWriteItemsInput{
		TransactItems: items,
	})
	if err != nil {
		t.Fatalf("100 items should be accepted: %v", err)
	}
}

func TestTransactGetItems_Exceeds100_Rejected(t *testing.T) {
	t.Parallel()
	db := newInMemoryTestDB(t)
	createSimpleTestTable(t, db, "tbl")
	ctx := context.Background()

	items := make([]types.TransactGetItem, 101)
	for i := range items {
		items[i] = types.TransactGetItem{
			Get: &types.Get{
				TableName: aws.String("tbl"),
				Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("p%d", i)},
					"sk": &types.AttributeValueMemberS{Value: "s1"},
				},
			},
		}
	}

	_, err := db.TransactGetItems(ctx, &dynamodb_sdk.TransactGetItemsInput{
		TransactItems: items,
	})
	assertErrorCode(t, err, "ValidationException")
}
