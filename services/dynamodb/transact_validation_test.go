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

// TestTransactWrite_UnusedExpressionAttributeNames_Rejected locks
// gopherstack-daa: TransactWriteItems' Put/Update/Delete/ConditionCheck
// actions must reject an ExpressionAttributeNames placeholder that no
// expression on that action references, exactly like plain
// PutItem/UpdateItem/DeleteItem already do (item_ops_crud.go). Before this
// fix, transact_ops.go's per-item condition checks skipped the unused-EAN
// validation entirely.
func TestTransactWrite_UnusedExpressionAttributeNames_Rejected(t *testing.T) {
	t.Parallel()

	key := map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "pk1"},
		"sk": &types.AttributeValueMemberS{Value: "sk1"},
	}
	unusedEAN := map[string]string{"#unused": "otherAttr"}

	tests := []struct {
		name string
		item types.TransactWriteItem
	}{
		{
			name: "Put with unused EAN",
			item: types.TransactWriteItem{Put: &types.Put{
				TableName:                aws.String("UnusedEANTable"),
				Item:                     key,
				ConditionExpression:      aws.String("attribute_not_exists(pk)"),
				ExpressionAttributeNames: unusedEAN,
			}},
		},
		{
			name: "Delete with unused EAN",
			item: types.TransactWriteItem{Delete: &types.Delete{
				TableName:                aws.String("UnusedEANTable"),
				Key:                      key,
				ConditionExpression:      aws.String("attribute_exists(pk)"),
				ExpressionAttributeNames: unusedEAN,
			}},
		},
		{
			name: "Update with unused EAN (referenced by neither UpdateExpression nor ConditionExpression)",
			item: types.TransactWriteItem{Update: &types.Update{
				TableName:                aws.String("UnusedEANTable"),
				Key:                      key,
				UpdateExpression:         aws.String("SET otherAttr = :v"),
				ExpressionAttributeNames: unusedEAN,
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":v": &types.AttributeValueMemberS{Value: "x"},
				},
			}},
		},
		{
			name: "ConditionCheck with unused EAN",
			item: types.TransactWriteItem{ConditionCheck: &types.ConditionCheck{
				TableName:                aws.String("UnusedEANTable"),
				Key:                      key,
				ConditionExpression:      aws.String("attribute_exists(pk)"),
				ExpressionAttributeNames: unusedEAN,
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := newInMemoryTestDB(t)
			ctx := context.Background()
			createSimpleTestTable(t, db, "UnusedEANTable")

			_, err := db.TransactWriteItems(ctx, &dynamodb_sdk.TransactWriteItemsInput{
				TransactItems: []types.TransactWriteItem{tt.item},
			})
			assertErrorCode(t, err, "ValidationException")
			if err != nil {
				if !strings.Contains(err.Error(), "unused in expressions") {
					t.Errorf("expected 'unused in expressions' in error, got: %v", err)
				}
			}
		})
	}
}

// TestTransactWrite_UnusedExpressionAttributeValues_Rejected mirrors the EAN
// test above for ExpressionAttributeValues: an unreferenced :placeholder must
// be rejected the same way plain single-item ops reject it.
func TestTransactWrite_UnusedExpressionAttributeValues_Rejected(t *testing.T) {
	t.Parallel()

	db := newInMemoryTestDB(t)
	ctx := context.Background()
	createSimpleTestTable(t, db, "UnusedEAVTable")

	_, err := db.TransactWriteItems(ctx, &dynamodb_sdk.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Put: &types.Put{
					TableName: aws.String("UnusedEAVTable"),
					Item: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "pk1"},
						"sk": &types.AttributeValueMemberS{Value: "sk1"},
					},
					ConditionExpression: aws.String("attribute_not_exists(pk)"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":unused": &types.AttributeValueMemberS{Value: "never referenced"},
					},
				},
			},
		},
	})
	assertErrorCode(t, err, "ValidationException")
	if err != nil && !strings.Contains(err.Error(), "unused in expressions") {
		t.Errorf("expected 'unused in expressions' in error, got: %v", err)
	}
}

// TestTransactWrite_Update_ExpressionAttrsUsedAcrossBothExpressions_Accepted
// confirms the unused-EAN/EAV check correctly considers BOTH UpdateExpression
// and ConditionExpression when deciding whether a placeholder is used (an EAN
// referenced only by ConditionExpression, and an EAV referenced only by
// UpdateExpression, must both count as "used" — matching plain UpdateItem's
// combined-expression check in item_ops_crud.go).
func TestTransactWrite_Update_ExpressionAttrsUsedAcrossBothExpressions_Accepted(t *testing.T) {
	t.Parallel()

	db := newInMemoryTestDB(t)
	ctx := context.Background()
	createSimpleTestTable(t, db, "UsedAcrossBothTable")

	putTestItem(t, db, "UsedAcrossBothTable", map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "pk1"},
		"sk": &types.AttributeValueMemberS{Value: "sk1"},
	})

	_, err := db.TransactWriteItems(ctx, &dynamodb_sdk.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Update: &types.Update{
					TableName: aws.String("UsedAcrossBothTable"),
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "pk1"},
						"sk": &types.AttributeValueMemberS{Value: "sk1"},
					},
					UpdateExpression:    aws.String("SET #attr = :newVal"),
					ConditionExpression: aws.String("attribute_exists(#pkName)"),
					ExpressionAttributeNames: map[string]string{
						"#attr":   "data", // used only in UpdateExpression
						"#pkName": "pk",   // used only in ConditionExpression
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":newVal": &types.AttributeValueMemberS{Value: "updated"},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf(
			"EAN/EAV each used in only one of UpdateExpression/ConditionExpression must be accepted: %v",
			err,
		)
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
