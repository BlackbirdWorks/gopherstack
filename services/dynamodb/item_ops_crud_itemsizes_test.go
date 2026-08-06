package dynamodb_test

// Regression tests for the itemSizes/Items invariant: doUpdate/doPut/
// deleteItemAtIndex all index table.itemSizes by the same position as
// table.Items (item_ops_crud.go). If a write path grows or shrinks Items
// without keeping itemSizes in step, a later UpdateItem/PutItem panics with
// "index out of range" at item_ops_crud.go:817.

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestUpdateItem_AfterBatchWriteInsert_NoPanic(t *testing.T) {
	t.Parallel()
	d := newBatchTestDB(t)
	createBatchTestTable(t, d)

	// Insert a brand-new item via BatchWriteItem. This grows table.Items; the
	// bug left table.itemSizes empty.
	if _, err := d.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			batchTestTableName: {
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"pk":  &types.AttributeValueMemberS{Value: "k1"},
							"val": &types.AttributeValueMemberN{Value: "1"},
						},
					},
				},
			},
		},
	}); err != nil {
		t.Fatalf("BatchWriteItem: %v", err)
	}

	// Before the fix this panicked at item_ops_crud.go:817 with
	// "index out of range [0] with length 0".
	_, err := d.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName:        aws.String(batchTestTableName),
		Key:              map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "k1"}},
		UpdateExpression: aws.String("SET val = :v"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":v": &types.AttributeValueMemberN{Value: "2"},
		},
	})
	if err != nil {
		t.Fatalf("UpdateItem after batch insert: %v", err)
	}
}

func TestPutItem_AfterBatchWriteInsert_NoPanic(t *testing.T) {
	t.Parallel()
	d := newBatchTestDB(t)
	createBatchTestTable(t, d)

	if _, err := d.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			batchTestTableName: {
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "k1"},
						},
					},
				},
			},
		},
	}); err != nil {
		t.Fatalf("BatchWriteItem: %v", err)
	}

	// Overwriting the same key exercises the matchIndex != -1 branch of doPut,
	// which indexes table.itemSizes[matchIndex].
	putBatchTestItem(t, d, map[string]types.AttributeValue{
		"pk":  &types.AttributeValueMemberS{Value: "k1"},
		"val": &types.AttributeValueMemberS{Value: "overwritten"},
	})
}

func TestUpdateItem_AfterBatchDelete_NoPanic(t *testing.T) {
	t.Parallel()
	d := newBatchTestDB(t)
	createBatchTestTable(t, d)

	// Seed two items, then batch-delete one. applyBatchDeletes swap-truncates
	// Items and must keep itemSizes aligned so the surviving item can be updated.
	putBatchTestItem(t, d, map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}})
	putBatchTestItem(t, d, map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "b"}})

	if _, err := d.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			batchTestTableName: {
				{
					DeleteRequest: &types.DeleteRequest{
						Key: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}},
					},
				},
			},
		},
	}); err != nil {
		t.Fatalf("BatchWriteItem delete: %v", err)
	}

	_, err := d.UpdateItem(context.Background(), &dynamodb.UpdateItemInput{
		TableName:        aws.String(batchTestTableName),
		Key:              map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "b"}},
		UpdateExpression: aws.String("SET n = :v"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":v": &types.AttributeValueMemberN{Value: "5"},
		},
	})
	if err != nil {
		t.Fatalf("UpdateItem after batch delete: %v", err)
	}
}
