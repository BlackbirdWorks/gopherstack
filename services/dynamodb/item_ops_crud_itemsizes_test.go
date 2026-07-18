package dynamodb_test

// item_ops_crud_itemsizes_test.go — regression tests for the itemSizes/Items
// invariant.
//
// doUpdate/doPut/deleteItemAtIndex all index table.itemSizes by the same
// position as table.Items (see item_ops_crud.go). If a write path grows or
// shrinks Items without keeping itemSizes in step, a later UpdateItem/PutItem
// panics with "index out of range" at item_ops_crud.go:817. The batch-write
// path used to append to Items without touching itemSizes, so a BatchWriteItem
// into an empty table followed by an UpdateItem panicked with
// "index out of range [0] with length 0".

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func TestUpdateItem_AfterBatchWriteInsert_NoPanic(t *testing.T) {
	t.Parallel()
	d := b2NewDB(t)
	b2CreateTable(t, d)

	// Insert a brand-new item via BatchWriteItem. This grows table.Items; the
	// bug left table.itemSizes empty.
	if _, err := d.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			b2TableName: {
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
		TableName:        aws.String(b2TableName),
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
	d := b2NewDB(t)
	b2CreateTable(t, d)

	if _, err := d.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			b2TableName: {
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
	b2PutItem(t, d, map[string]types.AttributeValue{
		"pk":  &types.AttributeValueMemberS{Value: "k1"},
		"val": &types.AttributeValueMemberS{Value: "overwritten"},
	})
}

func TestUpdateItem_AfterBatchDelete_NoPanic(t *testing.T) {
	t.Parallel()
	d := b2NewDB(t)
	b2CreateTable(t, d)

	// Seed two items, then batch-delete one. applyBatchDeletes swap-truncates
	// Items and must keep itemSizes aligned so the surviving item can be updated.
	b2PutItem(t, d, map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "a"}})
	b2PutItem(t, d, map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "b"}})

	if _, err := d.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			b2TableName: {
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
		TableName:        aws.String(b2TableName),
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
