package dynamodb_test

// batch_accuracy_b3_test.go — DynamoDB batch-2 ops AWS-accuracy audit (go-zrek)
//
// Covers two accuracy gaps in BatchGetItem and BatchWriteItem:
//   1. BatchGetItem: Responses always includes all requested tables even when
//      all keys miss (zero-hit table was previously omitted).
//   2. BatchWriteItem: ConsumedCapacity WCU is proportional to item size
//      (ceil(size/1KB)); previously charged a flat 1 WCU per request.

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ─── Section 34: BatchGetItem always returns all requested tables ─────────────

func TestBatchGetItem_ZeroHitTable_IncludedInResponses(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		keys []map[string]types.AttributeValue
	}{
		{
			name: "single nonexistent key",
			keys: []map[string]types.AttributeValue{
				{"pk": &types.AttributeValueMemberS{Value: "nonexistent"}},
			},
		},
		{
			name: "multiple nonexistent keys",
			keys: []map[string]types.AttributeValue{
				{"pk": &types.AttributeValueMemberS{Value: "missing1"}},
				{"pk": &types.AttributeValueMemberS{Value: "missing2"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d := b2NewDB(t)
			b2CreateTable(t, d)

			out, err := d.BatchGetItem(context.Background(), &dynamodb.BatchGetItemInput{
				RequestItems: map[string]types.KeysAndAttributes{
					b2TableName: {Keys: tt.keys},
				},
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			tableItems, ok := out.Responses[b2TableName]
			if !ok {
				t.Fatalf("table %q missing from Responses; AWS always includes all requested tables", b2TableName)
			}
			if len(tableItems) != 0 {
				t.Fatalf("expected empty list for all-miss batch, got %d items", len(tableItems))
			}
		})
	}
}

func TestBatchGetItem_MixedHitMiss_TableAlwaysInResponses(t *testing.T) {
	t.Parallel()
	d := b2NewDB(t)
	b2CreateTable(t, d)
	b2PutItem(t, d, map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "exists"},
	})

	out, err := d.BatchGetItem(context.Background(), &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			b2TableName: {
				Keys: []map[string]types.AttributeValue{
					{"pk": &types.AttributeValueMemberS{Value: "exists"}},
					{"pk": &types.AttributeValueMemberS{Value: "missing"}},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tableItems, ok := out.Responses[b2TableName]
	if !ok {
		t.Fatalf("table %q missing from Responses", b2TableName)
	}
	if len(tableItems) != 1 {
		t.Fatalf("expected 1 found item, got %d", len(tableItems))
	}
}

// ─── Section 35: BatchWriteItem WCU proportional to item size ────────────────

func TestBatchWriteItem_SmallItem_ChargesOneWCU(t *testing.T) {
	t.Parallel()
	d := b2NewDB(t)
	b2CreateTable(t, d)

	// Item well under 1 KB → 1 WCU.
	out, err := d.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		RequestItems: map[string][]types.WriteRequest{
			b2TableName: {
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"pk":   &types.AttributeValueMemberS{Value: "k1"},
							"data": &types.AttributeValueMemberS{Value: "small"},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out.ConsumedCapacity) == 0 {
		t.Fatal("expected ConsumedCapacity in response")
	}
	gotWCU := aws.ToFloat64(out.ConsumedCapacity[0].CapacityUnits)
	if gotWCU < 1.0 {
		t.Errorf("small item: want WCU >= 1.0, got %.2f", gotWCU)
	}
}

func TestBatchWriteItem_LargeItem_ChargesMoreThanOneWCU(t *testing.T) {
	t.Parallel()
	d := b2NewDB(t)
	b2CreateTable(t, d)

	// Item ~2 KB → at least 2 WCU. Use a 2048-byte value to ensure we exceed 1 KB.
	bigVal := strings.Repeat("x", 2048)

	out, err := d.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		RequestItems: map[string][]types.WriteRequest{
			b2TableName: {
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"pk":   &types.AttributeValueMemberS{Value: "big"},
							"data": &types.AttributeValueMemberS{Value: bigVal},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out.ConsumedCapacity) == 0 {
		t.Fatal("expected ConsumedCapacity in response")
	}
	gotWCU := aws.ToFloat64(out.ConsumedCapacity[0].CapacityUnits)
	if gotWCU <= 1.0 {
		t.Errorf("2KB item: want WCU > 1.0, got %.2f", gotWCU)
	}
}

func TestBatchWriteItem_MultipleItems_WCUSumsCorrectly(t *testing.T) {
	t.Parallel()
	d := b2NewDB(t)
	b2CreateTable(t, d)

	// One small item (1 WCU) + one ~2KB item (≥2 WCU) → total ≥3 WCU.
	bigVal := strings.Repeat("y", 2048)

	out, err := d.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		RequestItems: map[string][]types.WriteRequest{
			b2TableName: {
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "small"},
						},
					},
				},
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"pk":   &types.AttributeValueMemberS{Value: "big"},
							"data": &types.AttributeValueMemberS{Value: bigVal},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out.ConsumedCapacity) == 0 {
		t.Fatal("expected ConsumedCapacity in response")
	}
	gotWCU := aws.ToFloat64(out.ConsumedCapacity[0].CapacityUnits)
	if gotWCU < 3.0 {
		t.Errorf("small+2KB items: want WCU >= 3.0, got %.2f", gotWCU)
	}
}
