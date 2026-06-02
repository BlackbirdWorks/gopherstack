package dynamodb_test

// batch_accuracy_b2_test.go — DynamoDB batch-2 ops AWS-accuracy audit (go-citg)
//
// Covers seven accuracy gaps in BatchGetItem and BatchWriteItem:
//   1. BatchGetItem: empty RequestItems → ValidationException
//   2. BatchGetItem: empty Keys for a table → ValidationException
//   3. BatchGetItem: AttributesToGet projection applied when ProjectionExpression absent
//   4. BatchGetItem: ProjectionExpression + AttributesToGet mutual exclusion
//   5. BatchGetItem: ConsistentRead doubles RCU in ConsumedCapacity
//   6. BatchWriteItem: null WriteRequest (neither Put nor Delete) → ValidationException
//   7. BatchWriteItem: PutRequest item exceeding 400 KB → ValidationException
//   8. BatchWriteItem: PutRequest missing primary key → ValidationException
//   9. BatchWriteItem: DeleteRequest missing primary key → ValidationException

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	ddb "github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// ─── helpers ────────────────────────────────────────────────────────────────

func b2NewDB(t *testing.T) *ddb.InMemoryDB {
	t.Helper()
	d := ddb.NewInMemoryDB()
	d.SetDefaultRegion("us-east-1")
	return d
}

func b2CreateTable(t *testing.T, d *ddb.InMemoryDB, name string) {
	t.Helper()
	_, err := d.CreateTable(context.Background(), &dynamodb.CreateTableInput{
		TableName: aws.String(name),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("create table %q: %v", name, err)
	}
}

func b2PutItem(t *testing.T, d *ddb.InMemoryDB, table string, item map[string]types.AttributeValue) {
	t.Helper()
	_, err := d.PutItem(context.Background(), &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item:      item,
	})
	if err != nil {
		t.Fatalf("put item: %v", err)
	}
}

func b2AssertValidationErr(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected ValidationException but got nil")
	}
	if !strings.Contains(err.Error(), "ValidationException") {
		t.Fatalf("expected ValidationException, got: %v", err)
	}
}

// ─── Section 30: BatchGetItem empty-request validation ──────────────────────

func TestBatchGetItem_EmptyRequestItems_Rejected(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input *dynamodb.BatchGetItemInput
	}{
		{
			name:  "nil RequestItems",
			input: &dynamodb.BatchGetItemInput{RequestItems: nil},
		},
		{
			name:  "empty RequestItems map",
			input: &dynamodb.BatchGetItemInput{RequestItems: map[string]types.KeysAndAttributes{}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d := b2NewDB(t)
			_, err := d.BatchGetItem(context.Background(), tt.input)
			b2AssertValidationErr(t, err)
		})
	}
}

func TestBatchGetItem_EmptyKeysForTable_Rejected(t *testing.T) {
	t.Parallel()
	d := b2NewDB(t)
	b2CreateTable(t, d, "tbl")

	_, err := d.BatchGetItem(context.Background(), &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			"tbl": {Keys: []map[string]types.AttributeValue{}},
		},
	})
	b2AssertValidationErr(t, err)
}

// ─── Section 31: BatchGetItem AttributesToGet projection ────────────────────

func TestBatchGetItem_AttributesToGet_AppliedWhenNoProjectionExpression(t *testing.T) {
	t.Parallel()
	d := b2NewDB(t)
	b2CreateTable(t, d, "tbl")
	b2PutItem(t, d, "tbl", map[string]types.AttributeValue{
		"pk":    &types.AttributeValueMemberS{Value: "k1"},
		"name":  &types.AttributeValueMemberS{Value: "alice"},
		"email": &types.AttributeValueMemberS{Value: "a@example.com"},
	})

	out, err := d.BatchGetItem(context.Background(), &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			"tbl": {
				Keys: []map[string]types.AttributeValue{
					{"pk": &types.AttributeValueMemberS{Value: "k1"}},
				},
				AttributesToGet: []string{"pk", "name"},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	items := out.Responses["tbl"]
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	item := items[0]
	if _, ok := item["email"]; ok {
		t.Error("email should have been projected out")
	}
	if _, ok := item["name"]; !ok {
		t.Error("name should be present in projected item")
	}
}

func TestBatchGetItem_ProjectionExpression_AndAttributesToGet_BothSet_Rejected(t *testing.T) {
	t.Parallel()
	d := b2NewDB(t)
	b2CreateTable(t, d, "tbl")

	projExpr := "pk"
	_, err := d.BatchGetItem(context.Background(), &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			"tbl": {
				Keys: []map[string]types.AttributeValue{
					{"pk": &types.AttributeValueMemberS{Value: "k1"}},
				},
				ProjectionExpression: &projExpr,
				AttributesToGet:      []string{"pk", "name"},
			},
		},
	})
	b2AssertValidationErr(t, err)
}

// ─── Section 32: BatchGetItem ConsistentRead billing ────────────────────────

func TestBatchGetItem_ConsistentRead_DoublesRCU(t *testing.T) {
	t.Parallel()
	d := b2NewDB(t)
	b2CreateTable(t, d, "tbl")
	b2PutItem(t, d, "tbl", map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "k1"},
	})

	tests := []struct {
		name           string
		consistentRead *bool
		wantMinRCU     float64
	}{
		{
			name:           "eventually consistent",
			consistentRead: aws.Bool(false),
			wantMinRCU:     0.5,
		},
		{
			name:           "strongly consistent",
			consistentRead: aws.Bool(true),
			wantMinRCU:     1.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			out, err := d.BatchGetItem(context.Background(), &dynamodb.BatchGetItemInput{
				ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
				RequestItems: map[string]types.KeysAndAttributes{
					"tbl": {
						Keys: []map[string]types.AttributeValue{
							{"pk": &types.AttributeValueMemberS{Value: "k1"}},
						},
						ConsistentRead: tt.consistentRead,
					},
				},
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(out.ConsumedCapacity) == 0 {
				t.Fatal("expected ConsumedCapacity in response")
			}
			gotRCU := aws.ToFloat64(out.ConsumedCapacity[0].CapacityUnits)
			if gotRCU < tt.wantMinRCU {
				t.Errorf("ConsistentRead=%v: want RCU >= %.1f, got %.2f", aws.ToBool(tt.consistentRead), tt.wantMinRCU, gotRCU)
			}
		})
	}
}

func TestBatchGetItem_ConsistentRead_RCU_GreaterThan_EventuallyConsistent(t *testing.T) {
	t.Parallel()
	d := b2NewDB(t)
	b2CreateTable(t, d, "tbl")
	for i := range 5 {
		b2PutItem(t, d, "tbl", map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: string(rune('a' + i))},
		})
	}

	keys := make([]map[string]types.AttributeValue, 5)
	for i := range 5 {
		keys[i] = map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: string(rune('a' + i))},
		}
	}

	eventual, err := d.BatchGetItem(context.Background(), &dynamodb.BatchGetItemInput{
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		RequestItems: map[string]types.KeysAndAttributes{
			"tbl": {Keys: keys, ConsistentRead: aws.Bool(false)},
		},
	})
	if err != nil {
		t.Fatalf("eventual read: %v", err)
	}

	consistent, err := d.BatchGetItem(context.Background(), &dynamodb.BatchGetItemInput{
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		RequestItems: map[string]types.KeysAndAttributes{
			"tbl": {Keys: keys, ConsistentRead: aws.Bool(true)},
		},
	})
	if err != nil {
		t.Fatalf("consistent read: %v", err)
	}

	eRCU := aws.ToFloat64(eventual.ConsumedCapacity[0].CapacityUnits)
	cRCU := aws.ToFloat64(consistent.ConsumedCapacity[0].CapacityUnits)
	if cRCU <= eRCU {
		t.Errorf("ConsistentRead should cost more: eventual=%.2f consistent=%.2f", eRCU, cRCU)
	}
}

// ─── Section 33: BatchWriteItem per-item validation ─────────────────────────

func TestBatchWriteItem_NullWriteRequest_Rejected(t *testing.T) {
	t.Parallel()
	d := b2NewDB(t)
	b2CreateTable(t, d, "tbl")

	_, err := d.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"tbl": {
				{PutRequest: nil, DeleteRequest: nil},
			},
		},
	})
	b2AssertValidationErr(t, err)
}

func TestBatchWriteItem_OversizedItem_Rejected(t *testing.T) {
	t.Parallel()
	d := b2NewDB(t)
	b2CreateTable(t, d, "tbl")

	// Build a value that pushes the item over 400 KB.
	bigVal := strings.Repeat("x", 400*1024+1)

	_, err := d.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"tbl": {
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"pk":  &types.AttributeValueMemberS{Value: "k1"},
							"big": &types.AttributeValueMemberS{Value: bigVal},
						},
					},
				},
			},
		},
	})
	b2AssertValidationErr(t, err)
}

func TestBatchWriteItem_PutRequest_MissingPK_Rejected(t *testing.T) {
	t.Parallel()
	d := b2NewDB(t)
	b2CreateTable(t, d, "tbl")

	_, err := d.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"tbl": {
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"name": &types.AttributeValueMemberS{Value: "alice"},
						},
					},
				},
			},
		},
	})
	b2AssertValidationErr(t, err)
}

func TestBatchWriteItem_DeleteRequest_MissingPK_Rejected(t *testing.T) {
	t.Parallel()
	d := b2NewDB(t)
	b2CreateTable(t, d, "tbl")

	_, err := d.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"tbl": {
				{
					DeleteRequest: &types.DeleteRequest{
						Key: map[string]types.AttributeValue{},
					},
				},
			},
		},
	})
	b2AssertValidationErr(t, err)
}

// Regression: valid batch write still succeeds after adding validation.
func TestBatchWriteItem_ValidRequests_NotAffectedByValidation(t *testing.T) {
	t.Parallel()
	d := b2NewDB(t)
	b2CreateTable(t, d, "tbl")

	_, err := d.BatchWriteItem(context.Background(), &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"tbl": {
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"pk":   &types.AttributeValueMemberS{Value: "k1"},
							"data": &types.AttributeValueMemberS{Value: "hello"},
						},
					},
				},
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "k2"},
						},
					},
				},
				{
					DeleteRequest: &types.DeleteRequest{
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "k1"},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("valid batch write should succeed: %v", err)
	}
}

// Regression: valid batch get with no projection still returns all attributes.
func TestBatchGetItem_NoProjection_ReturnsAllAttributes(t *testing.T) {
	t.Parallel()
	d := b2NewDB(t)
	b2CreateTable(t, d, "tbl")
	b2PutItem(t, d, "tbl", map[string]types.AttributeValue{
		"pk":    &types.AttributeValueMemberS{Value: "k1"},
		"name":  &types.AttributeValueMemberS{Value: "bob"},
		"email": &types.AttributeValueMemberS{Value: "b@example.com"},
	})

	out, err := d.BatchGetItem(context.Background(), &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			"tbl": {
				Keys: []map[string]types.AttributeValue{
					{"pk": &types.AttributeValueMemberS{Value: "k1"}},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	items := out.Responses["tbl"]
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if _, ok := items[0]["email"]; !ok {
		t.Error("email should be present when no projection specified")
	}
}
