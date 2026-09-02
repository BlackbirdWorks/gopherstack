package dynamodb_test

import (
	"context"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchWriteItem(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, db *dynamodb.InMemoryDB)
		verify     func(t *testing.T, db *dynamodb.InMemoryDB)
		input      models.BatchWriteItemInput
		name       string
		errContain string
		wantErr    bool
	}{
		{
			name: "BasicPutAndDelete",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, db, "Table1", "pk")
				createTableHelper(t, db, "Table2", "pk")
			},
			input: models.BatchWriteItemInput{
				RequestItems: map[string][]models.WriteRequest{
					"Table1": {
						{
							PutRequest: &models.PutRequest{
								Item: map[string]any{
									"pk":  map[string]any{"S": "item1"},
									"val": map[string]any{"S": "v1"},
								},
							},
						},
					},
					"Table2": {
						{
							PutRequest: &models.PutRequest{
								Item: map[string]any{"pk": map[string]any{"S": "item3"}},
							},
						},
					},
				},
			},
			verify: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				verifyItem(t, db, "Table1", "item1", true)
				verifyItem(t, db, "Table2", "item3", true)
			},
		},
		{
			name: "DeleteItems",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, db, "Table1", "pk")
				_, _ = db.PutItem(t.Context(), &sdk.PutItemInput{
					TableName: aws.String("Table1"),
					Item: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "item1"},
					},
				})
			},
			input: models.BatchWriteItemInput{
				RequestItems: map[string][]models.WriteRequest{
					"Table1": {
						{
							DeleteRequest: &models.DeleteRequest{
								Key: map[string]any{"pk": map[string]any{"S": "item1"}},
							},
						},
					},
				},
			},
			verify: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				verifyItem(t, db, "Table1", "item1", false)
			},
		},
		{
			name: "MultipleDeletes",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, db, "Table1", "pk")
				// Seed 3 items
				for i := 1; i <= 3; i++ {
					_, _ = db.PutItem(t.Context(), &sdk.PutItemInput{
						TableName: aws.String("Table1"),
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "item" + string(rune('0'+i))},
						},
					})
				}
			},
			input: models.BatchWriteItemInput{
				RequestItems: map[string][]models.WriteRequest{
					"Table1": {
						{
							DeleteRequest: &models.DeleteRequest{
								Key: map[string]any{"pk": map[string]any{"S": "item1"}},
							},
						},
						{
							DeleteRequest: &models.DeleteRequest{
								Key: map[string]any{"pk": map[string]any{"S": "item2"}},
							},
						},
						{
							DeleteRequest: &models.DeleteRequest{
								Key: map[string]any{"pk": map[string]any{"S": "item3"}},
							},
						},
					},
				},
			},
			verify: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				verifyItem(t, db, "Table1", "item1", false)
				verifyItem(t, db, "Table1", "item2", false)
				verifyItem(t, db, "Table1", "item3", false)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()
			if tt.setup != nil {
				tt.setup(t, db)
			}

			sdkInput, _ := models.ToSDKBatchWriteItemInput(&tt.input)
			_, err := db.BatchWriteItem(t.Context(), sdkInput)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errContain != "" {
					assert.Contains(t, err.Error(), tt.errContain)
				}

				return
			}

			require.NoError(t, err)
			if tt.verify != nil {
				tt.verify(t, db)
			}
		})
	}
}

func TestBatchGetItem(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, db *dynamodb.InMemoryDB)
		input      models.BatchGetItemInput
		want       map[string][]map[string]any
		name       string
		errMessage string
		wantErr    bool
	}{
		{
			name: "MalformedProjectionExpression",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, db, "Table1", "pk")
				_, _ = db.PutItem(t.Context(), &sdk.PutItemInput{
					TableName: aws.String("Table1"),
					Item: map[string]types.AttributeValue{
						"pk":  &types.AttributeValueMemberS{Value: "item1"},
						"val": &types.AttributeValueMemberS{Value: "v1"},
					},
				})
			},
			input: models.BatchGetItemInput{
				RequestItems: map[string]models.KeysAndAttributes{
					"Table1": {
						Keys: []map[string]any{
							{"pk": map[string]any{"S": "item1"}},
						},
						ProjectionExpression: "val[",
					},
				},
			},
			wantErr:    true,
			errMessage: "ValidationException",
		},
		{
			name: "MultiItemGet",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, db, "Table1", "pk")
				_, _ = db.PutItem(t.Context(), &sdk.PutItemInput{
					TableName: aws.String("Table1"),
					Item: map[string]types.AttributeValue{
						"pk":  &types.AttributeValueMemberS{Value: "item1"},
						"val": &types.AttributeValueMemberS{Value: "v1"},
					},
				})
				_, _ = db.PutItem(t.Context(), &sdk.PutItemInput{
					TableName: aws.String("Table1"),
					Item: map[string]types.AttributeValue{
						"pk":  &types.AttributeValueMemberS{Value: "item2"},
						"val": &types.AttributeValueMemberS{Value: "v2"},
					},
				})
			},
			input: models.BatchGetItemInput{
				RequestItems: map[string]models.KeysAndAttributes{
					"Table1": {
						Keys: []map[string]any{
							{"pk": map[string]any{"S": "item1"}},
							{"pk": map[string]any{"S": "item2"}},
						},
					},
				},
			},
			want: map[string][]map[string]any{
				"Table1": {
					{"pk": map[string]any{"S": "item1"}, "val": map[string]any{"S": "v1"}},
					{"pk": map[string]any{"S": "item2"}, "val": map[string]any{"S": "v2"}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()
			if tt.setup != nil {
				tt.setup(t, db)
			}

			sdkInput, _ := models.ToSDKBatchGetItemInput(&tt.input)
			res, err := db.BatchGetItem(t.Context(), sdkInput)

			if tt.wantErr {
				require.Error(t, err)
				if tt.errMessage != "" {
					assert.Contains(t, err.Error(), tt.errMessage)
				}

				return
			}

			require.NoError(t, err)
			require.NotNil(t, res)

			got := make(map[string][]map[string]any)
			for table, items := range res.Responses {
				for _, item := range items {
					got[table] = append(got[table], models.FromSDKItem(item))
				}
			}

			// Sort slices for comparison if necessary, or use cmpopts.SortSlices
			assert.Empty(
				t,
				cmp.Diff(tt.want, got, cmpopts.SortSlices(func(a, b map[string]any) bool {
					return a["pk"].(map[string]any)["S"].(string) < b["pk"].(map[string]any)["S"].(string)
				})),
				"BatchGetItem responses mismatch",
			)
		})
	}
}

// TestBatchGetItem_ReturnConsumedCapacity_SurvivesWireConversion verifies that
// ToSDKBatchGetItemInput actually copies ReturnConsumedCapacity from the wire-format
// models.BatchGetItemInput onto the SDK input struct. models.BatchGetItemInput
// previously had no ReturnConsumedCapacity field at all, so a real client's
// "ReturnConsumedCapacity": "TOTAL" was silently dropped when parsed off the wire --
// the backend always saw ReturnConsumedCapacity == "" regardless of what was
// requested, exactly like an unrecognised awsjson1.0 key.
func TestBatchGetItem_ReturnConsumedCapacity_SurvivesWireConversion(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	createTableHelper(t, db, "Table1", "pk")
	_, err := db.PutItem(t.Context(), &sdk.PutItemInput{
		TableName: aws.String("Table1"),
		Item: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "item1"},
		},
	})
	require.NoError(t, err)

	input := models.BatchGetItemInput{
		ReturnConsumedCapacity: "TOTAL",
		RequestItems: map[string]models.KeysAndAttributes{
			"Table1": {Keys: []map[string]any{{"pk": map[string]any{"S": "item1"}}}},
		},
	}

	sdkInput, convErr := models.ToSDKBatchGetItemInput(&input)
	require.NoError(t, convErr)
	require.Equal(t, types.ReturnConsumedCapacityTotal, sdkInput.ReturnConsumedCapacity)

	res, getErr := db.BatchGetItem(t.Context(), sdkInput)
	require.NoError(t, getErr)
	require.NotEmpty(t, res.ConsumedCapacity, "ConsumedCapacity must be populated when requested")
}

func TestBatchWriteItem_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, db *dynamodb.InMemoryDB)
		name       string
		input      models.BatchWriteItemInput
		errContain string
	}{
		{
			name: "TableNotFound",
			input: models.BatchWriteItemInput{
				RequestItems: map[string][]models.WriteRequest{
					"MissingTable": {
						{
							PutRequest: &models.PutRequest{
								Item: map[string]any{"pk": map[string]any{"S": "item1"}},
							},
						},
					},
				},
			},
			errContain: "not found",
		},
		{
			name: "EmptyRequest",
			input: models.BatchWriteItemInput{
				RequestItems: map[string][]models.WriteRequest{},
			},
			errContain: "cannot be empty",
		},
		{
			name: "LimitExceeded",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, db, "T", "pk")
			},
			input: func() models.BatchWriteItemInput {
				reqs := make([]models.WriteRequest, 26)
				for i := range 26 {
					reqs[i] = models.WriteRequest{
						PutRequest: &models.PutRequest{
							Item: map[string]any{"pk": map[string]any{"S": "i"}},
						},
					}
				}

				return models.BatchWriteItemInput{
					RequestItems: map[string][]models.WriteRequest{"T": reqs},
				}
			}(),
			errContain: "limit exceeded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()
			if tt.setup != nil {
				tt.setup(t, db)
			}

			sdkInput, _ := models.ToSDKBatchWriteItemInput(&tt.input)
			_, err := db.BatchWriteItem(t.Context(), sdkInput)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errContain)
		})
	}
}

func verifyItem(t *testing.T, db *dynamodb.InMemoryDB, tableName, pk string, shouldExist bool) {
	t.Helper()
	input := models.GetItemInput{
		TableName: tableName,
		Key:       map[string]any{"pk": map[string]any{"S": pk}},
	}
	sdkInput, _ := models.ToSDKGetItemInput(&input)

	res, err := db.GetItem(t.Context(), sdkInput)
	require.NoError(t, err)

	if shouldExist {
		assert.NotEmpty(t, res.Item, "Item %s should exist in %s", pk, tableName)
	} else {
		assert.Empty(t, res.Item, "Item %s should NOT exist in %s", pk, tableName)
	}
}

// TestBatchWriteItem_OversizedItem_ReturnedAsValidationError verifies that items
// exceeding the 400 KB per-item limit are rejected with ValidationException.
// Note: The 16 MB total-batch limit cannot be reached with valid items because
// 25 items × 400 KB = 10 MB < 16 MB, so per-item validation fires first.
func TestBatchWriteItem_OversizedItem_ReturnedAsValidationError(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	createTableHelper(t, db, "BigTable", "pk")

	// A single item exceeding 400 KB is rejected immediately.
	const valueSizeBytes = 400*1024 + 1
	bigValue := strings.Repeat("x", valueSizeBytes)

	sdkInput, err := models.ToSDKBatchWriteItemInput(&models.BatchWriteItemInput{
		RequestItems: map[string][]models.WriteRequest{
			"BigTable": {
				{
					PutRequest: &models.PutRequest{
						Item: map[string]any{
							"pk":  map[string]any{"S": "item0"},
							"big": map[string]any{"S": bigValue},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, sdkInput)

	_, err = db.BatchWriteItem(t.Context(), sdkInput)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ValidationException")
}

// Covers BatchGetItem/BatchWriteItem accuracy gaps: empty RequestItems/Keys,
// AttributesToGet projection, ProjectionExpression+AttributesToGet mutual
// exclusion, ConsistentRead doubling RCU, null WriteRequest, oversized
// PutRequest item, and missing primary key on Put/DeleteRequest -- all
// ValidationException except the RCU/projection cases.

func newBatchTestDB(t *testing.T) *dynamodb.InMemoryDB {
	t.Helper()
	d := dynamodb.NewInMemoryDB()
	d.SetDefaultRegion("us-east-1")

	return d
}

const batchTestTableName = "tbl"

func createBatchTestTable(t *testing.T, d *dynamodb.InMemoryDB) {
	t.Helper()
	_, err := d.CreateTable(context.Background(), &sdk.CreateTableInput{
		TableName: aws.String(batchTestTableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	if err != nil {
		t.Fatalf("create table %q: %v", batchTestTableName, err)
	}
}

func putBatchTestItem(t *testing.T, d *dynamodb.InMemoryDB, item map[string]types.AttributeValue) {
	t.Helper()
	_, err := d.PutItem(context.Background(), &sdk.PutItemInput{
		TableName: aws.String(batchTestTableName),
		Item:      item,
	})
	if err != nil {
		t.Fatalf("put item: %v", err)
	}
}

func assertBatchValidationErr(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected ValidationException but got nil")
	}
	if !strings.Contains(err.Error(), "ValidationException") {
		t.Fatalf("expected ValidationException, got: %v", err)
	}
}

func TestBatchGetItem_EmptyRequestItems_Rejected(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input *sdk.BatchGetItemInput
		name  string
	}{
		{
			name:  "nil RequestItems",
			input: &sdk.BatchGetItemInput{RequestItems: nil},
		},
		{
			name:  "empty RequestItems map",
			input: &sdk.BatchGetItemInput{RequestItems: map[string]types.KeysAndAttributes{}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d := newBatchTestDB(t)
			_, err := d.BatchGetItem(context.Background(), tt.input)
			assertBatchValidationErr(t, err)
		})
	}
}

func TestBatchGetItem_EmptyKeysForTable_Rejected(t *testing.T) {
	t.Parallel()
	d := newBatchTestDB(t)
	createBatchTestTable(t, d)

	_, err := d.BatchGetItem(context.Background(), &sdk.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			"tbl": {Keys: []map[string]types.AttributeValue{}},
		},
	})
	assertBatchValidationErr(t, err)
}

// TestBatchGetItem_DuplicateKeys_Rejected verifies that a per-table Keys list
// containing the same key twice is rejected with a ValidationException, matching AWS,
// rather than returning the matching item more than once.
func TestBatchGetItem_DuplicateKeys_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		keys    []map[string]types.AttributeValue
		wantErr bool
	}{
		{
			name: "unique_keys_ok",
			keys: []map[string]types.AttributeValue{
				{"pk": &types.AttributeValueMemberS{Value: "a"}},
				{"pk": &types.AttributeValueMemberS{Value: "b"}},
			},
			wantErr: false,
		},
		{
			name: "duplicate_keys_rejected",
			keys: []map[string]types.AttributeValue{
				{"pk": &types.AttributeValueMemberS{Value: "a"}},
				{"pk": &types.AttributeValueMemberS{Value: "a"}},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			d := newBatchTestDB(t)
			createBatchTestTable(t, d)

			_, err := d.BatchGetItem(context.Background(), &sdk.BatchGetItemInput{
				RequestItems: map[string]types.KeysAndAttributes{
					batchTestTableName: {Keys: tt.keys},
				},
			})

			if tt.wantErr {
				assertBatchValidationErr(t, err)

				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestBatchGetItem_AttributesToGet_AppliedWhenNoProjectionExpression(t *testing.T) {
	t.Parallel()
	d := newBatchTestDB(t)
	createBatchTestTable(t, d)
	putBatchTestItem(t, d, map[string]types.AttributeValue{
		"pk":    &types.AttributeValueMemberS{Value: "k1"},
		"name":  &types.AttributeValueMemberS{Value: "alice"},
		"email": &types.AttributeValueMemberS{Value: "a@example.com"},
	})

	out, err := d.BatchGetItem(context.Background(), &sdk.BatchGetItemInput{
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
	d := newBatchTestDB(t)
	createBatchTestTable(t, d)

	projExpr := "pk"
	_, err := d.BatchGetItem(context.Background(), &sdk.BatchGetItemInput{
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
	assertBatchValidationErr(t, err)
}

func TestBatchGetItem_ConsistentRead_DoublesRCU(t *testing.T) {
	t.Parallel()
	d := newBatchTestDB(t)
	createBatchTestTable(t, d)
	putBatchTestItem(t, d, map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "k1"},
	})

	tests := []struct {
		consistentRead *bool
		name           string
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
			out, err := d.BatchGetItem(context.Background(), &sdk.BatchGetItemInput{
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
				t.Errorf(
					"ConsistentRead=%v: want RCU >= %.1f, got %.2f",
					aws.ToBool(tt.consistentRead),
					tt.wantMinRCU,
					gotRCU,
				)
			}
		})
	}
}

func TestBatchGetItem_ConsistentRead_RCU_GreaterThan_EventuallyConsistent(t *testing.T) {
	t.Parallel()
	d := newBatchTestDB(t)
	createBatchTestTable(t, d)
	for i := range 5 {
		putBatchTestItem(t, d, map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: string(rune('a' + i))},
		})
	}

	keys := make([]map[string]types.AttributeValue, 5)
	for i := range 5 {
		keys[i] = map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: string(rune('a' + i))},
		}
	}

	eventual, err := d.BatchGetItem(context.Background(), &sdk.BatchGetItemInput{
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		RequestItems: map[string]types.KeysAndAttributes{
			"tbl": {Keys: keys, ConsistentRead: aws.Bool(false)},
		},
	})
	if err != nil {
		t.Fatalf("eventual read: %v", err)
	}

	consistent, err := d.BatchGetItem(context.Background(), &sdk.BatchGetItemInput{
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

func TestBatchWriteItem_NullWriteRequest_Rejected(t *testing.T) {
	t.Parallel()
	d := newBatchTestDB(t)
	createBatchTestTable(t, d)

	_, err := d.BatchWriteItem(context.Background(), &sdk.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"tbl": {
				{PutRequest: nil, DeleteRequest: nil},
			},
		},
	})
	assertBatchValidationErr(t, err)
}

func TestBatchWriteItem_OversizedItem_Rejected(t *testing.T) {
	t.Parallel()
	d := newBatchTestDB(t)
	createBatchTestTable(t, d)

	// Build a value that pushes the item over 400 KB.
	bigVal := strings.Repeat("x", 400*1024+1)

	_, err := d.BatchWriteItem(context.Background(), &sdk.BatchWriteItemInput{
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
	assertBatchValidationErr(t, err)
}

func TestBatchWriteItem_PutRequest_MissingPK_Rejected(t *testing.T) {
	t.Parallel()
	d := newBatchTestDB(t)
	createBatchTestTable(t, d)

	_, err := d.BatchWriteItem(context.Background(), &sdk.BatchWriteItemInput{
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
	assertBatchValidationErr(t, err)
}

func TestBatchWriteItem_DeleteRequest_MissingPK_Rejected(t *testing.T) {
	t.Parallel()
	d := newBatchTestDB(t)
	createBatchTestTable(t, d)

	_, err := d.BatchWriteItem(context.Background(), &sdk.BatchWriteItemInput{
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
	assertBatchValidationErr(t, err)
}

// The Delete key is deliberately distinct from both Put keys: AWS DynamoDB
// rejects a BatchWriteItem whose per-table request list targets the same
// primary key more than once (e.g. Put(k1) + Delete(k1) together) with
// "ValidationException: Provided list of item keys contains duplicates".
func TestBatchWriteItem_ValidRequests_NotAffectedByValidation(t *testing.T) {
	t.Parallel()
	d := newBatchTestDB(t)
	createBatchTestTable(t, d)

	_, err := d.BatchWriteItem(context.Background(), &sdk.BatchWriteItemInput{
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
							"pk": &types.AttributeValueMemberS{Value: "k3"},
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

// TestBatchWriteItem_ReturnConsumedCapacity_SurvivesWireConversion verifies that
// ToSDKBatchWriteItemInput actually copies ReturnConsumedCapacity from the
// wire-format models.BatchWriteItemInput onto the SDK input struct.
// models.BatchWriteItemInput previously had no ReturnConsumedCapacity field at all
// (nor ReturnItemCollectionMetrics), so a real client's "ReturnConsumedCapacity":
// "TOTAL" was silently dropped when parsed off the wire -- the backend always saw
// ReturnConsumedCapacity == "" regardless of what was requested, exactly like an
// unrecognised awsjson1.0 key.
func TestBatchWriteItem_ReturnConsumedCapacity_SurvivesWireConversion(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	createTableHelper(t, db, "Table1", "pk")

	input := models.BatchWriteItemInput{
		ReturnConsumedCapacity: "TOTAL",
		RequestItems: map[string][]models.WriteRequest{
			"Table1": {
				{PutRequest: &models.PutRequest{Item: map[string]any{"pk": map[string]any{"S": "item1"}}}},
			},
		},
	}

	sdkInput, convErr := models.ToSDKBatchWriteItemInput(&input)
	require.NoError(t, convErr)
	require.Equal(t, types.ReturnConsumedCapacityTotal, sdkInput.ReturnConsumedCapacity)

	res, writeErr := db.BatchWriteItem(t.Context(), sdkInput)
	require.NoError(t, writeErr)
	require.NotEmpty(t, res.ConsumedCapacity, "ConsumedCapacity must be populated when requested")
}

// TestBatchWriteItem_DuplicateKey_PutAndDelete_Rejected verifies that AWS's
// "one action per item per BatchWriteItem" rule is enforced across request
// kinds, not just within a single Put or Delete list: targeting the same
// primary key with both a Put and a Delete in one call is rejected.
func TestBatchWriteItem_DuplicateKey_PutAndDelete_Rejected(t *testing.T) {
	t.Parallel()
	d := newBatchTestDB(t)
	createBatchTestTable(t, d)

	_, err := d.BatchWriteItem(context.Background(), &sdk.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			"tbl": {
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "k1"},
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
	assertBatchValidationErr(t, err)
}

// Regression: valid batch get with no projection still returns all attributes.
func TestBatchGetItem_NoProjection_ReturnsAllAttributes(t *testing.T) {
	t.Parallel()
	d := newBatchTestDB(t)
	createBatchTestTable(t, d)
	putBatchTestItem(t, d, map[string]types.AttributeValue{
		"pk":    &types.AttributeValueMemberS{Value: "k1"},
		"name":  &types.AttributeValueMemberS{Value: "bob"},
		"email": &types.AttributeValueMemberS{Value: "b@example.com"},
	})

	out, err := d.BatchGetItem(context.Background(), &sdk.BatchGetItemInput{
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

//
// Covers two accuracy gaps in BatchGetItem and BatchWriteItem:
//   1. BatchGetItem: Responses always includes all requested tables even when
//      all keys miss (zero-hit table was previously omitted).
//   2. BatchWriteItem: ConsumedCapacity WCU is proportional to item size
//      (ceil(size/1KB)); previously charged a flat 1 WCU per request.

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
			d := newBatchTestDB(t)
			createBatchTestTable(t, d)

			out, err := d.BatchGetItem(context.Background(), &sdk.BatchGetItemInput{
				RequestItems: map[string]types.KeysAndAttributes{
					batchTestTableName: {Keys: tt.keys},
				},
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			tableItems, ok := out.Responses[batchTestTableName]
			if !ok {
				t.Fatalf(
					"table %q missing from Responses; AWS always includes all requested tables",
					batchTestTableName,
				)
			}
			if len(tableItems) != 0 {
				t.Fatalf("expected empty list for all-miss batch, got %d items", len(tableItems))
			}
		})
	}
}

func TestBatchGetItem_MixedHitMiss_TableAlwaysInResponses(t *testing.T) {
	t.Parallel()
	d := newBatchTestDB(t)
	createBatchTestTable(t, d)
	putBatchTestItem(t, d, map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "exists"},
	})

	out, err := d.BatchGetItem(context.Background(), &sdk.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			batchTestTableName: {
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

	tableItems, ok := out.Responses[batchTestTableName]
	if !ok {
		t.Fatalf("table %q missing from Responses", batchTestTableName)
	}
	if len(tableItems) != 1 {
		t.Fatalf("expected 1 found item, got %d", len(tableItems))
	}
}

func TestBatchWriteItem_SmallItem_ChargesOneWCU(t *testing.T) {
	t.Parallel()
	d := newBatchTestDB(t)
	createBatchTestTable(t, d)

	// Item well under 1 KB → 1 WCU.
	out, err := d.BatchWriteItem(context.Background(), &sdk.BatchWriteItemInput{
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		RequestItems: map[string][]types.WriteRequest{
			batchTestTableName: {
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
	d := newBatchTestDB(t)
	createBatchTestTable(t, d)

	// Item ~2 KB → at least 2 WCU. Use a 2048-byte value to ensure we exceed 1 KB.
	bigVal := strings.Repeat("x", 2048)

	out, err := d.BatchWriteItem(context.Background(), &sdk.BatchWriteItemInput{
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		RequestItems: map[string][]types.WriteRequest{
			batchTestTableName: {
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
	d := newBatchTestDB(t)
	createBatchTestTable(t, d)

	// One small item (1 WCU) + one ~2KB item (≥2 WCU) → total ≥3 WCU.
	bigVal := strings.Repeat("y", 2048)

	out, err := d.BatchWriteItem(context.Background(), &sdk.BatchWriteItemInput{
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		RequestItems: map[string][]types.WriteRequest{
			batchTestTableName: {
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
