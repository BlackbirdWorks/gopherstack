package dynamodb_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/dynamodb"
	"github.com/blackbirdworks/gopherstack/dynamodb/models"

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
			_, err := db.BatchWriteItem(context.Background(), sdkInput)

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
		setup   func(t *testing.T, db *dynamodb.InMemoryDB)
		input   models.BatchGetItemInput
		want    map[string][]map[string]any
		name    string
		wantErr bool
	}{
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
			if diff := cmp.Diff(tt.want, got, cmpopts.SortSlices(func(a, b map[string]any) bool {
				return a["pk"].(map[string]any)["S"].(string) < b["pk"].(map[string]any)["S"].(string)
			})); diff != "" {
				t.Errorf("BatchGetItem responses mismatch (-want +got):\n%s", diff)
			}
		})
	}
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
			_, err := db.BatchWriteItem(context.Background(), sdkInput)

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

// TestBatchWriteItem_UnprocessedItems verifies that items exceeding the 16 MB limit are
// returned in UnprocessedItems rather than being written.
func TestBatchWriteItem_UnprocessedItems(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	createTableHelper(t, db, "BigTable", "pk")

	// Build 20 items each with ~1 MB of data; total > 16 MB triggers the limit.
	const numItems = 20
	const valueSizeBytes = 1024 * 1024 // 1 MB per item
	bigValue := strings.Repeat("x", valueSizeBytes)

	reqs := make([]models.WriteRequest, numItems)
	for i := range numItems {
		reqs[i] = models.WriteRequest{
			PutRequest: &models.PutRequest{
				Item: map[string]any{
					"pk":  map[string]any{"S": fmt.Sprintf("item%d", i)},
					"big": map[string]any{"S": bigValue},
				},
			},
		}
	}

	sdkInput, err := models.ToSDKBatchWriteItemInput(&models.BatchWriteItemInput{
		RequestItems: map[string][]models.WriteRequest{"BigTable": reqs},
	})
	require.NoError(t, err)
	require.NotNil(t, sdkInput)

	out, err := db.BatchWriteItem(context.Background(), sdkInput)
	require.NoError(t, err)
	require.NotNil(t, out)

	assert.NotEmpty(t, out.UnprocessedItems, "expected some items to be returned as UnprocessedItems")
	assert.NotEmpty(t, out.UnprocessedItems["BigTable"])
}
