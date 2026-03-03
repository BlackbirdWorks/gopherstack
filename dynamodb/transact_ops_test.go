package dynamodb_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/dynamodb"
	"github.com/blackbirdworks/gopherstack/dynamodb/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTransactDB(t *testing.T, tableName string) *dynamodb.InMemoryDB {
	t.Helper()
	db := dynamodb.NewInMemoryDB()
	createTableHelper(t, db, tableName, "pk")

	return db
}

func seedItem(t *testing.T, db *dynamodb.InMemoryDB, tableName, val string) {
	t.Helper()
	_, err := db.PutItem(t.Context(), &sdk.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]types.AttributeValue{
			"pk":  &types.AttributeValueMemberS{Value: "item1"},
			"val": &types.AttributeValueMemberS{Value: val},
		},
	})
	require.NoError(t, err)
}

func TestTransactWriteItems(t *testing.T) {
	t.Parallel()

	const tbl = "TestTable"

	tests := []struct {
		name    string
		setup   func(*testing.T, *dynamodb.InMemoryDB)
		items   []types.TransactWriteItem
		wantErr bool
	}{
		{
			name:    "EmptyItems",
			items:   []types.TransactWriteItem{},
			wantErr: true,
		},
		{
			name: "BasicPut",
			items: []types.TransactWriteItem{
				{
					Put: &types.Put{
						TableName: aws.String(tbl),
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "item1"},
						},
					},
				},
			},
		},
		{
			name: "ConditionalPut_Success",
			items: []types.TransactWriteItem{
				{
					Put: &types.Put{
						TableName:           aws.String(tbl),
						ConditionExpression: aws.String("attribute_not_exists(pk)"),
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "item-new"},
						},
					},
				},
			},
		},
		{
			name: "ConditionalPut_Failure",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				seedItem(t, db, tbl, "existing")
			},
			items: []types.TransactWriteItem{
				{
					Put: &types.Put{
						TableName:           aws.String(tbl),
						ConditionExpression: aws.String("attribute_not_exists(pk)"),
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "item1"},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "ConditionCheck_Success",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				seedItem(t, db, tbl, "existing")
			},
			items: []types.TransactWriteItem{
				{
					ConditionCheck: &types.ConditionCheck{
						TableName: aws.String(tbl),
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "item1"},
						},
						ConditionExpression: aws.String("attribute_exists(pk)"),
					},
				},
				{
					Update: &types.Update{
						TableName: aws.String(tbl),
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "item1"},
						},
						UpdateExpression: aws.String("SET val = :v"),
						ExpressionAttributeValues: map[string]types.AttributeValue{
							":v": &types.AttributeValueMemberS{Value: "updated"},
						},
					},
				},
			},
		},
		{
			name: "ConditionCheck_Failure",
			items: []types.TransactWriteItem{
				{
					ConditionCheck: &types.ConditionCheck{
						TableName: aws.String(tbl),
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "nonexistent"},
						},
						ConditionExpression: aws.String("attribute_exists(pk)"),
					},
				},
			},
			wantErr: true,
		},
		{
			name: "Delete_Success",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				seedItem(t, db, tbl, "existing")
			},
			items: []types.TransactWriteItem{
				{
					Delete: &types.Delete{
						TableName: aws.String(tbl),
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "item1"},
						},
					},
				},
			},
		},
		{
			name: "TableNotFound",
			items: []types.TransactWriteItem{
				{
					Put: &types.Put{
						TableName: aws.String("NonExistent"),
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "v"},
						},
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := newTransactDB(t, tbl)
			if tt.setup != nil {
				tt.setup(t, db)
			}

			_, err := db.TransactWriteItems(t.Context(), &sdk.TransactWriteItemsInput{
				TransactItems: tt.items,
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestTransactGetItems(t *testing.T) {
	t.Parallel()

	const tbl = "GetTable"

	tests := []struct {
		name     string
		setup    func(*testing.T, *dynamodb.InMemoryDB)
		items    []types.TransactGetItem
		expected []types.ItemResponse
		wantErr  bool
	}{
		{
			name:    "EmptyItems",
			items:   []types.TransactGetItem{},
			wantErr: true,
		},
		{
			name: "BasicGet",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				seedItem(t, db, tbl, "foo")
			},
			items: []types.TransactGetItem{
				{
					Get: &types.Get{
						TableName: aws.String(tbl),
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "item1"},
						},
					},
				},
			},
			expected: []types.ItemResponse{
				{
					Item: map[string]types.AttributeValue{
						"pk":  &types.AttributeValueMemberS{Value: "item1"},
						"val": &types.AttributeValueMemberS{Value: "foo"},
					},
				},
			},
		},
		{
			name: "ItemNotFound",
			items: []types.TransactGetItem{
				{
					Get: &types.Get{
						TableName: aws.String(tbl),
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "missing"},
						},
					},
				},
			},
			expected: []types.ItemResponse{{}},
		},
		{
			name: "TableNotFound",
			items: []types.TransactGetItem{
				{
					Get: &types.Get{
						TableName: aws.String("NonExistent"),
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "pk"},
						},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "ProjectionExpression",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				seedItem(t, db, tbl, "foo")
			},
			items: []types.TransactGetItem{
				{
					Get: &types.Get{
						TableName: aws.String(tbl),
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "item1"},
						},
						ProjectionExpression: aws.String("pk"),
					},
				},
			},
			expected: []types.ItemResponse{
				{
					Item: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "item1"},
					},
				},
			},
		},
		{
			name:     "NilGet",
			items:    []types.TransactGetItem{{}},
			expected: []types.ItemResponse{{}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := newTransactDB(t, tbl)
			if tt.setup != nil {
				tt.setup(t, db)
			}

			out, err := db.TransactGetItems(t.Context(), &sdk.TransactGetItemsInput{
				TransactItems: tt.items,
			})
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.Len(t, out.Responses, len(tt.expected))

			for i := range out.Responses {
				actualItem := models.FromSDKItem(out.Responses[i].Item)
				expectedItem := models.FromSDKItem(tt.expected[i].Item)
				assert.Empty(t, cmp.Diff(expectedItem, actualItem), "Response %d mismatch", i)
			}
		})
	}
}

func TestTransactWriteItems_Idempotency(t *testing.T) {
	t.Parallel()

	const tbl = "IdempotencyTable"
	db := newTransactDB(t, tbl)

	token := "my-idempotency-token"
	item := map[string]types.AttributeValue{
		"pk":  &types.AttributeValueMemberS{Value: "item-idem"},
		"val": &types.AttributeValueMemberS{Value: "original"},
	}

	input := &sdk.TransactWriteItemsInput{
		ClientRequestToken: aws.String(token),
		TransactItems: []types.TransactWriteItem{
			{Put: &types.Put{TableName: aws.String(tbl), Item: item}},
		},
	}

	// First call: writes the item
	_, err := db.TransactWriteItems(t.Context(), input)
	require.NoError(t, err)

	// Modify the item externally to detect whether a re-apply would change it
	_, err = db.PutItem(t.Context(), &sdk.PutItemInput{
		TableName: aws.String(tbl),
		Item: map[string]types.AttributeValue{
			"pk":  &types.AttributeValueMemberS{Value: "item-idem"},
			"val": &types.AttributeValueMemberS{Value: "modified"},
		},
	})
	require.NoError(t, err)

	// Second call with same token: must not re-apply the write
	_, err = db.TransactWriteItems(t.Context(), input)
	require.NoError(t, err)

	got, err := db.GetItem(t.Context(), &sdk.GetItemInput{
		TableName: aws.String(tbl),
		Key:       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "item-idem"}},
	})
	require.NoError(t, err)
	assert.Equal(t, "modified", got.Item["val"].(*types.AttributeValueMemberS).Value,
		"idempotent replay must not overwrite the item again")
}

func TestTransactWriteItems_ConsumedCapacity(t *testing.T) {
	t.Parallel()

	const tbl = "TxCapTable"
	db := newTransactDB(t, tbl)

	out, err := db.TransactWriteItems(t.Context(), &sdk.TransactWriteItemsInput{
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
		TransactItems: []types.TransactWriteItem{
			{Put: &types.Put{
				TableName: aws.String(tbl),
				Item:      map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "x"}},
			}},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.ConsumedCapacity, "ConsumedCapacity should be populated when requested")
}
