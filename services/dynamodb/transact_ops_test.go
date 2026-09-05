package dynamodb_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"

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
		setup      func(*testing.T, *dynamodb.InMemoryDB)
		name       string
		errMessage string
		items      []types.TransactGetItem
		expected   []types.ItemResponse
		wantErr    bool
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
		{
			name: "MalformedProjectionExpression",
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
						ProjectionExpression: aws.String("val["),
					},
				},
			},
			wantErr:    true,
			errMessage: "ValidationException",
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
				if tt.errMessage != "" {
					assert.Contains(t, err.Error(), tt.errMessage)
				}

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
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: "item-idem"},
		},
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
				Item: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "x"},
				},
			}},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.ConsumedCapacity, "ConsumedCapacity should be populated when requested")
}

// TestTransactWriteItems_ReturnItemCollectionMetrics_SurvivesWireConversion verifies
// that ToSDKTransactWriteItemsInput actually copies ReturnItemCollectionMetrics from
// the wire-format models.TransactWriteItemsInput onto the SDK input struct.
// ToSDKTransactWriteItemsInput previously left this field unset (marked only by a
// dangling "// ReturnItemCollectionMetrics" comment), so a real client's
// "ReturnItemCollectionMetrics": "SIZE" was silently dropped when parsed off the
// wire -- the backend always saw the zero value regardless of what was requested.
// TestTransactWriteItems_ConsumedCapacity exercises this op via the SDK-typed input
// directly, which bypasses this exact conversion step; this test goes through the
// wire model instead, the same path a real client's JSON body takes.
func TestTransactWriteItems_ReturnItemCollectionMetrics_SurvivesWireConversion(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	_, err := db.CreateTable(t.Context(), &sdk.CreateTableInput{
		TableName: aws.String("LsiTable"),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
			{AttributeName: aws.String("lsi_sk"), AttributeType: types.ScalarAttributeTypeS},
		},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{
			{
				IndexName: aws.String("lsi1"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("lsi_sk"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	input := models.TransactWriteItemsInput{
		ReturnItemCollectionMetrics: "SIZE",
		TransactItems: []models.TransactWriteItem{
			{Put: &models.PutItemInput{
				TableName: "LsiTable",
				Item: map[string]any{
					"pk":     map[string]any{"S": "user1"},
					"sk":     map[string]any{"S": "ord1"},
					"lsi_sk": map[string]any{"S": "ord1"},
				},
			}},
		},
	}

	sdkInput, convErr := models.ToSDKTransactWriteItemsInput(&input)
	require.NoError(t, convErr)
	require.Equal(t, types.ReturnItemCollectionMetricsSize, sdkInput.ReturnItemCollectionMetrics)

	out, writeErr := db.TransactWriteItems(t.Context(), sdkInput)
	require.NoError(t, writeErr)
	require.NotEmpty(t, out.ItemCollectionMetrics["LsiTable"],
		"ItemCollectionMetrics must be populated when requested on an LSI table")
}

func TestTransactWriteItems_TokenNotCommittedOnFailure(t *testing.T) {
	t.Parallel()

	const tbl = "TokenFailTable"
	db := newTransactDB(t, tbl)

	// Seed pk="item1" so that attribute_not_exists(pk) fails for that key.
	seedItem(t, db, tbl, "exists")

	token := "fail-token"
	input := &sdk.TransactWriteItemsInput{
		ClientRequestToken: aws.String(token),
		TransactItems: []types.TransactWriteItem{
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
	}

	// First call: fails because of condition expression (item1 already exists).
	_, err := db.TransactWriteItems(t.Context(), input)
	require.Error(t, err, "first call should fail due to condition")

	// Second call with same token must also execute (token was not committed on failure).
	// It should fail again (same condition), not return success silently.
	_, err = db.TransactWriteItems(t.Context(), input)
	require.Error(t, err, "second call with uncommitted token should also fail")
}

// TestTransactWriteItems_Update_RejectsKeyModification verifies a
// TransactWriteItems Update action is rejected when its UpdateExpression
// touches a key attribute, matching plain UpdateItem's restriction -- without
// it, updateIndexes only adds/overwrites the new key's index slot and never
// removes the old one, corrupting subsequent lookups by the original key.
func TestTransactWriteItems_Update_RejectsKeyModification(t *testing.T) {
	t.Parallel()

	const tbl = "TxUpdateKeyTable"
	db := newTransactDB(t, tbl)
	seedItem(t, db, tbl, "v1")

	_, err := db.TransactWriteItems(t.Context(), &sdk.TransactWriteItemsInput{
		TransactItems: []types.TransactWriteItem{
			{
				Update: &types.Update{
					TableName: aws.String(tbl),
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "item1"},
					},
					UpdateExpression: aws.String("SET pk = :newpk"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":newpk": &types.AttributeValueMemberS{Value: "item2"},
					},
				},
			},
		},
	})
	require.Error(t, err, "TransactWriteItems must reject an Update that modifies a key attribute")
	assert.Contains(t, err.Error(), "ValidationException")

	// Confirm no corruption occurred: the original item must still be reachable
	// by its original key, and no phantom item under the new key was created.
	out, getErr := db.GetItem(t.Context(), &sdk.GetItemInput{
		TableName: aws.String(tbl),
		Key:       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "item1"}},
	})
	require.NoError(t, getErr)
	assert.NotEmpty(t, out.Item, "original item under item1 must still exist")

	outNew, getErr := db.GetItem(t.Context(), &sdk.GetItemInput{
		TableName: aws.String(tbl),
		Key:       map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "item2"}},
	})
	require.NoError(t, getErr)
	assert.Empty(t, outNew.Item, "rejected transaction must not create an item under item2")
}
