package dynamodb_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/dynamodb"
	"github.com/blackbirdworks/gopherstack/dynamodb/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/go-cmp/cmp"
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
				if diff := cmp.Diff(expectedItem, actualItem); diff != "" {
					t.Errorf("Response %d mismatch (-want +got):\n%s", i, diff)
				}
			}
		})
	}
}
