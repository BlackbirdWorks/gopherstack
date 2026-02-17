package dynamodb_test

import (
	"Gopherstack/dynamodb/models"
	"context"
	"testing"

	"Gopherstack/dynamodb"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPutItem(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*dynamodb.InMemoryDB)
		validate func(*testing.T, any, error)
		input    models.PutItemInput
		name     string
	}{
		{
			name: "Success",
			setup: func(db *dynamodb.InMemoryDB) {
				createTableHelper(t, db, "ItemsTable", "id") // Assuming wrapper in test_utils handles SDK conversion
			},
			input: models.PutItemInput{
				TableName: "ItemsTable",
				Item: map[string]any{
					"id":  map[string]any{"S": "1"},
					"val": map[string]any{"S": "data"},
				},
			},
			validate: func(t *testing.T, _ any, err error) {
				t.Helper()
				require.NoError(t, err)
			},
		},
		{
			name: "Overwrite",
			setup: func(db *dynamodb.InMemoryDB) {
				createTableHelper(t, db, "ItemsTable", "id")
				putItem(db, "1", "original")
			},
			input: models.PutItemInput{
				TableName: "ItemsTable",
				Item: map[string]any{
					"id":  map[string]any{"S": "1"},
					"val": map[string]any{"S": "updated"},
				},
			},
			validate: func(t *testing.T, _ any, err error) {
				t.Helper()
				require.NoError(t, err)
			},
		},
		{
			name: "ReturnValues_ALL_OLD",
			setup: func(db *dynamodb.InMemoryDB) {
				createTableHelper(t, db, "ItemsTable", "id")
				putItem(db, "1", "original")
			},
			input: models.PutItemInput{
				TableName: "ItemsTable",
				Item: map[string]any{
					"id":  map[string]any{"S": "1"},
					"val": map[string]any{"S": "updated"},
				},
				ReturnValues: models.ReturnValuesAllOld,
			},
			validate: func(t *testing.T, resp any, err error) {
				t.Helper()
				require.NoError(t, err)
				output := resp.(*dynamodb_sdk.PutItemOutput)
				require.NotNil(t, output.Attributes, "Expected Attributes to be returned")
				val := output.Attributes["val"].(*types.AttributeValueMemberS).Value
				assert.Equal(t, "original", val)
			},
		},
		{
			name: "ReturnConsumedCapacity",
			setup: func(db *dynamodb.InMemoryDB) {
				createTableHelper(t, db, "ItemsTable", "id")
			},
			input: models.PutItemInput{
				TableName:              "ItemsTable",
				Item:                   map[string]any{"id": map[string]any{"S": "1"}},
				ReturnConsumedCapacity: "TOTAL",
			},
			validate: func(t *testing.T, resp any, err error) {
				t.Helper()
				require.NoError(t, err)
				output := resp.(*dynamodb_sdk.PutItemOutput)
				require.NotNil(t, output.ConsumedCapacity, "Expected ConsumedCapacity to be returned")
				assert.InDelta(t, 1.0, *output.ConsumedCapacity.CapacityUnits, 0.0001)
			},
		},
		{
			name: "ReturnItemCollectionMetrics",
			setup: func(db *dynamodb.InMemoryDB) {
				createTableHelper(t, db, "ItemsTable", "id")
			},
			input: models.PutItemInput{
				TableName:                   "ItemsTable",
				Item:                        map[string]any{"id": map[string]any{"S": "1"}},
				ReturnItemCollectionMetrics: "SIZE",
			},
			validate: func(t *testing.T, resp any, err error) {
				t.Helper()
				require.NoError(t, err)
				output := resp.(*dynamodb_sdk.PutItemOutput)
				require.NotNil(t, output.ItemCollectionMetrics, "Expected ItemCollectionMetrics to be returned")
				pkVal := output.ItemCollectionMetrics.ItemCollectionKey["id"].(*types.AttributeValueMemberS).Value
				assert.Equal(t, "1", pkVal)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()
			if tt.setup != nil {
				tt.setup(db)
			}

			sdkInput, _ := models.ToSDKPutItemInput(&tt.input)
			resp, err := db.PutItem(t.Context(), sdkInput)

			if tt.validate != nil {
				tt.validate(t, resp, err)
			}
		})
	}
}

func TestGetItem(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*dynamodb.InMemoryDB)
		validate func(*testing.T, any, error)
		input    models.GetItemInput
		name     string
	}{
		{
			name: "Success",
			setup: func(db *dynamodb.InMemoryDB) {
				createTableHelper(t, db, "ItemsTable", "id")
				putItem(db, "1", "data")
			},
			input: models.GetItemInput{
				TableName: "ItemsTable",
				Key:       map[string]any{"id": map[string]any{"S": "1"}},
			},
			validate: func(t *testing.T, resp any, err error) {
				t.Helper()
				require.NoError(t, err)
				output := resp.(*dynamodb_sdk.GetItemOutput)
				expected := map[string]any{
					"id":  map[string]any{"S": "1"},
					"val": map[string]any{"S": "data"},
				}
				// Convert output item to wire format for comparison
				got := models.FromSDKItem(output.Item)
				assert.Equal(t, expected, got)
			},
		},
		{
			name: "NotFound",
			setup: func(db *dynamodb.InMemoryDB) {
				createTableHelper(t, db, "ItemsTable", "id")
			},
			input: models.GetItemInput{
				TableName: "ItemsTable",
				Key:       map[string]any{"id": map[string]any{"S": "999"}},
			},
			validate: func(t *testing.T, resp any, err error) {
				t.Helper()
				require.NoError(t, err)
				output := resp.(*dynamodb_sdk.GetItemOutput)
				assert.Empty(t, output.Item)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()
			if tt.setup != nil {
				tt.setup(db)
			}

			sdkInput, _ := models.ToSDKGetItemInput(&tt.input)
			resp, err := db.GetItem(t.Context(), sdkInput)

			if tt.validate != nil {
				tt.validate(t, resp, err)
			}
		})
	}
}

func TestDeleteItem(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*dynamodb.InMemoryDB)
		validate func(*testing.T, *dynamodb.InMemoryDB, any, error)
		input    models.DeleteItemInput
		name     string
	}{
		{
			name: "Success",
			setup: func(db *dynamodb.InMemoryDB) {
				createTableHelper(t, db, "ItemsTable", "id")
				putItem(db, "1", "data")
			},
			input: models.DeleteItemInput{
				TableName: "ItemsTable",
				Key:       map[string]any{"id": map[string]any{"S": "1"}},
			},
			validate: func(t *testing.T, db *dynamodb.InMemoryDB, _ any, err error) {
				t.Helper()
				require.NoError(t, err)
				// Verify item is gone
				getInput := models.GetItemInput{
					TableName: "ItemsTable",
					Key:       map[string]any{"id": map[string]any{"S": "1"}},
				}
				sdkGet, _ := models.ToSDKGetItemInput(&getInput)
				getResp, _ := db.GetItem(t.Context(), sdkGet)
				assert.Empty(t, getResp.Item)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()
			if tt.setup != nil {
				tt.setup(db)
			}

			sdkInput, _ := models.ToSDKDeleteItemInput(&tt.input)
			resp, err := db.DeleteItem(t.Context(), sdkInput)

			if tt.validate != nil {
				tt.validate(t, db, resp, err)
			}
		})
	}
}

func TestItemOps_Scan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*dynamodb.InMemoryDB)
		validate func(*testing.T, any, error)
		input    models.ScanInput
		name     string
	}{
		{
			name: "Success",
			setup: func(db *dynamodb.InMemoryDB) {
				createTableHelper(t, db, "ItemsTable", "id")
				putItem(db, "1", "data1")
				putItem(db, "2", "data2")
			},
			input: models.ScanInput{TableName: "ItemsTable"},
			validate: func(t *testing.T, resp any, err error) {
				t.Helper()
				require.NoError(t, err)
				output := resp.(*dynamodb_sdk.ScanOutput)
				assert.Equal(t, int32(2), output.Count)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()
			if tt.setup != nil {
				tt.setup(db)
			}

			sdkInput, _ := models.ToSDKScanInput(&tt.input)
			resp, err := db.Scan(t.Context(), sdkInput)

			if tt.validate != nil {
				tt.validate(t, resp, err)
			}
		})
	}
}

func putItem(db *dynamodb.InMemoryDB, id, val string) {
	input := models.PutItemInput{
		TableName: "ItemsTable",
		Item: map[string]any{
			"id":  map[string]any{"S": id},
			"val": map[string]any{"S": val},
		},
	}
	sdkInput, _ := models.ToSDKPutItemInput(&input)
	_, _ = db.PutItem(context.Background(), sdkInput)
}
func TestItem_Expiration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tableName string
		ttlAttr   string
		ttlValue  string
	}{
		{
			name:      "ExpiredItem",
			tableName: "TTLTable",
			ttlAttr:   "ttl",
			ttlValue:  "1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()

			createTableHelper(t, db, tt.tableName, "id")

			// Enable TTL
			_, err := db.UpdateTimeToLive(t.Context(), &dynamodb_sdk.UpdateTimeToLiveInput{
				TableName: &tt.tableName,
				TimeToLiveSpecification: &types.TimeToLiveSpecification{
					AttributeName: aws.String(tt.ttlAttr),
					Enabled:       aws.Bool(true),
				},
			})
			require.NoError(t, err)

			// Put expired item
			_, err = db.PutItem(t.Context(), &dynamodb_sdk.PutItemInput{
				TableName: &tt.tableName,
				Item: map[string]types.AttributeValue{
					"id":       &types.AttributeValueMemberS{Value: "exp1"},
					tt.ttlAttr: &types.AttributeValueMemberN{Value: tt.ttlValue},
				},
			})
			require.NoError(t, err)

			// Get should return nothing
			out, err := db.GetItem(t.Context(), &dynamodb_sdk.GetItemInput{
				TableName: &tt.tableName,
				Key:       map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: "exp1"}},
			})
			require.NoError(t, err)
			assert.Empty(t, out.Item)
		})
	}
}

func TestPutItem_ConditionExpression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		condition  string
		name       string
		errContain string
		wantErr    bool
	}{
		{
			name:       "FailIfExists",
			condition:  "attribute_not_exists(id)",
			wantErr:    true,
			errContain: "ConditionalCheckFailed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()
			tableName := "CondTable"
			createTableHelper(t, db, tableName, "id")

			// Put initial item
			_, err := db.PutItem(t.Context(), &dynamodb_sdk.PutItemInput{
				TableName: &tableName,
				Item:      map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: "1"}},
			})
			require.NoError(t, err)

			// Try to put again with condition
			_, err = db.PutItem(t.Context(), &dynamodb_sdk.PutItemInput{
				TableName:           &tableName,
				ConditionExpression: aws.String(tt.condition),
				Item:                map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: "1"}},
			})

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContain)

				return
			}
			require.NoError(t, err)
		})
	}
}
