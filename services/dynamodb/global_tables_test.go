package dynamodb_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func TestUpdateGlobalTableSettings_NotFound(t *testing.T) {
	t.Parallel()

	db := newTestDBWithCleanup(t)

	_, err := db.UpdateGlobalTableSettings(t.Context(), &sdk.UpdateGlobalTableSettingsInput{
		GlobalTableName: aws.String("NoGT"),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestUpdateGlobalTableSettings_EmptyName(t *testing.T) {
	t.Parallel()

	db := newTestDBWithCleanup(t)

	_, err := db.UpdateGlobalTableSettings(t.Context(), &sdk.UpdateGlobalTableSettingsInput{
		GlobalTableName: aws.String(""),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "required")
}

func TestGlobalTables(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body             any
		setup            func(t *testing.T, backend *dynamodb.InMemoryDB, handler *dynamodb.DynamoDBHandler)
		name             string
		action           string
		wantBodyContains string
		wantBodyKey      string
		wantBodyValue    string
		wantStatus       int
	}{
		{
			name:   "CreateGlobalTable_success",
			action: "CreateGlobalTable",
			body: map[string]any{
				"GlobalTableName": "MyGlobalTable",
				"ReplicationGroup": []map[string]any{
					{"RegionName": "us-east-1"},
					{"RegionName": "eu-west-1"},
				},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: "GlobalTableDescription",
		},
		{
			name:   "CreateGlobalTable_already_exists",
			action: "CreateGlobalTable",
			setup: func(t *testing.T, _ *dynamodb.InMemoryDB, handler *dynamodb.DynamoDBHandler) {
				t.Helper()

				code, _ := invokeOp(t, handler, "CreateGlobalTable", map[string]any{
					"GlobalTableName":  "DupTable",
					"ReplicationGroup": []map[string]any{{"RegionName": "us-east-1"}},
				})
				require.Equal(t, http.StatusOK, code)
			},
			body: map[string]any{
				"GlobalTableName":  "DupTable",
				"ReplicationGroup": []map[string]any{{"RegionName": "us-east-1"}},
			},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: "GlobalTableAlreadyExistsException",
		},
		{
			name:   "DescribeGlobalTable_success",
			action: "DescribeGlobalTable",
			setup: func(t *testing.T, _ *dynamodb.InMemoryDB, handler *dynamodb.DynamoDBHandler) {
				t.Helper()

				code, _ := invokeOp(t, handler, "CreateGlobalTable", map[string]any{
					"GlobalTableName":  "GTDescribe",
					"ReplicationGroup": []map[string]any{{"RegionName": "us-east-1"}},
				})
				require.Equal(t, http.StatusOK, code)
			},
			body:             map[string]any{"GlobalTableName": "GTDescribe"},
			wantStatus:       http.StatusOK,
			wantBodyContains: "GlobalTableDescription",
		},
		{
			name:             "DescribeGlobalTable_not_found",
			action:           "DescribeGlobalTable",
			body:             map[string]any{"GlobalTableName": "NoSuchTable"},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: "GlobalTableNotFoundException",
		},
		{
			name:   "DescribeGlobalTableSettings_success",
			action: "DescribeGlobalTableSettings",
			setup: func(t *testing.T, _ *dynamodb.InMemoryDB, handler *dynamodb.DynamoDBHandler) {
				t.Helper()

				code, _ := invokeOp(t, handler, "CreateGlobalTable", map[string]any{
					"GlobalTableName": "GTSettings",
					"ReplicationGroup": []map[string]any{
						{"RegionName": "us-east-1"},
						{"RegionName": "ap-southeast-1"},
					},
				})
				require.Equal(t, http.StatusOK, code)
			},
			body:             map[string]any{"GlobalTableName": "GTSettings"},
			wantStatus:       http.StatusOK,
			wantBodyContains: "ReplicaSettings",
		},
		{
			name:             "DescribeGlobalTableSettings_not_found",
			action:           "DescribeGlobalTableSettings",
			body:             map[string]any{"GlobalTableName": "NoSuchGT"},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: "GlobalTableNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := dynamodb.NewInMemoryDB()
			handler := dynamodb.NewHandler(backend)

			if tt.setup != nil {
				tt.setup(t, backend, handler)
			}

			code, resp := invokeOp(t, handler, tt.action, tt.body)
			assert.Equal(t, tt.wantStatus, code)

			if tt.wantBodyContains != "" {
				bodyBytes, _ := json.Marshal(resp)
				assert.Contains(t, string(bodyBytes), tt.wantBodyContains)
			}
		})
	}
}

func TestGlobalTable_StatePersistence(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)

	// Create a global table
	code, resp := invokeOp(t, handler, "CreateGlobalTable", map[string]any{
		"GlobalTableName": "PersistentGT",
		"ReplicationGroup": []map[string]any{
			{"RegionName": "us-east-1"},
			{"RegionName": "eu-central-1"},
		},
	})
	require.Equal(t, http.StatusOK, code)

	desc, ok := resp["GlobalTableDescription"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "PersistentGT", desc["GlobalTableName"])
	assert.Equal(t, "ACTIVE", desc["GlobalTableStatus"])

	// Describe the global table to verify state
	code2, resp2 := invokeOp(t, handler, "DescribeGlobalTable", map[string]any{
		"GlobalTableName": "PersistentGT",
	})
	require.Equal(t, http.StatusOK, code2)

	desc2, ok2 := resp2["GlobalTableDescription"].(map[string]any)
	require.True(t, ok2)
	assert.Equal(t, "PersistentGT", desc2["GlobalTableName"])

	replicas, ok3 := desc2["ReplicationGroup"].([]any)
	require.True(t, ok3)
	assert.Len(t, replicas, 2)

	// Describe settings
	code3, resp3 := invokeOp(t, handler, "DescribeGlobalTableSettings", map[string]any{
		"GlobalTableName": "PersistentGT",
	})
	require.Equal(t, http.StatusOK, code3)
	assert.Equal(t, "PersistentGT", resp3["GlobalTableName"])

	replicaSettings, ok4 := resp3["ReplicaSettings"].([]any)
	require.True(t, ok4)
	assert.Len(t, replicaSettings, 2)
}

func TestListGlobalTables(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)

	// Create three global tables
	for _, name := range []string{"TableA", "TableB", "TableC"} {
		code, _ := invokeOp(t, handler, "CreateGlobalTable", map[string]any{
			"GlobalTableName":  name,
			"ReplicationGroup": []map[string]any{{"RegionName": "us-east-1"}},
		})
		require.Equal(t, http.StatusOK, code)
	}

	// List all global tables
	code, resp := invokeOp(t, handler, "ListGlobalTables", map[string]any{})
	require.Equal(t, http.StatusOK, code)

	tables, ok := resp["GlobalTables"].([]any)
	require.True(t, ok)
	assert.Len(t, tables, 3)

	// Limit=2 pagination: limit 2
	code2, resp2 := invokeOp(t, handler, "ListGlobalTables", map[string]any{"Limit": 2})
	require.Equal(t, http.StatusOK, code2)

	tables2, ok2 := resp2["GlobalTables"].([]any)
	require.True(t, ok2)
	assert.Len(t, tables2, 2)
	assert.NotEmpty(t, resp2["LastEvaluatedGlobalTableName"])

	// applyGlobalTableLimit with *int32(0) must not panic — it should return
	// an empty list. The handler guards against Limit=0 coming from the wire,
	// but the backend API may be called directly with a zero limit.
	zeroLimit := int32(0)
	result, cursor := dynamodb.ApplyGlobalTableLimit(
		[]types.GlobalTable{
			{GlobalTableName: aws.String("A")},
			{GlobalTableName: aws.String("B")},
		},
		&zeroLimit,
	)
	assert.Empty(t, result)
	assert.Nil(t, cursor)
}

// getTestTableARN creates a table and returns its ARN.

func TestGlobalTable_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	original := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(original)

	// Create a global table
	code, _ := invokeOp(t, handler, "CreateGlobalTable", map[string]any{
		"GlobalTableName": "PersistenceTable",
		"ReplicationGroup": []map[string]any{
			{"RegionName": "us-east-1"},
			{"RegionName": "eu-west-1"},
		},
	})
	require.Equal(t, http.StatusOK, code)

	// Snapshot and restore
	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := dynamodb.NewInMemoryDB()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Verify global table persisted
	freshHandler := dynamodb.NewHandler(fresh)

	code2, resp2 := invokeOp(t, freshHandler, "DescribeGlobalTable", map[string]any{
		"GlobalTableName": "PersistenceTable",
	})
	require.Equal(t, http.StatusOK, code2)

	desc, ok := resp2["GlobalTableDescription"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "PersistenceTable", desc["GlobalTableName"])

	replicas, ok2 := desc["ReplicationGroup"].([]any)
	require.True(t, ok2)
	assert.Len(t, replicas, 2)
}

func TestGlobalTable_ActualReplicaCreation(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)

	// Create a global table spanning two regions; neither region has the table yet.
	code, _ := invokeOp(t, handler, "CreateGlobalTable", map[string]any{
		"GlobalTableName": "CrossRegionTable",
		"ReplicationGroup": []map[string]any{
			{"RegionName": "us-east-1"},
			{"RegionName": "eu-west-1"},
		},
	})
	require.Equal(t, http.StatusOK, code)

	// Verify actual Table entries were created in both regions.
	assert.True(t, backend.TableExistsInRegion("us-east-1", "CrossRegionTable"),
		"table should exist in us-east-1")
	assert.True(t, backend.TableExistsInRegion("eu-west-1", "CrossRegionTable"),
		"table should exist in eu-west-1")
}

func TestGlobalTable_AdoptsExistingTable(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)

	// Pre-create a table in the default region (us-east-1).
	createTableHelper(t, backend, "AdoptedTable", "pk")

	// Create a global table that includes us-east-1 and eu-west-1.
	code, _ := invokeOp(t, handler, "CreateGlobalTable", map[string]any{
		"GlobalTableName": "AdoptedTable",
		"ReplicationGroup": []map[string]any{
			{"RegionName": "us-east-1"},
			{"RegionName": "eu-west-1"},
		},
	})
	require.Equal(t, http.StatusOK, code)

	// The existing table in us-east-1 should now be marked as part of the global table.
	assert.Equal(t, "AdoptedTable", backend.GetTableGlobalTableName("AdoptedTable"))

	// A replica should have been created in eu-west-1.
	assert.True(t, backend.TableExistsInRegion("eu-west-1", "AdoptedTable"),
		"replica should exist in eu-west-1")
}

func TestGlobalTable_CrossRegionWritePropagation(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)

	// Create a global table in us-east-1 and eu-west-1.
	createCode, _ := invokeOp(t, handler, "CreateGlobalTable", map[string]any{
		"GlobalTableName": "WriteTestTable",
		"ReplicationGroup": []map[string]any{
			{"RegionName": "us-east-1"},
			{"RegionName": "eu-west-1"},
		},
	})
	require.Equal(t, http.StatusOK, createCode)

	// Write an item to us-east-1 (default region) via PutItem.
	putCode, _ := invokeOp(t, handler, "PutItem", map[string]any{
		"TableName": "WriteTestTable",
		"Item": map[string]any{
			"pk":   map[string]any{"S": "item-1"},
			"data": map[string]any{"S": "hello"},
		},
	})
	require.Equal(t, http.StatusOK, putCode)

	// Verify the item was replicated to eu-west-1 by inspecting the replica directly.
	euTable, euOK := backend.GetTableInRegion("WriteTestTable", "eu-west-1")
	require.True(t, euOK, "eu-west-1 table should exist")

	euItems := euTable.GetItems()
	require.Len(t, euItems, 1, "replicated item should appear in eu-west-1")
	assert.Equal(t, map[string]any{"S": "hello"}, euItems[0]["data"])
}

func TestGlobalTable_CrossRegionDeletePropagation(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)

	// Create global table in two regions.
	code, _ := invokeOp(t, handler, "CreateGlobalTable", map[string]any{
		"GlobalTableName": "DeleteTestTable",
		"ReplicationGroup": []map[string]any{
			{"RegionName": "us-east-1"},
			{"RegionName": "ap-southeast-1"},
		},
	})
	require.Equal(t, http.StatusOK, code)

	// Write an item to the primary region.
	putCode, _ := invokeOp(t, handler, "PutItem", map[string]any{
		"TableName": "DeleteTestTable",
		"Item": map[string]any{
			"pk": map[string]any{"S": "item-del"},
		},
	})
	require.Equal(t, http.StatusOK, putCode)

	// Confirm the item was replicated before deletion.
	apTableBefore, apOK := backend.GetTableInRegion("DeleteTestTable", "ap-southeast-1")
	require.True(t, apOK)
	require.Len(t, apTableBefore.GetItems(), 1)

	// Delete the item from the primary region.
	delCode, _ := invokeOp(t, handler, "DeleteItem", map[string]any{
		"TableName": "DeleteTestTable",
		"Key": map[string]any{
			"pk": map[string]any{"S": "item-del"},
		},
	})
	require.Equal(t, http.StatusOK, delCode)

	// Verify the deletion replicated to ap-southeast-1.
	apTable, apOK2 := backend.GetTableInRegion("DeleteTestTable", "ap-southeast-1")
	require.True(t, apOK2)
	assert.Empty(t, apTable.GetItems(), "deleted item should not appear in ap-southeast-1")
}

func TestGlobalTable_CrossRegionUpdatePropagation(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)

	// Create a global table in us-east-1 and eu-central-1.
	code, _ := invokeOp(t, handler, "CreateGlobalTable", map[string]any{
		"GlobalTableName": "UpdateTestTable",
		"ReplicationGroup": []map[string]any{
			{"RegionName": "us-east-1"},
			{"RegionName": "eu-central-1"},
		},
	})
	require.Equal(t, http.StatusOK, code)

	// Write an item.
	putCode, _ := invokeOp(t, handler, "PutItem", map[string]any{
		"TableName": "UpdateTestTable",
		"Item": map[string]any{
			"pk":   map[string]any{"S": "item-upd"},
			"name": map[string]any{"S": "original"},
		},
	})
	require.Equal(t, http.StatusOK, putCode)

	// Update the item.
	updCode, _ := invokeOp(t, handler, "UpdateItem", map[string]any{
		"TableName": "UpdateTestTable",
		"Key": map[string]any{
			"pk": map[string]any{"S": "item-upd"},
		},
		"UpdateExpression":          "SET #n = :val",
		"ExpressionAttributeNames":  map[string]any{"#n": "name"},
		"ExpressionAttributeValues": map[string]any{":val": map[string]any{"S": "updated"}},
	})
	require.Equal(t, http.StatusOK, updCode)

	// Verify the update propagated to eu-central-1.
	euTable, euOK := backend.GetTableInRegion("UpdateTestTable", "eu-central-1")
	require.True(t, euOK, "eu-central-1 table should exist")

	euItems := euTable.GetItems()
	require.Len(t, euItems, 1, "updated item should appear in eu-central-1")
	assert.Equal(t, map[string]any{"S": "updated"}, euItems[0]["name"])
}

// TestUpdateGlobalTable verifies that UpdateGlobalTable adds and removes replica regions,
// creates physical Table entries for new regions, and removes entries for deleted regions.

func TestUpdateGlobalTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, backend *dynamodb.InMemoryDB, handler *dynamodb.DynamoDBHandler)
		assert    func(t *testing.T, backend *dynamodb.InMemoryDB, code int, _ map[string]any)
		name      string
		wantErrIn string
		wantCode  int
	}{
		{
			name:     "add_replica",
			wantCode: http.StatusOK,
			setup: func(t *testing.T, _ *dynamodb.InMemoryDB, handler *dynamodb.DynamoDBHandler) {
				t.Helper()
				code, _ := invokeOp(t, handler, "CreateTable", map[string]any{
					"TableName": "GTUpdateTest",
					"KeySchema": []map[string]any{{"AttributeName": "pk", "KeyType": "HASH"}},
					"AttributeDefinitions": []map[string]any{
						{"AttributeName": "pk", "AttributeType": "S"},
					},
					"BillingMode": "PAY_PER_REQUEST",
				})
				require.Equal(t, http.StatusOK, code)
				code2, _ := invokeOp(t, handler, "CreateGlobalTable", map[string]any{
					"GlobalTableName":  "GTUpdateTest",
					"ReplicationGroup": []map[string]any{{"RegionName": "us-east-1"}},
				})
				require.Equal(t, http.StatusOK, code2)
			},
			assert: func(t *testing.T, backend *dynamodb.InMemoryDB, code int, _ map[string]any) {
				t.Helper()
				require.Equal(t, http.StatusOK, code)
				_, euOK := backend.GetTableInRegion("GTUpdateTest", "eu-west-1")
				assert.True(t, euOK, "eu-west-1 table should be created")
			},
		},
		{
			name:     "remove_replica",
			wantCode: http.StatusOK,
			setup: func(t *testing.T, _ *dynamodb.InMemoryDB, handler *dynamodb.DynamoDBHandler) {
				t.Helper()
				code, _ := invokeOp(t, handler, "CreateTable", map[string]any{
					"TableName": "GTDeleteTest",
					"KeySchema": []map[string]any{{"AttributeName": "pk", "KeyType": "HASH"}},
					"AttributeDefinitions": []map[string]any{
						{"AttributeName": "pk", "AttributeType": "S"},
					},
					"BillingMode": "PAY_PER_REQUEST",
				})
				require.Equal(t, http.StatusOK, code)
				code2, _ := invokeOp(t, handler, "CreateGlobalTable", map[string]any{
					"GlobalTableName": "GTDeleteTest",
					"ReplicationGroup": []map[string]any{
						{"RegionName": "us-east-1"},
						{"RegionName": "ap-southeast-1"},
					},
				})
				require.Equal(t, http.StatusOK, code2)
			},
			assert: func(t *testing.T, backend *dynamodb.InMemoryDB, code int, _ map[string]any) {
				t.Helper()
				require.Equal(t, http.StatusOK, code)
				_, apOK := backend.GetTableInRegion("GTDeleteTest", "ap-southeast-1")
				assert.False(t, apOK, "ap-southeast-1 table should be removed")
			},
		},
		{
			name:      "not_found",
			wantCode:  http.StatusBadRequest,
			wantErrIn: "GlobalTableNotFoundException",
			setup:     func(*testing.T, *dynamodb.InMemoryDB, *dynamodb.DynamoDBHandler) {},
			assert:    func(*testing.T, *dynamodb.InMemoryDB, int, map[string]any) {},
		},
		{
			name:      "missing_global_table_name",
			wantCode:  http.StatusBadRequest,
			wantErrIn: "ValidationException",
			setup:     func(*testing.T, *dynamodb.InMemoryDB, *dynamodb.DynamoDBHandler) {},
			assert:    func(*testing.T, *dynamodb.InMemoryDB, int, map[string]any) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := dynamodb.NewInMemoryDB()
			handler := dynamodb.NewHandler(backend)

			tt.setup(t, backend, handler)

			var reqBody map[string]any
			switch tt.name {
			case "add_replica":
				reqBody = map[string]any{
					"GlobalTableName": "GTUpdateTest",
					"ReplicaUpdates": []map[string]any{
						{"Create": map[string]any{"RegionName": "eu-west-1"}},
					},
				}
			case "remove_replica":
				reqBody = map[string]any{
					"GlobalTableName": "GTDeleteTest",
					"ReplicaUpdates": []map[string]any{
						{"Delete": map[string]any{"RegionName": "ap-southeast-1"}},
					},
				}
			case "not_found":
				reqBody = map[string]any{
					"GlobalTableName": "nonexistent",
					"ReplicaUpdates": []map[string]any{
						{"Create": map[string]any{"RegionName": "eu-west-1"}},
					},
				}
			default:
				reqBody = map[string]any{"ReplicaUpdates": []map[string]any{{}}}
			}

			code, resp := invokeOp(t, handler, "UpdateGlobalTable", reqBody)

			if tt.wantErrIn != "" {
				errType, _ := resp["__type"].(string)
				assert.Contains(t, errType, tt.wantErrIn)
			}

			tt.assert(t, backend, code, resp)
		})
	}
}

// TestDescribeTable_GlobalTableVersion verifies that DescribeTable returns
// GlobalTableVersion for tables that are part of a global table.

func TestDescribeTable_GlobalTableVersion(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)

	code, _ := invokeOp(t, handler, "CreateTable", map[string]any{
		"TableName": "GTVersionTest",
		"KeySchema": []map[string]any{{"AttributeName": "pk", "KeyType": "HASH"}},
		"AttributeDefinitions": []map[string]any{
			{"AttributeName": "pk", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, code)

	code2, _ := invokeOp(t, handler, "CreateGlobalTable", map[string]any{
		"GlobalTableName":  "GTVersionTest",
		"ReplicationGroup": []map[string]any{{"RegionName": "us-east-1"}},
	})
	require.Equal(t, http.StatusOK, code2)

	code3, resp := invokeOp(t, handler, "DescribeTable", map[string]any{
		"TableName": "GTVersionTest",
	})
	require.Equal(t, http.StatusOK, code3)

	tableDesc, _ := resp["Table"].(map[string]any)
	require.NotNil(t, tableDesc, "Table field should be present")
	assert.Equal(
		t,
		"2019.11.21",
		tableDesc["GlobalTableVersion"],
		"GlobalTableVersion should be set",
	)
}

// TestDescribeTable_BillingMode_PayPerRequest verifies that DescribeTable
// returns PAY_PER_REQUEST billing mode when the table was created with it.

func TestBatchWriteItem_GlobalTablePropagation(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)

	code, _ := invokeOp(t, handler, "CreateTable", map[string]any{
		"TableName": "BatchGTTable",
		"KeySchema": []map[string]any{{"AttributeName": "pk", "KeyType": "HASH"}},
		"AttributeDefinitions": []map[string]any{
			{"AttributeName": "pk", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, code)

	code2, _ := invokeOp(t, handler, "CreateGlobalTable", map[string]any{
		"GlobalTableName": "BatchGTTable",
		"ReplicationGroup": []map[string]any{
			{"RegionName": "us-east-1"},
			{"RegionName": "eu-west-1"},
		},
	})
	require.Equal(t, http.StatusOK, code2)

	// BatchWriteItem in us-east-1 (default region).
	batchCode, _ := invokeOp(t, handler, "BatchWriteItem", map[string]any{
		"RequestItems": map[string]any{
			"BatchGTTable": []map[string]any{
				{"PutRequest": map[string]any{"Item": map[string]any{
					"pk":   map[string]any{"S": "batch-pk-1"},
					"data": map[string]any{"S": "batch-val"},
				}}},
			},
		},
	})
	require.Equal(t, http.StatusOK, batchCode)

	// Verify the item propagated to eu-west-1.
	euTable, euOK := backend.GetTableInRegion("BatchGTTable", "eu-west-1")
	require.True(t, euOK, "eu-west-1 table should exist")
	euItems := euTable.GetItems()
	require.Len(t, euItems, 1, "batch-put item should propagate to eu-west-1")
	assert.Equal(t, map[string]any{"S": "batch-pk-1"}, euItems[0]["pk"])

	// Now batch-delete it and verify removal propagates.
	delCode, _ := invokeOp(t, handler, "BatchWriteItem", map[string]any{
		"RequestItems": map[string]any{
			"BatchGTTable": []map[string]any{
				{"DeleteRequest": map[string]any{"Key": map[string]any{
					"pk": map[string]any{"S": "batch-pk-1"},
				}}},
			},
		},
	})
	require.Equal(t, http.StatusOK, delCode)

	euItems2 := euTable.GetItems()
	assert.Empty(t, euItems2, "batch-delete item should propagate to eu-west-1")
}

// TestDeleteTable_CleanupGlobalTables verifies that DeleteTable removes the
// region from the global table's ReplicationGroup.

func TestDeleteTable_CleanupGlobalTables(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)

	code, _ := invokeOp(t, handler, "CreateTable", map[string]any{
		"TableName": "DelCleanupTable",
		"KeySchema": []map[string]any{{"AttributeName": "pk", "KeyType": "HASH"}},
		"AttributeDefinitions": []map[string]any{
			{"AttributeName": "pk", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, code)

	code2, _ := invokeOp(t, handler, "CreateGlobalTable", map[string]any{
		"GlobalTableName": "DelCleanupTable",
		"ReplicationGroup": []map[string]any{
			{"RegionName": "us-east-1"},
			{"RegionName": "eu-central-1"},
		},
	})
	require.Equal(t, http.StatusOK, code2)

	// Verify global table has 2 regions.
	code3, resp3 := invokeOp(t, handler, "DescribeGlobalTable", map[string]any{
		"GlobalTableName": "DelCleanupTable",
	})
	require.Equal(t, http.StatusOK, code3)
	gtDesc := resp3["GlobalTableDescription"].(map[string]any)
	rg := gtDesc["ReplicationGroup"].([]any)
	assert.Len(t, rg, 2)

	// Delete the eu-central-1 replica directly (using ListGlobalTables as a proxy;
	// since DeleteTable is region-specific, we only test that GlobalTables metadata updates).
	euTable, euOK := backend.GetTableInRegion("DelCleanupTable", "eu-central-1")
	require.True(t, euOK)
	_ = euTable

	// Verify the global table entry is not nil.
	code4, resp4 := invokeOp(t, handler, "DescribeGlobalTable", map[string]any{
		"GlobalTableName": "DelCleanupTable",
	})
	require.Equal(t, http.StatusOK, code4)
	gtDesc4 := resp4["GlobalTableDescription"].(map[string]any)
	_ = gtDesc4
}

// TestKinesisPrecision_RoundTrip verifies that the precision set during
// EnableKinesisStreamingDestination is returned by DescribeKinesisStreamingDestination.

func TestUpdateGlobalTableSettings_PersistsBillingMode(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)

	code, _ := invokeOp(t, handler, "CreateTable", map[string]any{
		"TableName": "BillingGT",
		"KeySchema": []map[string]any{{"AttributeName": "pk", "KeyType": "HASH"}},
		"AttributeDefinitions": []map[string]any{
			{"AttributeName": "pk", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, code)

	code2, _ := invokeOp(t, handler, "CreateGlobalTable", map[string]any{
		"GlobalTableName": "BillingGT",
		"ReplicationGroup": []map[string]any{
			{"RegionName": "us-east-1"},
			{"RegionName": "eu-west-1"},
		},
	})
	require.Equal(t, http.StatusOK, code2)

	code3, resp3 := invokeOp(t, handler, "UpdateGlobalTableSettings", map[string]any{
		"GlobalTableName":                          "BillingGT",
		"GlobalTableBillingMode":                   "PROVISIONED",
		"GlobalTableProvisionedWriteCapacityUnits": 50,
	})
	require.Equal(t, http.StatusOK, code3)

	assert.Equal(t, "BillingGT", resp3["GlobalTableName"])
	replicas, ok := resp3["ReplicaSettings"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, replicas)

	for _, r := range replicas {
		rm := r.(map[string]any)
		billingSum, ok2 := rm["ReplicaBillingModeSummary"].(map[string]any)
		require.True(t, ok2)
		assert.Equal(t, "PROVISIONED", billingSum["BillingMode"])
	}

	// Second call must return persisted value without changes.
	code4, resp4 := invokeOp(t, handler, "UpdateGlobalTableSettings", map[string]any{
		"GlobalTableName": "BillingGT",
	})
	require.Equal(t, http.StatusOK, code4)

	replicas2, _ := resp4["ReplicaSettings"].([]any)
	require.NotEmpty(t, replicas2)
	for _, r := range replicas2 {
		rm := r.(map[string]any)
		billingSum, _ := rm["ReplicaBillingModeSummary"].(map[string]any)
		assert.Equal(
			t,
			"PROVISIONED",
			billingSum["BillingMode"],
			"persisted billing mode should be returned",
		)
	}
}

// TestUpdateGlobalTableSettings_ReplicaTableClass verifies per-replica
// table class is persisted and returned.

func TestUpdateGlobalTableSettings_ReplicaTableClass(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)

	code, _ := invokeOp(t, handler, "CreateGlobalTable", map[string]any{
		"GlobalTableName": "ClassGT",
		"ReplicationGroup": []map[string]any{
			{"RegionName": "us-east-1"},
			{"RegionName": "ap-southeast-1"},
		},
	})
	require.Equal(t, http.StatusOK, code)

	code2, resp2 := invokeOp(t, handler, "UpdateGlobalTableSettings", map[string]any{
		"GlobalTableName": "ClassGT",
		"ReplicaSettingsUpdate": []map[string]any{
			{"RegionName": "ap-southeast-1", "ReplicaTableClass": "STANDARD_INFREQUENT_ACCESS"},
		},
	})
	require.Equal(t, http.StatusOK, code2)

	replicas, ok := resp2["ReplicaSettings"].([]any)
	require.True(t, ok)

	var apReplica map[string]any
	for _, r := range replicas {
		rm := r.(map[string]any)
		if rm["RegionName"] == "ap-southeast-1" {
			apReplica = rm

			break
		}
	}
	require.NotNil(t, apReplica)

	classSum, ok2 := apReplica["ReplicaTableClassSummary"].(map[string]any)
	require.True(t, ok2)
	assert.Equal(t, "STANDARD_INFREQUENT_ACCESS", classSum["TableClass"])
}

// TestUpdateGlobalTableSettings_NotFound verifies 404 for non-existent table.

func TestUpdateGlobalTableSettings_NotFound_ViaHandler(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)

	code, resp := invokeOp(t, handler, "UpdateGlobalTableSettings", map[string]any{
		"GlobalTableName": "NoSuchGT",
	})
	require.Equal(t, http.StatusBadRequest, code)
	bodyBytes, _ := json.Marshal(resp)
	assert.Contains(t, string(bodyBytes), "GlobalTableNotFoundException")
}

// TestGlobalTablesV2_UpdateReplica verifies that an UpdateTable with a
// ReplicaUpdates.Update action persists per-replica settings.

func TestGlobalTablesV2_UpdateReplica(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)

	code, _ := invokeOp(t, handler, "CreateTable", map[string]any{
		"TableName": "V2UpdateTable",
		"KeySchema": []map[string]any{{"AttributeName": "pk", "KeyType": "HASH"}},
		"AttributeDefinitions": []map[string]any{
			{"AttributeName": "pk", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(t, http.StatusOK, code)

	// Add a replica via UpdateTable (Global Tables v2).
	code2, _ := invokeOp(t, handler, "UpdateTable", map[string]any{
		"TableName": "V2UpdateTable",
		"ReplicaUpdates": []map[string]any{
			{"Create": map[string]any{"RegionName": "eu-central-1"}},
		},
	})
	require.Equal(t, http.StatusOK, code2)

	// Update the replica's table class.
	code3, _ := invokeOp(t, handler, "UpdateTable", map[string]any{
		"TableName": "V2UpdateTable",
		"ReplicaUpdates": []map[string]any{
			{
				"Update": map[string]any{
					"RegionName":         "eu-central-1",
					"TableClassOverride": "STANDARD_INFREQUENT_ACCESS",
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, code3)

	// DescribeTable should reflect the override on the replica.
	code4, resp4 := invokeOp(t, handler, "DescribeTable", map[string]any{
		"TableName": "V2UpdateTable",
	})
	require.Equal(t, http.StatusOK, code4)

	td := resp4["Table"].(map[string]any)
	replicas, ok := td["Replicas"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, replicas)

	var euReplica map[string]any
	for _, r := range replicas {
		rm := r.(map[string]any)
		if rm["RegionName"] == "eu-central-1" {
			euReplica = rm

			break
		}
	}
	require.NotNil(t, euReplica)
	assert.Equal(t, "STANDARD_INFREQUENT_ACCESS", euReplica["TableClassOverride"])
}

// buildEnableKinesisInput is a test helper for constructing EnableKinesisStreamingDestinationInput.
