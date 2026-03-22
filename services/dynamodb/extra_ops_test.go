package dynamodb_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// invokeOp is a helper that makes an HTTP request to the DynamoDB handler and
// returns the status code and response body as a generic map.
func invokeOp(
	t *testing.T,
	handler *dynamodb.DynamoDBHandler,
	action string,
	body any,
) (int, map[string]any) {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+action)
	w := httptest.NewRecorder()

	_ = serveEchoHandler(handler.Handler(), w, req)

	var resp map[string]any
	if w.Body.Len() > 0 {
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	}

	return w.Code, resp
}

func TestDynamoDB_GlobalTables(t *testing.T) {
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

func TestDynamoDB_GlobalTable_StatePersistence(t *testing.T) {
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

func TestDynamoDB_KinesisDestinations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body             any
		setup            func(t *testing.T, backend *dynamodb.InMemoryDB, handler *dynamodb.DynamoDBHandler)
		name             string
		action           string
		wantBodyContains string
		wantStatus       int
	}{
		{
			name:   "DescribeKinesisStreamingDestination_empty",
			action: "DescribeKinesisStreamingDestination",
			setup: func(t *testing.T, backend *dynamodb.InMemoryDB, _ *dynamodb.DynamoDBHandler) {
				t.Helper()
				createTableHelper(t, backend, "KinesisTable", "pk")
			},
			body:             map[string]any{"TableName": "KinesisTable"},
			wantStatus:       http.StatusOK,
			wantBodyContains: "KinesisDataStreamDestinations",
		},
		{
			name:             "DescribeKinesisStreamingDestination_table_not_found",
			action:           "DescribeKinesisStreamingDestination",
			body:             map[string]any{"TableName": "NoTable"},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: "ResourceNotFoundException",
		},
		{
			name:   "DisableKinesisStreamingDestination_success",
			action: "DisableKinesisStreamingDestination",
			setup: func(t *testing.T, backend *dynamodb.InMemoryDB, _ *dynamodb.DynamoDBHandler) {
				t.Helper()
				createTableHelper(t, backend, "KinesisDisableTable", "pk")
				backend.AddKinesisDestination("KinesisDisableTable", "arn:aws:kinesis:us-east-1:123:stream/my-stream")
			},
			body: map[string]any{
				"TableName": "KinesisDisableTable",
				"StreamArn": "arn:aws:kinesis:us-east-1:123:stream/my-stream",
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: "DISABLING",
		},
		{
			name:   "DisableKinesisStreamingDestination_stream_not_found",
			action: "DisableKinesisStreamingDestination",
			setup: func(t *testing.T, backend *dynamodb.InMemoryDB, _ *dynamodb.DynamoDBHandler) {
				t.Helper()
				createTableHelper(t, backend, "KinesisDisableTable2", "pk")
			},
			body: map[string]any{
				"TableName": "KinesisDisableTable2",
				"StreamArn": "arn:aws:kinesis:us-east-1:123:stream/no-such-stream",
			},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: "ResourceNotFoundException",
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

func TestDynamoDB_KinesisDestinations_StatePersistence(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)

	createTableHelper(t, backend, "KinesisStateTable", "pk")

	streamARN := "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream"
	backend.AddKinesisDestination("KinesisStateTable", streamARN)

	// Verify stream is tracked
	code, resp := invokeOp(t, handler, "DescribeKinesisStreamingDestination", map[string]any{
		"TableName": "KinesisStateTable",
	})
	require.Equal(t, http.StatusOK, code)

	destinations, ok := resp["KinesisDataStreamDestinations"].([]any)
	require.True(t, ok)
	require.Len(t, destinations, 1)

	dest := destinations[0].(map[string]any)
	assert.Equal(t, streamARN, dest["StreamArn"])
	assert.Equal(t, "ACTIVE", dest["DestinationStatus"])

	// Disable the stream
	code2, resp2 := invokeOp(t, handler, "DisableKinesisStreamingDestination", map[string]any{
		"TableName": "KinesisStateTable",
		"StreamArn": streamARN,
	})
	require.Equal(t, http.StatusOK, code2)
	assert.Equal(t, "DISABLING", resp2["DestinationStatus"])

	// Verify stream is removed
	code3, resp3 := invokeOp(t, handler, "DescribeKinesisStreamingDestination", map[string]any{
		"TableName": "KinesisStateTable",
	})
	require.Equal(t, http.StatusOK, code3)

	destinations2, ok2 := resp3["KinesisDataStreamDestinations"].([]any)
	require.True(t, ok2)
	assert.Empty(t, destinations2)
}

func TestDynamoDB_DescribeLimits(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)

	code, resp := invokeOp(t, handler, "DescribeLimits", map[string]any{})
	require.Equal(t, http.StatusOK, code)

	assert.NotNil(t, resp["AccountMaxReadCapacityUnits"])
	assert.NotNil(t, resp["AccountMaxWriteCapacityUnits"])
	assert.NotNil(t, resp["TableMaxReadCapacityUnits"])
	assert.NotNil(t, resp["TableMaxWriteCapacityUnits"])

	// Values should be positive numbers
	rcu, ok := resp["AccountMaxReadCapacityUnits"].(float64)
	require.True(t, ok)
	assert.Greater(t, rcu, float64(0))
}

func TestDynamoDB_DescribeEndpoints(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)

	code, resp := invokeOp(t, handler, "DescribeEndpoints", map[string]any{})
	require.Equal(t, http.StatusOK, code)

	endpoints, ok := resp["Endpoints"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, endpoints)

	ep := endpoints[0].(map[string]any)
	assert.NotEmpty(t, ep["Address"])
	assert.NotNil(t, ep["CachePeriodInMinutes"])
}

func TestDynamoDB_DescribeContributorInsights(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		setup      func(t *testing.T, backend *dynamodb.InMemoryDB)
		name       string
		wantStatus int
	}{
		{
			name: "success_disabled",
			setup: func(t *testing.T, backend *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, backend, "ContribTable", "pk")
			},
			body:       map[string]any{"TableName": "ContribTable"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "table_not_found",
			body:       map[string]any{"TableName": "NoSuchTable"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := dynamodb.NewInMemoryDB()
			handler := dynamodb.NewHandler(backend)

			if tt.setup != nil {
				tt.setup(t, backend)
			}

			code, resp := invokeOp(t, handler, "DescribeContributorInsights", tt.body)
			assert.Equal(t, tt.wantStatus, code)

			if tt.wantStatus == http.StatusOK {
				assert.Equal(t, "DISABLED", resp["ContributorInsightsStatus"])
				assert.NotNil(t, resp["ContributorInsightsRuleList"])
			}
		})
	}
}

func TestDynamoDB_DeleteResourcePolicy(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)

	code, _ := invokeOp(t, handler, "DeleteResourcePolicy", map[string]any{
		"ResourceArn": "arn:aws:dynamodb:us-east-1:123456789012:table/MyTable",
	})
	assert.Equal(t, http.StatusOK, code)
}

func TestDynamoDB_DescribeImport(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body             any
		name             string
		wantBodyContains string
		wantStatus       int
	}{
		{
			name: "success",
			body: map[string]any{
				"ImportArn": "arn:aws:dynamodb:us-east-1:123456789012:table/MyTable/import/01000000-0000-0000-0000-000000000001",
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: "COMPLETED",
		},
		{
			name:             "empty_import_arn",
			body:             map[string]any{"ImportArn": ""},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := dynamodb.NewInMemoryDB()
			handler := dynamodb.NewHandler(backend)

			code, resp := invokeOp(t, handler, "DescribeImport", tt.body)
			assert.Equal(t, tt.wantStatus, code)

			if tt.wantBodyContains != "" {
				bodyBytes, _ := json.Marshal(resp)
				assert.Contains(t, string(bodyBytes), tt.wantBodyContains)
			}
		})
	}
}

func TestDynamoDB_EnableKinesisStreamingDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body             any
		setup            func(t *testing.T, backend *dynamodb.InMemoryDB)
		name             string
		wantBodyContains string
		wantStatus       int
	}{
		{
			name: "success",
			setup: func(t *testing.T, backend *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, backend, "EnableKinesisTable", "pk")
			},
			body: map[string]any{
				"TableName": "EnableKinesisTable",
				"StreamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream",
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: "ENABLING",
		},
		{
			name: "table_not_found",
			body: map[string]any{
				"TableName": "NoTable",
				"StreamArn": "arn:aws:kinesis:us-east-1:123:stream/s",
			},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: "ResourceNotFoundException",
		},
		{
			name:             "missing_stream_arn",
			body:             map[string]any{"TableName": "SomeTable"},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := dynamodb.NewInMemoryDB()
			handler := dynamodb.NewHandler(backend)

			if tt.setup != nil {
				tt.setup(t, backend)
			}

			code, resp := invokeOp(t, handler, "EnableKinesisStreamingDestination", tt.body)
			assert.Equal(t, tt.wantStatus, code)

			if tt.wantBodyContains != "" {
				bodyBytes, _ := json.Marshal(resp)
				assert.Contains(t, string(bodyBytes), tt.wantBodyContains)
			}
		})
	}
}

func TestDynamoDB_EnableDisableKinesis_StatePersistence(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)
	createTableHelper(t, backend, "KinesisFullTable", "pk")

	streamARN := "arn:aws:kinesis:us-east-1:123456789012:stream/full-stream"

	// Enable the stream
	code, resp := invokeOp(t, handler, "EnableKinesisStreamingDestination", map[string]any{
		"TableName": "KinesisFullTable",
		"StreamArn": streamARN,
	})
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "ENABLING", resp["DestinationStatus"])

	// Verify it appears as ACTIVE in DescribeKinesisStreamingDestination
	code2, resp2 := invokeOp(t, handler, "DescribeKinesisStreamingDestination", map[string]any{
		"TableName": "KinesisFullTable",
	})
	require.Equal(t, http.StatusOK, code2)

	destinations, ok := resp2["KinesisDataStreamDestinations"].([]any)
	require.True(t, ok)
	require.Len(t, destinations, 1)
	assert.Equal(t, streamARN, destinations[0].(map[string]any)["StreamArn"])

	// Enable the same stream again (idempotent)
	code3, _ := invokeOp(t, handler, "EnableKinesisStreamingDestination", map[string]any{
		"TableName": "KinesisFullTable",
		"StreamArn": streamARN,
	})
	require.Equal(t, http.StatusOK, code3)

	// Still only one destination
	code4, resp4 := invokeOp(t, handler, "DescribeKinesisStreamingDestination", map[string]any{
		"TableName": "KinesisFullTable",
	})
	require.Equal(t, http.StatusOK, code4)

	destinations2, ok2 := resp4["KinesisDataStreamDestinations"].([]any)
	require.True(t, ok2)
	assert.Len(t, destinations2, 1)

	// Disable the stream
	code5, _ := invokeOp(t, handler, "DisableKinesisStreamingDestination", map[string]any{
		"TableName": "KinesisFullTable",
		"StreamArn": streamARN,
	})
	require.Equal(t, http.StatusOK, code5)

	// Verify no destinations remain
	code6, resp6 := invokeOp(t, handler, "DescribeKinesisStreamingDestination", map[string]any{
		"TableName": "KinesisFullTable",
	})
	require.Equal(t, http.StatusOK, code6)

	destinations3, ok3 := resp6["KinesisDataStreamDestinations"].([]any)
	require.True(t, ok3)
	assert.Empty(t, destinations3)
}

func TestDynamoDB_ListGlobalTables(t *testing.T) {
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

	// Pagination: limit 2
	code2, resp2 := invokeOp(t, handler, "ListGlobalTables", map[string]any{"Limit": 2})
	require.Equal(t, http.StatusOK, code2)

	tables2, ok2 := resp2["GlobalTables"].([]any)
	require.True(t, ok2)
	assert.Len(t, tables2, 2)
	assert.NotEmpty(t, resp2["LastEvaluatedGlobalTableName"])
}

func TestDynamoDB_ResourcePolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body             any
		name             string
		action           string
		wantBodyContains string
		wantStatus       int
	}{
		{
			name:       "GetResourcePolicy_success",
			action:     "GetResourcePolicy",
			body:       map[string]any{"ResourceArn": "arn:aws:dynamodb:us-east-1:123:table/MyTable"},
			wantStatus: http.StatusOK,
		},
		{
			name:             "GetResourcePolicy_missing_arn",
			action:           "GetResourcePolicy",
			body:             map[string]any{"ResourceArn": ""},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: "ValidationException",
		},
		{
			name:   "PutResourcePolicy_success",
			action: "PutResourcePolicy",
			body: map[string]any{
				"ResourceArn": "arn:aws:dynamodb:us-east-1:123:table/MyTable",
				"Policy":      `{"Version":"2012-10-17","Statement":[]}`,
			},
			wantStatus: http.StatusOK,
		},
		{
			name:             "PutResourcePolicy_missing_policy",
			action:           "PutResourcePolicy",
			body:             map[string]any{"ResourceArn": "arn:aws:dynamodb:us-east-1:123:table/MyTable"},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: "ValidationException",
		},
		{
			name:       "DeleteResourcePolicy_success",
			action:     "DeleteResourcePolicy",
			body:       map[string]any{"ResourceArn": "arn:aws:dynamodb:us-east-1:123:table/MyTable"},
			wantStatus: http.StatusOK,
		},
		{
			name:             "DeleteResourcePolicy_missing_arn",
			action:           "DeleteResourcePolicy",
			body:             map[string]any{"ResourceArn": ""},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := dynamodb.NewInMemoryDB()
			handler := dynamodb.NewHandler(backend)

			code, resp := invokeOp(t, handler, tt.action, tt.body)
			assert.Equal(t, tt.wantStatus, code)

			if tt.wantBodyContains != "" {
				bodyBytes, _ := json.Marshal(resp)
				assert.Contains(t, string(bodyBytes), tt.wantBodyContains)
			}
		})
	}
}

func TestDynamoDB_GlobalTable_PersistenceRoundTrip(t *testing.T) {
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
	snap := original.Snapshot()
	require.NotNil(t, snap)

	fresh := dynamodb.NewInMemoryDB()
	require.NoError(t, fresh.Restore(snap))

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
