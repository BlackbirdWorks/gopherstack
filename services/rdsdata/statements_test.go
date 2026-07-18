package rdsdata_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rdsdata"
)

const (
	stmtResourceARN = "arn:aws:rds:us-east-1:000000000000:cluster:parity-cluster"
	stmtSecretARN   = "arn:aws:secretsmanager:us-east-1:000000000000:secret:parity-secret"
)

func TestHandler_ExecuteStatement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantBody   string
		bodyRaw    []byte
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
				"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
				"sql":         "SELECT 1",
			},
			wantStatus: http.StatusOK,
			wantBody:   "records",
		},
		{
			name:       "invalid_json",
			bodyRaw:    []byte("not-json"),
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_resource_arn",
			body: map[string]any{
				"sql": "SELECT 1",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_sql",
			body: map[string]any{
				"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
				"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder
			if tt.bodyRaw != nil {
				rec = doRDSDataRawRequest(t, h, "/Execute", tt.bodyRaw)
			} else {
				rec = doRDSDataRequest(t, h, "/Execute", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_ExecuteStatement_WithTransaction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	beginRec := doRDSDataRequest(t, h, "/BeginTransaction", map[string]any{
		"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
	})
	require.Equal(t, http.StatusOK, beginRec.Code)

	var beginResp map[string]any
	require.NoError(t, json.Unmarshal(beginRec.Body.Bytes(), &beginResp))
	txID := beginResp["transactionId"].(string)

	rec := doRDSDataRequest(t, h, "/Execute", map[string]any{
		"resourceArn":   "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"secretArn":     "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
		"sql":           "INSERT INTO test VALUES (1)",
		"transactionId": txID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ExecuteStatement_InvalidTransaction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRDSDataRequest(t, h, "/Execute", map[string]any{
		"resourceArn":   "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"secretArn":     "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
		"sql":           "INSERT INTO test VALUES (1)",
		"transactionId": "txn-does-not-exist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_ExecuteStatement_TracksStatement verifies statements are recorded.
func TestHandler_ExecuteStatement_TracksStatement(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	h := rdsdata.NewHandler(b)

	rec := doRDSDataRequest(t, h, "/Execute", map[string]any{
		"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
		"sql":         "UPDATE t SET a = 1",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, rdsdata.ExecutedStatementCount(b))
}

// TestHandler_ExecuteStatement_Response verifies the full response shape.
func TestHandler_ExecuteStatement_Response(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRDSDataRequest(t, h, "/Execute", map[string]any{
		"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
		"sql":         "SELECT 1",
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	_, hasRecords := resp["records"]
	assert.True(t, hasRecords, "records key must be present")
	// columnMetadata is omitted when includeResultMetadata is not set (real AWS behavior).
	_, hasCols := resp["columnMetadata"]
	assert.False(t, hasCols, "columnMetadata must be absent when includeResultMetadata is false")
	_, hasUpdated := resp["numberOfRecordsUpdated"]
	assert.True(t, hasUpdated, "numberOfRecordsUpdated key must be present")
	_, hasGenerated := resp["generatedFields"]
	assert.True(t, hasGenerated, "generatedFields key must be present")
}

// TestHandler_ExecuteStatement_IncludeResultMetadata verifies that columnMetadata is
// omitted by default and only included when includeResultMetadata=true, matching real
// AWS RDS Data API behavior.
func TestHandler_ExecuteStatement_IncludeResultMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		includeMetadata    bool
		wantColumnMetadata bool
	}{
		{
			name:               "omitted_by_default",
			includeMetadata:    false,
			wantColumnMetadata: false,
		},
		{
			name:               "included_when_true",
			includeMetadata:    true,
			wantColumnMetadata: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{
				"resourceArn":           stmtResourceARN,
				"secretArn":             stmtSecretARN,
				"sql":                   "SELECT 1",
				"includeResultMetadata": tt.includeMetadata,
			}

			rec := doRDSDataRequest(t, h, "/Execute", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			_, hasCols := resp["columnMetadata"]
			assert.Equal(t, tt.wantColumnMetadata, hasCols,
				"columnMetadata presence mismatch for includeResultMetadata=%v", tt.includeMetadata)
		})
	}
}

// TestHandler_ExecuteStatement_ResponseShape verifies the full response
// field set for ExecuteStatement matches real AWS structure.
func TestHandler_ExecuteStatement_ResponseShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		sql    string
		fields []string
	}{
		{
			name:   "select_without_metadata",
			sql:    "SELECT 1",
			fields: []string{"records", "numberOfRecordsUpdated", "generatedFields"},
		},
		{
			name:   "insert_without_metadata",
			sql:    "INSERT INTO t VALUES (1)",
			fields: []string{"records", "numberOfRecordsUpdated", "generatedFields"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRDSDataRequest(t, h, "/Execute", map[string]any{
				"resourceArn": stmtResourceARN,
				"secretArn":   stmtSecretARN,
				"sql":         tt.sql,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			for _, field := range tt.fields {
				_, ok := resp[field]
				assert.True(t, ok, "field %q must be present in response", field)
			}
		})
	}
}

// TestHandler_ExecuteStatement_FormatRecordsAsJSON verifies that formatRecordsAs=JSON
// on a SELECT returns formattedRecords (a JSON string of the result set) and
// omits records/columnMetadata, matching real AWS RDS Data API behavior.
func TestHandler_ExecuteStatement_FormatRecordsAsJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRDSDataRequest(t, h, "/Execute", map[string]any{
		"resourceArn":           stmtResourceARN,
		"secretArn":             stmtSecretARN,
		"sql":                   "SELECT 42",
		"formatRecordsAs":       "JSON",
		"includeResultMetadata": true,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	_, hasRecords := resp["records"]
	assert.False(t, hasRecords, "records must be omitted when formatRecordsAs=JSON")
	_, hasCols := resp["columnMetadata"]
	assert.False(t, hasCols, "columnMetadata must be omitted when formatRecordsAs=JSON")

	formatted, ok := resp["formattedRecords"].(string)
	require.True(t, ok, "formattedRecords must be a JSON string")

	var rows []map[string]any
	require.NoError(t, json.Unmarshal([]byte(formatted), &rows))
	require.Len(t, rows, 1)
	require.Len(t, rows[0], 1, "row must have exactly one column")

	for _, v := range rows[0] {
		assert.InDelta(t, float64(42), v, 0)
	}
}

// TestHandler_ExecuteStatement_FormatRecordsAsIgnoredForDML verifies that
// formatRecordsAs is ignored for non-SELECT statements, matching real AWS
// behavior ("This parameter only applies to SELECT statements").
func TestHandler_ExecuteStatement_FormatRecordsAsIgnoredForDML(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRDSDataRequest(t, h, "/Execute", map[string]any{
		"resourceArn":     stmtResourceARN,
		"secretArn":       stmtSecretARN,
		"sql":             "INSERT INTO t VALUES (1)",
		"formatRecordsAs": "JSON",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	_, hasFormatted := resp["formattedRecords"]
	assert.False(t, hasFormatted, "formattedRecords must not appear for a DML statement")
	_, hasRecords := resp["records"]
	assert.True(t, hasRecords, "records must still be present for a DML statement")
}

// TestHandler_ExecuteStatement_FormatRecordsAsInvalid verifies that an unrecognized
// formatRecordsAs value is rejected as a BadRequestException, matching real
// AWS enum validation.
func TestHandler_ExecuteStatement_FormatRecordsAsInvalid(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRDSDataRequest(t, h, "/Execute", map[string]any{
		"resourceArn":     stmtResourceARN,
		"secretArn":       stmtSecretARN,
		"sql":             "SELECT 1",
		"formatRecordsAs": "XML",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "BadRequestException", resp["__type"])
}

// TestHandler_ExecuteStatement_ColumnMetadataFullShape verifies that columnMetadata
// entries carry the full real-AWS field set (not just name/typeName), and
// that the JDBC-style type/nullable codes are populated per column.
func TestHandler_ExecuteStatement_ColumnMetadataFullShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRDSDataRequest(t, h, "/Execute", map[string]any{
		"resourceArn":           stmtResourceARN,
		"secretArn":             stmtSecretARN,
		"sql":                   "SELECT 42",
		"includeResultMetadata": true,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	cols, ok := resp["columnMetadata"].([]any)
	require.True(t, ok)
	require.Len(t, cols, 1)

	col, ok := cols[0].(map[string]any)
	require.True(t, ok)

	for _, key := range []string{
		"arrayBaseColumnType", "isAutoIncrement", "isCaseSensitive", "isCurrency",
		"isSigned", "label", "name", "nullable", "precision", "scale",
		"schemaName", "tableName", "type", "typeName",
	} {
		_, present := col[key]
		assert.True(t, present, "columnMetadata must include %q", key)
	}
}

// TestHandler_ExecuteStatement_SQLParameterTypeHint verifies that a typeHint on a
// parameter is accepted (not rejected as an unknown field) and the statement
// still executes successfully, matching real AWS wire acceptance of typeHint.
func TestHandler_ExecuteStatement_SQLParameterTypeHint(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRDSDataRequest(t, h, "/Execute", map[string]any{
		"resourceArn": stmtResourceARN,
		"secretArn":   stmtSecretARN,
		"sql":         "SELECT :d",
		"parameters": []any{
			map[string]any{
				"name":     "d",
				"typeHint": "DECIMAL",
				"value":    map[string]any{"stringValue": "3.14"},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_BatchExecuteStatement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		wantBody   string
		bodyRaw    []byte
		wantStatus int
	}{
		{
			name: "success_with_params",
			body: map[string]any{
				"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
				"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
				"sql":         "INSERT INTO test VALUES (:val)",
				"parameterSets": []any{
					[]any{map[string]any{"name": "val", "value": map[string]any{"stringValue": "a"}}},
				},
			},
			wantStatus: http.StatusOK,
			wantBody:   "updateResults",
		},
		{
			name: "success_empty_params",
			body: map[string]any{
				"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
				"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
				"sql":         "INSERT INTO test VALUES (:val)",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_json",
			bodyRaw:    []byte("not-json"),
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_resource_arn",
			body: map[string]any{
				"sql": "INSERT INTO test VALUES (:val)",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_sql_batch",
			body: map[string]any{
				"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
				"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid_transaction_id",
			body: map[string]any{
				"resourceArn":   "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
				"secretArn":     "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
				"sql":           "INSERT INTO test VALUES (:val)",
				"transactionId": "txn-does-not-exist",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder
			if tt.bodyRaw != nil {
				rec = doRDSDataRawRequest(t, h, "/BatchExecute", tt.bodyRaw)
			} else {
				rec = doRDSDataRequest(t, h, "/BatchExecute", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestHandler_BatchExecuteStatement_EmptyParamSets verifies empty updateResults slice (not nil).
func TestHandler_BatchExecuteStatement_EmptyParamSets(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRDSDataRequest(t, h, "/BatchExecute", map[string]any{
		"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
		"sql":         "INSERT INTO t VALUES (:v)",
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	results, ok := resp["updateResults"]
	require.True(t, ok, "updateResults key must exist")
	assert.NotNil(t, results, "updateResults must not be null")
}

// TestHandler_BatchExecuteStatement_TracksStatement verifies batch statements are recorded.
func TestHandler_BatchExecuteStatement_TracksStatement(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	h := rdsdata.NewHandler(b)

	rec := doRDSDataRequest(t, h, "/BatchExecute", map[string]any{
		"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
		"sql":         "DELETE FROM t WHERE id = :id",
		"parameterSets": []any{
			[]any{map[string]any{"name": "id", "value": map[string]any{"longValue": 1}}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, rdsdata.ExecutedStatementCount(b))
}

// TestHandler_BatchExecuteStatement_MultipleParamSets verifies multiple result sets.
func TestHandler_BatchExecuteStatement_MultipleParamSets(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRDSDataRequest(t, h, "/BatchExecute", map[string]any{
		"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
		"sql":         "INSERT INTO t VALUES (:v)",
		"parameterSets": []any{
			[]any{map[string]any{"name": "v", "value": map[string]any{"stringValue": "a"}}},
			[]any{map[string]any{"name": "v", "value": map[string]any{"stringValue": "b"}}},
			[]any{map[string]any{"name": "v", "value": map[string]any{"stringValue": "c"}}},
		},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	results := resp["updateResults"].([]any)
	assert.Len(t, results, 3)
}

// TestHandler_BatchExecuteStatement_OneResultPerParamSet verifies that
// BatchExecuteStatement returns exactly one UpdateResult per parameter set,
// matching real AWS behavior.
func TestHandler_BatchExecuteStatement_OneResultPerParamSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		paramSetCount int
	}{
		{name: "zero_param_sets", paramSetCount: 0},
		{name: "one_param_set", paramSetCount: 1},
		{name: "three_param_sets", paramSetCount: 3},
		{name: "ten_param_sets", paramSetCount: 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			paramSets := make([]any, tt.paramSetCount)
			for i := range paramSets {
				paramSets[i] = []any{
					map[string]any{
						"name":  "val",
						"value": map[string]any{"longValue": i},
					},
				}
			}

			h := newTestHandler(t)
			body := map[string]any{
				"resourceArn": stmtResourceARN,
				"secretArn":   stmtSecretARN,
				"sql":         "INSERT INTO t VALUES (:val)",
			}
			if tt.paramSetCount > 0 {
				body["parameterSets"] = paramSets
			}

			rec := doRDSDataRequest(t, h, "/BatchExecute", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			results, ok := resp["updateResults"].([]any)
			require.True(t, ok, "updateResults must be an array")
			assert.Len(t, results, tt.paramSetCount,
				"must have one UpdateResult per parameter set")
		})
	}
}

func TestBackend_ListExecutedStatements(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")

	_, _, _, err := b.ExecuteStatement(
		context.Background(),
		"arn:aws:rds:us-east-1:000000000000:cluster:test",
		"SELECT 1",
		"",
	)
	require.NoError(t, err)

	stmts := b.ListExecutedStatements(context.Background())
	require.Len(t, stmts, 1)
	assert.Equal(t, "SELECT 1", stmts[0].SQL)
}

// TestBackend_ListExecutedStatements_Empty verifies empty list on fresh backend.
func TestBackend_ListExecutedStatements_Empty(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	stmts := b.ListExecutedStatements(context.Background())
	assert.NotNil(t, stmts)
	assert.Empty(t, stmts)
}
