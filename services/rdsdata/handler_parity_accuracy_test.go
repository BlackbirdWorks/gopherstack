package rdsdata_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	parityResourceARN = "arn:aws:rds:us-east-1:000000000000:cluster:parity-cluster"
	paritySecretARN   = "arn:aws:secretsmanager:us-east-1:000000000000:secret:parity-secret"
)

// TestParityAccuracy_IncludeResultMetadata verifies that columnMetadata is omitted
// by default and only included when includeResultMetadata=true, matching real AWS
// RDS Data API behavior.
func TestParityAccuracy_IncludeResultMetadata(t *testing.T) {
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
				"resourceArn":           parityResourceARN,
				"secretArn":             paritySecretARN,
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

// TestParityAccuracy_ExecuteStatement_ResponseShape verifies the full response
// field set for ExecuteStatement matches real AWS structure.
func TestParityAccuracy_ExecuteStatement_ResponseShape(t *testing.T) {
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
				"resourceArn": parityResourceARN,
				"secretArn":   paritySecretARN,
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

// TestParityAccuracy_TransactionStatus_Constants verifies the exact status strings
// returned by CommitTransaction and RollbackTransaction match real AWS responses.
func TestParityAccuracy_TransactionStatus_Constants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		wantStatus string
	}{
		{
			name:       "commit_returns_exact_string",
			path:       "/CommitTransaction",
			wantStatus: "Transaction committed",
		},
		{
			name:       "rollback_returns_exact_string",
			path:       "/RollbackTransaction",
			wantStatus: "Transaction rolled back",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			beginRec := doRDSDataRequest(t, h, "/BeginTransaction", map[string]any{
				"resourceArn": parityResourceARN,
				"secretArn":   paritySecretARN,
			})
			require.Equal(t, http.StatusOK, beginRec.Code)

			var beginResp map[string]any
			require.NoError(t, json.Unmarshal(beginRec.Body.Bytes(), &beginResp))
			txID := beginResp["transactionId"].(string)

			rec := doRDSDataRequest(t, h, tt.path, map[string]any{
				"resourceArn":   parityResourceARN,
				"secretArn":     paritySecretARN,
				"transactionId": txID,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantStatus, resp["transactionStatus"],
				"transactionStatus must match exact AWS string")
		})
	}
}

// TestParityAccuracy_TransactionNotFound_ErrorType verifies that operations on
// a non-existent transaction return TransactionNotFoundException with HTTP 400,
// matching real AWS RDS Data API error semantics.
func TestParityAccuracy_TransactionNotFound_ErrorType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "execute_statement", path: "/Execute"},
		{name: "batch_execute", path: "/BatchExecute"},
		{name: "commit", path: "/CommitTransaction"},
		{name: "rollback", path: "/RollbackTransaction"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{
				"resourceArn":   parityResourceARN,
				"secretArn":     paritySecretARN,
				"sql":           "SELECT 1",
				"transactionId": "txn-nonexistent-abc123",
			}

			rec := doRDSDataRequest(t, h, tt.path, body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "TransactionNotFoundException", resp["__type"],
				"path %s: error type must be TransactionNotFoundException", tt.path)
		})
	}
}

// TestParityAccuracy_BatchExecuteStatement_OneResultPerParamSet verifies that
// BatchExecuteStatement returns exactly one UpdateResult per parameter set,
// matching real AWS behavior.
func TestParityAccuracy_BatchExecuteStatement_OneResultPerParamSet(t *testing.T) {
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
				"resourceArn": parityResourceARN,
				"secretArn":   paritySecretARN,
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

// TestParityAccuracy_BeginTransaction_UniqueIDs verifies that each BeginTransaction
// call returns a distinct transactionId, matching real AWS behavior.
func TestParityAccuracy_BeginTransaction_UniqueIDs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seen := make(map[string]bool)

	for range 5 {
		rec := doRDSDataRequest(t, h, "/BeginTransaction", map[string]any{
			"resourceArn": parityResourceARN,
			"secretArn":   paritySecretARN,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		txID, ok := resp["transactionId"].(string)
		require.True(t, ok)
		require.NotEmpty(t, txID)
		assert.False(t, seen[txID], "transactionId %q was returned twice", txID)
		seen[txID] = true
	}
}

// TestParityAccuracy_CommitThenReuse verifies that a committed transaction cannot
// be reused for Execute, Commit, or Rollback — matching real AWS behavior where
// committed transactions are removed from the active set.
func TestParityAccuracy_CommitThenReuse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "execute_after_commit", path: "/Execute"},
		{name: "commit_after_commit", path: "/CommitTransaction"},
		{name: "rollback_after_commit", path: "/RollbackTransaction"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			beginRec := doRDSDataRequest(t, h, "/BeginTransaction", map[string]any{
				"resourceArn": parityResourceARN,
				"secretArn":   paritySecretARN,
			})
			require.Equal(t, http.StatusOK, beginRec.Code)

			var beginResp map[string]any
			require.NoError(t, json.Unmarshal(beginRec.Body.Bytes(), &beginResp))
			txID := beginResp["transactionId"].(string)

			commitRec := doRDSDataRequest(t, h, "/CommitTransaction", map[string]any{
				"resourceArn":   parityResourceARN,
				"secretArn":     paritySecretARN,
				"transactionId": txID,
			})
			require.Equal(t, http.StatusOK, commitRec.Code)

			rec := doRDSDataRequest(t, h, tt.path, map[string]any{
				"resourceArn":   parityResourceARN,
				"secretArn":     paritySecretARN,
				"sql":           "SELECT 1",
				"transactionId": txID,
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code,
				"path %s: reusing committed txID must fail", tt.path)

			var errResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, "TransactionNotFoundException", errResp["__type"])
		})
	}
}

// TestParityAccuracy_ExecuteStatement_WithTransaction verifies that ExecuteStatement
// within a valid transaction succeeds, and that statementing outside the transaction
// after commit fails — matching real AWS atomicity semantics.
func TestParityAccuracy_ExecuteStatement_WithTransaction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		closeWith  string
		wantStatus int
	}{
		{
			name:       "commit_then_execute_fails",
			closeWith:  "/CommitTransaction",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "rollback_then_execute_fails",
			closeWith:  "/RollbackTransaction",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			beginRec := doRDSDataRequest(t, h, "/BeginTransaction", map[string]any{
				"resourceArn": parityResourceARN,
				"secretArn":   paritySecretARN,
			})
			require.Equal(t, http.StatusOK, beginRec.Code)

			var beginResp map[string]any
			require.NoError(t, json.Unmarshal(beginRec.Body.Bytes(), &beginResp))
			txID := beginResp["transactionId"].(string)

			// Execute within transaction — should succeed.
			execRec := doRDSDataRequest(t, h, "/Execute", map[string]any{
				"resourceArn":   parityResourceARN,
				"secretArn":     paritySecretARN,
				"sql":           "INSERT INTO t VALUES (1)",
				"transactionId": txID,
			})
			require.Equal(t, http.StatusOK, execRec.Code)

			// Close transaction.
			closeRec := doRDSDataRequest(t, h, tt.closeWith, map[string]any{
				"resourceArn":   parityResourceARN,
				"secretArn":     paritySecretARN,
				"transactionId": txID,
			})
			require.Equal(t, http.StatusOK, closeRec.Code)

			// Execute after close — must fail.
			rec := doRDSDataRequest(t, h, "/Execute", map[string]any{
				"resourceArn":   parityResourceARN,
				"secretArn":     paritySecretARN,
				"sql":           "INSERT INTO t VALUES (2)",
				"transactionId": txID,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
