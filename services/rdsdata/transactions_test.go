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
	txnResourceARN = "arn:aws:rds:us-east-1:000000000000:cluster:parity-cluster"
	txnSecretARN   = "arn:aws:secretsmanager:us-east-1:000000000000:secret:parity-secret"
)

func TestHandler_BeginTransaction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		name       string
		bodyRaw    []byte
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
				"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_json",
			bodyRaw:    []byte("not-json"),
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_resource_arn",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var rec *httptest.ResponseRecorder
			if tt.bodyRaw != nil {
				rec = doRDSDataRawRequest(t, h, "/BeginTransaction", tt.bodyRaw)
			} else {
				rec = doRDSDataRequest(t, h, "/BeginTransaction", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if rec.Code == http.StatusOK {
				assert.Contains(t, rec.Body.String(), "transactionId")
			}
		})
	}
}

// TestHandler_BeginTransaction_ReturnsTransactionID verifies the response.
func TestHandler_BeginTransaction_ReturnsTransactionID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRDSDataRequest(t, h, "/BeginTransaction", map[string]any{
		"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	txID, ok := resp["transactionId"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, txID)
}

// TestHandler_BeginTransaction_UniqueIDs verifies that each BeginTransaction
// call returns a distinct transactionId, matching real AWS behavior.
func TestHandler_BeginTransaction_UniqueIDs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seen := make(map[string]bool)

	for range 5 {
		rec := doRDSDataRequest(t, h, "/BeginTransaction", map[string]any{
			"resourceArn": txnResourceARN,
			"secretArn":   txnSecretARN,
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

func TestHandler_CommitTransaction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       any
		bodyRaw    []byte
		wantStatus int
		startTxn   bool
	}{
		{
			name:       "success",
			startTxn:   true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_json",
			bodyRaw:    []byte("not-json"),
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_transaction_id",
			body: map[string]any{
				"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
				"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "transaction_not_found",
			body: map[string]any{
				"resourceArn":   "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
				"secretArn":     "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
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

			switch {
			case tt.startTxn:
				beginRec := doRDSDataRequest(t, h, "/BeginTransaction", map[string]any{
					"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
					"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
				})
				require.Equal(t, http.StatusOK, beginRec.Code)

				var beginResp map[string]any
				require.NoError(t, json.Unmarshal(beginRec.Body.Bytes(), &beginResp))
				txID := beginResp["transactionId"].(string)

				rec = doRDSDataRequest(t, h, "/CommitTransaction", map[string]any{
					"resourceArn":   "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
					"secretArn":     "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
					"transactionId": txID,
				})
			case tt.bodyRaw != nil:
				rec = doRDSDataRawRequest(t, h, "/CommitTransaction", tt.bodyRaw)
			default:
				rec = doRDSDataRequest(t, h, "/CommitTransaction", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if rec.Code == http.StatusOK {
				assert.Contains(t, rec.Body.String(), "transactionStatus")
			}
		})
	}
}

func TestHandler_RollbackTransaction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       any
		bodyRaw    []byte
		wantStatus int
		startTxn   bool
	}{
		{
			name:       "success",
			startTxn:   true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_json",
			bodyRaw:    []byte("not-json"),
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_transaction_id",
			body: map[string]any{
				"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
				"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "transaction_not_found",
			body: map[string]any{
				"resourceArn":   "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
				"secretArn":     "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
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

			switch {
			case tt.startTxn:
				beginRec := doRDSDataRequest(t, h, "/BeginTransaction", map[string]any{
					"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
					"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
				})
				require.Equal(t, http.StatusOK, beginRec.Code)

				var beginResp map[string]any
				require.NoError(t, json.Unmarshal(beginRec.Body.Bytes(), &beginResp))
				txID := beginResp["transactionId"].(string)

				rec = doRDSDataRequest(t, h, "/RollbackTransaction", map[string]any{
					"resourceArn":   "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
					"secretArn":     "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
					"transactionId": txID,
				})
			case tt.bodyRaw != nil:
				rec = doRDSDataRawRequest(t, h, "/RollbackTransaction", tt.bodyRaw)
			default:
				rec = doRDSDataRequest(t, h, "/RollbackTransaction", tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
			if rec.Code == http.StatusOK {
				assert.Contains(t, rec.Body.String(), "transactionStatus")
			}
		})
	}
}

// TestHandler_TransactionLifecycle verifies begin->execute->commit lifecycle.
func TestHandler_TransactionLifecycle(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	h := rdsdata.NewHandler(b)

	// Begin
	beginRec := doRDSDataRequest(t, h, "/BeginTransaction", map[string]any{
		"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
	})
	require.Equal(t, http.StatusOK, beginRec.Code)
	assert.Equal(t, 1, rdsdata.TransactionCount(b))

	var beginResp map[string]any
	require.NoError(t, json.Unmarshal(beginRec.Body.Bytes(), &beginResp))
	txID := beginResp["transactionId"].(string)

	// Execute
	execRec := doRDSDataRequest(t, h, "/Execute", map[string]any{
		"resourceArn":   "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"secretArn":     "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
		"sql":           "INSERT INTO t VALUES (1)",
		"transactionId": txID,
	})
	require.Equal(t, http.StatusOK, execRec.Code)

	// Commit
	commitRec := doRDSDataRequest(t, h, "/CommitTransaction", map[string]any{
		"resourceArn":   "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"secretArn":     "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
		"transactionId": txID,
	})
	require.Equal(t, http.StatusOK, commitRec.Code)
	assert.Equal(t, 0, rdsdata.TransactionCount(b))

	var commitResp map[string]any
	require.NoError(t, json.Unmarshal(commitRec.Body.Bytes(), &commitResp))
	assert.Equal(t, "Transaction committed", commitResp["transactionStatus"])
}

// TestHandler_TransactionRollbackLifecycle verifies begin->rollback lifecycle.
func TestHandler_TransactionRollbackLifecycle(t *testing.T) {
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

	rollbackRec := doRDSDataRequest(t, h, "/RollbackTransaction", map[string]any{
		"resourceArn":   "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"secretArn":     "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
		"transactionId": txID,
	})
	require.Equal(t, http.StatusOK, rollbackRec.Code)

	var rollbackResp map[string]any
	require.NoError(t, json.Unmarshal(rollbackRec.Body.Bytes(), &rollbackResp))
	assert.Equal(t, "Transaction rolled back", rollbackResp["transactionStatus"])
}

// TestHandler_CommitAfterRollback_Fails verifies committed/rolled-back transactions can't be reused.
func TestHandler_CommitAfterRollback_Fails(t *testing.T) {
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

	// Rollback first
	rollbackRec := doRDSDataRequest(t, h, "/RollbackTransaction", map[string]any{
		"resourceArn":   "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"secretArn":     "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
		"transactionId": txID,
	})
	require.Equal(t, http.StatusOK, rollbackRec.Code)

	// Now commit should fail
	commitRec := doRDSDataRequest(t, h, "/CommitTransaction", map[string]any{
		"resourceArn":   "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"secretArn":     "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
		"transactionId": txID,
	})
	assert.Equal(t, http.StatusBadRequest, commitRec.Code)
}

// TestHandler_CommitThenReuse verifies that a committed transaction cannot
// be reused for Execute, Commit, or Rollback — matching real AWS behavior where
// committed transactions are removed from the active set.
func TestHandler_CommitThenReuse(t *testing.T) {
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
				"resourceArn": txnResourceARN,
				"secretArn":   txnSecretARN,
			})
			require.Equal(t, http.StatusOK, beginRec.Code)

			var beginResp map[string]any
			require.NoError(t, json.Unmarshal(beginRec.Body.Bytes(), &beginResp))
			txID := beginResp["transactionId"].(string)

			commitRec := doRDSDataRequest(t, h, "/CommitTransaction", map[string]any{
				"resourceArn":   txnResourceARN,
				"secretArn":     txnSecretARN,
				"transactionId": txID,
			})
			require.Equal(t, http.StatusOK, commitRec.Code)

			rec := doRDSDataRequest(t, h, tt.path, map[string]any{
				"resourceArn":   txnResourceARN,
				"secretArn":     txnSecretARN,
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

// TestHandler_ExecuteAfterTransactionClose verifies that ExecuteStatement
// within a valid transaction succeeds, and that statementing outside the transaction
// after commit fails — matching real AWS atomicity semantics.
func TestHandler_ExecuteAfterTransactionClose(t *testing.T) {
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
				"resourceArn": txnResourceARN,
				"secretArn":   txnSecretARN,
			})
			require.Equal(t, http.StatusOK, beginRec.Code)

			var beginResp map[string]any
			require.NoError(t, json.Unmarshal(beginRec.Body.Bytes(), &beginResp))
			txID := beginResp["transactionId"].(string)

			// Execute within transaction — should succeed.
			execRec := doRDSDataRequest(t, h, "/Execute", map[string]any{
				"resourceArn":   txnResourceARN,
				"secretArn":     txnSecretARN,
				"sql":           "INSERT INTO t VALUES (1)",
				"transactionId": txID,
			})
			require.Equal(t, http.StatusOK, execRec.Code)

			// Close transaction.
			closeRec := doRDSDataRequest(t, h, tt.closeWith, map[string]any{
				"resourceArn":   txnResourceARN,
				"secretArn":     txnSecretARN,
				"transactionId": txID,
			})
			require.Equal(t, http.StatusOK, closeRec.Code)

			// Execute after close — must fail.
			rec := doRDSDataRequest(t, h, "/Execute", map[string]any{
				"resourceArn":   txnResourceARN,
				"secretArn":     txnSecretARN,
				"sql":           "INSERT INTO t VALUES (2)",
				"transactionId": txID,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_BatchExecuteWithTransaction verifies batch execute respects a transaction.
func TestHandler_BatchExecuteWithTransaction(t *testing.T) {
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

	rec := doRDSDataRequest(t, h, "/BatchExecute", map[string]any{
		"resourceArn":   "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"secretArn":     "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
		"sql":           "INSERT INTO t VALUES (:val)",
		"transactionId": txID,
		"parameterSets": []any{
			[]any{map[string]any{"name": "val", "value": map[string]any{"stringValue": "x"}}},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestHandler_TransactionStatusConstants verifies the exact status strings
// returned by CommitTransaction and RollbackTransaction match real AWS responses.
func TestHandler_TransactionStatusConstants(t *testing.T) {
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
				"resourceArn": txnResourceARN,
				"secretArn":   txnSecretARN,
			})
			require.Equal(t, http.StatusOK, beginRec.Code)

			var beginResp map[string]any
			require.NoError(t, json.Unmarshal(beginRec.Body.Bytes(), &beginResp))
			txID := beginResp["transactionId"].(string)

			rec := doRDSDataRequest(t, h, tt.path, map[string]any{
				"resourceArn":   txnResourceARN,
				"secretArn":     txnSecretARN,
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

// TestHandler_TransactionNotFoundErrorType verifies that operations on
// a non-existent transaction return TransactionNotFoundException with HTTP 400,
// matching real AWS RDS Data API error semantics.
func TestHandler_TransactionNotFoundErrorType(t *testing.T) {
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
				"resourceArn":   txnResourceARN,
				"secretArn":     txnSecretARN,
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

// TestHandler_TransactionNotFoundErrorShape verifies the __type field in the error response.
func TestHandler_TransactionNotFoundErrorShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRDSDataRequest(t, h, "/CommitTransaction", map[string]any{
		"resourceArn":   "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"secretArn":     "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
		"transactionId": "txn-nonexistent",
	})

	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "TransactionNotFoundException", errResp["__type"])
	assert.NotEmpty(t, errResp["message"])
}

func TestBackend_ListTransactions(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")

	txID, err := b.BeginTransaction(context.Background(), "arn:aws:rds:us-east-1:000000000000:cluster:test")
	require.NoError(t, err)

	txns := b.ListTransactions(context.Background())
	assert.Contains(t, txns, txID)
}

// TestBackend_ListTransactions_Empty verifies empty map on fresh backend.
func TestBackend_ListTransactions_Empty(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	txns := b.ListTransactions(context.Background())
	assert.NotNil(t, txns)
	assert.Empty(t, txns)
}

// TestBackend_CommitTransaction_StatusConstant verifies the commit response matches AWS.
func TestBackend_CommitTransaction_StatusConstant(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")

	txID, err := b.BeginTransaction(context.Background(), "arn")
	require.NoError(t, err)

	status, err := b.CommitTransaction(context.Background(), txID)
	require.NoError(t, err)
	assert.Equal(t, "Transaction committed", status)
}

// TestBackend_RollbackTransaction_StatusConstant verifies the rollback response matches AWS.
func TestBackend_RollbackTransaction_StatusConstant(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")

	txID, err := b.BeginTransaction(context.Background(), "arn")
	require.NoError(t, err)

	status, err := b.RollbackTransaction(context.Background(), txID)
	require.NoError(t, err)
	assert.Equal(t, "Transaction rolled back", status)
}
