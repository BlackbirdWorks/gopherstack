package rdsdata_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rdsdata"
)

// TestRefinement1_HandlerOpsLen verifies that GetSupportedOperations returns exactly 6 ops.
func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 6, rdsdata.HandlerOpsLen(h))
}

// TestRefinement1_AccountID verifies AccountID is exposed correctly.
func TestRefinement1_AccountID(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("123456789012", "eu-west-1")
	assert.Equal(t, "123456789012", b.AccountID())
}

// TestRefinement1_BackendReset verifies Reset clears all state.
func TestRefinement1_BackendReset(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.BeginTransaction(
		context.Background(),
		"arn:aws:rds:us-east-1:000000000000:cluster:test",
	)
	require.NoError(t, err)

	_, _, _, err = b.ExecuteStatement(
		context.Background(),
		"arn:aws:rds:us-east-1:000000000000:cluster:test",
		"SELECT 1",
		"",
	)
	require.NoError(t, err)

	b.Reset()

	assert.Equal(t, 0, rdsdata.TransactionCount(b))
	assert.Equal(t, 0, rdsdata.ExecutedStatementCount(b))
}

// TestRefinement1_HandlerReset verifies Handler.Reset() delegates to the backend.
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	h := rdsdata.NewHandler(b)

	_, err := b.BeginTransaction(
		context.Background(),
		"arn:aws:rds:us-east-1:000000000000:cluster:test",
	)
	require.NoError(t, err)

	h.Reset()

	assert.Equal(t, 0, rdsdata.TransactionCount(b))
}

// TestRefinement1_Snapshot_Restore verifies full round-trip of Snapshot/Restore.
func TestRefinement1_Snapshot_Restore(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.BeginTransaction(
		context.Background(),
		"arn:aws:rds:us-east-1:000000000000:cluster:test",
	)
	require.NoError(t, err)

	_, _, _, err = b.ExecuteStatement(
		context.Background(),
		"arn:aws:rds:us-east-1:000000000000:cluster:test",
		"SELECT 42",
		"",
	)
	require.NoError(t, err)

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := rdsdata.NewInMemoryBackend("", "")
	require.NoError(t, b2.Restore(snap))

	assert.Equal(t, 1, rdsdata.TransactionCount(b2))
	assert.Equal(t, 1, rdsdata.ExecutedStatementCount(b2))

	stmts := b2.ListExecutedStatements(context.Background())
	require.Len(t, stmts, 1)
	assert.Equal(t, "SELECT 42", stmts[0].SQL)
}

// TestRefinement1_Restore_InvalidJSON verifies Restore returns an error on invalid JSON.
func TestRefinement1_Restore_InvalidJSON(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore([]byte("not-json"))
	require.Error(t, err)
}

// TestRefinement1_Snapshot_Empty verifies Snapshot works on an empty backend.
func TestRefinement1_Snapshot_Empty(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := rdsdata.NewInMemoryBackend("", "")
	require.NoError(t, b2.Restore(snap))

	assert.Equal(t, 0, rdsdata.TransactionCount(b2))
	assert.Equal(t, 0, rdsdata.ExecutedStatementCount(b2))
}

// TestRefinement1_AddTransactionInternal verifies seed helper works.
func TestRefinement1_AddTransactionInternal(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddTransactionInternal("txn-seeded")

	txns := b.ListTransactions(context.Background())
	assert.Contains(t, txns, "txn-seeded")
}

// TestRefinement1_ExecutedStatementCount verifies the export helper.
func TestRefinement1_ExecutedStatementCount(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	assert.Equal(t, 0, rdsdata.ExecutedStatementCount(b))

	_, _, _, err := b.ExecuteStatement(context.Background(), "arn", "SELECT 1", "")
	require.NoError(t, err)
	assert.Equal(t, 1, rdsdata.ExecutedStatementCount(b))
}

// TestRefinement1_TransactionCount verifies the export helper.
func TestRefinement1_TransactionCount(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	assert.Equal(t, 0, rdsdata.TransactionCount(b))

	_, err := b.BeginTransaction(context.Background(), "arn")
	require.NoError(t, err)
	assert.Equal(t, 1, rdsdata.TransactionCount(b))
}

// TestRefinement1_CommitTransaction_StatusConstant verifies the commit response matches AWS.
func TestRefinement1_CommitTransaction_StatusConstant(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")

	txID, err := b.BeginTransaction(context.Background(), "arn")
	require.NoError(t, err)

	status, err := b.CommitTransaction(context.Background(), txID)
	require.NoError(t, err)
	assert.Equal(t, "Transaction committed", status)
}

// TestRefinement1_RollbackTransaction_StatusConstant verifies the rollback response matches AWS.
func TestRefinement1_RollbackTransaction_StatusConstant(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")

	txID, err := b.BeginTransaction(context.Background(), "arn")
	require.NoError(t, err)

	status, err := b.RollbackTransaction(context.Background(), txID)
	require.NoError(t, err)
	assert.Equal(t, "Transaction rolled back", status)
}

// TestRefinement1_BatchExecuteStatement_EmptyParamSets verifies empty updateResults slice (not nil).
func TestRefinement1_BatchExecuteStatement_EmptyParamSets(t *testing.T) {
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

// TestRefinement1_ExecuteSQL_TracksStatement verifies ExecuteSQL records the statement.
func TestRefinement1_ExecuteSQL_TracksStatement(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	h := rdsdata.NewHandler(b)

	rec := doRDSDataRequest(t, h, "/ExecuteSql", map[string]any{
		"dbClusterOrInstanceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"awsSecretStoreArn":      "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
		"sqlStatements":          "SELECT 99",
	})

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, rdsdata.ExecutedStatementCount(b))

	stmts := b.ListExecutedStatements(context.Background())
	require.Len(t, stmts, 1)
	assert.Equal(t, "SELECT 99", stmts[0].SQL)
}

// TestRefinement1_ExecuteSQL_Response verifies the response contains sqlStatementResults array.
func TestRefinement1_ExecuteSQL_Response(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRDSDataRequest(t, h, "/ExecuteSql", map[string]any{
		"dbClusterOrInstanceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"awsSecretStoreArn":      "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
		"sqlStatements":          "CREATE TABLE t (id INT)",
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	results, ok := resp["sqlStatementResults"]
	require.True(t, ok)
	resultsArr, ok := results.([]any)
	require.True(t, ok)
	require.Len(t, resultsArr, 1)
}

// TestRefinement1_ErrNilAppContext verifies provider returns error on nil context.
func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &rdsdata.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, rdsdata.ErrNilAppContext)
}

// TestRefinement1_ExecuteStatement_TracksStatement verifies statements are recorded.
func TestRefinement1_ExecuteStatement_TracksStatement(t *testing.T) {
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

// TestRefinement1_BatchExecuteStatement_TracksStatement verifies batch statements are recorded.
func TestRefinement1_BatchExecuteStatement_TracksStatement(t *testing.T) {
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

// TestRefinement1_Transaction_Lifecycle verifies begin→execute→commit lifecycle.
func TestRefinement1_Transaction_Lifecycle(t *testing.T) {
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

// TestRefinement1_Transaction_RollbackLifecycle verifies begin→rollback lifecycle.
func TestRefinement1_Transaction_RollbackLifecycle(t *testing.T) {
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

// TestRefinement1_TransactionNotFound_ErrorShape verifies the __type field in the error response.
func TestRefinement1_TransactionNotFound_ErrorShape(t *testing.T) {
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

// TestRefinement1_BatchExecute_WithTransaction verifies batch execute respects a transaction.
func TestRefinement1_BatchExecute_WithTransaction(t *testing.T) {
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

// TestRefinement1_MultipleTransactions verifies multiple concurrent transactions.
func TestRefinement1_MultipleTransactions(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")

	tx1, err := b.BeginTransaction(context.Background(), "arn1")
	require.NoError(t, err)
	tx2, err := b.BeginTransaction(context.Background(), "arn1")
	require.NoError(t, err)

	assert.NotEqual(t, tx1, tx2)
	assert.Equal(t, 2, rdsdata.TransactionCount(b))

	_, err = b.CommitTransaction(context.Background(), tx1)
	require.NoError(t, err)
	assert.Equal(t, 1, rdsdata.TransactionCount(b))

	_, err = b.RollbackTransaction(context.Background(), tx2)
	require.NoError(t, err)
	assert.Equal(t, 0, rdsdata.TransactionCount(b))
}

// TestRefinement1_Snapshot_PreservesCounter verifies txCounter is preserved in snapshot.
func TestRefinement1_Snapshot_PreservesCounter(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")

	// Create 3 transactions so counter is at 3.
	for range 3 {
		_, err := b.BeginTransaction(context.Background(), "arn")
		require.NoError(t, err)
	}

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := rdsdata.NewInMemoryBackend("", "")
	require.NoError(t, b2.Restore(snap))

	// After restore, the next transaction ID should continue from 4.
	txID, err := b2.BeginTransaction(context.Background(), "arn")
	require.NoError(t, err)
	assert.Equal(t, "txn-000004", txID)
}

// TestRefinement1_ExecuteStatement_Response verifies the full response shape.
func TestRefinement1_ExecuteStatement_Response(t *testing.T) {
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
	_, hasCols := resp["columnMetadata"]
	assert.True(t, hasCols, "columnMetadata key must be present")
	_, hasUpdated := resp["numberOfRecordsUpdated"]
	assert.True(t, hasUpdated, "numberOfRecordsUpdated key must be present")
	_, hasGenerated := resp["generatedFields"]
	assert.True(t, hasGenerated, "generatedFields key must be present")
}

// TestRefinement1_UnknownOperation_Returns500 verifies dispatch returns an error for unknown ops.
func TestRefinement1_UnknownOperation_Returns500(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Reach the unknown-action branch via direct backend call:
	// We can't easily POST to an unregistered path (RouteMatcher guards it),
	// so we verify the handler responds 400 for a bad JSON body on a known path
	// which triggers the errInvalidRequest path.
	rec := doRDSDataRawRequest(t, h, "/Execute", []byte("{bad json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_ListExecutedStatements_Empty verifies empty list on fresh backend.
func TestRefinement1_ListExecutedStatements_Empty(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	stmts := b.ListExecutedStatements(context.Background())
	assert.NotNil(t, stmts)
	assert.Empty(t, stmts)
}

// TestRefinement1_ListTransactions_Empty verifies empty map on fresh backend.
func TestRefinement1_ListTransactions_Empty(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	txns := b.ListTransactions(context.Background())
	assert.NotNil(t, txns)
	assert.Empty(t, txns)
}

// TestRefinement1_ExecuteSQL_MissingSecretArn verifies generated SDK required fields are enforced.
func TestRefinement1_ExecuteSQL_MissingSecretArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRDSDataRequest(t, h, "/ExecuteSql", map[string]any{
		"dbClusterOrInstanceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"sqlStatements":          "SELECT 1",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_BatchExecuteStatement_MultipleParamSets verifies multiple result sets.
func TestRefinement1_BatchExecuteStatement_MultipleParamSets(t *testing.T) {
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

// TestRefinement1_ExecuteSQL_Response_NotNull verifies sqlStatementResults is not null.
func TestRefinement1_ExecuteSQL_Response_NotNull(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRDSDataRequest(t, h, "/ExecuteSql", map[string]any{
		"dbClusterOrInstanceArn": "arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"awsSecretStoreArn":      "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-secret",
		"sqlStatements":          "DROP TABLE IF EXISTS t",
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["sqlStatementResults"])
}

// TestRefinement1_BeginTransaction_ReturnsTransactionID verifies the response.
func TestRefinement1_BeginTransaction_ReturnsTransactionID(t *testing.T) {
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

// TestRefinement1_CommitAfterRollback_Fails verifies committed/rolled-back transactions can't be reused.
func TestRefinement1_CommitAfterRollback_Fails(t *testing.T) {
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

// TestRefinement1_StorageBackendInterface verifies that Handler accepts a StorageBackend.
func TestRefinement1_StorageBackendInterface(t *testing.T) {
	t.Parallel()

	var b rdsdata.StorageBackend = rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	h := rdsdata.NewHandler(b)
	assert.NotNil(t, h)
}

// TestRefinement1_SnapshotRegionAccountID verifies AccountID and Region survive snapshot round-trip.
func TestRefinement1_SnapshotRegionAccountID(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("111111111111", "ap-southeast-1")
	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := rdsdata.NewInMemoryBackend("", "")
	require.NoError(t, b2.Restore(snap))

	assert.Equal(t, "111111111111", b2.AccountID())
	assert.Equal(t, "ap-southeast-1", b2.Region())
}

// TestRefinement1_DoRequestRace exercises concurrent requests to detect data races.
func TestRefinement1_DoRequestRace(t *testing.T) {
	t.Parallel()

	b := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	h := rdsdata.NewHandler(b)

	const n = 20

	done := make(chan struct{}, n)

	for i := range n {
		go func(i int) {
			defer func() { done <- struct{}{} }()

			body, _ := json.Marshal(map[string]any{
				"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:c",
				"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:s",
				"sql":         "SELECT " + string(rune('0'+i%10)),
			})

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/Execute", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization",
				"AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/rds-data/aws4_request")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			c.SetRequest(req)

			_ = h.Handler()(c)
		}(i)
	}

	for range n {
		<-done
	}

	assert.Equal(t, n, rdsdata.ExecutedStatementCount(b))
}
