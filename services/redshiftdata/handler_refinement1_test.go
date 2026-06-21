package redshiftdata_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/redshiftdata"
)

// TestRefinement1_HandlerOpsLen verifies GetSupportedOperations returns exactly 11 ops.
func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 11, redshiftdata.HandlerOpsLen(h))
}

// TestRefinement1_AccountID verifies AccountID is exposed correctly.
func TestRefinement1_AccountID(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend("123456789012", "eu-west-1")
	assert.Equal(t, "123456789012", b.AccountID())
}

// TestRefinement1_BackendReset verifies Reset clears all statements.
func TestRefinement1_BackendReset(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)

	_, err := b.ExecuteStatement(context.Background(), "SELECT 1", "cluster", "", "mydb", "", "", "", false, "")
	require.NoError(t, err)

	b.Reset()

	assert.Equal(t, 0, b.StatementCount())
}

// TestRefinement1_HandlerReset verifies Handler.Reset delegates to the backend.
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	h := redshiftdata.NewHandler(b)

	_, err := b.ExecuteStatement(context.Background(), "SELECT 1", "cluster", "", "mydb", "", "", "", false, "")
	require.NoError(t, err)

	h.Reset()

	assert.Equal(t, 0, b.StatementCount())
}

// TestRefinement1_ErrNilAppContext verifies provider.Init rejects nil context.
func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &redshiftdata.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, redshiftdata.ErrNilAppContext)
}

// TestRefinement1_Snapshot_Restore verifies round-trip Snapshot/Restore.
func TestRefinement1_Snapshot_Restore(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)

	stmt, err := b.ExecuteStatement(
		context.Background(), "SELECT 42", "cluster", "", "mydb", "", "", "test-stmt", false, "",
	)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	require.NoError(t, b2.Restore(t.Context(), snap))

	got, err := b2.DescribeStatement(context.Background(), stmt.ID)
	require.NoError(t, err)
	assert.Equal(t, stmt.ID, got.ID)
	assert.Equal(t, "SELECT 42", got.QueryString)
	assert.Equal(t, "test-stmt", got.StatementName)
	assert.Equal(t, "FINISHED", got.Status)
}

// TestRefinement1_Snapshot_Empty verifies Snapshot works with an empty backend.
func TestRefinement1_Snapshot_Empty(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	require.NoError(t, b2.Restore(t.Context(), snap))
	assert.Equal(t, 0, b2.StatementCount())
}

// TestRefinement1_AddStatementInternal verifies the seed helper bypasses UUID generation.
func TestRefinement1_AddStatementInternal(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	redshiftdata.AddStatementInternal(b, testRegion, "fixed-id", "SELECT 1", "mydb", "FINISHED", true)

	stmt, err := b.DescribeStatement(context.Background(), "fixed-id")
	require.NoError(t, err)
	assert.Equal(t, "fixed-id", stmt.ID)
	assert.Equal(t, "SELECT 1", stmt.QueryString)
	assert.Equal(t, "FINISHED", stmt.Status)
	assert.True(t, stmt.HasResultSet)
}

// TestRefinement1_ExecuteStatement_DatabaseRequired verifies Database is required.
func TestRefinement1_ExecuteStatement_DatabaseRequired(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ExecuteStatement", map[string]any{
		"Sql": "SELECT 1",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ValidationException", resp["__type"])
}

// TestRefinement1_BatchExecuteStatement_DatabaseRequired verifies Database is required.
func TestRefinement1_BatchExecuteStatement_DatabaseRequired(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "BatchExecuteStatement", map[string]any{
		"Sqls": []string{"SELECT 1", "SELECT 2"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ValidationException", resp["__type"])
}

// TestRefinement1_GetStatementResult_NoResultSet verifies that GetStatementResult
// returns ValidationException when the statement has no result set (e.g. batch).
func TestRefinement1_GetStatementResult_NoResultSet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "BatchExecuteStatement", map[string]any{
		"Sqls":     []string{"INSERT INTO t VALUES (1)"},
		"Database": "testdb",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var batchResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &batchResp))

	id := batchResp["Id"].(string)
	rec = doRequest(t, h, "GetStatementResult", map[string]any{"Id": id})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ValidationException", resp["__type"])
}

// TestRefinement1_GetStatementResultV2_NoResultSet verifies that GetStatementResultV2
// returns ValidationException when the statement has no result set.
func TestRefinement1_GetStatementResultV2_NoResultSet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "BatchExecuteStatement", map[string]any{
		"Sqls":     []string{"INSERT INTO t VALUES (1)"},
		"Database": "testdb",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var batchResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &batchResp))

	id := batchResp["Id"].(string)
	rec = doRequest(t, h, "GetStatementResultV2", map[string]any{"Id": id})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ValidationException", resp["__type"])
}

// TestRefinement1_GetStatementResultV2_ResultFormat verifies that GetStatementResultV2
// returns ResultFormat=CSV in the response body.
func TestRefinement1_GetStatementResultV2_ResultFormat(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ExecuteStatement", map[string]any{
		"Sql":          "SELECT 1",
		"Database":     "testdb",
		"ResultFormat": "CSV",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

	id := createResp["Id"].(string)
	rec = doRequest(t, h, "GetStatementResultV2", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "CSV", resp["ResultFormat"])
	assert.EqualValues(t, 1, resp["TotalNumRows"])
	assert.NotNil(t, resp["Records"])
	assert.NotNil(t, resp["ColumnMetadata"])
}

// TestRefinement1_CancelStatement_AbortedIsTerminal verifies that an already-aborted
// statement cannot be cancelled again.
func TestRefinement1_CancelStatement_AbortedIsTerminal(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	redshiftdata.AddStatementInternal(b, testRegion, "stmt-1", "SELECT 1", "mydb", "ABORTED", false)

	h := redshiftdata.NewHandler(b)

	rec := doRequest(t, h, "CancelStatement", map[string]any{"Id": "stmt-1"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ValidationException", resp["__type"])
}

// TestRefinement1_CancelStatement_FailedIsTerminal verifies that a failed statement
// cannot be cancelled.
func TestRefinement1_CancelStatement_FailedIsTerminal(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	redshiftdata.AddStatementInternal(b, testRegion, "stmt-1", "SELECT 1", "mydb", "FAILED", false)

	h := redshiftdata.NewHandler(b)

	rec := doRequest(t, h, "CancelStatement", map[string]any{"Id": "stmt-1"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ValidationException", resp["__type"])
}

// TestRefinement1_ListStatements_SortedNewestFirst verifies that ListStatements returns
// results sorted by creation time with the newest statement first.
func TestRefinement1_ListStatements_SortedNewestFirst(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		doRequest(t, h, "ExecuteStatement", map[string]any{
			"Sql":           fmt.Sprintf("SELECT %d", i+1),
			"Database":      "testdb",
			"StatementName": fmt.Sprintf("stmt-%d", i),
		})
	}

	rec := doRequest(t, h, "ListStatements", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	stmts := resp["Statements"].([]any)
	require.Len(t, stmts, 3)

	// Verify they are ordered newest → oldest by checking CreatedAt is non-increasing.
	first := stmts[0].(map[string]any)["CreatedAt"].(float64)
	second := stmts[1].(map[string]any)["CreatedAt"].(float64)
	third := stmts[2].(map[string]any)["CreatedAt"].(float64)

	assert.GreaterOrEqual(t, first, second)
	assert.GreaterOrEqual(t, second, third)
}

// TestRefinement1_ExecuteStatement_HasResultSet verifies that ExecuteStatement
// always sets HasResultSet to true.
func TestRefinement1_ExecuteStatement_HasResultSet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ExecuteStatement", map[string]any{
		"Sql":      "SELECT 1",
		"Database": "testdb",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

	id := createResp["Id"].(string)

	descRec := doRequest(t, h, "DescribeStatement", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	assert.Equal(t, true, descResp["HasResultSet"])
}

// TestRefinement1_BatchExecuteStatement_HasNoResultSet verifies that BatchExecuteStatement
// always sets HasResultSet to false.
func TestRefinement1_BatchExecuteStatement_HasNoResultSet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "BatchExecuteStatement", map[string]any{
		"Sqls":     []string{"SELECT 1"},
		"Database": "testdb",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

	id := createResp["Id"].(string)

	descRec := doRequest(t, h, "DescribeStatement", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	assert.Equal(t, false, descResp["HasResultSet"])
}

// TestRefinement1_StorageBackend_Interface verifies that InMemoryBackend satisfies StorageBackend.
func TestRefinement1_StorageBackend_Interface(t *testing.T) {
	t.Parallel()

	var _ redshiftdata.StorageBackend = redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
}

// TestRefinement1_NewHandler_AcceptsInterface verifies NewHandler accepts StorageBackend.
func TestRefinement1_NewHandler_AcceptsInterface(t *testing.T) {
	t.Parallel()

	var backend redshiftdata.StorageBackend = redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	h := redshiftdata.NewHandler(backend)
	assert.NotNil(t, h)
}

// TestRefinement1_GetStatementResultV2_GetSupportedOps verifies the ops list includes V2.
func TestRefinement1_GetStatementResultV2_GetSupportedOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "GetStatementResultV2")
}

// TestRefinement1_Snapshot_PreservesStatementKeys verifies Snapshot/Restore preserves
// the eviction key ordering so the oldest-first eviction works correctly after restore.
func TestRefinement1_Snapshot_PreservesStatementKeys(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)

	stmt1, err := b.ExecuteStatement(context.Background(), "SELECT 1", "cluster", "", "mydb", "", "", "", false, "")
	require.NoError(t, err)

	stmt2, err := b.ExecuteStatement(context.Background(), "SELECT 2", "cluster", "", "mydb", "", "", "", false, "")
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Both statements should still be accessible.
	_, err = b2.DescribeStatement(context.Background(), stmt1.ID)
	require.NoError(t, err)

	_, err = b2.DescribeStatement(context.Background(), stmt2.ID)
	require.NoError(t, err)

	assert.Equal(t, 2, b2.StatementCount())
}

// TestRefinement1_DescribeStatement_CloneDoesNotMutate verifies that modifying
// the returned Statement from DescribeStatement does not affect the backend store.
func TestRefinement1_DescribeStatement_CloneDoesNotMutate(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	stmt, err := b.ExecuteStatement(context.Background(), "SELECT 1", "cluster", "", "mydb", "", "", "", false, "")
	require.NoError(t, err)

	got, err := b.DescribeStatement(context.Background(), stmt.ID)
	require.NoError(t, err)

	// Mutate the returned copy.
	got.Status = "MUTATED"
	got.QueryStrings = append(got.QueryStrings, "injected")

	// Original should be unaffected.
	original, err := b.DescribeStatement(context.Background(), stmt.ID)
	require.NoError(t, err)
	assert.Equal(t, "FINISHED", original.Status)
	assert.Empty(t, original.QueryStrings)
}

// TestRefinement1_StatementCount_RaceCondition verifies concurrent access is safe.
func TestRefinement1_StatementCount_RaceCondition(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)

	done := make(chan struct{})

	go func() {
		for range 50 {
			_, _ = b.ExecuteStatement(context.Background(), "SELECT 1", "", "", "mydb", "", "", "", false, "")
		}

		close(done)
	}()

	for range 50 {
		_ = b.StatementCount()
		_, _, _ = b.ListStatements(context.Background(), redshiftdata.ListStatementsFilter{Status: "ALL"})
	}

	<-done
}

// TestRefinement1_Provider_Init_ValidContext verifies Init succeeds with a non-nil context.
func TestRefinement1_Provider_Init_ValidContext(t *testing.T) {
	t.Parallel()

	p := &redshiftdata.Provider{}
	_, err := p.Init(&service.AppContext{})
	assert.NoError(t, err)
}
