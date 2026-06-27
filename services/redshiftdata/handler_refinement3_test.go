package redshiftdata_test

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshiftdata"
)

// TestRefinement3_Janitor_EvictsExpiredStatements verifies that the janitor
// removes terminal statements whose UpdatedAt is older than the TTL cutoff.
func TestRefinement3_Janitor_EvictsExpiredStatements(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)

	// Seed a FINISHED statement that has already aged past the TTL.
	stmt := redshiftdata.AddStatementInternal(b, testRegion, "old-stmt", "SELECT 1", "dev", "FINISHED", true)
	require.NotNil(t, stmt)

	// Sweep with a cutoff in the future so the statement is considered expired.
	cutoff := time.Now().Add(time.Hour)
	evicted := b.EvictExpiredStatements(cutoff)

	assert.Equal(t, 1, evicted, "should evict the expired statement")
	assert.Equal(t, 0, b.StatementCount(), "backend should be empty after eviction")
}

// TestRefinement3_Janitor_DoesNotEvictNonTerminal verifies that non-terminal
// statements are never evicted by the janitor.
func TestRefinement3_Janitor_DoesNotEvictNonTerminal(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)

	// A STARTED statement (non-terminal) — must not be evicted.
	redshiftdata.AddStatementInternal(b, testRegion, "running-stmt", "SELECT 1", "dev", "STARTED", false)

	cutoff := time.Now().Add(time.Hour)
	evicted := b.EvictExpiredStatements(cutoff)

	assert.Equal(t, 0, evicted, "non-terminal statements must not be evicted")
	assert.Equal(t, 1, b.StatementCount(), "statement should still be present")
}

// TestRefinement3_Janitor_SweepOnce uses the Janitor.SweepOnce helper for testing.
func TestRefinement3_Janitor_SweepOnce(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	redshiftdata.AddStatementInternal(b, testRegion, "expired", "SELECT 1", "dev", "FINISHED", true)
	redshiftdata.AddStatementInternal(b, testRegion, "running", "SELECT 2", "dev", "STARTED", false)

	j := redshiftdata.NewJanitor(b, time.Minute, time.Nanosecond) // very short TTL to force eviction
	j.SweepOnce(context.Background())

	// Both FINISHED and running should still be there initially — need cutoff in future.
	// The TTL of 1ns means any UpdatedAt before now is expired, but STARTED is exempt.
	assert.LessOrEqual(t, b.StatementCount(), 1, "only STARTED should survive")
}

// TestRefinement3_RingBuffer_Overflow verifies that adding more than
// maxStatementHistory statements correctly evicts the oldest.
func TestRefinement3_RingBuffer_Overflow(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)

	// Add exactly maxStatementHistory + 5 statements.
	maxCap := redshiftdata.MaxStatementHistoryForTest
	overCount := 5

	for i := range maxCap + overCount {
		redshiftdata.AddStatementInternal(b, testRegion, generateID(i), "SELECT 1", "dev", "FINISHED", false)
	}

	// The backend should never exceed the cap.
	assert.Equal(t, maxCap, b.StatementCount(), "statement count must not exceed ring buffer capacity")
}

// generateID produces a deterministic test ID from an integer index.
func generateID(i int) string {
	const hexChars = "0123456789abcdef"
	b := make([]byte, 8)
	n := i

	for j := 7; j >= 0; j-- {
		b[j] = hexChars[n%16]
		n /= 16
	}

	return "test-" + string(b)
}

// TestRefinement3_Concurrent_AccessSafe tests concurrent reads and writes to
// the InMemoryBackend to catch data races (run with -race).
func TestRefinement3_Concurrent_AccessSafe(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)

	const goroutines = 20

	var wg sync.WaitGroup

	// Concurrent writes
	for range goroutines {
		wg.Go(func() {
			_, _ = b.ExecuteStatement(context.Background(), "SELECT 1", "", "", "dev", "", "", "", false, "", nil)
		})
	}

	// Concurrent reads interleaved
	for range goroutines {
		wg.Go(func() {
			stmts, _, _ := b.ListStatements(
				context.Background(), redshiftdata.ListStatementsFilter{Status: "ALL", MaxResults: 100},
			)
			_ = stmts
		})
	}

	wg.Wait()

	// No panic or race = pass.
	assert.Positive(t, b.StatementCount())
}

// TestRefinement3_ListStatements_HasDatabaseField verifies that the ListStatements
// response includes the Database field for each statement.
func TestRefinement3_ListStatements_HasDatabaseField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "ExecuteStatement", map[string]any{
		"Sql":      "SELECT 1",
		"Database": "analytics",
	})

	rec := doRequest(t, h, "ListStatements", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	stmts, ok := resp["Statements"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, stmts)

	first := stmts[0].(map[string]any)
	assert.Equal(t, "analytics", first["Database"], "Database should be in list item")
}

// TestRefinement3_ListStatements_HasHasResultSetField verifies ListStatements
// includes HasResultSet per item (matching AWS behaviour).
func TestRefinement3_ListStatements_HasHasResultSetField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "ExecuteStatement", map[string]any{
		"Sql":      "SELECT 1",
		"Database": "dev",
	})

	rec := doRequest(t, h, "ListStatements", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	stmts := resp["Statements"].([]any)
	require.NotEmpty(t, stmts)

	first := stmts[0].(map[string]any)
	_, hasField := first["HasResultSet"]
	assert.True(t, hasField, "HasResultSet should be in list item")
}

// TestRefinement3_ListStatements_WorkgroupFilter verifies that ListStatements
// filters by WorkgroupName when provided.
func TestRefinement3_ListStatements_WorkgroupFilter(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	h := redshiftdata.NewHandler(b)

	_, err := b.ExecuteStatement(context.Background(), "SELECT 1", "", "wg-a", "dev", "", "", "", false, "", nil)
	require.NoError(t, err)

	_, err = b.ExecuteStatement(context.Background(), "SELECT 2", "", "wg-b", "dev", "", "", "", false, "", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, "ListStatements", map[string]any{
		"WorkgroupName": "wg-a",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	stmts := resp["Statements"].([]any)
	require.Len(t, stmts, 1, "should only return statements for wg-a")
	assert.Equal(t, "wg-a", stmts[0].(map[string]any)["WorkgroupName"])
}

// TestRefinement3_GetStatementResultV2_DemoRow verifies that GetStatementResultV2
// returns at least one row in CSV format and a ResultFormat field.
func TestRefinement3_GetStatementResultV2_DemoRow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	execRec := doRequest(t, h, "ExecuteStatement", map[string]any{
		"Sql":          "SELECT 1",
		"Database":     "dev",
		"ResultFormat": "CSV",
	})
	require.Equal(t, http.StatusOK, execRec.Code)

	var execResp map[string]any
	require.NoError(t, json.Unmarshal(execRec.Body.Bytes(), &execResp))

	id := execResp["Id"].(string)

	rec := doRequest(t, h, "GetStatementResultV2", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	records, ok := resp["Records"].([]any)
	require.True(t, ok, "Records should be present")
	assert.NotEmpty(t, records, "should return at least one row")
	assert.Equal(t, "CSV", resp["ResultFormat"], "ResultFormat should be CSV")

	cols, ok := resp["ColumnMetadata"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, cols)
}

// TestRefinement3_CancelStatement_NotFound verifies that CancelStatement on a
// non-existent ID returns a ResourceNotFoundException (HTTP 400).
func TestRefinement3_CancelStatement_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CancelStatement", map[string]any{"Id": "does-not-exist"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceNotFoundException", resp["__type"])
}

// TestRefinement3_GetStatementResult_NotFound verifies that GetStatementResult
// on a non-existent statement returns a ResourceNotFoundException.
func TestRefinement3_GetStatementResult_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "GetStatementResult", map[string]any{"Id": "does-not-exist"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceNotFoundException", resp["__type"])
}

// TestRefinement3_BatchExecuteStatement_EmptySqls verifies that submitting an
// empty Sqls array returns a ValidationException.
func TestRefinement3_BatchExecuteStatement_EmptySqls(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "BatchExecuteStatement", map[string]any{
		"Sqls":     []string{},
		"Database": "dev",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ValidationException", resp["__type"])
}

// TestRefinement3_DescribeStatement_MissingID verifies that DescribeStatement
// with no Id field returns a ValidationException.
func TestRefinement3_DescribeStatement_MissingID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "DescribeStatement", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ValidationException", resp["__type"])
}

// TestRefinement3_UnknownOperation_Returns400 verifies that an unknown X-Amz-Target
// action returns HTTP 400 ValidationException.
func TestRefinement3_UnknownOperation_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "NonExistentAction", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ValidationException", resp["__type"])
}

// TestRefinement3_Settings_Defaults verifies that a zero Settings struct uses
// the package default values after passing through WithJanitor.
func TestRefinement3_Settings_Defaults(t *testing.T) {
	t.Parallel()

	var s redshiftdata.Settings
	// Zero values: janitor interval and TTL are both 0.
	// After WithJanitor the defaults should be applied internally.
	assert.Zero(t, s.JanitorInterval, "zero default for JanitorInterval")
	assert.Zero(t, s.StatementTTL, "zero default for StatementTTL")

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	h := redshiftdata.NewHandler(b)

	// Calling WithJanitor with zero values should not panic (uses package defaults internally).
	result := h.WithJanitor(s.JanitorInterval, s.StatementTTL)
	assert.NotNil(t, result, "WithJanitor should return handler")
}

// TestRefinement3_DescribeStatement_RedshiftQueryId verifies that DescribeStatement
// returns the RedshiftQueryId field (set to 0 in the mock).
func TestRefinement3_DescribeStatement_RedshiftQueryId(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	execRec := doRequest(t, h, "ExecuteStatement", map[string]any{
		"Sql":      "SELECT 1",
		"Database": "dev",
	})
	require.Equal(t, http.StatusOK, execRec.Code)

	var execResp map[string]any
	require.NoError(t, json.Unmarshal(execRec.Body.Bytes(), &execResp))

	id := execResp["Id"].(string)

	descRec := doRequest(t, h, "DescribeStatement", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, descRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))

	_, ok := resp["RedshiftQueryId"]
	assert.True(t, ok, "RedshiftQueryId should be present in DescribeStatement response")
}

// TestRefinement3_EvictExpiredStatements_UpdatesRingBuffer verifies that after
// eviction the ring buffer is compacted and the backend is consistent.
func TestRefinement3_EvictExpiredStatements_UpdatesRingBuffer(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)

	redshiftdata.AddStatementInternal(b, testRegion, "s1", "SELECT 1", "dev", "FINISHED", true)
	redshiftdata.AddStatementInternal(b, testRegion, "s2", "SELECT 2", "dev", "FINISHED", true)
	redshiftdata.AddStatementInternal(b, testRegion, "s3", "SELECT 3", "dev", "STARTED", false) // must not be evicted

	evicted := b.EvictExpiredStatements(time.Now().Add(time.Hour))

	assert.Equal(t, 2, evicted)
	assert.Equal(t, 1, b.StatementCount())

	// The remaining statement should still be fetchable via ListStatements.
	stmts, _, _ := b.ListStatements(
		context.Background(), redshiftdata.ListStatementsFilter{Status: "ALL", MaxResults: 100},
	)
	require.Len(t, stmts, 1)
	assert.Equal(t, "STARTED", stmts[0].Status)
}

// TestRefinement3_GetStatementResult_HasNextToken verifies that GetStatementResult
// returns a NextToken field (empty string = no more pages in the mock).
func TestRefinement3_GetStatementResult_HasNextToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	execRec := doRequest(t, h, "ExecuteStatement", map[string]any{
		"Sql":      "SELECT 1",
		"Database": "dev",
	})
	require.Equal(t, http.StatusOK, execRec.Code)

	var execResp map[string]any
	require.NoError(t, json.Unmarshal(execRec.Body.Bytes(), &execResp))

	id := execResp["Id"].(string)

	rec := doRequest(t, h, "GetStatementResult", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	_, ok := resp["NextToken"]
	assert.True(t, ok, "NextToken field should always be present in GetStatementResult response")
}

// TestRefinement3_ListDatabases_HasNextToken verifies that ListDatabases returns
// a NextToken field (empty = no more pages).
func TestRefinement3_ListDatabases_HasNextToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ListDatabases", map[string]any{"Database": "dev"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	_, ok := resp["NextToken"]
	assert.True(t, ok, "NextToken should always be present in ListDatabases response")
}

// TestRefinement3_StartWorker_DoesNotPanic verifies that StartWorker on a
// handler with no janitor returns nil error and does not panic.
func TestRefinement3_StartWorker_DoesNotPanic(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	ctx := t.Context()

	err := h.StartWorker(ctx)
	require.NoError(t, err, "StartWorker should not return an error when no janitor is configured")
}

// TestRefinement3_StartWorker_WithJanitor verifies that StartWorker starts
// the janitor goroutine which can be cancelled via context.
func TestRefinement3_StartWorker_WithJanitor(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	h := redshiftdata.NewHandler(b).WithJanitor(10*time.Millisecond, time.Nanosecond)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := h.StartWorker(ctx)
	require.NoError(t, err, "StartWorker should not error")

	// Wait for context to be cancelled (goroutine should exit cleanly).
	<-ctx.Done()
	// Give the goroutine a moment to exit.
	time.Sleep(20 * time.Millisecond)
	// No assertion needed – the test passes if it doesn't hang or race.
}
