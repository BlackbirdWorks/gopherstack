package redshiftdata_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshiftdata"
)

// TestRefinement2_CancelStatement_SuccessStatusBoolean verifies AWS boolean response shape.
func TestRefinement2_CancelStatement_SuccessStatusBoolean(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	redshiftdata.AddStatementInternal(b, "stmt-pending", "SELECT 1", "mydb", "STARTED", false)

	h := redshiftdata.NewHandler(b)

	rec := doRequest(t, h, "CancelStatement", map[string]any{"Id": "stmt-pending"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	status, ok := resp["Status"].(bool)
	require.True(t, ok, "Status should be a boolean")
	assert.True(t, status)
}

// TestRefinement2_BatchExecuteStatement_QueryStringIsFirstSQL verifies that
// BatchExecuteStatement sets QueryString to the first SQL in the batch, matching
// real AWS behaviour (used by DescribeStatement and ListStatements).
func TestRefinement2_BatchExecuteStatement_QueryStringIsFirstSQL(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "BatchExecuteStatement", map[string]any{
		"Sqls":     []string{"SELECT 1", "SELECT 2", "SELECT 3"},
		"Database": "testdb",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

	id := createResp["Id"].(string)

	descRec := doRequest(t, h, "DescribeStatement", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, descRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))

	assert.Equal(t, "SELECT 1", resp["QueryString"], "QueryString should equal the first SQL")
}

// TestRefinement2_DescribeStatement_ResultSizeAndRows verifies that DescribeStatement
// includes ResultSize and ResultRows for a FINISHED single statement.
func TestRefinement2_DescribeStatement_ResultSizeAndRows(t *testing.T) {
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

	var resp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))

	assert.NotNil(t, resp["ResultRows"], "ResultRows should be present")
	assert.NotNil(t, resp["ResultSize"], "ResultSize should be present")
	assert.EqualValues(t, 1, resp["ResultRows"])
}

// TestRefinement2_DescribeStatement_SubStatements verifies that DescribeStatement
// for a batch includes a non-empty SubStatements array with per-SQL details.
func TestRefinement2_DescribeStatement_SubStatements(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	sqls := []string{"SELECT 1", "SELECT 2"}
	rec := doRequest(t, h, "BatchExecuteStatement", map[string]any{
		"Sqls":     sqls,
		"Database": "testdb",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

	id := createResp["Id"].(string)

	descRec := doRequest(t, h, "DescribeStatement", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, descRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))

	subs, ok := resp["SubStatements"].([]any)
	require.True(t, ok, "SubStatements should be present")
	assert.Len(t, subs, 2, "should have one sub-statement per SQL")

	first := subs[0].(map[string]any)
	assert.Equal(t, "SELECT 1", first["QueryString"])
	assert.Equal(t, "FINISHED", first["Status"])
}

// TestRefinement2_GetStatementResult_ReturnsDemoRow verifies that GetStatementResult
// returns at least one row and one column in the demo result set.
func TestRefinement2_GetStatementResult_ReturnsDemoRow(t *testing.T) {
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

	resultRec := doRequest(t, h, "GetStatementResult", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, resultRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(resultRec.Body.Bytes(), &resp))

	records, ok := resp["Records"].([]any)
	require.True(t, ok, "Records should be a slice")
	assert.NotEmpty(t, records, "should return at least one row")

	cols, ok := resp["ColumnMetadata"].([]any)
	require.True(t, ok, "ColumnMetadata should be a slice")
	assert.NotEmpty(t, cols, "should return at least one column")

	assert.EqualValues(t, 1, resp["TotalNumRows"])
}

// TestRefinement2_ListStatements_MaxResults limits the page size.
func TestRefinement2_ListStatements_MaxResults(t *testing.T) {
	t.Parallel()

	// testPaginationStatements is the number of statements to create to test pagination.
	// It must be greater than testMaxResultsLimit so that results are truncated.
	const (
		testPaginationStatements = 5
		testMaxResultsLimit      = 3
	)

	h := newTestHandler(t)

	for range testPaginationStatements {
		doRequest(t, h, "ExecuteStatement", map[string]any{
			"Sql":      "SELECT 1",
			"Database": "testdb",
		})
	}

	rec := doRequest(t, h, "ListStatements", map[string]any{
		"MaxResults": testMaxResultsLimit,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	stmts := resp["Statements"].([]any)
	assert.Len(t, stmts, testMaxResultsLimit, "should be truncated to MaxResults")

	// A next-token should be present when there are more results.
	assert.NotEmpty(t, resp["NextToken"], "NextToken should be set when results are truncated")
}

// TestRefinement2_ListStatements_MaxResultsTooLarge returns ValidationException.
func TestRefinement2_ListStatements_MaxResultsTooLarge(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ListStatements", map[string]any{
		"MaxResults": 101,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ValidationException", resp["__type"])
}

// TestRefinement2_DescribeTable_ReturnsDemoColumns verifies that DescribeTable
// returns a non-empty ColumnList with at least one column descriptor.
func TestRefinement2_DescribeTable_ReturnsDemoColumns(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "DescribeTable", map[string]any{
		"Database": "testdb",
		"Schema":   "public",
		"Table":    "users",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	cols, ok := resp["ColumnList"].([]any)
	require.True(t, ok, "ColumnList should be a slice")
	assert.NotEmpty(t, cols, "should return at least one column")

	firstCol := cols[0].(map[string]any)
	assert.NotEmpty(t, firstCol["name"], "column name should be populated")
	assert.NotEmpty(t, firstCol["typeName"], "column typeName should be populated")
}

// TestRefinement2_WithEvent_StoredAndReturned verifies that WithEvent is stored
// and reflected back in DescribeStatement.
func TestRefinement2_WithEvent_StoredAndReturned(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ExecuteStatement", map[string]any{
		"Sql":       "SELECT 1",
		"Database":  "testdb",
		"WithEvent": true,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

	id := createResp["Id"].(string)

	descRec := doRequest(t, h, "DescribeStatement", map[string]any{"Id": id})
	require.Equal(t, http.StatusOK, descRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))

	assert.Equal(t, true, resp["WithEvent"], "WithEvent should be reflected in DescribeStatement")
}

// TestRefinement2_ListDatabases_ReturnsDemoData verifies that ListDatabases returns
// a non-empty list of demo databases.
func TestRefinement2_ListDatabases_ReturnsDemoData(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ListDatabases", map[string]any{
		"Database": "dev",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	dbs, ok := resp["Databases"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, dbs, "should return at least one database")
}

// TestRefinement2_ListSchemas_ReturnsDemoData verifies that ListSchemas returns
// a non-empty list of demo schemas.
func TestRefinement2_ListSchemas_ReturnsDemoData(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ListSchemas", map[string]any{
		"Database":      "dev",
		"SchemaPattern": "%",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	schemas, ok := resp["Schemas"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, schemas, "should return at least one schema")
}

// TestRefinement2_ListTables_ReturnsDemoData verifies that ListTables returns
// a non-empty list of demo tables.
func TestRefinement2_ListTables_ReturnsDemoData(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ListTables", map[string]any{
		"Database":      "dev",
		"SchemaPattern": "%",
		"TablePattern":  "%",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tables, ok := resp["Tables"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, tables, "should return at least one table")

	first := tables[0].(map[string]any)
	assert.NotEmpty(t, first["name"], "table name should be populated")
	assert.NotEmpty(t, first["schema"], "table schema should be populated")
}

// TestRefinement2_ListStatements_StatusFilter verifies that ListStatements
// properly filters by Status.
func TestRefinement2_ListStatements_StatusFilter(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	redshiftdata.AddStatementInternal(b, "stmt-finished", "SELECT 1", "mydb", "FINISHED", true)
	redshiftdata.AddStatementInternal(b, "stmt-failed", "SELECT 2", "mydb", "FAILED", false)

	h := redshiftdata.NewHandler(b)

	rec := doRequest(t, h, "ListStatements", map[string]any{
		"Status": "FAILED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	stmts := resp["Statements"].([]any)
	require.Len(t, stmts, 1)
	assert.Equal(t, "FAILED", stmts[0].(map[string]any)["Status"])
}
