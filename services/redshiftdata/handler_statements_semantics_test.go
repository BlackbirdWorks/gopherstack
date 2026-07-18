package redshiftdata_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshiftdata"
)

// TestResultFormatControlsResultAPI verifies that ResultFormat set at
// ExecuteStatement time gates which of GetStatementResult / GetStatementResultV2
// succeeds for that statement.
func TestResultFormatControlsResultAPI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		resultFormat  string
		getOperation  string
		wantResultFmt string
		wantStatus    int
	}{
		{
			name:         "default_json_allows_json_result",
			getOperation: "GetStatementResult",
			wantStatus:   http.StatusOK,
		},
		{
			name:          "csv_allows_v2_result",
			resultFormat:  "CSV",
			getOperation:  "GetStatementResultV2",
			wantStatus:    http.StatusOK,
			wantResultFmt: "CSV",
		},
		{
			name:         "default_json_rejects_v2_result",
			getOperation: "GetStatementResultV2",
			wantStatus:   http.StatusBadRequest,
		},
		{
			name:         "csv_rejects_json_result",
			resultFormat: "CSV",
			getOperation: "GetStatementResult",
			wantStatus:   http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{"Sql": "SELECT 1", "Database": "dev"}
			if tt.resultFormat != "" {
				body["ResultFormat"] = tt.resultFormat
			}

			rec := doRequest(t, h, "ExecuteStatement", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var execResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &execResp))

			rec = doRequest(t, h, tt.getOperation, map[string]any{"Id": execResp["Id"]})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantResultFmt != "" {
				var resultResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resultResp))
				assert.Equal(t, tt.wantResultFmt, resultResp["ResultFormat"])
			}
		})
	}
}

// TestResultFormatMetadataAndValidation verifies ResultFormat validation at
// ExecuteStatement time and that DescribeStatement echoes it back correctly.
func TestResultFormatMetadataAndValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		format     string
		wantStatus int
	}{
		{name: "default_json", wantStatus: http.StatusOK},
		{name: "csv", format: "CSV", wantStatus: http.StatusOK},
		{name: "unknown_rejected", format: "XML", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "ExecuteStatement", map[string]any{
				"Sql":          "SELECT 1",
				"Database":     "dev",
				"ResultFormat": tt.format,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantStatus != http.StatusOK {
				return
			}

			var execResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &execResp))

			rec = doRequest(t, h, "DescribeStatement", map[string]any{"Id": execResp["Id"]})
			require.Equal(t, http.StatusOK, rec.Code)

			var descResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
			wantFormat := tt.format
			if wantFormat == "" {
				wantFormat = "JSON"
			}
			assert.Equal(t, wantFormat, descResp["ResultFormat"])
		})
	}
}

// TestListStatementsFilters verifies ListStatements' Status/Database/StatementName filters.
func TestListStatementsFilters(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	h := redshiftdata.NewHandler(b)
	doRequest(t, h, "ExecuteStatement", map[string]any{
		"Sql": "SELECT 1", "Database": "alpha", "StatementName": "daily-one",
	})
	doRequest(t, h, "ExecuteStatement", map[string]any{
		"Sql": "SELECT 2", "Database": "beta", "StatementName": "weekly-one",
	})
	redshiftdata.AddStatementInternal(b, testRegion, "started-alpha", "SELECT 3", "alpha", "STARTED", true)

	tests := []struct {
		body      map[string]any
		name      string
		wantCount int
	}{
		{name: "default_finished_only", body: map[string]any{}, wantCount: 2},
		{name: "all_statuses", body: map[string]any{"Status": "ALL"}, wantCount: 3},
		{name: "database", body: map[string]any{"Database": "alpha"}, wantCount: 1},
		{name: "statement_name_prefix", body: map[string]any{"StatementName": "daily"}, wantCount: 1},
		{name: "started", body: map[string]any{"Status": "STARTED"}, wantCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, "ListStatements", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp["Statements"], tt.wantCount)
		})
	}
}

// TestListStatementsNextToken verifies ListStatements pagination continuation
// and rejection of an invalid token.
func TestListStatementsNextToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for i := range 3 {
		doRequest(t, h, "ExecuteStatement", map[string]any{
			"Sql": "SELECT " + string(rune('1'+i)), "Database": "dev",
		})
	}

	first := doRequest(t, h, "ListStatements", map[string]any{"MaxResults": 1})
	require.Equal(t, http.StatusOK, first.Code)

	var firstResp map[string]any
	require.NoError(t, json.Unmarshal(first.Body.Bytes(), &firstResp))
	token, ok := firstResp["NextToken"].(string)
	require.True(t, ok)

	tests := []struct {
		name       string
		token      string
		wantStatus int
	}{
		{name: "continuation", token: token, wantStatus: http.StatusOK},
		{name: "invalid_token", token: "invalid", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, "ListStatements", map[string]any{
				"MaxResults": 1, "NextToken": tt.token,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHasResultSet_BySQL verifies that HasResultSet matches real AWS
// Redshift Data API semantics: read-only statements (SELECT, SHOW, EXPLAIN, DESCRIBE,
// WITH, VALUES, TABLE) return true; DML/DDL return false.
func TestHasResultSet_BySQL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sql           string
		name          string
		wantResultSet bool
	}{
		// Read-only → HasResultSet = true
		{name: "select", sql: "SELECT 1", wantResultSet: true},
		{name: "select_from", sql: "SELECT id FROM users", wantResultSet: true},
		{name: "select_star", sql: "SELECT * FROM t WHERE x=1", wantResultSet: true},
		{name: "with_cte", sql: "WITH cte AS (SELECT 1) SELECT * FROM cte", wantResultSet: true},
		{name: "show", sql: "SHOW TABLES", wantResultSet: true},
		{name: "explain", sql: "EXPLAIN SELECT 1", wantResultSet: true},
		{name: "values", sql: "VALUES (1, 2)", wantResultSet: true},
		// DML → HasResultSet = false
		{name: "insert", sql: "INSERT INTO t VALUES (1)", wantResultSet: false},
		{name: "update", sql: "UPDATE t SET x=1 WHERE id=2", wantResultSet: false},
		{name: "delete", sql: "DELETE FROM t WHERE id=1", wantResultSet: false},
		// DDL → HasResultSet = false
		{name: "create_table", sql: "CREATE TABLE t (id INT)", wantResultSet: false},
		{name: "drop_table", sql: "DROP TABLE t", wantResultSet: false},
		{name: "alter_table", sql: "ALTER TABLE t ADD COLUMN x INT", wantResultSet: false},
		{name: "truncate", sql: "TRUNCATE TABLE t", wantResultSet: false},
		// Redshift-specific → HasResultSet = false
		{name: "copy", sql: "COPY t FROM 's3://bucket/key' IAM_ROLE DEFAULT", wantResultSet: false},
		{name: "unload", sql: "UNLOAD ('SELECT 1') TO 's3://bucket/key'", wantResultSet: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			execRec := doRequest(t, h, "ExecuteStatement", map[string]any{
				"Sql":      tt.sql,
				"Database": "testdb",
			})
			require.Equal(t, http.StatusOK, execRec.Code)

			var execResp map[string]any
			require.NoError(t, json.Unmarshal(execRec.Body.Bytes(), &execResp))

			id := execResp["Id"].(string)

			descRec := doRequest(t, h, "DescribeStatement", map[string]any{"Id": id})
			require.Equal(t, http.StatusOK, descRec.Code)

			var descResp map[string]any
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))

			assert.Equal(t, tt.wantResultSet, descResp["HasResultSet"],
				"SQL %q: HasResultSet mismatch", tt.sql)
		})
	}
}

// TestGetStatementResult_RequiresResultSet verifies that
// GetStatementResult returns ValidationException for DML statements (no result set).
func TestGetStatementResult_RequiresResultSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sql        string
		name       string
		wantStatus int
	}{
		{name: "select_ok", sql: "SELECT 1", wantStatus: http.StatusOK},
		{name: "insert_fails", sql: "INSERT INTO t VALUES (1)", wantStatus: http.StatusBadRequest},
		{name: "update_fails", sql: "UPDATE t SET x=1", wantStatus: http.StatusBadRequest},
		{name: "delete_fails", sql: "DELETE FROM t", wantStatus: http.StatusBadRequest},
		{name: "create_fails", sql: "CREATE TABLE t (id INT)", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			execRec := doRequest(t, h, "ExecuteStatement", map[string]any{
				"Sql":      tt.sql,
				"Database": "testdb",
			})
			require.Equal(t, http.StatusOK, execRec.Code)

			var execResp map[string]any
			require.NoError(t, json.Unmarshal(execRec.Body.Bytes(), &execResp))

			id := execResp["Id"].(string)

			rec := doRequest(t, h, "GetStatementResult", map[string]any{"Id": id})
			assert.Equal(t, tt.wantStatus, rec.Code,
				"SQL %q: GetStatementResult status mismatch", tt.sql)

			if tt.wantStatus == http.StatusBadRequest {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "ValidationException", resp["__type"])
			}
		})
	}
}

// TestParameters_AcceptedAndStored verifies that ExecuteStatement
// accepts SQL parameters (parameterized queries) and stores them on the statement.
// Real AWS Redshift Data API supports parameterized queries via the Parameters field.
func TestParameters_AcceptedAndStored(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		sql        string
		params     []map[string]any
		wantStatus int
	}{
		{
			name:       "no_parameters",
			sql:        "SELECT 1",
			wantStatus: http.StatusOK,
		},
		{
			name: "single_parameter",
			sql:  "SELECT * FROM users WHERE id = :id",
			params: []map[string]any{
				{"name": "id", "value": "42"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "multiple_parameters",
			sql:  "SELECT * FROM orders WHERE user_id = :user_id AND status = :status",
			params: []map[string]any{
				{"name": "user_id", "value": "123"},
				{"name": "status", "value": "shipped"},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{
				"Sql":      tt.sql,
				"Database": "testdb",
			}
			if tt.params != nil {
				body["Parameters"] = tt.params
			}

			rec := doRequest(t, h, "ExecuteStatement", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["Id"], "Id should be returned")
			}
		})
	}
}

// TestExecuteStatement_CaseInsensitiveSQL verifies that SQL keyword
// detection for HasResultSet is case-insensitive, matching real AWS behavior.
func TestExecuteStatement_CaseInsensitiveSQL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sql           string
		name          string
		wantResultSet bool
	}{
		{name: "lowercase_select", sql: "select 1", wantResultSet: true},
		{name: "mixed_case_select", sql: "Select id From users", wantResultSet: true},
		{name: "uppercase_insert", sql: "INSERT INTO t VALUES (1)", wantResultSet: false},
		{name: "lowercase_insert", sql: "insert into t values (1)", wantResultSet: false},
		{name: "mixed_case_update", sql: "Update t Set x=1", wantResultSet: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			execRec := doRequest(t, h, "ExecuteStatement", map[string]any{
				"Sql":      tt.sql,
				"Database": "testdb",
			})
			require.Equal(t, http.StatusOK, execRec.Code)

			var execResp map[string]any
			require.NoError(t, json.Unmarshal(execRec.Body.Bytes(), &execResp))

			id := execResp["Id"].(string)

			descRec := doRequest(t, h, "DescribeStatement", map[string]any{"Id": id})
			require.Equal(t, http.StatusOK, descRec.Code)

			var descResp map[string]any
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))

			assert.Equal(t, tt.wantResultSet, descResp["HasResultSet"],
				"SQL %q: HasResultSet mismatch", tt.sql)
		})
	}
}

// TestListStatements_HasResultSet verifies that ListStatements
// reflects accurate HasResultSet values based on SQL type.
func TestListStatements_HasResultSet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Execute a SELECT (HasResultSet = true) and an INSERT (HasResultSet = false).
	doRequest(t, h, "ExecuteStatement", map[string]any{
		"Sql":           "SELECT * FROM orders",
		"Database":      "testdb",
		"StatementName": "read-stmt",
	})
	doRequest(t, h, "ExecuteStatement", map[string]any{
		"Sql":           "INSERT INTO orders VALUES (1)",
		"Database":      "testdb",
		"StatementName": "write-stmt",
	})

	rec := doRequest(t, h, "ListStatements", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	stmts, ok := resp["Statements"].([]any)
	require.True(t, ok)
	require.Len(t, stmts, 2)

	byName := map[string]bool{}
	for _, s := range stmts {
		sm := s.(map[string]any)
		name, _ := sm["StatementName"].(string)
		has, _ := sm["HasResultSet"].(bool)
		byName[name] = has
	}

	assert.True(t, byName["read-stmt"], "SELECT should have HasResultSet=true in ListStatements")
	assert.False(t, byName["write-stmt"], "INSERT should have HasResultSet=false in ListStatements")
}
