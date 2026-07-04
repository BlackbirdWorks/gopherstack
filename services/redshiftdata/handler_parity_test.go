package redshiftdata_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	redshiftdata "github.com/blackbirdworks/gopherstack/services/redshiftdata"
)

// ======================================================================
// ValidateListStatementsStatus
// ======================================================================

func TestValidateListStatementsStatus_ValidValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status string
	}{
		{name: "empty_string", status: ""},
		{name: "ALL", status: "ALL"},
		{name: "ABORTED", status: "ABORTED"},
		{name: "FAILED", status: "FAILED"},
		{name: "FINISHED", status: "FINISHED"},
		{name: "PICKED", status: "PICKED"},
		{name: "STARTED", status: "STARTED"},
		{name: "SUBMITTED", status: "SUBMITTED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := redshiftdata.ValidateListStatementsStatus(tt.status)
			require.NoError(t, err, "status %q should be valid", tt.status)
		})
	}
}

func TestValidateListStatementsStatus_InvalidValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status string
	}{
		{name: "lowercase_all", status: "all"},
		{name: "partial", status: "FINISH"},
		{name: "unknown", status: "RUNNING"},
		{name: "whitespace", status: " ALL"},
		{name: "mixed_case", status: "Finished"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := redshiftdata.ValidateListStatementsStatus(tt.status)
			require.Error(t, err)
			require.ErrorIs(t, err, redshiftdata.ErrValidation)
			assert.Contains(t, err.Error(), tt.status)
		})
	}
}

func TestHandler_ListStatements_InvalidStatus_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ListStatements", map[string]any{"Status": "INVALID_STATUS"})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
	assert.Contains(t, rec.Body.String(), "INVALID_STATUS")
}

// ======================================================================
// BatchExecuteStatement — empty SQL validation
// ======================================================================

func TestBackend_BatchExecuteStatement_EmptySqlItem_Returns400(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
		sqls []string
	}{
		{
			name: "first_empty",
			sqls: []string{"", "SELECT 2"},
			want: "Sqls[0]",
		},
		{
			name: "second_empty",
			sqls: []string{"SELECT 1", ""},
			want: "Sqls[1]",
		},
		{
			name: "all_empty",
			sqls: []string{"", ""},
			want: "Sqls[0]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, "BatchExecuteStatement", map[string]any{
				"Sqls":              tt.sqls,
				"Database":          "dev",
				"ClusterIdentifier": "cluster-a",
			})

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.want)
		})
	}
}

func TestHandler_BatchExecuteStatement_EmptySqlItem_Returns400(t *testing.T) {
	t.Parallel()

	rec := doRequest(t, newTestHandler(t), "BatchExecuteStatement", map[string]any{
		"Sqls":              []string{"SELECT 1", ""},
		"Database":          "dev",
		"ClusterIdentifier": "cluster-a",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}

// ======================================================================
// ListDatabases — pagination + MaxResults validation
// ======================================================================

func TestHandler_ListDatabases_ReturnsNonEmpty(t *testing.T) {
	t.Parallel()

	rec := doRequest(t, newTestHandler(t), "ListDatabases", map[string]any{"Database": "dev"})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	dbs, ok := resp["Databases"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, dbs)
}

func TestHandler_ListDatabases_AlwaysHasNextTokenField(t *testing.T) {
	t.Parallel()

	rec := doRequest(t, newTestHandler(t), "ListDatabases", map[string]any{})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	_, ok := resp["NextToken"]
	assert.True(t, ok, "NextToken should always be present in ListDatabases response")
}

func TestHandler_ListDatabases_MaxResults1_PaginatesWithToken(t *testing.T) {
	t.Parallel()

	rec := doRequest(t, newTestHandler(t), "ListDatabases", map[string]any{"MaxResults": 1})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	dbs, ok := resp["Databases"].([]any)
	require.True(t, ok)
	assert.Len(t, dbs, 1, "should return exactly 1 database")

	token, _ := resp["NextToken"].(string)
	assert.NotEmpty(t, token, "NextToken should be set when results truncated")
}

func TestHandler_ListDatabases_MaxResultsTooHigh_Returns400(t *testing.T) {
	t.Parallel()

	rec := doRequest(t, newTestHandler(t), "ListDatabases", map[string]any{"MaxResults": 1000})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}

func TestHandler_ListDatabases_NextToken_ResumesFromCursor(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Page 1: get first item and its token.
	rec1 := doRequest(t, h, "ListDatabases", map[string]any{"MaxResults": 1})
	require.Equal(t, http.StatusOK, rec1.Code)

	var page1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &page1))

	token, _ := page1["NextToken"].(string)
	require.NotEmpty(t, token)

	// Page 2: use token.
	rec2 := doRequest(t, h, "ListDatabases", map[string]any{"MaxResults": 1, "NextToken": token})
	require.Equal(t, http.StatusOK, rec2.Code)

	var page2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &page2))

	dbs2, _ := page2["Databases"].([]any)
	dbs1, _ := page1["Databases"].([]any)
	require.NotEmpty(t, dbs2)
	assert.NotEqual(t, dbs1[0], dbs2[0], "page 2 should start after page 1")
}

// ======================================================================
// ListSchemas — SchemaPattern SQL LIKE filtering + MaxResults validation
// ======================================================================

func TestHandler_ListSchemas_ReturnsNonEmpty(t *testing.T) {
	t.Parallel()

	rec := doRequest(t, newTestHandler(t), "ListSchemas", map[string]any{"Database": "dev"})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	schemas, ok := resp["Schemas"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, schemas)
}

func TestHandler_ListSchemas_SchemaPattern_WildcardMatchesAll(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
	}{
		{name: "percent_wildcard", pattern: "%"},
		{name: "leading_percent", pattern: "%public"},
		{name: "trailing_percent", pattern: "pub%"},
		{name: "underscore_single", pattern: "_ublic"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, newTestHandler(t), "ListSchemas", map[string]any{
				"Database":      "dev",
				"SchemaPattern": tt.pattern,
			})

			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			schemas, ok := resp["Schemas"].([]any)
			require.True(t, ok)
			assert.NotEmpty(t, schemas, "pattern %q should match at least one schema", tt.pattern)
		})
	}
}

func TestHandler_ListSchemas_SchemaPattern_NoMatch(t *testing.T) {
	t.Parallel()

	rec := doRequest(t, newTestHandler(t), "ListSchemas", map[string]any{
		"Database":      "dev",
		"SchemaPattern": "nonexistent_schema_xyz",
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	schemas, _ := resp["Schemas"].([]any)
	assert.Empty(t, schemas, "non-matching pattern should return empty schemas")
}

func TestHandler_ListSchemas_MaxResultsTooHigh_Returns400(t *testing.T) {
	t.Parallel()

	rec := doRequest(t, newTestHandler(t), "ListSchemas", map[string]any{"MaxResults": 9999})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}

// ======================================================================
// ListTables — pattern filtering + MaxResults validation
// ======================================================================

func TestHandler_ListTables_ReturnsNonEmpty(t *testing.T) {
	t.Parallel()

	rec := doRequest(t, newTestHandler(t), "ListTables", map[string]any{"Database": "dev"})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tables, ok := resp["Tables"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, tables)
}

func TestHandler_ListTables_TableType_FiltersCorrectly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		tableType string
		wantEmpty bool
	}{
		{name: "TABLE", tableType: "TABLE", wantEmpty: false},
		{name: "VIEW", tableType: "VIEW", wantEmpty: false},
		{name: "unknown_type", tableType: "SEQUENCE", wantEmpty: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, newTestHandler(t), "ListTables", map[string]any{
				"Database":  "dev",
				"TableType": tt.tableType,
			})

			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			tables, _ := resp["Tables"].([]any)
			if tt.wantEmpty {
				assert.Empty(t, tables, "TableType=%q should return no tables", tt.tableType)
			} else {
				assert.NotEmpty(t, tables, "TableType=%q should return tables", tt.tableType)
			}
		})
	}
}

func TestHandler_ListTables_SchemaPattern_WildcardMatchesAll(t *testing.T) {
	t.Parallel()

	rec := doRequest(t, newTestHandler(t), "ListTables", map[string]any{
		"Database":      "dev",
		"SchemaPattern": "%",
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tables, ok := resp["Tables"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, tables, "% pattern should match all tables")
}

func TestHandler_ListTables_TablePattern_WildcardMatchesAll(t *testing.T) {
	t.Parallel()

	rec := doRequest(t, newTestHandler(t), "ListTables", map[string]any{
		"Database":     "dev",
		"TablePattern": "%",
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tables, ok := resp["Tables"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, tables)
}

func TestHandler_ListTables_TablePattern_PrefixMatch(t *testing.T) {
	t.Parallel()

	rec := doRequest(t, newTestHandler(t), "ListTables", map[string]any{
		"Database":     "dev",
		"TablePattern": "user%",
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tables, _ := resp["Tables"].([]any)
	require.NotEmpty(t, tables, "user% should match 'users'")

	for _, row := range tables {
		name, _ := row.(map[string]any)["name"].(string)
		assert.True(t, len(name) >= 4 && name[:4] == "user", "table %q should start with 'user'", name)
	}
}

func TestHandler_ListTables_MaxResultsTooHigh_Returns400(t *testing.T) {
	t.Parallel()

	rec := doRequest(t, newTestHandler(t), "ListTables", map[string]any{"MaxResults": 9999})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}

func TestHandler_ListTables_MaxResults1_Paginates(t *testing.T) {
	t.Parallel()

	rec := doRequest(t, newTestHandler(t), "ListTables", map[string]any{"MaxResults": 1})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tables, ok := resp["Tables"].([]any)
	require.True(t, ok)
	assert.Len(t, tables, 1)

	token, _ := resp["NextToken"].(string)
	assert.NotEmpty(t, token)
}

// ======================================================================
// matchSQLLike — unit tests via ListSchemas (internal behavior)
// ======================================================================

func TestHandler_ListSchemas_SQLLike_UnderscoreWildcard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		want    bool
	}{
		{name: "exact_match", pattern: "public", want: true},
		{name: "underscore_any_char", pattern: "p_blic", want: true},
		{name: "no_match", pattern: "z_blic", want: false},
		{name: "percent_prefix", pattern: "%catalog", want: true},
		{name: "percent_all", pattern: "%", want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, newTestHandler(t), "ListSchemas", map[string]any{
				"Database":      "dev",
				"SchemaPattern": tt.pattern,
			})

			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			schemas, _ := resp["Schemas"].([]any)
			if tt.want {
				assert.NotEmpty(t, schemas, "pattern %q should match schema(s)", tt.pattern)
			} else {
				assert.Empty(t, schemas, "pattern %q should not match any schema", tt.pattern)
			}
		})
	}
}

// ======================================================================
// ExecuteStatement / BatchExecuteStatement — permissive connection target
// ======================================================================

func TestHandler_ExecuteStatement_AllowsBothClusterAndWorkgroup(t *testing.T) {
	t.Parallel()

	rec := doRequest(t, newTestHandler(t), "ExecuteStatement", map[string]any{
		"Sql":               "SELECT 1",
		"Database":          "dev",
		"ClusterIdentifier": "my-cluster",
		"WorkgroupName":     "my-workgroup",
	})

	assert.Equal(t, http.StatusOK, rec.Code, "mock should accept both ClusterIdentifier and WorkgroupName")
}

func TestHandler_ExecuteStatement_AllowsNeitherClusterNorWorkgroup(t *testing.T) {
	t.Parallel()

	rec := doRequest(t, newTestHandler(t), "ExecuteStatement", map[string]any{
		"Sql":      "SELECT 1",
		"Database": "dev",
	})

	assert.Equal(t, http.StatusOK, rec.Code, "mock should accept request without ClusterIdentifier or WorkgroupName")
}

func TestHandler_BatchExecuteStatement_AllowsNeitherClusterNorWorkgroup(t *testing.T) {
	t.Parallel()

	rec := doRequest(t, newTestHandler(t), "BatchExecuteStatement", map[string]any{
		"Sqls":     []string{"SELECT 1", "SELECT 2"},
		"Database": "dev",
	})

	assert.Equal(t, http.StatusOK, rec.Code, "mock should accept batch without ClusterIdentifier or WorkgroupName")
}

// ======================================================================
// ListStatements — MaxResults + NextToken pagination
// ======================================================================

func TestHandler_ListStatements_MaxResults_Paginates(t *testing.T) {
	t.Parallel()

	b := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)

	ids := []string{"parity-stmt-1", "parity-stmt-2", "parity-stmt-3", "parity-stmt-4", "parity-stmt-5"}
	for i, id := range ids {
		redshiftdata.AddStatementInternal(b, testRegion, id, "SELECT "+string(rune('1'+i)), "dev", "FINISHED", true)
	}

	h := redshiftdata.NewHandler(b)

	rec := doRequest(t, h, "ListStatements", map[string]any{
		"Status":     "ALL",
		"MaxResults": 2,
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	stmts, _ := resp["Statements"].([]any)
	assert.Len(t, stmts, 2)

	token, _ := resp["NextToken"].(string)
	assert.NotEmpty(t, token, "NextToken should be set when more results exist")
}

func TestHandler_ListStatements_MaxResultsTooHigh_Returns400(t *testing.T) {
	t.Parallel()

	rec := doRequest(t, newTestHandler(t), "ListStatements", map[string]any{
		"MaxResults": 9999,
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}
