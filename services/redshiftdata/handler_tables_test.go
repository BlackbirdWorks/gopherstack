package redshiftdata_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ListTables(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListTables", map[string]any{"Database": "dev"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Tables"])
}

func TestHandler_ListTables_MissingDatabase_Returns400(t *testing.T) {
	t.Parallel()

	rec := doRequest(t, newTestHandler(t), "ListTables", map[string]any{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}

func TestHandler_DescribeTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DescribeTable", map[string]any{"Database": "dev"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["ColumnList"])
}

func TestHandler_DescribeTable_MissingDatabase_Returns400(t *testing.T) {
	t.Parallel()

	rec := doRequest(t, newTestHandler(t), "DescribeTable", map[string]any{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}

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

	rec := doRequest(t, newTestHandler(t), "ListTables", map[string]any{"Database": "dev", "MaxResults": 9999})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}

func TestHandler_ListTables_MaxResults1_Paginates(t *testing.T) {
	t.Parallel()

	rec := doRequest(t, newTestHandler(t), "ListTables", map[string]any{"Database": "dev", "MaxResults": 1})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tables, ok := resp["Tables"].([]any)
	require.True(t, ok)
	assert.Len(t, tables, 1)

	token, _ := resp["NextToken"].(string)
	assert.NotEmpty(t, token)
}

// TestDescribeTable_ReturnsDemoColumns verifies that DescribeTable
// returns a non-empty ColumnList with at least one column descriptor.
func TestDescribeTable_ReturnsDemoColumns(t *testing.T) {
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

// TestListTables_ReturnsDemoData verifies that ListTables returns
// a non-empty list of demo tables.
func TestListTables_ReturnsDemoData(t *testing.T) {
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
