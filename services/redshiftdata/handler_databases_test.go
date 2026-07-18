package redshiftdata_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ListDatabases(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListDatabases", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Databases"])
}

func TestHandler_ListSchemas(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListSchemas", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Schemas"])
}

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

// TestListDatabases_ReturnsDemoData verifies that ListDatabases returns
// a non-empty list of demo databases.
func TestListDatabases_ReturnsDemoData(t *testing.T) {
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

// TestListSchemas_ReturnsDemoData verifies that ListSchemas returns
// a non-empty list of demo schemas.
func TestListSchemas_ReturnsDemoData(t *testing.T) {
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

// TestListDatabases_HasNextToken verifies that ListDatabases returns
// a NextToken field (empty = no more pages).
func TestListDatabases_HasNextToken(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ListDatabases", map[string]any{"Database": "dev"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	_, ok := resp["NextToken"]
	assert.True(t, ok, "NextToken should always be present in ListDatabases response")
}
