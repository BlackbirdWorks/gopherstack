package cosmosdb_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

// docInfos builds DocumentInfo values from raw JSON bodies for query-engine
// tests, deriving each's id from the body's own "id" field.
func docInfos(t *testing.T, bodies ...string) []cosmosdb.DocumentInfo {
	t.Helper()

	out := make([]cosmosdb.DocumentInfo, 0, len(bodies))

	for i, raw := range bodies {
		body := decodeJSONBody(t, raw)

		id, _ := body["id"].(string)
		if id == "" {
			id = "auto"
		}

		out = append(out, cosmosdb.DocumentInfo{
			ID:        id,
			RID:       cosmosdb.FakeRID("doc" + id),
			Self:      "docs/" + id + "/",
			ETag:      cosmosdb.EtagFor(time.Unix(int64(i), 0)),
			Timestamp: time.Unix(int64(i), 0),
			Body:      body,
		})
	}

	return out
}

func TestExecuteQuery_SelectStar(t *testing.T) {
	t.Parallel()

	docs := docInfos(t, `{"id":"1","name":"alice","age":30}`, `{"id":"2","name":"bob","age":25}`)

	rows, err := cosmosdb.ExecuteQuery("SELECT * FROM c", nil, docs)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, "alice", rows[0]["name"])
}

func TestExecuteQuery_ProjectionWithAlias(t *testing.T) {
	t.Parallel()

	docs := docInfos(t, `{"id":"1","name":"alice","age":30}`)

	rows, err := cosmosdb.ExecuteQuery("SELECT c.name AS n, c.age FROM c", nil, docs)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "alice", rows[0]["n"])
	assert.Equal(t, json.Number("30"), rows[0]["age"])
}

func TestExecuteQuery_WhereComparisons(t *testing.T) {
	t.Parallel()

	docs := docInfos(t,
		`{"id":"1","name":"alice","age":30}`,
		`{"id":"2","name":"bob","age":25}`,
		`{"id":"3","name":"carol","age":40}`,
	)

	tests := []struct {
		name    string
		query   string
		wantIDs []string
	}{
		{name: "eq", query: "SELECT * FROM c WHERE c.name = 'alice'", wantIDs: []string{"1"}},
		{name: "neq", query: "SELECT * FROM c WHERE c.name != 'alice'", wantIDs: []string{"2", "3"}},
		{name: "gt", query: "SELECT * FROM c WHERE c.age > 28", wantIDs: []string{"1", "3"}},
		{name: "lte", query: "SELECT * FROM c WHERE c.age <= 25", wantIDs: []string{"2"}},
		{
			name: "and/or/parens", query: "SELECT * FROM c WHERE (c.age > 20 AND c.age < 30) OR c.name = 'carol'",
			wantIDs: []string{"2", "3"},
		},
		{name: "not", query: "SELECT * FROM c WHERE NOT c.name = 'alice'", wantIDs: []string{"2", "3"}},
		{name: "missing field never matches", query: "SELECT * FROM c WHERE c.nope = 1", wantIDs: nil},
		{
			// Real Cosmos: an undefined (missing) field is not the same
			// value as JSON null, so IS NULL against it is false, not
			// true -- see sql_exec.go's evalSQLIsNull doc comment.
			name:    "is null on missing field is false, not true",
			query:   "SELECT * FROM c WHERE c.nope IS NULL",
			wantIDs: nil,
		},
		{
			name:    "is not null on missing field is also false",
			query:   "SELECT * FROM c WHERE c.nope IS NOT NULL",
			wantIDs: nil,
		},
		{
			// The crux of the three-valued-logic fix: "c.nope = 1" is
			// Undefined (not a plain false), so NOT(...) must still not
			// match -- Undefined never flips to true under NOT.
			name:    "NOT over a missing-field comparison stays excluded, does not flip to a match",
			query:   "SELECT * FROM c WHERE NOT (c.nope = 1)",
			wantIDs: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows, err := cosmosdb.ExecuteQuery(tt.query, nil, docs)
			require.NoError(t, err)

			gotIDs := make([]string, 0, len(rows))
			for _, r := range rows {
				id, _ := r["id"].(string)
				gotIDs = append(gotIDs, id)
			}

			assert.ElementsMatch(t, tt.wantIDs, gotIDs)
		})
	}
}

func TestExecuteQuery_Parameters(t *testing.T) {
	t.Parallel()

	docs := docInfos(t, `{"id":"1","age":30}`, `{"id":"2","age":25}`)

	rows, err := cosmosdb.ExecuteQuery(
		"SELECT * FROM c WHERE c.age = @age", []cosmosdb.QueryParameter{{Name: "@age", Value: float64(30)}}, docs,
	)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "1", rows[0]["id"])
}

func TestExecuteQuery_OrderByAndTop(t *testing.T) {
	t.Parallel()

	docs := docInfos(t, `{"id":"1","age":30}`, `{"id":"2","age":25}`, `{"id":"3","age":40}`)

	rows, err := cosmosdb.ExecuteQuery("SELECT TOP 2 * FROM c ORDER BY c.age DESC", nil, docs)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, "3", rows[0]["id"])
	assert.Equal(t, "1", rows[1]["id"])
}

func TestExecuteQuery_OffsetLimit(t *testing.T) {
	t.Parallel()

	docs := docInfos(t, `{"id":"1"}`, `{"id":"2"}`, `{"id":"3"}`)

	rows, err := cosmosdb.ExecuteQuery("SELECT * FROM c ORDER BY c.id OFFSET 1 LIMIT 1", nil, docs)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "2", rows[0]["id"])
}

func TestExecuteQuery_Int64PrecisionInComparison(t *testing.T) {
	t.Parallel()

	docs := docInfos(t, `{"id":"1","big":9007199254740993}`, `{"id":"2","big":9007199254740992}`)

	rows, err := cosmosdb.ExecuteQuery("SELECT * FROM c WHERE c.big = 9007199254740993", nil, docs)
	require.NoError(t, err)
	require.Len(t, rows, 1, "must distinguish magnitudes beyond 2^53, not collapse via float64")
	assert.Equal(t, "1", rows[0]["id"])
}

func TestExecuteQuery_ParseErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
	}{
		{name: "missing FROM", query: "SELECT * WHERE c.x = 1"},
		{name: "unbalanced paren", query: "SELECT * FROM c WHERE (c.x = 1"},
		{name: "trailing garbage", query: "SELECT * FROM c WHERE c.x = 1 GARBAGE"},
		{name: "empty", query: ""},
		{name: "bad operator", query: "SELECT * FROM c WHERE c.x ~~ 1"},
		{name: "lone minus is not a number", query: "SELECT * FROM c WHERE c.value = -"},
		{name: "unqualified field in SELECT list", query: "SELECT name FROM c"},
		{name: "unqualified field in WHERE", query: "SELECT * FROM c WHERE value = 42"},
		{name: "unknown alias in WHERE", query: "SELECT * FROM c WHERE other.value = 42"},
		{name: "unknown alias in ORDER BY", query: "SELECT * FROM c ORDER BY other.value"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := cosmosdb.ExecuteQuery(tt.query, nil, nil)
			require.Error(t, err)
			require.ErrorIs(t, err, cosmosdb.ErrQueryParse)
		})
	}
}

// TestExecuteQuery_AliasResolution covers the fix requiring every field
// reference to be qualified with whichever alias FROM declares (including a
// re-aliased source, "FROM root r"), rather than the old behavior of
// unconditionally stripping whatever the leading path segment happened to
// be.
func TestExecuteQuery_AliasResolution(t *testing.T) {
	t.Parallel()

	docs := docInfos(t, `{"id":"1","name":"alice"}`)

	tests := []struct {
		name  string
		query string
	}{
		{name: "qualified with the declared alias", query: "SELECT c.name FROM c"},
		{name: "qualified with a re-aliased source", query: "SELECT r.name FROM root r WHERE r.name = 'alice'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows, err := cosmosdb.ExecuteQuery(tt.query, nil, docs)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			assert.Equal(t, "alice", rows[0]["name"])
		})
	}
}

// TestExecuteQuery_BareAliasProjectsWholeRow covers "SELECT c FROM c": a
// bare alias reference (no dotted field) resolves to an empty path, meaning
// "the whole row" -- it must still parse and execute, not be rejected as an
// unqualified field the way "SELECT name FROM c" is.
func TestExecuteQuery_BareAliasProjectsWholeRow(t *testing.T) {
	t.Parallel()

	docs := docInfos(t, `{"id":"1","name":"alice"}`)

	rows, err := cosmosdb.ExecuteQuery("SELECT c FROM c", nil, docs)
	require.NoError(t, err)
	require.Len(t, rows, 1)

	whole, ok := rows[0]["c"].(map[string]any)
	require.True(t, ok, "bare alias projects the whole row under its own alias key")
	assert.Equal(t, "1", whole["id"])
	assert.Equal(t, "alice", whole["name"])
}

func TestParseQuery_DeepNestingBounded(t *testing.T) {
	t.Parallel()

	const depth = 500

	query := "SELECT * FROM c WHERE " + strings.Repeat("(", depth) + "c.x = 1" + strings.Repeat(")", depth)

	_, err := cosmosdb.ParseQuery(query)
	require.ErrorIs(t, err, cosmosdb.ErrQueryTooDeep)
}

func TestParseQuery_ModeratelyNestedParensAccepted(t *testing.T) {
	t.Parallel()

	const depth = 20

	query := "SELECT * FROM c WHERE " + strings.Repeat("(", depth) + "c.x = 1" + strings.Repeat(")", depth)

	_, err := cosmosdb.ParseQuery(query)
	require.NoError(t, err)
}
