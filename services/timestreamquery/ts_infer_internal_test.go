package timestreamquery

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

func TestEveryEngineFunctionHasReturnTypeRule(t *testing.T) {
	t.Parallel()

	registerTimestreamFuncs()

	names := map[string]bool{"create_time_series": true}

	for _, n := range athena.EmbeddedFuncNames() {
		names[strings.TrimPrefix(n, "athena_")] = true
	}

	for _, f := range tsScalarFuncs() {
		names[f.name] = true
	}

	require.Greater(t, len(names), 100)

	rules := funcRules()

	for name := range names {
		assert.Contains(t, rules, name, "engine function %q has no return-type rule", name)
	}
}

func TestParseSQLBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		ok    bool
	}{
		{"simple", "SELECT 1", true},
		{"deep parens", "SELECT " + strings.Repeat("(", 5000) + "1" + strings.Repeat(")", 5000), false},
		{"deep subqueries", strings.Repeat("SELECT * FROM (", 3000) + "SELECT 1" + strings.Repeat(")", 3000), false},
		{"unterminated string", "SELECT 'abc", false},
		{"garbage", "SELECT ) FROM", false},
		{"lone operators", "SELECT + - * /", false},
		{"unterminated comment", "SELECT 1 /* abc", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := parseSQL(tc.query)
			assert.Equal(t, tc.ok, err == nil)
		})
	}
}

func TestStripSQLComments(t *testing.T) {
	t.Parallel()

	tests := []struct{ name, in, want string }{
		{"line", "SELECT 1 -- c\nFROM t", "SELECT 1  \nFROM t"},
		{"block", "SELECT /* c */ 1", "SELECT   1"},
		{"string", "SELECT '-- x', '/* y */'", "SELECT '-- x', '/* y */'"},
		{"quoted ident", `SELECT "a--b"`, `SELECT "a--b"`},
		{"unterminated", "SELECT 1 /* x", "SELECT 1  "},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, stripSQLComments(tc.in))
		})
	}
}

func TestAnnotateReportsUnsupportedFunction(t *testing.T) {
	t.Parallel()

	ann := annotateQuery("SELECT some_future_function(1), 2", loadedSet{}, time.Time{}, nil)
	require.True(t, ann.ok)
	require.Len(t, ann.cols, 2)

	assert.False(t, ann.cols[0].typ.known())
	assert.Equal(t, tyInteger, ann.cols[1].typ.kind)
	assert.Equal(t, []string{"function some_future_function"}, ann.unsupported)
}

func TestAnnotateParsesAnalyticClauses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  tsKind
	}{
		{"window clause", "SELECT rank() OVER w FROM t WINDOW w AS (PARTITION BY a ORDER BY b)", tyBigint},
		{"grouping sets", "SELECT count(*) FROM t GROUP BY GROUPING SETS ((a), (b))", tyBigint},
		{"rollup", "SELECT count(*) FROM t GROUP BY ROLLUP (a, b)", tyBigint},
		{"filter clause", "SELECT count(*) FILTER (WHERE a > 1) FROM t", tyBigint},
		{"quoted identifiers", `SELECT "my col" FROM (SELECT 1 AS "my col")`, tyInteger},
		{"correlated subquery", "SELECT (SELECT max(1.5)) FROM (SELECT 1 AS a)", tyDouble},
		{"in subquery", "SELECT 1 IN (SELECT 2)", tyBoolean},
		{"union precedence", "SELECT 1 UNION SELECT 2.5 INTERSECT SELECT 3", tyDouble},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ann := annotateQuery(tc.query, loadedSet{}, time.Time{}, nil)
			require.True(t, ann.ok, "query must parse")
			require.NotEmpty(t, ann.cols)
			assert.Equal(t, tc.want, ann.cols[0].typ.kind)
		})
	}
}
