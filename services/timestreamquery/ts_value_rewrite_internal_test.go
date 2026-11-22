package timestreamquery

import (
	"database/sql/driver"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{"scalar rows", "SELECT c FROM (VALUES 1, 2, 3) AS t(c)", "SELECT c FROM (VALUES (1), (2), (3)) AS t(c)"},
		{
			"row rows unchanged",
			"SELECT 1 FROM (VALUES (1, 2), (3, 4)) AS t(a, b)",
			"SELECT 1 FROM (VALUES (1, 2), (3, 4)) AS t(a, b)",
		},
		{"paired rows", "VALUES ((1, 2)), ((3, 4))", "VALUES (1, 2), (3, 4)"},
		{"expression rows", "VALUES (1 + 2) * 3, 4", "VALUES ((1 + 2) * 3), (4)"},
		{"call row", "VALUES f(1, 2), 3", "VALUES (f(1, 2)), (3)"},
		{
			"string commas",
			"SELECT 1 FROM (VALUES 'a, b', 'c') AS t(x)",
			"SELECT 1 FROM (VALUES ('a, b'), ('c')) AS t(x)",
		},
		{"order by after", "VALUES 2, 1 ORDER BY 1", "VALUES (2), (1) ORDER BY 1"},
		{"literal untouched", "SELECT 'VALUES 1, 2'", "SELECT 'VALUES 1, 2'"},
		{"quoted identifier untouched", `SELECT "values" FROM t`, `SELECT "values" FROM t`},
		{"no values", "SELECT 1", "SELECT 1"},
		{"bare keyword", "SELECT values FROM t", "SELECT values FROM t"},
		{"unterminated", "VALUES 1, 'a", "VALUES 1, 'a"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, normalizeValues(tt.query))
		})
	}
}

func TestRewriteValuesAliases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			"column list",
			"SELECT c FROM (VALUES (1), (2)) AS t(c)",
			"SELECT c FROM (SELECT column1 AS c FROM (VALUES (1), (2))) AS t",
		},
		{
			"two columns no as",
			"SELECT a FROM (VALUES (1, 2)) t(a, b)",
			"SELECT a FROM (SELECT column1 AS a, column2 AS b FROM (VALUES (1, 2))) AS t",
		},
		{"no column list", "SELECT * FROM (VALUES (1)) AS t", "SELECT * FROM (VALUES (1)) AS t"},
		{"literal untouched", "SELECT '(VALUES (1)) AS t(c)'", "SELECT '(VALUES (1)) AS t(c)'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, rewriteValuesAliases(tt.query))
		})
	}
}

func TestRewriteRegexpLambdas(t *testing.T) {
	t.Parallel()

	got := rewriteRegexpLambdas("SELECT regexp_replace(a, '(x)', y -> upper(y[1])), regexp_replace(b, 'q', 'r')")
	assert.Contains(t, got, "ts_regexp_splice(a, '(x)'")
	assert.Contains(t, got, "regexp_replace(b, 'q', 'r')")
	assert.Equal(
		t,
		"SELECT 'regexp_replace(a, p, x -> x)'",
		rewriteRegexpLambdas("SELECT 'regexp_replace(a, p, x -> x)'"),
	)
}

func TestFiniteDoubleSQLStaysNative(t *testing.T) {
	t.Parallel()

	for _, q := range []string{
		"SELECT 1.5 + 2.5 * 3.0, 1.5 < 2.5, 1.5 - 0.5",
		"SELECT sum(1.5 + 2.5), avg(2.0 * 3.0)",
	} {
		ann := annotateQuery(q, loadedSet{}, time.Time{}, nil)
		require.True(t, ann.ok, q)
		assert.NotContains(t, ann.query, "ts_d", q)
		assert.Equal(t, q, ann.query, q)
	}
}

func TestNewFunctionsTolerateAnyArgument(t *testing.T) {
	t.Parallel()

	values := []driver.Value{
		nil, int64(0), int64(-1), int64(math.MinInt64), int64(math.MaxInt64), 1.5, math.NaN(), math.Inf(1),
		"", "x", "NaN", arrayMark + "[1,2]", arrayMark + "[", arrayMark + "{}", []byte{0xff, 0}, true,
	}

	funcs := slices.Concat(tsTextFuncs(), tsArrayFuncs(), domainMathFuncs())

	for _, f := range funcs {
		t.Run(f.name, func(t *testing.T) {
			t.Parallel()

			for _, a := range values {
				for _, b := range values {
					arities := []int{int(f.nargs)}
					if f.nargs < 0 {
						arities = []int{0, 1, argsTwo, argsThree, maxZipArrays + 1}
					}

					for _, arity := range arities {
						args := []driver.Value{a, b, a, b, a, b}[:arity]

						assert.NotPanics(t, func() { _, _ = f.impl(nil, args) }, "%s %v %v", f.name, a, b)
					}
				}
			}
		})
	}
}

func TestBufferedAggregatesTolerateAnyArgument(t *testing.T) {
	t.Parallel()

	values := []driver.Value{nil, int64(0), int64(-5), 2.5, "x", "NaN", arrayMark + "[0.5]", []byte{1}, true}

	rows := make([][]driver.Value, 0, len(values))
	for _, v := range values {
		rows = append(rows, []driver.Value{v, v, v, v})
	}

	finishers := []func([]aggRow) (driver.Value, error){
		bitwiseAgg(func(a, b int64) int64 { return a & b }), skewnessOf, regressionAgg(false), regressionAgg(true),
		approxDistinct, topByKey(true), topByKey(false), weightedPercentile,
	}

	for _, fin := range finishers {
		assert.NotPanics(t, func() {
			_, _ = fin(rows)
			_, _ = fin(nil)
		})
	}

	for _, chk := range []func(aggRow) error{
		checkApproxError, checkTopN("max_by"), checkWeightedPercentile, checkWeight, checkAccuracy,
	} {
		for _, r := range rows {
			assert.NotPanics(t, func() { _ = chk(r) })
		}
	}

	assert.True(t, strings.HasPrefix(arrayMark, "\x01"))
}

func TestRewriteFunctionSyntax(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{"position", "SELECT position('b' IN 'abc')", "SELECT strpos('abc', 'b')"},
		{"position nested", "SELECT position(lower(x) IN a || b) FROM t", "SELECT strpos(a || b, lower(x)) FROM t"},
		{"position without in", "SELECT position(a) FROM t", "SELECT position(a) FROM t"},
		{"extract dow", "SELECT extract(DOW FROM t)", "SELECT day_of_week(t)"},
		{"extract yow", "SELECT extract( yow FROM time)", "SELECT year_of_week(time)"},
		{
			"extract nested",
			"SELECT extract(QUARTER FROM date_add('day', 1, t))",
			"SELECT quarter(date_add('day', 1, t))",
		},
		{"extract supported untouched", "SELECT extract(YEAR FROM t)", "SELECT extract(YEAR FROM t)"},
		{
			"literal untouched",
			"SELECT 'position(a IN b) extract(DOW FROM x)'",
			"SELECT 'position(a IN b) extract(DOW FROM x)'",
		},
		{"identifier untouched", `SELECT "position(a IN b)" FROM t`, `SELECT "position(a IN b)" FROM t`},
		{"unbalanced", "SELECT position(a IN b", "SELECT position(a IN b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, rewriteFunctionSyntax(tt.query))
		})
	}
}

func TestRewriteQuantified(t *testing.T) {
	t.Parallel()

	for _, q := range []string{
		"SELECT 'x > ALL (VALUES 1)'", "SELECT a FROM t WHERE a > 1", `SELECT "all" > 1`, "SELECT x > ALL (a)",
	} {
		assert.Equal(t, q, rewriteQuantified(q), q)
	}

	got := rewriteQuantified("SELECT a FROM t WHERE b + 1 >= ALL (SELECT c FROM u) AND d = 1")
	assert.Contains(t, got, `SELECT c AS "_v" FROM u`)
	assert.Contains(t, got, "(b + 1) >= \"_v\"")
	assert.True(t, strings.HasSuffix(got, ") AND d = 1"), got)

	assert.Contains(t, rewriteQuantified("SELECT 1 = ANY (VALUES 1, 2)"), `SELECT column1 AS "_v" FROM (VALUES 1, 2)`)
}
