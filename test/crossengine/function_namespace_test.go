package crossengine_test

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	_ "modernc.org/sqlite"

	"github.com/blackbirdworks/gopherstack/services/athena"
	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
	"github.com/blackbirdworks/gopherstack/services/timestreamquery"
)

func appFuncs(t *testing.T) []string {
	t.Helper()

	db, err := sql.Open("sqlite", ":memory:")
	require.NoError(t, err)

	defer db.Close()

	rows, err := db.QueryContext(t.Context(), "SELECT DISTINCT name FROM pragma_function_list WHERE builtin = 0")
	require.NoError(t, err)

	defer rows.Close()

	var out []string

	for rows.Next() {
		var n string
		require.NoError(t, rows.Scan(&n))

		out = append(out, n)
	}

	require.NoError(t, rows.Err())

	return out
}

type noCatalog struct{}

func (noCatalog) Table(bqsql.TableRef) (*bqsql.TableData, bool) { return nil, false }
func (noCatalog) DatasetExists(string, string) bool             { return true }

func bigQueryValue(t *testing.T, expr string) string {
	t.Helper()

	st, err := bqsql.ParseStatement("SELECT " + expr)
	require.NoError(t, err)

	res, err := bqsql.Run(t.Context(), st, &bqsql.Input{Catalog: noCatalog{}, Project: "p", Dataset: "ds"}, false)
	require.NoError(t, err, expr)
	require.Len(t, res.Rows, 1)

	return fmt.Sprint(bqsql.Row(res.Fields, res.Rows[0])["f"].([]map[string]any)[0]["v"])
}

func athenaValue(t *testing.T, expr string) string {
	t.Helper()

	q, _, err := athena.TranslateTrino("SELECT "+expr, func([]string) (string, string, bool) { return "", "", false })
	require.NoError(t, err, expr)

	db, err := athena.OpenEngineDB()
	require.NoError(t, err)

	defer db.Close()

	var v any

	require.NoError(t, db.QueryRowContext(t.Context(), q).Scan(&v), q)

	return fmt.Sprint(v)
}

func timestreamValue(t *testing.T, expr string) string {
	t.Helper()

	b := timestreamquery.NewInMemoryBackend("000000000000", "us-east-1")
	pg, err := b.QueryWithOptions(t.Context(), timestreamquery.QueryOptions{QueryString: "SELECT " + expr})
	require.NoError(t, err, expr)
	require.Len(t, pg.Rows, 1)

	d := pg.Rows[0].Data[0]
	if d.ScalarValue == nil {
		return "NULL"
	}

	return *d.ScalarValue
}

func TestEnginesKeepTheirOwnFunctionSemantics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		expr   string
		engine string
		want   string
	}{
		{"bq replace", "REPLACE('abc', 'b', 'X')", "bq", "aXc"},
		{"bq repeat", "REPEAT('ab', 3)", "bq", "ababab"},
		{"bq reverse", "REVERSE('abc')", "bq", "cba"},
		{"bq sqrt", "SQRT(16.0)", "bq", "4.0"},
		{"bq ln", "LN(1.0)", "bq", "0.0"},
		{"bq log10", "LOG10(1000.0)", "bq", "3.0"},
		{"bq pow", "POW(2, 10)", "bq", "1024.0"},
		{"bq abs", "ABS(-3)", "bq", "3"},
		{"bq sign", "SIGN(-2)", "bq", "-1"},
		{"bq round", "ROUND(2.5)", "bq", "3.0"},
		{"bq strpos", "STRPOS('abc', 'c')", "bq", "3"},
		{"bq concat", "CONCAT('a', 'b')", "bq", "ab"},
		{"athena replace", "replace('abc', 'b', 'X')", "athena", "aXc"},
		{"athena lpad", "lpad('a', 3, 'x')", "athena", "xxa"},
		{"athena reverse", "reverse('abc')", "athena", "cba"},
		{"athena sqrt", "sqrt(16)", "athena", "4"},
		{"athena ln", "ln(1)", "athena", "0"},
		{"athena log10", "log10(1000)", "athena", "3"},
		{"athena pow", "pow(2, 10)", "athena", "1024"},
		{"athena abs", "abs(-3)", "athena", "3"},
		{"athena sign", "sign(-2)", "athena", "-1"},
		{"athena round", "round(2.5)", "athena", "3"},
		{"athena strpos", "strpos('abc', 'c')", "athena", "3"},
		{"athena concat", "concat('a', 'b')", "athena", "ab"},
		{"timestream replace", "replace('abc', 'b', 'X')", "ts", "aXc"},
		{"timestream reverse", "reverse('abc')", "ts", "cba"},
		{"timestream repeat", "cardinality(repeat('a', 3))", "ts", "3"},
		{"timestream zip", "cardinality(zip(ARRAY[1,2], ARRAY[3,4]))", "ts", "2"},
		{"timestream sqrt", "sqrt(16.0)", "ts", "4.0"},
		{"timestream strpos", "strpos('abc', 'c')", "ts", "3"},
		{"timestream concat", "concat('a', 'b')", "ts", "ab"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var got string

			switch tt.engine {
			case "bq":
				got = bigQueryValue(t, tt.expr)
			case "athena":
				got = athenaValue(t, tt.expr)
			default:
				got = timestreamValue(t, tt.expr)
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

// Runs before any parallel test: the driver reads its function registry unlocked on every connection open, so
// every engine must have registered by init, not lazily on first use.
func TestEngineFunctionsRegisteredAtInit(t *testing.T) { //nolint:paralleltest // must precede every engine call
	got := appFuncs(t)

	for _, name := range []string{"ts_sum", "athena_date_trunc", "bq_upper"} {
		assert.Contains(t, got, name)
	}
}

// Every engine's functions live under its own prefix; plain names belong to SQLite alone.
func TestEngineFunctionsArePrefixed(t *testing.T) {
	t.Parallel()

	engine := []string{"ts_", "athena_", "bq_"}
	// SQLite's own optional modules (FTS5, GeoPoly, R-tree) report as application-defined.
	sqliteModules := []string{"fts5", "geopoly_", "rtree", "bm25", "highlight", "snippet", "match"}

	var stray []string

	for _, name := range appFuncs(t) {
		hasPrefix := func(p string) bool { return strings.HasPrefix(name, p) }
		if !slices.ContainsFunc(engine, hasPrefix) && !slices.ContainsFunc(sqliteModules, hasPrefix) {
			stray = append(stray, name)
		}
	}

	slices.Sort(stray)
	assert.Empty(t, stray, "functions registered without an engine prefix are visible to every engine in the process")
}
