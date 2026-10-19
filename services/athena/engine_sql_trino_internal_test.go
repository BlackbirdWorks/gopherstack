package athena

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func noTables([]string) resolvedRef { return resolvedRef{} }

func TestTranslateTrinoSyntax(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want string
	}{
		{"subscript", "SELECT a[1]", "SELECT athena_subscript(a, 1)"},
		{"chained subscript", "SELECT f(x)[2][1]", "SELECT athena_subscript(athena_subscript(f(x), 2), 1)"},
		{"array literal untouched", "SELECT ARRAY[1, 2]", "SELECT athena_array(1, 2)"},
		{"interval plus", "SELECT d + INTERVAL '3' DAY", "SELECT athena_date_add('day', 3, d)"},
		{"interval minus", "SELECT d - INTERVAL '2' MONTH", "SELECT athena_date_add('month', -2, d)"},
		{"year to month", "SELECT d + INTERVAL '1-2' YEAR TO MONTH", "SELECT athena_date_add('month', 14, d)"},
		{"nulls last added", "SELECT 1 ORDER BY 1", "SELECT 1 ORDER BY 1 NULLS LAST"},
		{"desc untouched", "SELECT 1 ORDER BY 1 DESC", "SELECT 1 ORDER BY 1 DESC"},
		{"explicit nulls untouched", "SELECT 1 ORDER BY 1 NULLS FIRST", "SELECT 1 ORDER BY 1 NULLS FIRST"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, _, err := translateSQL(tc.in, noTables)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestTranslateTrinoErrors(t *testing.T) {
	t.Parallel()

	cases := []struct{ name, in, want string }{
		{"bare interval", "SELECT INTERVAL '1' DAY", "INTERVAL is only supported"},
		{"bad interval", "SELECT d + INTERVAL 'x' DAY", "Invalid INTERVAL value"},
		{"lambda arity", "SELECT transform(a, (x, y) -> x)", "Unexpected parameters for function transform"},
		{"grouping arg", "SELECT grouping(z) FROM t GROUP BY ROLLUP (a)", "arguments to GROUPING()"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, _, err := translateSQL(tc.in, func([]string) resolvedRef {
				return resolvedRef{schema: "s", name: "t", found: true}
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestExpandGroupingSets(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		in       string
		contains []string
		branches int
	}{
		{
			name: "rollup", in: "SELECT a, b, count(*) FROM t GROUP BY ROLLUP (a, b)", branches: 3,
			contains: []string{"NULL AS \"b\"", "NULL AS \"a\""},
		},
		{name: "cube", in: "SELECT a, b FROM t GROUP BY CUBE (a, b)", branches: 4, contains: []string{"GROUP BY a, b"}},
		{
			name: "sets", in: "SELECT a FROM t GROUP BY GROUPING SETS ((a), ())", branches: 2,
			contains: []string{"LIMIT 1"},
		},
		{
			name: "cross product", in: "SELECT a, b, c FROM t GROUP BY a, ROLLUP (b, c)", branches: 3,
			contains: []string{"GROUP BY a, b, c"},
		},
		{
			name: "grouping folded", in: "SELECT a, grouping(a) FROM t GROUP BY ROLLUP (a)", branches: 2,
			contains: []string{", 1 FROM", ", 0 FROM"},
		},
		{
			name: "aggregate arg kept", in: "SELECT a, sum(a) FROM t GROUP BY ROLLUP (a)", branches: 2,
			contains: []string{"sum(a)"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			toks, err := tokenize(tc.in)
			require.NoError(t, err)

			out, err := expandGroupingSets(toks)
			require.NoError(t, err)

			sql := joinTokens(out)
			assert.Equal(t, tc.branches-1, strings.Count(sql, "UNION ALL"))

			for _, want := range tc.contains {
				assert.Contains(t, sql, want)
			}
		})
	}
}

func TestExpandGroupingSetsPlainUntouched(t *testing.T) {
	t.Parallel()

	toks, err := tokenize("SELECT a, count(*) FROM t GROUP BY a ORDER BY a")
	require.NoError(t, err)

	out, err := expandGroupingSets(toks)
	require.NoError(t, err)
	assert.Equal(t, "SELECT a, count(*) FROM t GROUP BY a ORDER BY a", joinTokens(out))
}

func TestTrinoString(t *testing.T) {
	t.Parallel()

	cases := []struct{ name, in, decl, want, kind string }{
		{"array", containerMark + `[1,2,3]`, "", "[1, 2, 3]", kindArray},
		{"strings unquoted", containerMark + `["a","b"]`, "", "[a, b]", kindArray},
		{"map sorted", containerMark + `{"b":2,"a":1}`, "", "{a=1, b=2}", kindMap},
		{"nested null", containerMark + `[[1],null,{"k":[true]}]`, "", "[[1], null, {k=[true]}]", kindArray},
		{"row decl", `{"x":1,"y":"z"}`, declRow, "{x=1, y=z}", kindRow},
		{"floats keep text", containerMark + `[1.50,2e3]`, "", "[1.50, 2e3]", kindArray},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, kind, ok := displayComplex(tc.in, tc.decl)
			require.True(t, ok)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, tc.kind, kind)
		})
	}

	_, _, ok := displayComplex(`[1,2]`, declVarchar)
	assert.False(t, ok, "unmarked varchar JSON text is not a complex value")
}

func TestExtractWhereFilters(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		col  string
		want []wherePred
	}{
		{
			"eq string",
			"SELECT 1 FROM t WHERE dt = '2024-01-01'",
			"dt",
			[]wherePred{{op: "=", vals: []string{"2024-01-01"}}},
		},
		{
			"in list", "SELECT 1 FROM t WHERE r IN ('a', 'b') AND x = 1", "r",
			[]wherePred{{op: "in", vals: []string{"a", "b"}}},
		},
		{
			"between", "SELECT 1 FROM t WHERE y BETWEEN 2 AND 4 AND z = 1", "y",
			[]wherePred{{op: ">=", vals: []string{"2"}}, {op: "<=", vals: []string{"4"}}},
		},
		{"le", "SELECT 1 FROM t WHERE y <= 7", "y", []wherePred{{op: "<=", vals: []string{"7"}}}},
		{"or ignored", "SELECT 1 FROM t WHERE y = 1 OR y = 2", "y", nil},
		{"qualified", "SELECT 1 FROM t a WHERE a.y = 3", "y", []wherePred{{op: "=", vals: []string{"3"}}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := extractWhereFilters(tc.sql)[tc.col]
			if tc.want == nil {
				assert.Nil(t, f)

				return
			}

			require.NotNil(t, f)
			assert.Equal(t, tc.want, f.preds)
		})
	}
}

func TestProjectPartitions(t *testing.T) {
	t.Parallel()

	now := time.Date(2024, 3, 10, 15, 0, 0, 0, time.UTC)
	key := func(name, typ string) []Column { return []Column{{Name: name, Type: typ}} }
	loc := func(rest ...string) []string {
		out := make([]string, len(rest))
		for i, r := range rest {
			out[i] = "s3://b/t/" + r + "/"
		}

		return out
	}

	cases := []struct {
		params map[string]string
		name   string
		sql    string
		err    string
		keys   []Column
		want   []string
	}{
		{
			name:   "enum",
			params: map[string]string{"projection.r.type": "enum", "projection.r.values": "a,b,c"},
			keys:   key("r", "string"), sql: "SELECT 1 FROM t WHERE r = 'b'", want: loc("r=b"),
		},
		{
			name: "integer",
			params: map[string]string{
				"projection.n.type": "integer", "projection.n.range": "1,10", "projection.n.interval": "4",
			},
			keys: key("n", "int"), sql: "SELECT 1 FROM t", want: loc("n=1", "n=5", "n=9"),
		},
		{
			name:   "integer pruned",
			params: map[string]string{"projection.n.type": "integer", "projection.n.range": "1,10"},
			keys:   key("n", "int"), sql: "SELECT 1 FROM t WHERE n > 8", want: loc("n=9", "n=10"),
		},
		{
			name: "now",
			params: map[string]string{
				"projection.d.type": "date", "projection.d.range": "NOW-2DAYS,NOW", "projection.d.format": "yyyyMMdd",
			},
			keys: key("d", "string"), sql: "SELECT 1 FROM t", want: loc("d=20240308", "d=20240309", "d=20240310"),
		},
		{
			name: "hourly",
			params: map[string]string{
				"projection.h.type": "date", "projection.h.range": "2024-03-10 00,2024-03-10 05",
				"projection.h.format": "yyyy-MM-dd HH", "projection.h.interval": "2",
			},
			keys: key("h", "string"), sql: "SELECT 1 FROM t",
			want: loc("h=2024-03-10 00", "h=2024-03-10 02", "h=2024-03-10 04"),
		},
		{
			name:   "bad type",
			params: map[string]string{"projection.n.type": "weird"},
			keys:   key("n", "int"), sql: "SELECT 1 FROM t", err: "projection.n.type must be one of",
		},
		{
			name: "too many",
			params: map[string]string{
				"projection.n.type": "integer", "projection.n.range": "1,900",
				"projection.m.type": "integer", "projection.m.range": "1,900",
			},
			keys: append(key("n", "int"), Column{Name: "m", Type: "int"}), sql: "SELECT 1 FROM t",
			err: "EXCEEDED_LIMIT",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			def := &GlueTableDef{Location: "s3://b/t/", Parameters: tc.params, PartitionKeys: tc.keys}
			parts, err := projectPartitions(def, extractWhereFilters(tc.sql), now)

			if tc.err != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.err)

				return
			}

			require.NoError(t, err)

			got := make([]string, len(parts))
			for i, p := range parts {
				got[i] = p.Location
			}

			assert.Equal(t, tc.want, got)
		})
	}
}
