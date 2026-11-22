package bqsql_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNonNumericQueriesUnchanged(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		typ  string
		want []any
	}{
		{"float add", "SELECT 0.1 + 0.2", "FLOAT", []any{"0.30000000000000004"}},
		{"int div", "SELECT 1 / 3", "FLOAT", []any{"0.3333333333333333"}},
		{"int div exact", "SELECT 7 / 2", "FLOAT", []any{"3.5"}},
		{"neg int div", "SELECT -7 / 2", "FLOAT", []any{"-3.5"}},
		{"float mul", "SELECT 0.1 * 3", "FLOAT", []any{"0.30000000000000004"}},
		{"float sub", "SELECT 10 - 0.5", "FLOAT", []any{"9.5"}},
		{"float one", "SELECT 2 * 0.5", "FLOAT", []any{"1.0"}},
		{"exp literal", "SELECT 1e2 + 1", "FLOAT", []any{"101.0"}},
		{"int mul", "SELECT 2 * 3", "INTEGER", []any{"6"}},
		{"int sub", "SELECT 9223372036854775807 - 1", "INTEGER", []any{"9223372036854775806"}},
		{"div fn", "SELECT DIV(7, 2)", "INTEGER", []any{"3"}},
		{"mod fn", "SELECT MOD(7, -2)", "INTEGER", []any{"1"}},
		{"int float eq", "SELECT 7 = 7.0", "BOOLEAN", []any{"true"}},
		{"abs", "SELECT ABS(-2.5)", "FLOAT", []any{"2.5"}},
		{"round", "SELECT ROUND(2.5)", "FLOAT", []any{"3.0"}},
		{"sqrt", "SELECT SQRT(2)", "FLOAT", []any{"1.4142135623730951"}},
		{"greatest", "SELECT GREATEST(1, 2.5)", "FLOAT", []any{"2.5"}},
		{"if", "SELECT IF(TRUE, 1, 2.5)", "FLOAT", []any{"1.0"}},
		{"coalesce", "SELECT COALESCE(1, 2.5)", "FLOAT", []any{"1.0"}},
		{"case", "SELECT CASE WHEN TRUE THEN 1 ELSE 2.5 END", "FLOAT", []any{"1.0"}},
		{"in", "SELECT 1 IN (1.0, 2)", "BOOLEAN", []any{"true"}},
		{"between", "SELECT 2 BETWEEN 1 AND 2.5", "BOOLEAN", []any{"true"}},
		{"sum float", "SELECT SUM(x) FROM UNNEST([0.1, 0.2]) x", "FLOAT", []any{"0.30000000000000004"}},
		{"sum int", "SELECT SUM(x) FROM UNNEST([1, 2, 4]) x", "INTEGER", []any{"7"}},
		{"avg int", "SELECT AVG(x) FROM UNNEST([1, 2, 4]) x", "FLOAT", []any{"2.3333333333333335"}},
		{"array literal", "SELECT ARRAY_LENGTH([1, 2.5])", "INTEGER", []any{"2"}},
		{"concat", "SELECT 5 || 'x'", "STRING", []any{"5x"}},
		{"like", "SELECT 'abc' LIKE 'a%'", "BOOLEAN", []any{"true"}},
		{"distinct from", "SELECT 1 IS DISTINCT FROM 1.0", "BOOLEAN", []any{"false"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, err := runSQL(t, numericCatalog(t), tt.sql)
			require.NoError(t, err)
			require.Len(t, res.Fields, 1)
			assert.Equal(t, tt.typ, res.Fields[0].Type)
			assert.Equal(t, [][]any{tt.want}, wireRows(res))
		})
	}
}

func TestNumericMixedComparisons(t *testing.T) {
	t.Parallel()

	tests := []fnCase{
		{"int column join", "SELECT x.id, y.id FROM nums x JOIN nums y ON x.id = y.b", [][]any{{"5", "5"}}},
		{"using join", "SELECT x.id FROM nums x JOIN (SELECT id AS b FROM nums) y USING (b)", [][]any{{"5"}}},
		{"using join count", "SELECT COUNT(*) FROM nums x JOIN nums y USING (n)", [][]any{{"6"}}},
		{
			"using full join",
			"SELECT x.id, b FROM nums x FULL JOIN (SELECT id AS b FROM nums) y USING (b) WHERE b >= 5",
			[][]any{{"2", "12345678901234567890123456789012345678.12345678901234567890123456789012345678"}, {"5", "5"}},
		},
		{"in subquery int in numeric", "SELECT id FROM nums WHERE id IN (SELECT b FROM nums)", [][]any{{"5"}}},
		{"in subquery numeric in int", "SELECT id FROM nums WHERE b IN (SELECT id FROM nums)", [][]any{{"5"}}},
		{"in unnest ints", "SELECT id FROM nums WHERE b IN UNNEST([5, 3]) ORDER BY id", [][]any{{"5"}}},
		{
			"in unnest decimals",
			"SELECT id FROM nums WHERE id IN UNNEST([NUMERIC '5', NUMERIC '3']) ORDER BY id",
			[][]any{{"3"}, {"5"}},
		},
		{"exists", "SELECT id FROM nums o WHERE EXISTS (SELECT 1 FROM nums i WHERE i.id = o.b)", [][]any{{"5"}}},
		{"intersect", "SELECT id AS v FROM nums INTERSECT DISTINCT SELECT b FROM nums", [][]any{{"5"}}},
		{
			"except",
			"SELECT id AS v FROM nums WHERE id IN (3, 5) EXCEPT DISTINCT SELECT b FROM nums ORDER BY v",
			[][]any{{"3"}},
		},
		{
			"union distinct",
			"SELECT id AS v FROM nums WHERE id = 5 UNION DISTINCT SELECT b FROM nums WHERE id = 5",
			[][]any{{"5"}},
		},
		{
			"group mixed",
			"SELECT v, COUNT(*) FROM (SELECT id AS v FROM nums UNION ALL SELECT b FROM nums WHERE id = 5) " +
				"GROUP BY v ORDER BY v LIMIT 5",
			[][]any{{"1", "1"}, {"2", "1"}, {"3", "1"}, {"4", "1"}, {"5", "2"}},
		},
		{"order mixed", "SELECT id FROM nums ORDER BY b - id", [][]any{{"4"}, {"3"}, {"1"}, {"5"}, {"2"}}},
		{"eq float column", "SELECT id FROM nums WHERE n = f", [][]any{}},
		{
			"between mixed",
			"SELECT id FROM nums WHERE b BETWEEN id - 1 AND id + 1 AND b IS NOT NULL ORDER BY id",
			[][]any{{"1"}, {"5"}},
		},
		{"greatest mixed", "SELECT GREATEST(n, b) FROM nums WHERE id = 3", [][]any{{"-0.3"}}},
		{"if mixed", "SELECT IF(id > 3, id, n) FROM nums WHERE id IN (1, 5) ORDER BY id", [][]any{{"0.1"}, {"5"}}},
		{"coalesce default", "SELECT COALESCE(n, 0) FROM nums WHERE id = 4", [][]any{{"0"}}},
		{
			"distinct from",
			"SELECT n IS DISTINCT FROM 0.1, n IS NOT DISTINCT FROM 0.1 FROM nums ORDER BY id",
			[][]any{{"false", "true"}, {"true", "false"}, {"true", "false"}, {"true", "false"}, {"false", "true"}},
		},
		{"array concat", "SELECT SUM(x) FROM UNNEST(ARRAY_CONCAT([1], [NUMERIC '1.5'])) x", [][]any{{"2.5"}}},
		{
			"array concat operator",
			"SELECT ARRAY_LENGTH([1.5] || [NUMERIC '2.5']), (SELECT SUM(x) FROM UNNEST([1.5] || [NUMERIC '2.5']) x)",
			[][]any{{"2", "4.0"}},
		},
		{
			"array concat column",
			"SELECT (SELECT SUM(x) FROM UNNEST(ARRAY_CONCAT([1], arr)) x) FROM nums WHERE id = 1",
			[][]any{{"4.75"}},
		},
		{"array if", "SELECT (SELECT SUM(x) FROM UNNEST(IF(FALSE, [1], [NUMERIC '2.5'])) x)", [][]any{{"2.5"}}},
		{
			"unpivot",
			"SELECT * FROM (SELECT id, n FROM nums WHERE id = 1) UNPIVOT (v FOR c IN (id, n)) ORDER BY c",
			[][]any{{"1", "id"}, {"0.1", "n"}},
		},
		{"search struct", "SELECT SEARCH(rec, '3.5'), SEARCH(rec, '4'), SEARCH(rec, '99') FROM nums WHERE id = 1",
			[][]any{{"true", "true", "false"}}},
		{
			"search array",
			"SELECT SEARCH(arr, '1.5'), SEARCH(arr, '1.50') FROM nums WHERE id = 1",
			[][]any{{"true", "false"}},
		},
		{
			"format nested",
			"SELECT FORMAT('%s|%t', arr, rec) FROM nums WHERE id = 1",
			[][]any{{`[1.5,2.25]|{"x":3.5,"y":4}`}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, err := runSQL(t, numericCatalog(t), tt.sql)
			require.NoError(t, err)
			assert.Equal(t, tt.want, wireRows(res))
		})
	}
}

func TestNumericIncompatibleOperandsError(t *testing.T) {
	t.Parallel()

	tests := []struct{ name, sql string }{
		{"eq string", "SELECT n = 'x' FROM nums"},
		{"lt string column", "SELECT n < g FROM nums"},
		{"add string", "SELECT n + 'x' FROM nums"},
		{"concat operator", "SELECT n || 'x' FROM nums"},
		{"like", "SELECT n LIKE '%1%' FROM nums"},
		{"between", "SELECT n BETWEEN 'a' AND 'b' FROM nums"},
		{"in list", "SELECT n IN ('a') FROM nums"},
		{"distinct from", "SELECT n IS DISTINCT FROM 'x' FROM nums"},
		{"if", "SELECT IF(id > 1, 'x', n) FROM nums"},
		{"case", "SELECT CASE WHEN id = 1 THEN g ELSE n END FROM nums"},
		{"coalesce", "SELECT COALESCE(g, n) FROM nums"},
		{"nullif", "SELECT NULLIF(g, n) FROM nums"},
		{"greatest", "SELECT GREATEST('a', n) FROM nums"},
		{"union", "SELECT g AS v FROM nums UNION ALL SELECT n FROM nums"},
		{"array literal", "SELECT ['x', n] FROM nums"},
		{"array to string", "SELECT ARRAY_TO_STRING(arr, ',') FROM nums"},
		{"cast array", "SELECT CAST(arr AS STRING) FROM nums"},
		{"cast struct", "SELECT CAST(rec AS STRING) FROM nums"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := runSQL(t, numericCatalog(t), tt.sql)
			require.Error(t, err)
			assert.NotRegexp(t, encodedDecimal, err.Error())
		})
	}
}

func TestNumericAdversarialMath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		err  string
		want string
	}{
		{
			"pow huge",
			"SELECT POW(NUMERIC '99999999999999999999999999999.999999999', NUMERIC '99999999999')",
			"overflow",
			"",
		},
		{"pow huge big", "SELECT POW(BIGNUMERIC '" + bigMax + "', BIGNUMERIC '" + bigMax + "')", "overflow", ""},
		{"pow tiny", "SELECT POW(NUMERIC '0.000000001', NUMERIC '99999999999')", "", "0"},
		{
			"pow negative huge",
			"SELECT POW(NUMERIC '-99999999999999999999999999999', NUMERIC '99999999999')",
			"overflow",
			"",
		},
		{"pow exact limit", "SELECT POW(NUMERIC '1.000000001', NUMERIC '256')", "", "1.000000256"},
		{"pow over limit", "SELECT POW(NUMERIC '10', NUMERIC '257')", "overflow", ""},
		{"pow fractional", "SELECT POW(NUMERIC '2', NUMERIC '0.5')", "", "1.414213562"},
		{"pow negative base", "SELECT POW(NUMERIC '-2', NUMERIC '0.5')", "invalid argument", ""},
		{"exp huge", "SELECT EXP(NUMERIC '99999999999999999999999999999')", "overflow", ""},
		{"exp very negative", "SELECT EXP(NUMERIC '-99999999999999999999999999999')", "", "0"},
		{"exp 200", "SELECT EXP(NUMERIC '201')", "overflow", ""},
		{
			"ln tiny",
			"SELECT LN(BIGNUMERIC '0.00000000000000000000000000000000000001')",
			"",
			"-87.49823353377373599268367527800583988884",
		},
		{"log10 big", "SELECT LOG10(BIGNUMERIC '" + bigMax + "')", "", ""},
		{"sqrt big", "SELECT SQRT(BIGNUMERIC '" + bigMax + "')", "", ""},
		{
			"round huge digits",
			"SELECT ROUND(NUMERIC '1.5', 9223372036854775807), ROUND(NUMERIC '1.5', -9223372036854775807)",
			"",
			"",
		},
		{"trunc huge digits", "SELECT TRUNC(NUMERIC '1.5', 9223372036854775807)", "", "1.5"},
		{
			"generate huge",
			"SELECT ARRAY_LENGTH(GENERATE_ARRAY(NUMERIC '0', NUMERIC '1000000000', NUMERIC '0.000000001'))",
			"too large",
			"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, err := runSQL(t, numericCatalog(t), tt.sql)
			if tt.err != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.err)

				return
			}

			require.NoError(t, err)

			if tt.want != "" {
				assert.Equal(t, tt.want, wireRows(res)[0][0])
			}
		})
	}
}
