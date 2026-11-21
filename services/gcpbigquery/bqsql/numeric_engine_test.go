package bqsql_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

func numericCatalog(t *testing.T) *fakeCatalog {
	t.Helper()

	fs := fieldsJSON(t, `[
		{"name":"id","type":"INTEGER"},
		{"name":"n","type":"NUMERIC"},
		{"name":"b","type":"BIGNUMERIC"},
		{"name":"g","type":"STRING"},
		{"name":"f","type":"FLOAT"},
		{"name":"arr","type":"NUMERIC","mode":"REPEATED"},
		{"name":"rec","type":"RECORD","fields":[{"name":"x","type":"NUMERIC"},{"name":"y","type":"BIGNUMERIC"}]}
	]`)

	rows := rowsJSON(t, fs, `[
		{"id":1,"n":"0.1","b":"0.00000000000000000000000000000000000001","g":"a","f":0.5,
		 "arr":["1.5","2.25"],"rec":{"x":"3.5","y":"4"}},
		{"id":2,"n":"0.2","b":"12345678901234567890123456789012345678.12345678901234567890123456789012345678",
		 "g":"a","f":1.5,"arr":[],"rec":{"x":"-1","y":"-2.5"}},
		{"id":3,"n":"-0.3","b":"-5","g":"b","f":2.5,"arr":["7"],"rec":null},
		{"id":4,"n":null,"b":null,"g":"b","f":null,"arr":[],"rec":null},
		{"id":5,"n":"0.1","b":"5","g":"b","f":0.25,"arr":[],"rec":null}
	]`)

	return &fakeCatalog{tables: map[string]*bqsql.TableData{
		"ds.nums": {Ref: bqsql.TableRef{Project: "p", Dataset: "ds", Table: "nums"}, Fields: fs, Rows: rows},
	}}
}

func TestNumericQueries(t *testing.T) {
	t.Parallel()

	tests := []fnCase{
		{"add", "SELECT NUMERIC '0.1' + NUMERIC '0.2'", [][]any{{"0.3"}}},
		{"third", "SELECT NUMERIC '1' / NUMERIC '3'", [][]any{{"0.333333333"}}},
		{
			"bigthird",
			"SELECT BIGNUMERIC '1' / BIGNUMERIC '3'",
			[][]any{{"0.33333333333333333333333333333333333333"}},
		},
		{"eq", "SELECT NUMERIC '0.1' + NUMERIC '0.2' = NUMERIC '0.3'", [][]any{{"true"}}},
		{"wide", "SELECT NUMERIC '99999999999999999999999999999.999999999' - NUMERIC '0.000000001'",
			[][]any{{"99999999999999999999999999999.999999998"}}},
		{
			"intmix",
			"SELECT 1 + NUMERIC '0.5', NUMERIC '0.5' * 3, 10 / NUMERIC '4'",
			[][]any{{"1.5", "1.5", "2.5"}},
		},
		{"floatlit", "SELECT NUMERIC '1.5' + 1.25", [][]any{{"2.75"}}},
		{"mixkinds", "SELECT NUMERIC '0.1' + BIGNUMERIC '0.00000000000000000000000000000000000001'",
			[][]any{{"0.10000000000000000000000000000000000001"}}},
		{"neg", "SELECT -NUMERIC '1.5', -(NUMERIC '0')", [][]any{{"-1.5", "0"}}},
		{"cmp", "SELECT NUMERIC '1.5' > 1, NUMERIC '1.5' < 2, NUMERIC '2' >= 2, NUMERIC '2' != 2",
			[][]any{{"true", "true", "true", "false"}}},
		{
			"safe",
			"SELECT SAFE_DIVIDE(NUMERIC '1', NUMERIC '0'), SAFE_ADD(NUMERIC '99999999999999999999999999999', " +
				"NUMERIC '1e28'), SAFE_NEGATE(NUMERIC '2')",
			[][]any{{nil, nil, "-2"}}},
		{
			"round",
			"SELECT ROUND(NUMERIC '2.5'), ROUND(NUMERIC '-2.5'), ROUND(NUMERIC '1.125', 2), ROUND(NUMERIC '2.5', 0, " +
				"'ROUND_HALF_EVEN'), ROUND(NUMERIC '1250', -2)",
			[][]any{{"3", "-3", "1.13", "2", "1300"}}},
		{
			"trunc",
			"SELECT TRUNC(NUMERIC '2.75'), TRUNC(NUMERIC '-2.75', 1), CEIL(NUMERIC '2.1'), FLOOR(NUMERIC '-2.1'), " +
				"CEILING(NUMERIC '-0.5')",
			[][]any{{"2", "-2.7", "3", "-3", "0"}}},
		{
			"absmod",
			"SELECT ABS(NUMERIC '-3.5'), SIGN(NUMERIC '-3.5'), MOD(NUMERIC '7.5', NUMERIC '2'), DIV(NUMERIC '7.5', " +
				"NUMERIC '2'), MOD(7, NUMERIC '2.5')",
			[][]any{{"3.5", "-1", "1.5", "3", "2"}}},
		{
			"powers",
			"SELECT POW(NUMERIC '2', NUMERIC '10'), SQRT(NUMERIC '2'), POWER(NUMERIC '2', 3), EXP(NUMERIC '0'), " +
				"LN(NUMERIC '1'), LOG10(NUMERIC '1000'), LOG(NUMERIC '8', 2)",
			[][]any{{"1024", "1.414213562", "8", "1", "0", "3", "3"}}},
		{
			"greatest",
			"SELECT GREATEST(NUMERIC '1.5', 2, NUMERIC '0.5'), LEAST(NUMERIC '1.5', 2, NUMERIC '0.5'), " +
				"GREATEST(NUMERIC '1', NULL)",
			[][]any{{"2", "0.5", nil}}},
		{
			"casts",
			"SELECT CAST('1.5' AS NUMERIC), CAST(NUMERIC '1.5' AS STRING), CAST(NUMERIC '2.5' AS INT64), " +
				"CAST(1 AS BIGNUMERIC), CAST(0.125 AS NUMERIC)",
			[][]any{{"1.5", "1.5", "3", "1", "0.125"}}},
		{
			"castnarrow",
			"SELECT CAST(BIGNUMERIC '0.0000000005' AS NUMERIC), CAST(NUMERIC '1.5' AS FLOAT64), " +
				"SAFE_CAST('x' AS NUMERIC), SAFE_CAST('1e2' AS BIGNUMERIC)",
			[][]any{{"0.000000001", "1.5", nil, "100"}}},
		{
			"params",
			"SELECT CAST(1.125 AS NUMERIC(5, 2)), CAST('1.234567891234' AS BIGNUMERIC(10, 3))",
			[][]any{{"1.13", "1.235"}},
		},
		{"parse", "SELECT PARSE_NUMERIC('1,234.5'), PARSE_BIGNUMERIC(' - 12.5 '), PARSE_NUMERIC('1e2')",
			[][]any{{"1234.5", "-12.5", "100"}}},
		{
			"format",
			"SELECT FORMAT('%f|%.2f|%t|%T|%10.1f', NUMERIC '1.5', NUMERIC '2.345', NUMERIC '1', BIGNUMERIC '1.5', " +
				"NUMERIC '-3.25')",
			[][]any{{"1.500000|2.35|1.0|BIGNUMERIC \"1.5\"|      -3.3"}}},
		{
			"json",
			"SELECT TO_JSON_STRING(NUMERIC '1.5'), TO_JSON_STRING(STRUCT(NUMERIC '1.5' AS a, 'x' AS b)), " +
				"TO_JSON_STRING([NUMERIC '1', NUMERIC '2.5'])",
			[][]any{{"1.5", `{"a":1.5,"b":"x"}`, "[1,2.5]"}}},
		{
			"ifcase",
			"SELECT IF(TRUE, 1, NUMERIC '2.5'), CASE WHEN FALSE THEN 1 ELSE NUMERIC '2.5' END, COALESCE(NULL, " +
				"NUMERIC '1.5'), IFNULL(NULL, 1) + NUMERIC '0.5', NULLIF(NUMERIC '1', 1)",
			[][]any{{"1", "2.5", "1.5", "1.5", nil}}},
		{"union", "SELECT 1 AS v UNION ALL SELECT NUMERIC '2.5' ORDER BY v", [][]any{{"1"}, {"2.5"}}},
		{
			"in",
			"SELECT NUMERIC '1.5' IN (1, NUMERIC '1.5'), NUMERIC '1' BETWEEN 0 AND NUMERIC '2', " +
				"3 IN UNNEST([NUMERIC '3'])",
			[][]any{{"true", "true", "true"}}},
		{
			"arrays",
			"SELECT ARRAY<NUMERIC>[1, 2.5], ARRAY_LENGTH([NUMERIC '1', NUMERIC '2']), [NUMERIC '1', " +
				"NUMERIC '2'][OFFSET(1)]",
			[][]any{{[]map[string]any{{"v": "1"}, {"v": "2.5"}}, "2", "2"}}},
		{"castarray", "SELECT CAST([1, 2] AS ARRAY<NUMERIC>)[OFFSET(1)] / 4", [][]any{{"0.5"}}},
		{"genarray", "SELECT GENERATE_ARRAY(NUMERIC '0', NUMERIC '1', NUMERIC '0.5')",
			[][]any{{[]map[string]any{{"v": "0"}, {"v": "0.5"}, {"v": "1"}}}}},
		{"struct", "SELECT STRUCT(NUMERIC '1.5' AS a).a + 1", [][]any{{"2.5"}}},
		{
			"unnest",
			"SELECT x + NUMERIC '0.5' FROM UNNEST([NUMERIC '1', NUMERIC '2']) AS x ORDER BY 1",
			[][]any{{"1.5"}, {"2.5"}},
		},
		{"stddev", "SELECT STDDEV_POP(x) FROM UNNEST([NUMERIC '1', NUMERIC '3']) AS x", [][]any{{"1.0"}}},
		{
			"variance",
			"SELECT VAR_SAMP(x), VAR_POP(x), VARIANCE(x) FROM UNNEST([NUMERIC '1', NUMERIC '2', NUMERIC '3', " +
				"NUMERIC '4']) AS x",
			[][]any{{"1.6666666666666667", "1.25", "1.6666666666666667"}}},
		{
			"varexact",
			"SELECT VAR_POP(x) FROM UNNEST([NUMERIC '99999999999999999999999999999.999999999', " +
				"NUMERIC '99999999999999999999999999999.999999997']) AS x",
			[][]any{{"1e-18"}}},
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

func TestNumericFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args string
		want string
	}{
		{"e", "'%e', NUMERIC '392.65'", "3.926500e+02"},
		{"upper", "'%E', NUMERIC '392.65'", "3.926500E+02"},
		{"g", "'%g', NUMERIC '392.65'", "392.65"},
		{"gexp", "'%g', NUMERIC '39265000'", "3.9265e+07"},
		{"gsmall", "'%g|%g', NUMERIC '0.0001', NUMERIC '0.00001'", "0.0001|1e-05"},
		{"gzero", "'%g', NUMERIC '0'", "0"},
		{"sharp", "'%#g', NUMERIC '1.5'", "1.50000"},
		{"prec", "'%.3e', BIGNUMERIC '12345678901234567890.123456789012345678'", "1.235e+19"},
		{"carry", "'%.2e', NUMERIC '9.999'", "1.00e+01"},
		{"neg", "'%e', NUMERIC '-0.5'", "-5.000000e-01"},
		{"f", "'%f', NUMERIC '392.65'", "392.650000"},
		{"fwide", "'%.12f', NUMERIC '1.5'", "1.500000000000"},
		{
			"fflags",
			"'%+.2f|%08.2f|%-8.1f|', NUMERIC '1.005', NUMERIC '-1.5', NUMERIC '2.25'",
			"+1.01|-0001.50|2.3     |",
		},
		{"fzero", "'%.0f|%#.0f', NUMERIC '2.5', NUMERIC '2.5'", "3|3."},
		{
			"digits",
			"'%.30f', BIGNUMERIC '0.12345678901234567890123456789012345678'",
			"0.123456789012345678901234567890",
		},
		{"tt", "'%t|%T', NUMERIC '12', NUMERIC '1.25'", "12.0|NUMERIC \"1.25\""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, err := runSQL(t, numericCatalog(t), "SELECT FORMAT("+tt.args+")")
			require.NoError(t, err)
			assert.Equal(t, [][]any{{tt.want}}, wireRows(res))
		})
	}
}

func TestNumericTables(t *testing.T) {
	t.Parallel()

	tests := []fnCase{
		{
			"order",
			"SELECT id FROM nums WHERE n IS NOT NULL ORDER BY n, id",
			[][]any{{"3"}, {"1"}, {"5"}, {"2"}},
		},
		{"orderdesc", "SELECT id FROM nums ORDER BY b DESC, id", [][]any{{"2"}, {"5"}, {"1"}, {"3"}, {"4"}}},
		{"group", "SELECT n, COUNT(*) FROM nums WHERE n IS NOT NULL GROUP BY n ORDER BY n",
			[][]any{{"-0.3", "1"}, {"0.1", "2"}, {"0.2", "1"}}},
		{
			"distinct",
			"SELECT DISTINCT n FROM nums WHERE n IS NOT NULL ORDER BY n",
			[][]any{{"-0.3"}, {"0.1"}, {"0.2"}},
		},
		{
			"sum",
			"SELECT SUM(n), AVG(n), MIN(n), MAX(n), COUNT(DISTINCT n), ANY_VALUE(g) FROM nums WHERE g = 'a'",
			[][]any{{"0.3", "0.15", "0.1", "0.2", "2", "a"}}},
		{"sumall", "SELECT SUM(n), AVG(n) FROM nums", [][]any{{"0.1", "0.025"}}},
		{
			"bigsum",
			"SELECT SUM(b) FROM nums WHERE id <= 2",
			[][]any{{"12345678901234567890123456789012345678.12345678901234567890123456789012345679"}},
		},
		{"emptysum", "SELECT SUM(n) FROM nums WHERE id > 100", [][]any{{nil}}},
		{"groupsum", "SELECT g, SUM(n) FROM nums GROUP BY g ORDER BY g", [][]any{{"a", "0.3"}, {"b", "-0.2"}}},
		{
			"window",
			"SELECT id, " +
				"SUM(n) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM nums WHERE n IS NOT NULL ORDER BY id",
			[][]any{{"1", "0.1"}, {"2", "0.3"}, {"3", "-0.1"}, {"5", "-0.2"}}},
		{
			"winavg",
			"SELECT id, AVG(n) OVER () FROM nums WHERE id <= 2 ORDER BY id",
			[][]any{{"1", "0.15"}, {"2", "0.15"}},
		},
		{
			"lag",
			"SELECT id, LAG(n) OVER (ORDER BY id) FROM nums WHERE id <= 2 ORDER BY id",
			[][]any{{"1", nil}, {"2", "0.1"}},
		},
		{"join", "SELECT a.id, c.id FROM nums a JOIN nums c ON a.n = c.n AND a.id < c.id", [][]any{{"1", "5"}}},
		{"arrayagg", "SELECT ARRAY_AGG(n ORDER BY n) FROM nums WHERE n IS NOT NULL",
			[][]any{{[]map[string]any{{"v": "-0.3"}, {"v": "0.1"}, {"v": "0.1"}, {"v": "0.2"}}}}},
		{"nested", "SELECT arr, rec FROM nums WHERE id = 1",
			[][]any{{
				[]map[string]any{{"v": "1.5"}, {"v": "2.25"}},
				map[string]any{"f": []map[string]any{{"v": "3.5"}, {"v": "4"}}},
			}}},
		{
			"nestedmath",
			"SELECT rec.x + rec.y, (SELECT SUM(a) FROM UNNEST(arr) AS a) FROM nums WHERE id = 1",
			[][]any{{"7.5", "3.75"}},
		},
		{"mixfloat", "SELECT n + f FROM nums WHERE id = 1", [][]any{{"0.6"}}},
		{"bigmix", "SELECT n + b FROM nums WHERE id = 3", [][]any{{"-5.3"}}},
		{"compare", "SELECT id FROM nums WHERE n > 0.1 OR n < 0 ORDER BY id", [][]any{{"2"}, {"3"}}},
		{
			"between",
			"SELECT id FROM nums WHERE n BETWEEN 0.1 AND 0.2 ORDER BY id",
			[][]any{{"1"}, {"2"}, {"5"}},
		},
		{"in", "SELECT id FROM nums WHERE n IN (0.1, 0.2) ORDER BY id", [][]any{{"1"}, {"2"}, {"5"}}},
		{
			"case",
			"SELECT CASE n WHEN 0.1 THEN 'x' ELSE 'y' END FROM nums WHERE id IN (1, 3) ORDER BY id",
			[][]any{{"x"}, {"y"}},
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

func TestNumericErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		msg  string
	}{
		{
			"overflow",
			"SELECT NUMERIC '99999999999999999999999999999.999999999' + NUMERIC '1'",
			"numeric overflow",
		},
		{"mulover", "SELECT NUMERIC '1e20' * NUMERIC '1e20'", "numeric overflow"},
		{"divzero", "SELECT NUMERIC '1' / NUMERIC '0'", "division by zero: 1 / 0"},
		{"bigover", "SELECT BIGNUMERIC '5e38' + BIGNUMERIC '5e38'", "BIGNUMERIC overflow"},
		{"castover", "SELECT CAST(BIGNUMERIC '1e30' AS NUMERIC)", "numeric overflow"},
		{"badstr", "SELECT CAST('abc' AS NUMERIC)", "invalid NUMERIC value"},
		{"nanfloat", "SELECT CAST(IEEE_DIVIDE(1, 0) AS NUMERIC)", "cannot convert"},
		{"constrain", "SELECT CAST(1000 AS NUMERIC(5, 2))", "out of range"},
		{"sqrtneg", "SELECT SQRT(NUMERIC '-1')", "invalid argument to SQRT"},
		{"ordinal", "SELECT ROUND(1.5, 0, 'ROUND_HALF_EVEN')", "rounding mode"},
		{"badmode", "SELECT ROUND(NUMERIC '1.5', 0, 'X')", "rounding mode"},
		{"fmtd", "SELECT FORMAT('%d', NUMERIC '1')", "Invalid format specifier"},
		{
			"sumover",
			"SELECT SUM(x) FROM UNNEST([NUMERIC '99999999999999999999999999999', " +
				"NUMERIC '99999999999999999999999999999']) AS x",
			"numeric overflow",
		},
		{"castbool", "SELECT CAST(TRUE AS NUMERIC)", "Invalid cast"},
		{
			"rangeframe",
			"SELECT SUM(id) OVER (ORDER BY n RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM nums",
			"RANGE window frame offsets",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := runSQL(t, numericCatalog(t), tt.sql)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.msg)
		})
	}
}

func TestNumericScriptsAndRoutines(t *testing.T) {
	t.Parallel()

	tests := []fnCase{
		{"rollup", "SELECT g, SUM(n) FROM nums GROUP BY ROLLUP(g) ORDER BY g NULLS FIRST",
			[][]any{{nil, "0.1"}, {"a", "0.3"}, {"b", "-0.2"}}},
		{"having", "SELECT g FROM nums GROUP BY g HAVING SUM(n) > 0 ORDER BY g", [][]any{{"a"}}},
		{
			"lagdefault",
			"SELECT LAG(n, 1, 0) OVER (ORDER BY id) FROM nums WHERE id <= 2 ORDER BY id",
			[][]any{{"0"}, {"0.1"}},
		},
		{
			"pivot",
			"SELECT * FROM (SELECT g, n FROM nums WHERE n IS NOT NULL) PIVOT (SUM(n) FOR g IN ('a', 'b'))",
			[][]any{{"0.3", "-0.2"}}},
		{
			"structarr",
			"SELECT s.x * 2 FROM UNNEST([STRUCT(NUMERIC '1.5' AS x), STRUCT(NUMERIC '2' AS x)]) AS s ORDER BY 1",
			[][]any{{"3"}, {"4"}}},
		{"quantiles", "SELECT APPROX_QUANTILES(x, 2) FROM UNNEST([NUMERIC '3', NUMERIC '1', NUMERIC '2']) AS x",
			[][]any{{[]map[string]any{{"v": "1"}, {"v": "2"}, {"v": "3"}}}}},
		{"distinctunion", "SELECT NUMERIC '1.0' UNION DISTINCT SELECT NUMERIC '1'", [][]any{{"1"}}},
		{"exceptall", "SELECT n FROM nums WHERE id = 1 EXCEPT DISTINCT SELECT NUMERIC '0.1'", [][]any{}},
		{
			"hll",
			"SELECT HLL_COUNT.EXTRACT(HLL_COUNT.INIT(x)) FROM UNNEST([NUMERIC '1', NUMERIC '1', NUMERIC '2']) AS x",
			[][]any{{"2"}}},
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
