package timestreamquery_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamquery"
)

func arithBackend() *timestreamquery.InMemoryBackend {
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	rec := func(ago int, host, val string) timestreamquery.SourceRecord {
		return timestreamquery.SourceRecord{
			Time: now.Add(-time.Duration(ago) * time.Minute), MeasureName: "m", MeasureValueType: "BIGINT",
			MeasureValue: val, Dimensions: []timestreamquery.SourceDimension{{Name: "host", Value: host}},
		}
	}
	multi := func(ago int, a, b string) timestreamquery.SourceRecord {
		return timestreamquery.SourceRecord{
			Time: now.Add(-time.Duration(ago) * time.Minute), MeasureName: "mm", MeasureValueType: "MULTI",
			Measures: []timestreamquery.SourceMeasure{
				{Name: "a", Value: a, Type: "BIGINT"}, {Name: "b", Value: b, Type: "BIGINT"},
			},
		}
	}

	src := &memSource{tables: map[string][]timestreamquery.SourceRecord{
		"db.q":  {rec(5, "c", "4"), rec(4, "b", "7"), rec(3, "b", "3"), rec(2, "a", "0"), rec(1, "a", "10")},
		"db.mm": {multi(2, "3", "4"), multi(1, "5", "6")},
	}}

	b := timestreamquery.NewInMemoryBackend("000000000000", "us-east-1")
	b.SetClock(func() time.Time { return now })
	b.SetRecordSource(src)

	return b
}

func arithDatum(d timestreamquery.Datum) string {
	switch {
	case d.NullValue != nil:
		return "null"
	case d.ScalarValue != nil:
		return *d.ScalarValue
	default:
		parts := make([]string, len(d.ArrayValue))
		for i, e := range d.ArrayValue {
			parts[i] = arithDatum(e)
		}

		return "[" + strings.Join(parts, ",") + "]"
	}
}

func arithRun(t *testing.T, b *timestreamquery.InMemoryBackend, q string) (string, string, error) {
	t.Helper()

	page, err := b.QueryWithOptions(t.Context(), timestreamquery.QueryOptions{QueryString: q})
	if err != nil {
		return "", "", err
	}

	rows := make([]string, 0, len(page.Rows))

	for _, r := range page.Rows {
		cells := make([]string, 0, len(r.Data))
		for _, d := range r.Data {
			cells = append(cells, arithDatum(d))
		}

		rows = append(rows, strings.Join(cells, "|"))
	}

	cols := make([]string, 0, len(page.Columns))
	for _, c := range page.Columns {
		cols = append(cols, c.Type.ScalarType)
	}

	return strings.Join(rows, "/"), strings.Join(cols, ","), nil
}

func TestArithCorpus_Positions(t *testing.T) {
	t.Parallel()

	const v = "measure_value::bigint"

	tests := []struct {
		name  string
		query string
		want  string
		types string
	}{
		{"select list", `SELECT ` + v + `+1 FROM db.q ORDER BY time`, "5/8/4/1/11", "BIGINT"},
		{"where", `SELECT ` + v + ` FROM db.q WHERE ` + v + `*2 > 10 ORDER BY 1`, "7/10", ""},
		{"join on", `SELECT a.` + v + ` FROM db.q a JOIN db.q b ON a.` + v + `+1 = b.` + v + ` ORDER BY 1`, "3", ""},
		{
			"group by expr",
			`SELECT ` + v + ` % 3 AS m, count(*) FROM db.q GROUP BY ` + v + ` % 3 ORDER BY m`,
			"0|2/1|3",
			"",
		},
		{"group by ordinal", `SELECT ` + v + ` % 3, count(*) FROM db.q GROUP BY 1 ORDER BY 1`, "0|2/1|3", ""},
		{"group by host", `SELECT host, sum(` + v + `) FROM db.q GROUP BY host ORDER BY host`, "a|10/b|10/c|4", ""},
		{
			"having",
			`SELECT host, sum(` + v + `) s FROM db.q GROUP BY host HAVING sum(` + v + `)+1 > 5 ORDER BY host`,
			"a|10/b|10", "",
		},
		{"order by expr", `SELECT ` + v + ` FROM db.q ORDER BY ` + v + `*-1`, "10/7/4/3/0", ""},
		{"order by ordinal", `SELECT ` + v + `*2 FROM db.q ORDER BY 1`, "0/6/8/14/20", ""},
		{"order by alias", `SELECT ` + v + `-5 AS d FROM db.q ORDER BY d DESC`, "5/2/-1/-2/-5", ""},
		{
			"window arg", `SELECT sum(` + v + `+1) OVER (ORDER BY time) FROM db.q ORDER BY time`,
			"5/13/17/18/29", "BIGINT",
		},
		{
			"window partition order",
			`SELECT row_number() OVER (PARTITION BY ` + v + ` % 2 ORDER BY ` + v + `*-1) FROM db.q ORDER BY time`,
			"2/1/2/3/1", "",
		},
		{
			"frame bound expr",
			`SELECT sum(` + v + `) OVER (ORDER BY time ROWS BETWEEN 1+1 PRECEDING AND CURRENT ROW) FROM db.q ORDER BY time`,
			"4/11/14/10/13",
			"BIGINT",
		},
		{
			"frame bound inverse",
			`SELECT sum(` + v + `) OVER (ORDER BY time ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM db.q ORDER BY time`,
			"4/11/10/3/10",
			"",
		},
		{
			"case",
			`SELECT CASE WHEN ` + v + `+1 > 5 THEN ` + v + `*2 ELSE ` + v + `-1 END FROM db.q ORDER BY time`,
			"3/14/2/-1/20", "",
		},
		{"in list", `SELECT ` + v + ` FROM db.q WHERE ` + v + ` IN (1+2, 5*2) ORDER BY 1`, "3/10", ""},
		{"between", `SELECT ` + v + ` FROM db.q WHERE ` + v + ` BETWEEN 1+1 AND 4*2 ORDER BY 1`, "3/4/7", ""},
		{
			"date_add arithmetic",
			`SELECT date_add('hour', 1+1, TIMESTAMP '2020-01-01 00:00:00')`,
			"2020-01-01 02:00:00.000000000",
			"",
		},
		{"bin", `SELECT bin(TIMESTAMP '2020-01-01 00:10:30', 5m)`, "2020-01-01 00:10:00.000000000", ""},
		{"time interval", `SELECT TIMESTAMP '2020-01-01 00:00:00' + 1h, TIMESTAMP '2020-01-01 00:00:00' - 30m`,
			"2020-01-01 01:00:00.000000000|2019-12-31 23:30:00.000000000", ""},
		{
			"timestamp subtraction",
			`SELECT TIMESTAMP '2020-01-02 00:00:00' - TIMESTAMP '2020-01-01 00:00:00'`,
			"1 00:00:00.000000000",
			"",
		},
		{
			"agg exprs",
			`SELECT sum(` + v + `*` + v + `), avg(` + v + `+1), count(*)+1, max(` + v + `)-min(` + v + `) FROM db.q`,
			"174|5.8|6|10",
			"",
		},
		{"subquery", `SELECT x*2 FROM (SELECT ` + v + `+1 AS x FROM db.q) ORDER BY 1`, "2/8/10/16/22", ""},
		{"cte", `WITH c AS (SELECT ` + v + `*3 AS y FROM db.q) SELECT sum(y)+1 FROM c`, "73", ""},
		{
			"union",
			`SELECT ` + v + `+1 FROM db.q WHERE ` + v + `=0 UNION ALL ` +
				`SELECT ` + v + `*2 FROM db.q WHERE ` + v + `=10 ORDER BY 1`,
			"1/20", "",
		},
		{"lambda transform", `SELECT transform(ARRAY[1,2,3], x -> x*2+1)`, "[3,5,7]", ""},
		{"lambda filter", `SELECT filter(ARRAY[1,2,3,4], x -> x % 2 = 0)`, "[2,4]", ""},
		{"lambda reduce", `SELECT reduce(ARRAY[1,2,3], 0, (s,x) -> s + x*x, s -> s)`, "14", ""},
		{
			"nested lambda",
			`SELECT transform(ARRAY[1,2], x -> reduce(ARRAY[1,2], 0, (s,y) -> s+y*x, s -> s))`,
			"[3,6]",
			"",
		},
		{
			"unnest timeseries",
			`SELECT sum(t.value*2) FROM (SELECT create_time_series(time, ` + v + `) ts FROM db.q) ` +
				`CROSS JOIN UNNEST(ts) AS t(time, value)`,
			"48", "",
		},
		{"multi measure", `SELECT a*b+1 FROM db.mm ORDER BY a`, "13/31", "BIGINT"},
		{"mixed double", `SELECT ` + v + `+1.5 FROM db.q ORDER BY time LIMIT 1`, "5.5", "DOUBLE"},
		{
			"mixed double typeof",
			`SELECT typeof(` + v + `*1.0), typeof(` + v + `+1), typeof(1+1), typeof(1+3000000000)  FROM db.q LIMIT 1`,
			"double|bigint|integer|bigint",
			"",
		},
		{"negative literals", `SELECT -5, - 5, -(-5), -(2+3), 3 - -2, -5 + 2`, "-5|-5|5|-5|5|-3", ""},
		{
			"neg column",
			`SELECT -` + v + `, -(` + v + `+1), - (` + v + `) FROM db.q ORDER BY time LIMIT 1`,
			"-4|-5|-4",
			"",
		},
		{
			"assoc",
			`SELECT 10 - 3 - 2, 10 - (3 - 2), 2*3+4*5, -2*3, 100 / 5 * 2, 100 % 7 % 4, 2 + 3 * 4, (2+3)*4`,
			"5|9|26|-6|40|2|14|20",
			"",
		},
		{
			"concat",
			`SELECT 'a' || CAST(1+2 AS varchar) || 'b', CAST(1+2 AS varchar) || CAST(2*3 AS varchar)`,
			"a3b|36",
			"",
		},
		{"comparison", `SELECT 1+1 = 2 AND 2*2 = 4 OR 1 = 0, 1+1 < 3 = true`, "true|true", ""},
		{
			"nulls",
			`SELECT NULL + 1, 1 * NULL, -CAST(NULL AS bigint), abs(CAST(NULL AS bigint)), ` +
				`1 / CAST(NULL AS bigint), mod(NULL, 2)`,
			"null|null|null|null|null|null",
			"",
		},
		{
			"operator chars in strings",
			`SELECT 'a+b', 'x - y', '1/0', 'a+b' || CAST(1+1 AS varchar)`,
			"a+b|x - y|1/0|a+b2",
			"",
		},
		{
			"operator chars in identifiers",
			`SELECT "col-1" + 1, "a*b" FROM (SELECT 1 AS "col-1", 2 AS "a*b")`,
			"2|2",
			"",
		},
		{"comments", "SELECT 1 + /* x */ 2, 3 * 4 -- tail\n", "3|12", ""},
		{
			"guard case",
			`SELECT CASE WHEN ` + v + ` = 0 THEN NULL ELSE 100 / ` + v + ` END FROM db.q ORDER BY time`,
			"25/14/33/null/10",
			"",
		},
		{"guard if", `SELECT IF(` + v + ` <> 0, 100 / ` + v + `, 0) FROM db.q ORDER BY time`, "25/14/33/0/10", ""},
		{"guard nullif", `SELECT COALESCE(100 / NULLIF(` + v + `, 0), 0) FROM db.q ORDER BY time`, "25/14/33/0/10", ""},
		{"guard nullif null", `SELECT 100 / NULLIF(` + v + `, 0) FROM db.q ORDER BY time`, "25/14/33/null/10", ""},
		{
			"guard where",
			`SELECT ` + v + ` FROM db.q WHERE ` + v + ` <> 0 AND 100 / ` + v + ` > 10 ORDER BY 1`,
			"3/4/7",
			"",
		},
		{"guard agg", `SELECT sum(CASE WHEN ` + v + `=0 THEN 0 ELSE 100/` + v + ` END) FROM db.q`, "82", ""},
		{"guard agg filter", `SELECT sum(100/` + v + `) FILTER (WHERE ` + v + ` <> 0) FROM db.q`, "82", ""},
		{"guard try", `SELECT try(100 / ` + v + `) FROM db.q ORDER BY time`, "25/14/33/null/10", ""},
		{
			"guard having",
			`SELECT host FROM db.q GROUP BY host HAVING min(` + v + `) <> 0 AND 100 / min(` + v + `) > 1 ORDER BY 1`,
			"b/c",
			"",
		},
		{
			"guard subquery",
			`SELECT x FROM (SELECT ` + v + ` AS x FROM db.q WHERE ` + v + ` <> 0) WHERE 100 / x > 10 ORDER BY 1`,
			"3/4/7",
			"",
		},
		{"guard constant case", `SELECT CASE WHEN ` + v + ` > 100 THEN 1 / 0 ELSE 5 END FROM db.q LIMIT 1`, "5", ""},
		{"guard constant coalesce", `SELECT COALESCE(5, 1 / 0), IF(1 = 2, 1 / 0, 3)`, "5|3", ""},
		{
			"max min n",
			`SELECT max(` + v + `, 2), min(` + v + `, 2), max(` + v + `, 9) FROM db.q`,
			"[10,7]|[0,3]|[10,7,4,3,0]",
			"",
		},
		{
			"max n grouped",
			`SELECT host, max(` + v + `, 2) FROM db.q GROUP BY host ORDER BY host`,
			"a|[10,0]/b|[7,3]/c|[4]",
			"",
		},
		{"max n text", `SELECT max(host, 2), min(host, 1) FROM db.q`, `[c,b]|[a]`, ""},
		{"max n double", `SELECT max(CAST(` + v + ` AS double), 2) FROM db.q`, "[10.0,7.0]", ""},
		{"max n empty", `SELECT max(` + v + `, 2), min(` + v + `, 2) FROM db.q WHERE ` + v + ` > 100`, "null|null", ""},
		{
			"percentile array",
			`SELECT approx_percentile(` + v + `, ARRAY[0.5, 0.9, 0.0]), approx_percentile(` + v + `, ARRAY[0.5]) FROM db.q`,
			"[4,10,0]|[4]",
			"",
		},
		{
			"percentile array double",
			`SELECT approx_percentile(CAST(` + v + ` AS double), ARRAY[1.0]) FROM db.q`,
			"[10.0]",
			"",
		},
		{"sum empty", `SELECT sum(` + v + `) FROM db.q WHERE ` + v + ` > 100`, "null", "BIGINT"},
		{
			"sum distinct filter",
			`SELECT sum(DISTINCT ` + v + ` % 2), sum(` + v + `) FILTER (WHERE ` + v + ` > 3)` + ` FROM db.q`,
			"1|21",
			"",
		},
		{
			"sum double",
			`SELECT sum(CAST(` + v + ` AS double)), typeof(sum(CAST(` + v + ` AS double)))  FROM db.q`,
			"24.0|double",
			"",
		},
		{
			"nan render",
			`SELECT 0.0/0, 1.0/0, -1.0/0, is_nan(0.0/0), is_infinite(1.0/0), is_finite(1.0/0), is_finite(1.0)`,
			"NaN|Infinity|-Infinity|true|true|false|true",
			"",
		},
		{"inf arithmetic", `SELECT 1.0/0 + 1, 1.0/0 * -1, -(1.0/0)`, "Infinity|-Infinity|-Infinity", ""},
		{
			"nan is null downstream",
			`SELECT sum(x), 0.0/0 + 1, x > 1.0, x = x FROM (SELECT 0.0/0 AS x)`,
			"null|NaN|null|null",
			"",
		},
		{
			"nan cast varchar",
			`SELECT CAST(0.0/0 AS varchar), is_nan(0.0/0), is_nan(1.0), is_finite(0.0/0)`,
			"NaN|true|false|false",
			"",
		},
		{
			"inf order",
			`SELECT x FROM (SELECT 1.0/0 AS x UNION ALL SELECT 1.0 UNION ALL SELECT -1.0/0) ORDER BY x`,
			"-Infinity/1.0/Infinity",
			"",
		},
		{"varchar NaN", `SELECT 'NaN', typeof('NaN'), 'NaN' = CAST(0.0/0 AS varchar)`, "NaN|varchar|true", ""},
		{
			"int literal limits",
			`SELECT 3000000000, typeof(3000000000), typeof(2147483647)`,
			"3000000000|bigint|integer",
			"",
		},
		{"count times", `SELECT count(*) * 1000, typeof(count(*) * 1000) FROM db.q`, "5000|bigint", ""},
		{
			"cast varchar forms",
			`SELECT CAST(' 5 ' AS bigint), CAST('+5' AS bigint), CAST('-0' AS integer)`,
			"5|5|0",
			"BIGINT,BIGINT,INTEGER",
		},
		{
			"try cast rejects",
			`SELECT TRY_CAST('1.0' AS bigint), TRY_CAST('1e3' AS bigint), TRY_CAST('0x10' AS bigint), ` +
				`TRY_CAST('yes' AS boolean), TRY_CAST('' AS bigint)`,
			"null|null|null|null|null",
			"",
		},
		{
			"cast double text",
			`SELECT CAST('NaN' AS double), CAST('Infinity' AS double), CAST('-Infinity' AS double), CAST(' 1.5 ' AS double)`,
			"NaN|Infinity|-Infinity|1.5",
			"",
		},
		{
			"cast rounding",
			`SELECT CAST(2.5 AS integer), CAST(-0.5 AS bigint), CAST(-1.5 AS bigint), CAST(0.49 AS bigint)`,
			"3|-1|-2|0",
			"",
		},
		{
			"cast bool spellings",
			`SELECT CAST('T' AS boolean), CAST('f' AS boolean), CAST('1' AS boolean), CAST('TRUE' AS boolean)`,
			"true|false|true|true",
			"",
		},
		{
			"cast bigint to integer",
			`SELECT CAST(bigint '2147483647' AS integer), CAST(bigint '-2147483648' AS integer)`,
			"2147483647|-2147483648",
			"INTEGER,INTEGER",
		},
	}

	b := arithBackend()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, types, err := arithRun(t, b, tc.query)
			require.NoError(t, err, tc.query)
			assert.Equal(t, tc.want, got, tc.query)

			if tc.types != "" {
				assert.Equal(t, tc.types, types)
			}
		})
	}
}

func TestArithCorpus_Errors(t *testing.T) {
	t.Parallel()

	const v = "measure_value::bigint"

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			"overflow in where",
			`SELECT 1 FROM db.q WHERE ` + v + ` * 4611686018427387904 > 0`,
			"bigint multiplication overflow",
		},
		{"overflow in sum", `SELECT sum(` + v + ` * 4611686018427387904) FROM db.q`, "overflow"},
		{"overflow in lambda", `SELECT transform(ARRAY[2147483647], x -> x + 1)`, "integer addition overflow"},
		{
			"overflow in group by",
			`SELECT 1 FROM db.q GROUP BY ` + v + ` + 9223372036854775807`,
			"bigint addition overflow",
		},
		{
			"zero in having",
			`SELECT host FROM db.q GROUP BY host HAVING 1 / (sum(` + v + `) - 10) > 0`,
			"Division by zero",
		},
		{"cast integer range", `SELECT CAST(bigint '2147483648' AS integer)`, "Out of range for integer: 2147483648"},
		{"cast double integer range", `SELECT CAST(1e10 AS integer)`, "Out of range for integer"},
		{"cast hex", `SELECT CAST('0x10' AS bigint)`, "Cannot cast '0x10' to BIGINT"},
		{"cast fractional text", `SELECT CAST('1.0' AS bigint)`, "Cannot cast '1.0' to BIGINT"},
		{"max n zero", `SELECT max(` + v + `, 0) FROM db.q`, "second argument must be positive"},
		{
			"percentile range",
			`SELECT approx_percentile(` + v + `, ARRAY[1.5]) FROM db.q`,
			"percentage must be between 0 and 1",
		},
		{"cast in where", `SELECT 1 FROM db.q WHERE CAST(host AS bigint) > 0`, "Cannot cast 'c' to BIGINT"},
	}

	b := arithBackend()

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, _, err := arithRun(t, b, tc.query)
			require.Error(t, err, tc.query)
			assert.Contains(t, err.Error(), tc.want)
			assert.NotContains(t, err.Error(), "\x1e")
		})
	}
}
