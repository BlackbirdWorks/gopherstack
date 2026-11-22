package timestreamquery_test

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamquery"
)

func corpusBackend(t *testing.T) *timestreamquery.InMemoryBackend {
	t.Helper()

	now := fnNow()
	mk := func(m time.Duration, name, typ, v, host, region string) timestreamquery.SourceRecord {
		return timestreamquery.SourceRecord{
			Time:             now.Add(-m),
			MeasureName:      name,
			MeasureValueType: typ,
			MeasureValue:     v,
			Dimensions: []timestreamquery.SourceDimension{
				{Name: "hostname", Value: host},
				{Name: "region", Value: region},
			},
		}
	}

	src := &memSource{tables: map[string][]timestreamquery.SourceRecord{"db.t": {
		mk(20*time.Minute, "cpu_utilization", "DOUBLE", "10.5", "a", "us-east-1"),
		mk(10*time.Minute, "cpu_utilization", "DOUBLE", "20", "a", "us-east-1"),
		mk(5*time.Minute, "cpu_utilization", "DOUBLE", "30", "b", "eu-west-1"),
		mk(5*time.Minute, "mem", "BIGINT", "7", "b", "eu-west-1"),
	}}}

	b := timestreamquery.NewInMemoryBackend("000000000000", "us-east-1")
	b.SetClock(fnNow)
	b.SetRecordSource(src)

	return b
}

func runCorpus(t *testing.T, q string) ([]string, [][]string) {
	t.Helper()

	page, err := corpusBackend(t).QueryWithOptions(t.Context(), timestreamquery.QueryOptions{QueryString: q})
	require.NoError(t, err, q)

	types := make([]string, 0, len(page.Columns))
	for _, c := range page.Columns {
		types = append(types, renderType(c.Type))
	}

	rows := make([][]string, 0, len(page.Rows))

	for _, r := range page.Rows {
		row := make([]string, 0, len(r.Data))
		for _, d := range r.Data {
			row = append(row, renderDatum(d))
		}

		rows = append(rows, row)
	}

	return types, rows
}

func TestQuery_StaticTypeCorpus(t *testing.T) {
	t.Parallel()

	const cpu = " FROM db.t WHERE measure_name = 'cpu_utilization'"

	tests := []struct {
		name  string
		query string
		types string
		first string
	}{
		{"count", "SELECT count(*), sum(measure_value::bigint), avg(measure_value::double) FROM db.t",
			"BIGINT,BIGINT,DOUBLE", "4|7|20.166666666666668"},
		{"group", "SELECT hostname, count(*) c FROM db.t GROUP BY hostname ORDER BY c DESC", "VARCHAR,BIGINT", "b|2"},
		{
			"bin",
			"SELECT bin(time, 1m) AS b, avg(measure_value::double) AS a" + cpu +
				" AND time > ago(1h) GROUP BY bin(time, 1m) ORDER BY b",
			"TIMESTAMP,DOUBLE",
			"2026-10-31 11:40:00.000000000|10.5",
		},
		{
			"case varchar",
			"SELECT CASE WHEN measure_value::double > 15 THEN 'high' ELSE 'low' END" + cpu + " ORDER BY time",
			"VARCHAR",
			"low",
		},
		{
			"case int",
			"SELECT CASE WHEN measure_value::double > 15 THEN 1 ELSE 0 END" + cpu + " ORDER BY time",
			"INTEGER",
			"0",
		},
		{"date_trunc", "SELECT date_trunc('hour', time), count(*) FROM db.t GROUP BY 1", "TIMESTAMP,BIGINT",
			"2026-10-31 11:00:00.000000000|4"},
		{
			"timeseries",
			"SELECT hostname, create_time_series(time, measure_value::double)" + cpu + " GROUP BY hostname ORDER BY 1",
			"VARCHAR,timeseries(DOUBLE)",
			"",
		},
		{
			"interpolate unnest",
			"SELECT hostname, time, value FROM (SELECT hostname, " +
				"INTERPOLATE_LINEAR(CREATE_TIME_SERIES(time, measure_value::double), " +
				"SEQUENCE(ago(1h), now(), 10m)) AS ts" + cpu + " GROUP BY hostname) " +
				"CROSS JOIN UNNEST(ts) AS u (time, value) ORDER BY 1, 2",
			"VARCHAR,TIMESTAMP,DOUBLE",
			"a|2026-10-31 11:40:00.000000000|10.5",
		},
		{"percentile", "SELECT approx_percentile(measure_value::double, 0.9), approx_distinct(hostname)" + cpu,
			"DOUBLE,BIGINT", "30.0|2"},
		{"cte join", "WITH a AS (SELECT hostname, avg(measure_value::double) av" + cpu + " GROUP BY hostname), " +
			"b AS (SELECT hostname, count(*) n FROM db.t GROUP BY hostname) " +
			"SELECT a.hostname, a.av, b.n FROM a JOIN b ON a.hostname = b.hostname ORDER BY 1",
			"VARCHAR,DOUBLE,BIGINT", "a|15.25|2"},
		{
			"window",
			"SELECT row_number() OVER (PARTITION BY hostname ORDER BY time), " +
				"lag(measure_value::double) OVER (ORDER BY time)" + cpu + " ORDER BY time",
			"BIGINT,DOUBLE",
			"1|<null>",
		},
		{"multi measure columns", "SELECT measure_value::bigint, measure_value::double FROM db.t LIMIT 1",
			"BIGINT,DOUBLE", ""},
		{
			"strings",
			"SELECT upper(hostname), length(hostname), concat(hostname, '-', region), split(region, '-') FROM db.t LIMIT 1",
			"VARCHAR,BIGINT,VARCHAR,array(VARCHAR)",
			"A|1|a-us-east-1|[us, east, 1]",
		},
		{
			"casts",
			"SELECT cast(1 AS double), cast('5' AS bigint), cast(time AS date), cast(1 AS varchar)" + cpu + " LIMIT 1",
			"DOUBLE,BIGINT,DATE,VARCHAR",
			"1.0|5|2026-10-31|1",
		},
		{
			"predicates",
			"SELECT hostname IN ('a'), hostname LIKE 'a%', hostname IS NOT NULL, NOT (hostname = 'a') FROM db.t LIMIT 1",
			"BOOLEAN,BOOLEAN,BOOLEAN,BOOLEAN",
			"true|true|true|false",
		},
		{"union widen", "SELECT 1 AS a UNION ALL SELECT 2.5 ORDER BY 1", "DOUBLE", "1.0"},
		{"union varchar", "SELECT hostname FROM db.t UNION SELECT region FROM db.t ORDER BY 1", "VARCHAR", "a"},
		{
			"typeof",
			"SELECT typeof(1), typeof(1.5), typeof(time), typeof(count(*)), typeof(ARRAY[1.5]) FROM db.t LIMIT 1",
			"VARCHAR,VARCHAR,VARCHAR,VARCHAR,VARCHAR",
			"integer|double|timestamp|bigint|array(double)",
		},
		{
			"arrays",
			"SELECT ARRAY[1, 2], ARRAY['a'], ARRAY[1.5, 2], sequence(1, 5, 2)",
			"array(INTEGER),array(VARCHAR),array(DOUBLE),array(INTEGER)",
			"[1, 2]|[a]|[1.5, 2.0]|[1, 3, 5]",
		},
		{"time arithmetic", "SELECT time - ago(1h), time + 1h, now() - time FROM db.t LIMIT 1",
			"INTERVAL_DAY_TO_SECOND,TIMESTAMP,INTERVAL_DAY_TO_SECOND", ""},
		{
			"integer widening",
			"SELECT 1 + CAST(2 AS bigint), 1 + 1.5, 7 / 2, 7.0 / 2, 2147483648",
			"BIGINT,DOUBLE,INTEGER,DOUBLE,BIGINT",
			"3|2.5|3|3.5|2147483648",
		},
		{"int32 overflow widens", "SELECT 2147483647 + 1", "BIGINT", "2147483648"},
		{"bigint overflow is not wrapped", "SELECT 9223372036854775807 + 1", "DOUBLE", "9.223372036854776E18"},
		{"empty result", "SELECT hostname, time, measure_value::double" + cpu + " AND hostname = 'zzz'",
			"VARCHAR,TIMESTAMP,DOUBLE", ""},
		{
			"empty aggregate",
			"SELECT count(*), max(measure_value::double) FROM db.t WHERE hostname = 'zzz'",
			"BIGINT,DOUBLE",
			"0|<null>",
		},
		{"null", "SELECT null, cast(null AS bigint), coalesce(null, 1)", "UNKNOWN,BIGINT,INTEGER", "<null>|<null>|1"},
		{"scalar subquery", "SELECT hostname, (SELECT count(*) FROM db.t) FROM db.t LIMIT 1", "VARCHAR,BIGINT", "a|4"},
		{"comments", "SELECT /* x */ 1 -- y\n, '-- not a comment'", "INTEGER,VARCHAR", "1|-- not a comment"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			types, rows := runCorpus(t, tc.query)
			assert.Equal(t, tc.types, strings.Join(types, ","))

			if tc.first != "" {
				require.NotEmpty(t, rows)
				assert.Equal(t, tc.first, strings.Join(rows[0], "|"))
			}
		})
	}
}

var (
	reCorpusInt  = regexp.MustCompile(`^-?\d+$`)
	reCorpusDbl  = regexp.MustCompile(`^(-?\d+\.\d+(E-?\d+)?|NaN|-?Infinity)$`)
	reCorpusBool = regexp.MustCompile(`^(true|false)$`)
	reCorpusTS   = regexp.MustCompile(`^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{9}$`)
	reCorpusDate = regexp.MustCompile(`^\d{4}-\d\d-\d\d$`)
)

// Every function with a fixed declared return type must produce a value of that storage class at runtime.
func TestQuery_FunctionReturnTypesMatchRuntime(t *testing.T) {
	t.Parallel()

	groups := []struct {
		typ   string
		re    *regexp.Regexp
		exprs []string
	}{
		{
			"BIGINT",
			reCorpusInt,
			[]string{
				"count(*)",
				"count_if(true)",
				"approx_distinct(hostname)",
				"length('abc')",
				"strpos('abc', 'b')",
				"cardinality(ARRAY[1])",
				"json_array_length('[1]')",
				"year(now())",
				"month(now())",
				"day(now())",
				"hour(now())",
				"minute(now())",
				"second(now())",
				"quarter(now())",
				"week(now())",
				"day_of_week(now())",
				"day_of_year(now())",
				"date_diff('second', now(), now())",
				"to_milliseconds(now())",
				"to_nanoseconds(now())",
				"codepoint('a')",
				"levenshtein_distance('a', 'b')",
				"bitwise_and(5, 3)",
				"row_number() OVER (ORDER BY time)",
				"rank() OVER (ORDER BY time)",
				"dense_rank() OVER (ORDER BY time)",
				"ntile(2) OVER (ORDER BY time)",
				"array_position(ARRAY[1, 2], 2)",
				"sum(1)",
				"extract(year FROM now())",
			},
		},
		{"DOUBLE", reCorpusDbl, []string{
			"avg(measure_value::double)", "stddev(measure_value::double)", "variance(measure_value::double)", "sqrt(4)",
			"ln(1)", "exp(0)", "power(2, 3)", "to_unixtime(now())", "pi()", "rand()", "log10(100)", "sin(0)", "cbrt(8)",
			"radians(180)", "degrees(1)", "percent_rank() OVER (ORDER BY time)", "cume_dist() OVER (ORDER BY time)",
			"avg(1)", "sum(1.5)", "ceil(1.2)", "floor(1.7)", "round(1.5)", "round(measure_value::double, 1)",
			"approx_percentile(measure_value::double, 0.5)", "geometric_mean(measure_value::double)", "abs(-1.5)",
		}},
		{
			"BOOLEAN",
			reCorpusBool,
			[]string{
				"contains(ARRAY[1], 1)",
				"regexp_like('a', 'a')",
				"starts_with('ab', 'a')",
				"any_match(ARRAY[1], x -> x > 0)",
				"bool_and(true)",
				"bool_or(false)",
				"'a' LIKE 'a%'",
				"1 IN (1, 2)",
				"1 BETWEEN 0 AND 2",
				"null IS NULL",
			},
		},
		{"TIMESTAMP", reCorpusTS, []string{
			"now()", "ago(1h)", "from_unixtime(1)", "from_milliseconds(1)", "date_parse('2020-01-01', '%Y-%m-%d')",
			"date_trunc('hour', now())", "date_add('day', 1, now())", "bin(now(), 1h)", "max(time)", "min(time)",
			"timestamp '2020-01-01 00:00:00'", "current_timestamp",
		}},
		{"DATE", reCorpusDate, []string{"current_date", "date '2020-01-01'", "cast(now() AS date)"}},
		{"VARCHAR", regexp.MustCompile(`.*`), []string{
			"lower('A')", "upper('a')", "trim(' a ')", "replace('abc', 'b', 'x')", "substr('abc', 2)",
			"date_format(now(), '%Y')", "format_datetime(now(), 'yyyy')", "to_iso8601(now())",
			"json_format(json_parse('[1]'))", "typeof(1)", "regexp_extract('a1', '[0-9]')", "lpad('a', 3, 'x')",
			"split_part('a,b', ',', 1)", "concat('a', 'b')", "chr(65)", "cast(1 AS varchar)", "arbitrary(hostname)",
			"min_by(hostname, time)", "json_extract_scalar('{\"a\":1}', '$.a')",
		}},
	}

	b := corpusBackend(t)

	for _, g := range groups {
		for _, expr := range g.exprs {
			t.Run(g.typ+"/"+expr, func(t *testing.T) {
				t.Parallel()

				page, err := b.QueryWithOptions(t.Context(), timestreamquery.QueryOptions{
					QueryString: "SELECT " + expr + " FROM db.t WHERE measure_name = 'cpu_utilization'",
				})
				require.NoError(t, err)
				require.Len(t, page.Columns, 1)
				require.NotEmpty(t, page.Rows)

				assert.Equal(
					t,
					g.typ,
					renderType(page.Columns[0].Type),
					"declared type must survive the runtime values",
				)

				for _, r := range page.Rows {
					if r.Data[0].ScalarValue != nil {
						assert.Regexp(t, g.re, *r.Data[0].ScalarValue)
					}
				}
			})
		}
	}
}

func TestQuery_TypeofEscapesRowFieldNames(t *testing.T) {
	t.Parallel()

	_, rows := runCorpus(t, `SELECT typeof(CAST(ROW(1) AS ROW("a'b" bigint)))`)
	if assert.NotEmpty(t, rows) {
		assert.Equal(t, []string{"row(a'b bigint)"}, rows[0])
	}
}

func TestQuery_InsightsTemporalRangeNeverNarrowerThanScan(t *testing.T) {
	t.Parallel()

	const (
		minute = int64(time.Minute)
		data   = 20 * minute
	)

	tests := []struct {
		name  string
		query string
		want  int64
	}{
		{"between ago and now", "SELECT * FROM db.t WHERE time BETWEEN ago(1h) AND now()", 60 * minute},
		{"gt and le", "SELECT * FROM db.t WHERE time > ago(30m) AND time <= ago(10m)", 20 * minute},
		{"no predicate scans the data extent", "SELECT * FROM db.t", data},
		{"or of ranges is not narrowed", "SELECT * FROM db.t WHERE time > ago(5m) OR hostname = 'a'", data},
		{"negated predicate", "SELECT * FROM db.t WHERE NOT (time > ago(5m))", data},
		{"not between", "SELECT * FROM db.t WHERE time NOT BETWEEN ago(5m) AND now()", data},
		{"non literal bound", "SELECT * FROM db.t WHERE time > (SELECT min(time) FROM db.t)", data},
		{"cte predicate", "WITH w AS (SELECT * FROM db.t WHERE time > ago(10m)) SELECT * FROM w", 10 * minute},
		{
			"derived column is not the table time",
			"SELECT * FROM (SELECT hostname AS time FROM db.t) WHERE time > 'x'",
			data,
		},
		{
			"left join on predicate does not prune the preserved side",
			"SELECT * FROM db.t a LEFT JOIN db.t b ON a.hostname = b.hostname AND a.time > ago(10m) WHERE b.time > ago(5m)",
			data,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := corpusBackend(t)

			page, err := b.QueryWithOptions(t.Context(), timestreamquery.QueryOptions{
				QueryString: tc.query, InsightsMode: "ENABLED_WITH_RATE_CONTROL",
			})
			require.NoError(t, err)
			require.NotNil(t, page.Insights)
			require.NotNil(t, page.Insights.TemporalRange)
			assert.Equal(t, tc.want, page.Insights.TemporalRange.Max.Value)
		})
	}
}

func TestQuery_InsightsUnsortedRecordsUseOldest(t *testing.T) {
	t.Parallel()

	now := fnNow()
	rec := func(ago time.Duration) timestreamquery.SourceRecord {
		return timestreamquery.SourceRecord{
			Time:             now.Add(-ago),
			MeasureName:      "m",
			MeasureValueType: "BIGINT",
			MeasureValue:     "1",
		}
	}

	b := timestreamquery.NewInMemoryBackend("000000000000", "us-east-1")
	b.SetClock(fnNow)
	b.SetRecordSource(&memSource{tables: map[string][]timestreamquery.SourceRecord{
		"db.t": {rec(time.Minute), rec(30 * time.Minute), rec(5 * time.Minute)},
	}})

	page, err := b.QueryWithOptions(t.Context(), timestreamquery.QueryOptions{
		QueryString: "SELECT * FROM db.t", InsightsMode: "ENABLED_WITH_RATE_CONTROL",
	})
	require.NoError(t, err)
	assert.Equal(t, int64(30*time.Minute), page.Insights.TemporalRange.Max.Value)
}
