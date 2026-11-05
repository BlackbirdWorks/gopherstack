package timestreamquery_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamquery"
)

var fnNow = time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)

func renderDatum(d timestreamquery.Datum) string {
	switch {
	case d.ScalarValue != nil:
		return *d.ScalarValue
	case d.TimeSeriesValue != nil:
		parts := make([]string, 0, len(d.TimeSeriesValue))
		for _, p := range d.TimeSeriesValue {
			parts = append(parts, p.Time+"="+renderDatum(p.Value))
		}

		return "ts[" + strings.Join(parts, ", ") + "]"
	case d.ArrayValue != nil:
		parts := make([]string, 0, len(d.ArrayValue))
		for _, e := range d.ArrayValue {
			parts = append(parts, renderDatum(e))
		}

		return "[" + strings.Join(parts, ", ") + "]"
	default:
		return "<null>"
	}
}

func renderType(c timestreamquery.ColumnType) string {
	switch {
	case c.TimeSeriesMeasureValueColumnInfo != nil:
		return "timeseries(" + renderType(c.TimeSeriesMeasureValueColumnInfo.Type) + ")"
	case c.ArrayColumnInfo != nil:
		return "array(" + renderType(c.ArrayColumnInfo.Type) + ")"
	default:
		return c.ScalarType
	}
}

func fnBackend(t *testing.T) (*timestreamquery.InMemoryBackend, *memSource) {
	t.Helper()

	dbl := func(m time.Duration, v string, host string) timestreamquery.SourceRecord {
		return timestreamquery.SourceRecord{
			Time: fnNow.Add(-m), MeasureName: "cpu", MeasureValueType: "DOUBLE", MeasureValue: v,
			Dimensions: []timestreamquery.SourceDimension{{Name: "host", Value: host}},
		}
	}

	src := &memSource{tables: map[string][]timestreamquery.SourceRecord{"db.t": {
		dbl(20*time.Minute, "10", "a"), dbl(10*time.Minute, "20", "a"), dbl(5*time.Minute, "30", "a"),
		dbl(20*time.Minute, "5", "b"), dbl(10*time.Minute, "10", "b"), dbl(5*time.Minute, "15", "b"),
	}}}

	b := timestreamquery.NewInMemoryBackend("000000000000", "us-east-1")
	b.SetClock(func() time.Time { return fnNow })
	b.SetRecordSource(src)

	return b, src
}

func runFn(t *testing.T, q string) ([][]string, []string) {
	t.Helper()

	b, _ := fnBackend(t)

	page, err := b.QueryWithOptions(t.Context(), timestreamquery.QueryOptions{QueryString: q})
	require.NoError(t, err, q)

	rows := make([][]string, 0, len(page.Rows))
	for _, r := range page.Rows {
		row := make([]string, 0, len(r.Data))
		for _, d := range r.Data {
			row = append(row, renderDatum(d))
		}

		rows = append(rows, row)
	}

	types := make([]string, 0, len(page.Columns))
	for _, c := range page.Columns {
		types = append(types, renderType(c.Type))
	}

	return rows, types
}

// Examples and results come from the Timestream Developer Guide, "Date / time functions".
func TestQuery_DateTimeFunctionsDocExamples(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{"date", `SELECT date(TIMESTAMP '2022-07-07 17:44:43.771000000')`, "2022-07-07"},
		{"last_day_of_month", `SELECT last_day_of_month(TIMESTAMP '2022-07-07 17:44:43.771000000')`, "2022-07-31"},
		{
			"from_iso8601_timestamp",
			`SELECT from_iso8601_timestamp('2022-06-17T08:04:05.000000000+05:00')`, "2022-06-17 03:04:05.000000000",
		},
		{"from_iso8601_date", `SELECT from_iso8601_date('2022-07-17')`, "2022-07-17"},
		{"to_iso8601", `SELECT to_iso8601(from_iso8601_date('2022-06-17'))`, "2022-06-17"},
		{"from_milliseconds", `SELECT from_milliseconds(1)`, "1970-01-01 00:00:00.001000000"},
		{"from_nanoseconds", `SELECT from_nanoseconds(300000001)`, "1970-01-01 00:00:00.300000001"},
		{"from_unixtime", `SELECT from_unixtime(1)`, "1970-01-01 00:00:01.000000000"},
		{"to_milliseconds interval", `SELECT to_milliseconds(INTERVAL '2' DAY + INTERVAL '3' HOUR)`, "183600000"},
		{
			"to_milliseconds timestamp",
			`SELECT to_milliseconds(TIMESTAMP '2022-06-17 17:44:43.771000000')`, "1655487883771",
		},
		{"to_nanoseconds interval", `SELECT to_nanoseconds(INTERVAL '2' DAY + INTERVAL '3' HOUR)`, "183600000000000"},
		{
			"to_nanoseconds timestamp",
			`SELECT to_nanoseconds(TIMESTAMP '2022-06-17 17:44:43.771000678')`, "1655487883771000678",
		},
		{
			"date_trunc",
			`SELECT date_trunc('minute', TIMESTAMP '2022-06-17 17:44:43.771000000')`, "2022-06-17 17:44:00.000000000",
		},
		{
			"date_add", `SELECT date_add('hour', 9, TIMESTAMP '2022-06-17 00:00:00')`, "2022-06-17 09:00:00.000000000",
		},
		{"date_diff", `SELECT date_diff('day', DATE '2020-03-01', DATE '2020-03-02')`, "1"},
		{"parse_duration", `SELECT parse_duration('42.8ms')`, "0 00:00:00.042800000"},
		{"bin 5m", `SELECT bin(TIMESTAMP '2022-06-17 10:15:20', 5m)`, "2022-06-17 10:15:00.000000000"},
		{"bin 1d", `SELECT bin(TIMESTAMP '2022-06-17 10:15:20', 1d)`, "2022-06-17 00:00:00.000000000"},
		{"bin 10day", `SELECT bin(TIMESTAMP '2022-06-17 10:15:20', 10day)`, "2022-06-17 00:00:00.000000000"},
		{"bin 30day", `SELECT bin(TIMESTAMP '2022-06-17 10:15:20', 30day)`, "2022-05-28 00:00:00.000000000"},
		{"ago", `SELECT ago(1d)`, "2026-10-30 12:00:00.000000000"},
		{"current_timezone", `SELECT current_timezone()`, "UTC"},
		{
			"date_format",
			`SELECT date_format(TIMESTAMP '2019-10-20 10:20:20', '%Y-%m-%d %H:%i:%s')`, "2019-10-20 10:20:20",
		},
		{
			"date_parse",
			`SELECT date_parse('2019-10-20 10:20:20', '%Y-%m-%d %H:%i:%s')`, "2019-10-20 10:20:20.000000000",
		},
		{"extract", `SELECT extract(YEAR FROM '2019-10-12 23:10:34.000000000')`, "2019"},
		{"day", `SELECT day('2019-10-12 23:10:34.000000000')`, "12"},
		{"day_of_week", `SELECT day_of_week('2019-10-12 23:10:34.000000000')`, "6"},
		{"day_of_year", `SELECT day_of_year('2019-10-12 23:10:34.000000000')`, "285"},
		{"dow", `SELECT dow('2019-10-12 23:10:34.000000000')`, "6"},
		{"doy", `SELECT doy('2019-10-12 23:10:34.000000000')`, "285"},
		{"hour", `SELECT hour('2019-10-12 23:10:34.000000000')`, "23"},
		{"millisecond", `SELECT millisecond('2019-10-12 23:10:34.000000000')`, "0"},
		{"minute", `SELECT minute('2019-10-12 23:10:34.000000000')`, "10"},
		{"month", `SELECT month('2019-10-12 23:10:34.000000000')`, "10"},
		{"nanosecond", `SELECT nanosecond(TIMESTAMP '2019-10-12 23:10:34.162000000')`, "162000000"},
		{"quarter", `SELECT quarter('2019-10-12 23:10:34.000000000')`, "4"},
		{"second", `SELECT second('2019-10-12 23:10:34.000000000')`, "34"},
		{"week", `SELECT week('2019-10-12 23:10:34.000000000')`, "41"},
		{"week_of_year", `SELECT week_of_year('2019-10-12 23:10:34.000000000')`, "41"},
		{"year", `SELECT year('2019-10-12 23:10:34.000000000')`, "2019"},
		{"year_of_week", `SELECT year_of_week('2019-10-12 23:10:34.000000000')`, "2019"},
		{"yow", `SELECT yow('2019-10-12 23:10:34.000000000')`, "2019"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows, _ := runFn(t, tt.query)
			require.Len(t, rows, 1)
			assert.Equal(t, tt.want, rows[0][0])
		})
	}
}

func TestQuery_TimeSeriesFunctions(t *testing.T) {
	t.Parallel()

	const (
		series = `create_time_series(time, measure_value::double)`
		fromT  = `FROM db.t WHERE host = 'a'`
	)

	tests := []struct {
		name      string
		query     string
		wantRow   string
		wantTypes string
	}{
		{
			"create_time_series", `SELECT ` + series + ` ` + fromT,
			"ts[2026-10-31 11:40:00.000000000=10.0, 2026-10-31 11:50:00.000000000=20.0, 2026-10-31 11:55:00.000000000=30.0]",
			"timeseries(DOUBLE)",
		},
		{
			"interpolate_linear array",
			`SELECT interpolate_linear(` + series + `, sequence(min(time), max(time), 5m)) ` + fromT,
			"ts[2026-10-31 11:40:00.000000000=10.0, 2026-10-31 11:45:00.000000000=15.0, " +
				"2026-10-31 11:50:00.000000000=20.0, 2026-10-31 11:55:00.000000000=30.0]",
			"timeseries(DOUBLE)",
		},
		{
			"interpolate_linear scalar",
			`SELECT interpolate_linear(` + series + `, min(time) + 5m) ` + fromT, "15.0", "DOUBLE",
		},
		{
			"interpolate_locf array",
			`SELECT interpolate_locf(` + series + `, sequence(min(time), max(time), 5m)) ` + fromT,
			"ts[2026-10-31 11:40:00.000000000=10.0, 2026-10-31 11:45:00.000000000=10.0, " +
				"2026-10-31 11:50:00.000000000=20.0, 2026-10-31 11:55:00.000000000=30.0]",
			"timeseries(DOUBLE)",
		},
		{
			"interpolate_locf scalar",
			`SELECT interpolate_locf(` + series + `, min(time) + 7m) ` + fromT, "10.0", "DOUBLE",
		},
		{
			"interpolate_fill array",
			`SELECT interpolate_fill(` + series + `, sequence(min(time), max(time), 5m), 99.0) ` + fromT,
			"ts[2026-10-31 11:40:00.000000000=10.0, 2026-10-31 11:45:00.000000000=99.0, " +
				"2026-10-31 11:50:00.000000000=20.0, 2026-10-31 11:55:00.000000000=30.0]",
			"timeseries(DOUBLE)",
		},
		{
			"interpolate_fill scalar",
			`SELECT interpolate_fill(` + series + `, min(time) + 5m, 99.0) ` + fromT, "99.0", "DOUBLE",
		},
		{
			"interpolate_spline_cubic hits observed points",
			`SELECT interpolate_spline_cubic(` + series + `, sequence(min(time), max(time), 5m)) ` + fromT,
			"ts[2026-10-31 11:40:00.000000000=10.0, 2026-10-31 11:45:00.000000000=13.75, " +
				"2026-10-31 11:50:00.000000000=20.0, 2026-10-31 11:55:00.000000000=30.0]",
			"timeseries(DOUBLE)",
		},
		{
			"derivative_linear",
			`SELECT derivative_linear(` + series + `, 1m) ` + fromT,
			"ts[2026-10-31 11:50:00.000000000=1.0, 2026-10-31 11:55:00.000000000=2.0]", "timeseries(DOUBLE)",
		},
		{
			"non_negative_derivative_linear",
			`SELECT non_negative_derivative_linear(create_time_series(time, 100.0 - measure_value::double), 1m) ` + fromT,
			"ts[]", "timeseries(DOUBLE)",
		},
		{"integral_trapezoidal interval", `SELECT integral_trapezoidal(` + series + `, 5m) ` + fromT, "55.0", "DOUBLE"},
		{"integral_trapezoidal default 1s", `SELECT integral_trapezoidal(` + series + `) ` + fromT, "16500.0", "DOUBLE"},
		{"correlate_pearson self", `SELECT correlate_pearson(` + series + `, ` + series + `) ` + fromT, "1.0", "DOUBLE"},
		{"correlate_spearman self", `SELECT correlate_spearman(` + series + `, ` + series + `) ` + fromT, "1.0", "DOUBLE"},
		{
			"correlate_pearson anti",
			`SELECT correlate_pearson(` + series + `, create_time_series(time, 100.0 - measure_value::double)) ` + fromT,
			"-1.0", "DOUBLE",
		},
		{
			"sequence timestamps",
			`SELECT sequence(TIMESTAMP '2022-06-17 00:00:00', TIMESTAMP '2022-06-17 00:00:30', 15s)`,
			"[2022-06-17 00:00:00.000000000, 2022-06-17 00:00:15.000000000, 2022-06-17 00:00:30.000000000]",
			"array(TIMESTAMP)",
		},
		{"sequence integers", `SELECT sequence(1, 5, 2)`, "[1, 3, 5]", "array(BIGINT)"},
		{
			"filter timeseries",
			`WITH v AS (SELECT ` + series + ` AS cpu ` + fromT + `) SELECT filter(cpu, x -> x.value > 15.0) FROM v`,
			"ts[2026-10-31 11:50:00.000000000=20.0, 2026-10-31 11:55:00.000000000=30.0]", "timeseries(DOUBLE)",
		},
		{
			"reduce sum of squares",
			`WITH v AS (SELECT ` + series + ` AS cpu ` + fromT + `) ` +
				`SELECT reduce(cpu, DOUBLE '0.0', (s, x) -> x.value * x.value + s, s -> s) FROM v`,
			"1400.0", "DOUBLE",
		},
		{
			"reduce row state fraction",
			`WITH v AS (SELECT ` + series + ` AS cpu ` + fromT + `) SELECT ROUND(REDUCE(cpu, ` +
				`CAST(ROW(0, 0) AS ROW(count_high BIGINT, count_total BIGINT)), ` +
				`(s, x) -> CAST(ROW(s.count_high + IF(x.value > 15.0, 1, 0), s.count_total + 1) ` +
				`AS ROW(count_high BIGINT, count_total BIGINT)), ` +
				`s -> IF(s.count_total = 0, NULL, CAST(s.count_high AS DOUBLE) / s.count_total)), 4) FROM v`,
			"0.6667", "DOUBLE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows, types := runFn(t, tt.query)
			require.Len(t, rows, 1)
			assert.Equal(t, tt.wantRow, rows[0][0])
			assert.Equal(t, []string{tt.wantTypes}, types)
		})
	}
}

func TestQuery_UnnestTimeSeries(t *testing.T) {
	t.Parallel()

	const interp = `WITH s AS (SELECT host, interpolate_linear(create_time_series(time, measure_value::double), ` +
		`sequence(min(time), max(time), 5m)) AS r FROM db.t GROUP BY host) `

	tests := []struct {
		name  string
		query string
		want  [][]string
	}{
		{
			"no alias uses time and value",
			interp + `SELECT time, value FROM s CROSS JOIN UNNEST(r) WHERE host = 'b' ORDER BY time`,
			[][]string{
				{"2026-10-31 11:40:00.000000000", "5.0"}, {"2026-10-31 11:45:00.000000000", "7.5"},
				{"2026-10-31 11:50:00.000000000", "10.0"}, {"2026-10-31 11:55:00.000000000", "15.0"},
			},
		},
		{
			"alias with column names",
			interp + `SELECT host, avg(t.cpu_util) FROM s CROSS JOIN UNNEST(r) AS t (time, cpu_util) GROUP BY host ORDER BY host`,
			[][]string{{"a", "18.75"}, {"b", "9.375"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rows, _ := runFn(t, tt.query)
			assert.Equal(t, tt.want, rows)
		})
	}
}

func TestQuery_TimeSeriesErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			"mismatched timestamps",
			`SELECT correlate_pearson(create_time_series(time, measure_value::double), ` +
				`create_time_series(time + 1m, measure_value::double)) FROM db.t WHERE host = 'a'`,
			"same timestamps",
		},
		{"not a series", `SELECT derivative_linear(1, 1m)`, "not a timeseries"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := fnBackend(t)
			_, err := b.QueryWithOptions(t.Context(), timestreamquery.QueryOptions{QueryString: tt.query})
			require.Error(t, err)
			assert.ErrorIs(t, err, timestreamquery.ErrValidation)
			assert.Contains(t, fmt.Sprint(err), tt.want)
		})
	}
}
