package timestreamquery_test

import (
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	tqsdk "github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	"github.com/aws/aws-sdk-go-v2/service/timestreamquery/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/timestreamquery"
)

func sdkNow() time.Time { return time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC) }

type sdkClock struct{ nanos atomic.Int64 }

func newSDKClock() *sdkClock {
	c := &sdkClock{}
	c.nanos.Store(sdkNow().UnixNano())

	return c
}

func (c *sdkClock) now() time.Time { return time.Unix(0, c.nanos.Load()).UTC() }

func (c *sdkClock) advance(d time.Duration) { c.nanos.Add(int64(d)) }

// hostPartitioned reports the host dimension as the customer-defined partition key of db.m.
type hostPartitioned struct{ *memSource }

func (hostPartitioned) TablePartitionKeys(_, _, table string) []string {
	if table == "m" {
		return []string{"host"}
	}

	return nil
}

func sdkTables() map[string][]timestreamquery.SourceRecord {
	at := func(d time.Duration) time.Time { return sdkNow().Add(-d) }
	dims := []timestreamquery.SourceDimension{{Name: "host", Value: "a"}}

	m := make([]timestreamquery.SourceRecord, 0, 4)

	for i, h := range []string{"h1", "h2", "h3", "h4"} {
		m = append(m, timestreamquery.SourceRecord{
			Time: at(time.Duration(i+1) * 10 * time.Minute), MeasureName: "cpu", MeasureValueType: "DOUBLE",
			MeasureValue: "5", Dimensions: []timestreamquery.SourceDimension{{Name: "host", Value: h}},
		})
	}

	return map[string][]timestreamquery.SourceRecord{
		"db.t": {
			{
				Time:             at(3 * time.Hour),
				MeasureName:      "cpu",
				MeasureValueType: "DOUBLE",
				MeasureValue:     "1",
				Dimensions:       dims,
			},
			{
				Time:             at(20 * time.Minute),
				MeasureName:      "cpu",
				MeasureValueType: "DOUBLE",
				MeasureValue:     "10",
				Dimensions:       dims,
			},
			{
				Time:             at(5 * time.Minute),
				MeasureName:      "cpu",
				MeasureValueType: "DOUBLE",
				MeasureValue:     "30",
				Dimensions:       dims,
			},
			{Time: at(time.Minute), MeasureName: "n", MeasureValueType: "BIGINT", MeasureValue: "7"},
		},
		"db.m": m,
	}
}

func newSDKQueryClient(t *testing.T) *tqsdk.Client {
	t.Helper()

	client, _ := newSDKQueryEnv(t)

	return client
}

func newSDKQueryEnv(t *testing.T) (*tqsdk.Client, *sdkClock) {
	t.Helper()

	clock := newSDKClock()
	backend := timestreamquery.NewInMemoryBackend(rtTestAccountID, rtTestRegion)
	backend.SetClock(clock.now)
	backend.SetRecordSource(hostPartitioned{&memSource{tables: sdkTables()}})

	registry := service.NewRegistry()
	require.NoError(t, registry.Register(timestreamquery.NewHandler(backend)))

	e := echo.New()
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion(rtTestRegion),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	return tqsdk.NewFromConfig(cfg, func(o *tqsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.Retryer = aws.NopRetryer{}
	}), clock
}

func typeLabel(t *types.Type) string {
	switch {
	case t == nil:
		return "<nil>"
	case t.ArrayColumnInfo != nil:
		return "ARRAY(" + typeLabel(t.ArrayColumnInfo.Type) + ")"
	case t.TimeSeriesMeasureValueColumnInfo != nil:
		return "TIMESERIES(" + typeLabel(t.TimeSeriesMeasureValueColumnInfo.Type) + ")"
	case t.RowColumnInfo != nil:
		parts := make([]string, len(t.RowColumnInfo))
		for i, f := range t.RowColumnInfo {
			parts[i] = aws.ToString(f.Name) + ":" + typeLabel(f.Type)
		}

		return "ROW(" + strings.Join(parts, ",") + ")"
	default:
		return string(t.ScalarType)
	}
}

func datumLabel(d types.Datum) string {
	switch {
	case d.NullValue != nil:
		return "null"
	case d.ScalarValue != nil:
		return *d.ScalarValue
	case d.RowValue != nil:
		parts := make([]string, len(d.RowValue.Data))
		for i, e := range d.RowValue.Data {
			parts[i] = datumLabel(e)
		}

		return "(" + strings.Join(parts, ",") + ")"
	case d.TimeSeriesValue != nil:
		parts := make([]string, len(d.TimeSeriesValue))
		for i, p := range d.TimeSeriesValue {
			parts[i] = datumLabel(*p.Value)
		}

		return "ts[" + strings.Join(parts, ",") + "]"
	default:
		parts := make([]string, len(d.ArrayValue))
		for i, e := range d.ArrayValue {
			parts[i] = datumLabel(e)
		}

		return "[" + strings.Join(parts, ",") + "]"
	}
}

func queryLabels(t *testing.T, client *tqsdk.Client, q string) (string, string) {
	t.Helper()

	out, err := client.Query(t.Context(), &tqsdk.QueryInput{QueryString: aws.String(q)})
	require.NoError(t, err)

	cols := make([]string, len(out.ColumnInfo))
	for i, c := range out.ColumnInfo {
		cols[i] = typeLabel(c.Type)
	}

	if len(out.Rows) == 0 {
		return strings.Join(cols, ","), ""
	}

	cells := make([]string, len(out.Rows[0].Data))
	for i, d := range out.Rows[0].Data {
		cells[i] = datumLabel(d)
	}

	return strings.Join(cols, ","), strings.Join(cells, "|")
}

func TestSDKQuery_StaticColumnTypes(t *testing.T) {
	t.Parallel()

	const (
		fromCPU = ` FROM db.t WHERE measure_name = 'cpu'`
		series  = `create_time_series(time, measure_value::double)`
	)

	tests := []struct {
		name      string
		query     string
		wantTypes string
		wantRow   string
	}{
		{
			"scalar types",
			`SELECT 1, 3000000000, 1.5, 'a', true, NULL, DATE '2020-01-02', TIMESTAMP '2020-01-02 03:04:05', ` +
				`INTERVAL '1' DAY, INTERVAL '1' YEAR, TIME '10:11:12'`,
			"INTEGER,BIGINT,DOUBLE,VARCHAR,BOOLEAN,UNKNOWN,DATE,TIMESTAMP,INTERVAL_DAY_TO_SECOND," +
				"INTERVAL_YEAR_TO_MONTH,TIME",
			"1|3000000000|1.5|a|true|null|2020-01-02|2020-01-02 03:04:05.000000000|1 00:00:00.000000000|1-0|" +
				"10:11:12.000000000",
		},
		{
			"nested types",
			`SELECT ARRAY[1,2,3], ROW(1,'a'), CAST(ROW(1,2) AS ROW(a bigint, b bigint)), ARRAY[ROW(1,'a')]`,
			"ARRAY(INTEGER),ROW(:INTEGER,:VARCHAR),ROW(a:BIGINT,b:BIGINT),ARRAY(ROW(:INTEGER,:VARCHAR))",
			"[1,2,3]|(1,a)|(1,2)|[(1,a)]",
		},
		{
			"empty result",
			`SELECT host, measure_value::double > 5 AS big, count(*), time, ` + series +
				` FROM db.t WHERE measure_name = 'nope' GROUP BY host, time`,
			"VARCHAR,BOOLEAN,BIGINT,TIMESTAMP,TIMESERIES(DOUBLE)",
			"",
		},
		{
			"empty star",
			`SELECT * FROM db.t WHERE 1 = 0`,
			"VARCHAR,VARCHAR,TIMESTAMP,BIGINT,DOUBLE",
			"",
		},
		{
			"boolean case cast",
			`SELECT CASE WHEN measure_value::double > 5 THEN true ELSE false END, ` +
				`CAST(measure_value::double > 5 AS boolean), coalesce(measure_value::double > 5, false), ` +
				`if(measure_value::double > 5, true, false), NOT contains(ARRAY[1], 1), ` +
				`CASE WHEN true THEN measure_value::double > 5 END` + fromCPU + ` ORDER BY time LIMIT 1`,
			"BOOLEAN,BOOLEAN,BOOLEAN,BOOLEAN,BOOLEAN,BOOLEAN",
			"false|false|false|false|false|false",
		},
		{
			"aggregates",
			`SELECT count(*), sum(measure_value::double), avg(measure_value::double), max(time), min(host), ` +
				`approx_percentile(measure_value::double, 0.5), count_if(measure_value::double > 5), ` +
				`sum(1), max(1.5)` + fromCPU,
			"BIGINT,DOUBLE,DOUBLE,TIMESTAMP,VARCHAR,DOUBLE,BIGINT,BIGINT,DOUBLE",
			"3|41.0|13.666666666666666|2026-10-31 11:55:00.000000000|a|10.0|2|3|1.5",
		},
		{
			"series column",
			`SELECT host, ` + series + fromCPU + ` GROUP BY host`,
			"VARCHAR,TIMESERIES(DOUBLE)",
			"a|ts[1.0,10.0,30.0]",
		},
		{
			"date functions",
			`SELECT now(), current_date, ago(1h), bin(time, 1h), date_trunc('day', time), ` +
				`to_milliseconds(time), parse_duration('1d'), time - ago(1h), year(time), date_diff('day', time, now())` +
				fromCPU + ` ORDER BY time LIMIT 1`,
			"TIMESTAMP,DATE,TIMESTAMP,TIMESTAMP,TIMESTAMP,BIGINT,INTERVAL_DAY_TO_SECOND,INTERVAL_DAY_TO_SECOND," +
				"BIGINT,BIGINT",
			"2026-10-31 12:00:00.000000000|2026-10-31|2026-10-31 11:00:00.000000000|2026-10-31 09:00:00.000000000|" +
				"2026-10-31 00:00:00.000000000|1793437200000|1 00:00:00.000000000|-0 02:00:00.000000000|2026|0",
		},
		{
			"set operation supertype",
			`SELECT * FROM (SELECT 1 AS a UNION ALL SELECT 2.5)`,
			"DOUBLE",
			"1.0",
		},
		{
			"join and cte",
			`WITH x AS (SELECT host AS h, count(*) AS c FROM db.t GROUP BY host) ` +
				`SELECT a.host, x.c, a.time FROM db.t a JOIN x ON a.host = x.h ORDER BY a.time LIMIT 1`,
			"VARCHAR,BIGINT,TIMESTAMP",
			"a|3|2026-10-31 09:00:00.000000000",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newSDKQueryClient(t)
			gotTypes, gotRow := queryLabels(t, client, tc.query)
			assert.Equal(t, tc.wantTypes, gotTypes)
			assert.Equal(t, tc.wantRow, gotRow)
		})
	}
}

func TestSDKQuery_Typeof(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			"scalars",
			`SELECT typeof(1), typeof(3000000000), typeof(1.5), typeof('a'), typeof(true), typeof(NULL), ` +
				`typeof(DATE '2020-01-02'), typeof(TIME '10:11:12'), typeof(TIMESTAMP '2020-01-02 03:04:05'), ` +
				`typeof(INTERVAL '1' DAY), typeof(INTERVAL '1' YEAR)`,
			"integer|bigint|double|varchar|boolean|unknown|date|time|timestamp|interval day to second|" +
				"interval year to month",
		},
		{
			"nested",
			`SELECT typeof(ARRAY[1,2]), typeof(ROW(1,'a')), typeof(CAST(ROW(1,2) AS ROW(a bigint, b varchar))), ` +
				`typeof(ARRAY[ARRAY[1.5]])`,
			"array(integer)|row(integer, varchar)|row(a bigint, b varchar)|array(array(double))",
		},
		{
			"columns",
			`SELECT typeof(host), typeof(time), typeof(measure_name), typeof(measure_value::double), ` +
				`typeof(count(*)), typeof(avg(measure_value::double)), typeof(1 = 1) ` +
				`FROM db.t WHERE measure_name = 'cpu'`,
			"varchar|timestamp|varchar|double|bigint|double|boolean",
		},
		{
			"series",
			`SELECT typeof(create_time_series(time, measure_value::double)) FROM db.t WHERE measure_name = 'cpu'`,
			"timeseries(double)",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newSDKQueryClient(t)
			_, row := queryLabels(t, client, tc.query)
			assert.Equal(t, tc.want, row)
		})
	}
}

func TestSDKQuery_SeriesLambdas(t *testing.T) {
	t.Parallel()

	const (
		series = `create_time_series(time, measure_value::double)`
		from   = ` FROM db.t WHERE measure_name = 'cpu'`
	)

	tests := []struct {
		name      string
		query     string
		wantTypes string
		wantRow   string
	}{
		{
			"filter ignoring element",
			`SELECT filter(` + series + `, x -> true)` + from,
			"TIMESERIES(DOUBLE)", "ts[1.0,10.0,30.0]",
		},
		{
			"filter reading element",
			`SELECT filter(` + series + `, x -> x.value > 5)` + from,
			"TIMESERIES(DOUBLE)", "ts[10.0,30.0]",
		},
		{
			"reduce ignoring element",
			`SELECT reduce(` + series + `, 0, (s, x) -> s + 1, s -> s)` + from,
			"INTEGER", "3",
		},
		{
			"reduce reading element",
			`SELECT reduce(` + series + `, 0.0, (s, x) -> s + x.value, s -> s)` + from,
			"DOUBLE", "41.0",
		},
		{
			"comment inside lambda",
			`SELECT filter(` + series + `, x -> x.value > 5 -- keep big` + "\n" + `) /* tail */` + from + ` -- end`,
			"TIMESERIES(DOUBLE)", "ts[10.0,30.0]",
		},
		{
			"comment markers in strings",
			`SELECT '-- not a comment', '/* nor this */' /* real */ -- real`,
			"VARCHAR,VARCHAR", "-- not a comment|/* nor this */",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newSDKQueryClient(t)
			gotTypes, gotRow := queryLabels(t, client, tc.query)
			assert.Equal(t, tc.wantTypes, gotTypes)
			assert.Equal(t, tc.wantRow, gotRow)
		})
	}
}
