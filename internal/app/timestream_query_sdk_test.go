package app

import (
	"log/slog"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	tqsdk "github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	tqtypes "github.com/aws/aws-sdk-go-v2/service/timestreamquery/types"
	twsdk "github.com/aws/aws-sdk-go-v2/service/timestreamwrite"
	twtypes "github.com/aws/aws-sdk-go-v2/service/timestreamwrite/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	timestreamquerybackend "github.com/blackbirdworks/gopherstack/services/timestreamquery"
	timestreamwritebackend "github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

type tsEnv struct {
	query *tqsdk.Client
	write *twsdk.Client
	now   *atomic.Int64
}

func newTimestreamEnv(t *testing.T, start time.Time) *tsEnv {
	t.Helper()

	cli := &CLI{AccountID: "000000000000", Region: "us-east-1"}
	appCtx := &service.AppContext{Logger: slog.Default(), Config: cli, JanitorCtx: t.Context()}
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServicesT(t, appCtx)
	require.NoError(t, err)

	byName := serviceByName(services)
	qh, ok := byName["TimestreamQuery"].(*timestreamquerybackend.Handler)
	require.True(t, ok)

	wh, ok := byName["TimestreamWrite"].(*timestreamwritebackend.Handler)
	require.True(t, ok)

	now := &atomic.Int64{}
	now.Store(start.UnixNano())
	clock := func() time.Time { return time.Unix(0, now.Load()).UTC() }

	qh.Backend.(*timestreamquerybackend.InMemoryBackend).SetClock(clock)
	wh.Backend.SetClock(clock)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(qh))
	require.NoError(t, registry.Register(wh))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	return &tsEnv{
		now:   now,
		query: tqsdk.NewFromConfig(cfg, func(o *tqsdk.Options) { o.BaseEndpoint = aws.String(srv.URL) }),
		write: twsdk.NewFromConfig(cfg, func(o *twsdk.Options) { o.BaseEndpoint = aws.String(srv.URL) }),
	}
}

func dim(name, value string) twtypes.Dimension {
	return twtypes.Dimension{Name: aws.String(name), Value: aws.String(value)}
}

func ms(t time.Time) *string { return aws.String(strconv.FormatInt(t.UnixMilli(), 10)) }

func single(name, value string, typ twtypes.MeasureValueType, at time.Time, dims ...twtypes.Dimension) twtypes.Record {
	return twtypes.Record{
		MeasureName: aws.String(name), MeasureValue: aws.String(value), MeasureValueType: typ,
		Time: ms(at), TimeUnit: twtypes.TimeUnitMilliseconds, Dimensions: dims,
	}
}

func (e *tsEnv) seed(t *testing.T, db, table string, props *twtypes.RetentionProperties, recs ...twtypes.Record) {
	t.Helper()

	_, err := e.write.CreateDatabase(t.Context(), &twsdk.CreateDatabaseInput{DatabaseName: aws.String(db)})
	if err != nil {
		require.ErrorContains(t, err, "ConflictException")
	}

	_, err = e.write.CreateTable(t.Context(), &twsdk.CreateTableInput{
		DatabaseName: aws.String(db), TableName: aws.String(table), RetentionProperties: props,
		MagneticStoreWriteProperties: &twtypes.MagneticStoreWriteProperties{EnableMagneticStoreWrites: aws.Bool(true)},
	})
	require.NoError(t, err)

	if len(recs) == 0 {
		return
	}

	_, err = e.write.WriteRecords(t.Context(), &twsdk.WriteRecordsInput{
		DatabaseName: aws.String(db), TableName: aws.String(table), Records: recs,
	})
	require.NoError(t, err)
}

func cell(d tqtypes.Datum) string {
	if d.NullValue != nil && *d.NullValue {
		return "<null>"
	}

	return aws.ToString(d.ScalarValue)
}

func rowsOf(out *tqsdk.QueryOutput) [][]string {
	rows := make([][]string, 0, len(out.Rows))
	for _, r := range out.Rows {
		row := make([]string, 0, len(r.Data))
		for _, d := range r.Data {
			row = append(row, cell(d))
		}

		rows = append(rows, row)
	}

	return rows
}

func typesOf(out *tqsdk.QueryOutput) []string {
	ts := make([]string, 0, len(out.ColumnInfo))
	for _, c := range out.ColumnInfo {
		ts = append(ts, aws.ToString(c.Name)+":"+string(c.Type.ScalarType))
	}

	return ts
}

func seedSensors(t *testing.T, e *tsEnv, t0 time.Time) {
	t.Helper()

	retention := &twtypes.RetentionProperties{
		MemoryStoreRetentionPeriodInHours: aws.Int64(24), MagneticStoreRetentionPeriodInDays: aws.Int64(365),
	}
	ago := func(d time.Duration) time.Time { return t0.Add(-d) }
	dbl := twtypes.MeasureValueTypeDouble

	e.seed(t, "sensors", "readings", retention,
		single("cpu", "10", dbl, ago(30*time.Minute), dim("host", "a"), dim("region", "us")),
		single("cpu", "20", dbl, ago(20*time.Minute), dim("host", "a"), dim("region", "us")),
		single("cpu", "30", dbl, ago(5*time.Minute), dim("host", "a"), dim("region", "us")),
		single("cpu", "50.5", dbl, ago(25*time.Minute), dim("host", "b"), dim("region", "eu")),
		single("cpu", "70", dbl, ago(3*time.Minute), dim("host", "b"), dim("region", "eu")),
		single("cpu", "1", dbl, ago(3*time.Hour), dim("host", "a"), dim("region", "us")),
		single("reqs", "5", twtypes.MeasureValueTypeBigint, ago(10*time.Minute), dim("host", "a"), dim("region", "us")),
		single("state", "up", twtypes.MeasureValueTypeVarchar, ago(time.Minute), dim("host", "a"), dim("region", "us")),
		twtypes.Record{
			MeasureName: aws.String("metrics"), MeasureValueType: twtypes.MeasureValueTypeMulti,
			Time: ms(ago(2 * time.Minute)), TimeUnit: twtypes.TimeUnitMilliseconds,
			Dimensions: []twtypes.Dimension{dim("host", "c")},
			MeasureValues: []twtypes.MeasureValue{
				{Name: aws.String("cpu_pct"), Value: aws.String("73.5"), Type: twtypes.MeasureValueTypeDouble},
				{Name: aws.String("mem"), Value: aws.String("2048"), Type: twtypes.MeasureValueTypeBigint},
				{Name: aws.String("ok"), Value: aws.String("true"), Type: twtypes.MeasureValueTypeBoolean},
			},
		},
	)
}

func TestTimestreamQuery_SDK_QueriesWrittenRecords(t *testing.T) {
	t.Parallel()

	t0 := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	env := newTimestreamEnv(t, t0)
	seedSensors(t, env, t0)

	const tbl = `"sensors"."readings"`

	cases := []struct {
		name      string
		query     string
		wantRows  [][]string
		wantTypes []string
	}{
		{
			name: "order by time desc limit with ago and double cast",
			query: `SELECT host, measure_value::double FROM ` + tbl +
				` WHERE measure_name = 'cpu' AND time > ago(1h) ORDER BY time DESC LIMIT 2`,
			wantRows:  [][]string{{"b", "70.0"}, {"a", "30.0"}},
			wantTypes: []string{"host:VARCHAR", "measure_value::double:DOUBLE"},
		},
		{
			name: "group by bin with avg and max",
			query: `SELECT bin(time, 10m) AS t, avg(measure_value::double) AS a, max(measure_value::double) AS m FROM ` +
				tbl + ` WHERE measure_name = 'cpu' AND host = 'a' AND time > ago(1h) GROUP BY bin(time, 10m) ORDER BY t`,
			wantRows: [][]string{
				{"2026-10-31 11:30:00.000000000", "10.0", "10.0"},
				{"2026-10-31 11:40:00.000000000", "20.0", "20.0"},
				{"2026-10-31 11:50:00.000000000", "30.0", "30.0"},
			},
			wantTypes: []string{"t:TIMESTAMP", "a:DOUBLE", "m:DOUBLE"},
		},
		{
			name: "count grouped by dimension",
			query: `SELECT host, count(*) AS n FROM ` + tbl +
				` WHERE measure_name = 'cpu' GROUP BY host ORDER BY host`,
			wantRows:  [][]string{{"a", "4"}, {"b", "2"}},
			wantTypes: []string{"host:VARCHAR", "n:BIGINT"},
		},
		{
			name: "time range with between and interval",
			query: `SELECT count(*) FROM ` + tbl +
				` WHERE measure_name = 'cpu' AND time BETWEEN now() - INTERVAL '15' minute AND now()`,
			wantRows:  [][]string{{"2"}},
			wantTypes: []string{"_col0:BIGINT"},
		},
		{
			name: "multi measure with null dimension",
			query: `SELECT host, region, cpu_pct, mem, ok, measure_name FROM ` + tbl +
				` WHERE measure_name = 'metrics'`,
			wantRows:  [][]string{{"c", "<null>", "73.5", "2048", "true", "metrics"}},
			wantTypes: []string{"host:VARCHAR", "region:VARCHAR", "cpu_pct:DOUBLE", "mem:BIGINT", "ok:BOOLEAN", "measure_name:VARCHAR"},
		},
		{
			name:      "bigint and varchar measures",
			query:     `SELECT measure_value::bigint, measure_value::varchar FROM ` + tbl + ` WHERE measure_name IN ('reqs','state') ORDER BY time`,
			wantRows:  [][]string{{"5", "<null>"}, {"<null>", "up"}},
			wantTypes: []string{"measure_value::bigint:BIGINT", "measure_value::varchar:VARCHAR"},
		},
		{
			name: "timestamp column formatting",
			query: `SELECT time FROM ` + tbl +
				` WHERE measure_name = 'state'`,
			wantRows:  [][]string{{"2026-10-31 11:59:00.000000000"}},
			wantTypes: []string{"time:TIMESTAMP"},
		},
		{
			name:  "approx_percentile",
			query: `SELECT approx_percentile(measure_value::double, 0.5) FROM ` + tbl + ` WHERE measure_name = 'cpu' AND host = 'b'`,
			wantRows: [][]string{{"50.5"}},
			wantTypes: []string{"_col0:DOUBLE"},
		},
		{
			name:      "describe",
			query:     `DESCRIBE ` + tbl,
			wantRows:  nil,
			wantTypes: []string{"Column:VARCHAR", "Type:VARCHAR", "Timestream attribute type:VARCHAR"},
		},
		{
			name:  "show measures",
			query: `SHOW MEASURES FROM ` + tbl,
			wantRows: [][]string{
				{"cpu", "double", `[{"data_type":"varchar","dimension_name":"host"},{"data_type":"varchar","dimension_name":"region"}]`},
				{"metrics", "multi", `[{"data_type":"varchar","dimension_name":"host"}]`},
				{"reqs", "bigint", `[{"data_type":"varchar","dimension_name":"host"},{"data_type":"varchar","dimension_name":"region"}]`},
				{"state", "varchar", `[{"data_type":"varchar","dimension_name":"host"},{"data_type":"varchar","dimension_name":"region"}]`},
			},
			wantTypes: []string{"measure_name:VARCHAR", "data_type:VARCHAR", "Dimensions:VARCHAR"},
		},
		{
			name:      "show tables",
			query:     `SHOW TABLES FROM sensors`,
			wantRows:  [][]string{{"readings"}},
			wantTypes: []string{"TableName:VARCHAR"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := env.query.Query(t.Context(), &tqsdk.QueryInput{QueryString: aws.String(tc.query)})
			require.NoError(t, err)
			assert.Equal(t, tc.wantTypes, typesOf(out))
			assert.NotEmpty(t, aws.ToString(out.QueryId))

			if tc.wantRows != nil {
				assert.Equal(t, tc.wantRows, rowsOf(out))
			}

			if tc.name == "describe" {
				assert.Contains(t, rowsOf(out), []string{"host", "varchar", "DIMENSION"})
				assert.Contains(t, rowsOf(out), []string{"time", "timestamp", "TIMESTAMP"})
				assert.Contains(t, rowsOf(out), []string{"measure_value::double", "double", "MEASURE_VALUE"})
				assert.Contains(t, rowsOf(out), []string{"cpu_pct", "double", "MULTI"})
			}
		})
	}
}

func TestTimestreamQuery_SDK_Errors(t *testing.T) {
	t.Parallel()

	t0 := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	env := newTimestreamEnv(t, t0)
	seedSensors(t, env, t0)

	cases := []struct {
		name    string
		query   string
		wantErr string
	}{
		{"unsupported function", `SELECT create_time_series(time, measure_value::double) FROM sensors.readings`, "create_time_series"},
		{"unknown column", `SELECT nope FROM sensors.readings`, "Column 'nope' does not exist"},
		{"unknown table", `SELECT * FROM sensors.missing`, "Table 'sensors.missing' does not exist"},
		{"unknown database", `SELECT * FROM nodb.readings`, "Database 'nodb' does not exist"},
		{"unqualified table", `SELECT * FROM readings`, "Schema must be specified"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := env.query.Query(t.Context(), &tqsdk.QueryInput{QueryString: aws.String(tc.query)})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "ValidationException")
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func TestTimestreamQuery_SDK_PagingAndCancel(t *testing.T) {
	t.Parallel()

	t0 := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	env := newTimestreamEnv(t, t0)
	seedSensors(t, env, t0)

	q := `SELECT host, time FROM sensors.readings ORDER BY time`
	first, err := env.query.Query(t.Context(), &tqsdk.QueryInput{QueryString: aws.String(q), MaxRows: aws.Int32(4)})
	require.NoError(t, err)
	require.Len(t, first.Rows, 4)
	require.NotNil(t, first.NextToken)

	all := rowsOf(first)
	token := first.NextToken
	pages := 1

	for token != nil {
		next, pErr := env.query.Query(t.Context(), &tqsdk.QueryInput{QueryString: aws.String(q), NextToken: token, MaxRows: aws.Int32(4)})
		require.NoError(t, pErr)
		assert.Equal(t, aws.ToString(first.QueryId), aws.ToString(next.QueryId))
		assert.Len(t, next.ColumnInfo, 2)

		all = append(all, rowsOf(next)...)
		token = next.NextToken
		pages++
	}

	assert.Equal(t, 3, pages)
	assert.Len(t, all, 9)
	assert.True(t, sortedByTime(all))

	// a token is good for five uses (one already spent paging above)
	for range 4 {
		_, rErr := env.query.Query(t.Context(), &tqsdk.QueryInput{QueryString: aws.String(q), NextToken: first.NextToken})
		require.NoError(t, rErr)
	}

	_, err = env.query.Query(t.Context(), &tqsdk.QueryInput{QueryString: aws.String(q), NextToken: first.NextToken})
	require.ErrorContains(t, err, "Invalid pagination token")

	// cancelling a query with pending pages stops further paging
	again, err := env.query.Query(t.Context(), &tqsdk.QueryInput{QueryString: aws.String(q), MaxRows: aws.Int32(2)})
	require.NoError(t, err)
	require.NotNil(t, again.NextToken)

	_, err = env.query.CancelQuery(t.Context(), &tqsdk.CancelQueryInput{QueryId: again.QueryId})
	require.NoError(t, err)

	_, err = env.query.Query(t.Context(), &tqsdk.QueryInput{QueryString: aws.String(q), NextToken: again.NextToken})
	require.ErrorContains(t, err, "cancelled query")

	// tokens expire after an hour
	third, err := env.query.Query(t.Context(), &tqsdk.QueryInput{QueryString: aws.String(q), MaxRows: aws.Int32(2)})
	require.NoError(t, err)

	env.now.Add(int64(61 * time.Minute))

	_, err = env.query.Query(t.Context(), &tqsdk.QueryInput{QueryString: aws.String(q), NextToken: third.NextToken})
	require.ErrorContains(t, err, "Invalid pagination token")
}

func sortedByTime(rows [][]string) bool {
	for i := 1; i < len(rows); i++ {
		if rows[i][1] < rows[i-1][1] {
			return false
		}
	}

	return true
}

func TestTimestreamQuery_SDK_StatusAndInsights(t *testing.T) {
	t.Parallel()

	t0 := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	env := newTimestreamEnv(t, t0)
	seedSensors(t, env, t0)

	out, err := env.query.Query(t.Context(), &tqsdk.QueryInput{
		QueryString:   aws.String(`SELECT host FROM sensors.readings`),
		QueryInsights: &tqtypes.QueryInsights{Mode: tqtypes.QueryInsightsModeEnabledWithRateControl},
	})
	require.NoError(t, err)
	assert.EqualValues(t, 100, out.QueryStatus.ProgressPercentage)
	assert.Positive(t, out.QueryStatus.CumulativeBytesScanned)
	assert.Less(t, out.QueryStatus.CumulativeBytesScanned, int64(10*1024*1024))
	assert.EqualValues(t, 10*1024*1024, out.QueryStatus.CumulativeBytesMetered)
	assert.EqualValues(t, 9, aws.ToInt64(out.QueryInsightsResponse.OutputRows))

	none, err := env.query.Query(t.Context(), &tqsdk.QueryInput{QueryString: aws.String(`SELECT 1`)})
	require.NoError(t, err)
	assert.Zero(t, none.QueryStatus.CumulativeBytesMetered)
	assert.Equal(t, [][]string{{"1"}}, rowsOf(none))
}

func TestTimestreamQuery_SDK_RetentionFollowsClock(t *testing.T) {
	t.Parallel()

	t0 := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	env := newTimestreamEnv(t, t0)

	retention := &twtypes.RetentionProperties{
		MemoryStoreRetentionPeriodInHours: aws.Int64(24), MagneticStoreRetentionPeriodInDays: aws.Int64(2),
	}
	dbl := twtypes.MeasureValueTypeDouble
	env.seed(t, "ret", "t", retention,
		single("m", "1", dbl, t0.Add(-time.Hour), dim("d", "memory")),
		single("m", "2", dbl, t0.Add(-36*time.Hour), dim("d", "magnetic")),
		single("m", "3", dbl, t0.Add(-72*time.Hour), dim("d", "expired")),
	)

	count := func() string {
		out, err := env.query.Query(t.Context(), &tqsdk.QueryInput{QueryString: aws.String(`SELECT d FROM ret.t ORDER BY d`)})
		require.NoError(t, err)

		return fmtRows(rowsOf(out))
	}

	assert.Equal(t, "magnetic,memory", count())

	env.now.Add(int64(30 * time.Hour))
	assert.Equal(t, "memory", count())

	env.now.Add(int64(48 * time.Hour))

	out, err := env.query.Query(t.Context(), &tqsdk.QueryInput{QueryString: aws.String(`SELECT count(*) FROM ret.t`)})
	require.NoError(t, err)
	assert.Equal(t, [][]string{{"0"}}, rowsOf(out))
}

func fmtRows(rows [][]string) string {
	s := ""

	for i, r := range rows {
		if i > 0 {
			s += ","
		}

		s += r[0]
	}

	return s
}

func TestTimestreamQuery_SDK_ScheduledQueryWritesTarget(t *testing.T) {
	t.Parallel()

	t0 := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	env := newTimestreamEnv(t, t0)
	seedSensors(t, env, t0)
	env.seed(t, "sensors", "rollup", &twtypes.RetentionProperties{
		MemoryStoreRetentionPeriodInHours: aws.Int64(24), MagneticStoreRetentionPeriodInDays: aws.Int64(365),
	})

	created, err := env.query.CreateScheduledQuery(t.Context(), &tqsdk.CreateScheduledQueryInput{
		Name: aws.String("hourly-cpu"),
		QueryString: aws.String(`SELECT host, bin(time, 1h) AS t, avg(measure_value::double) AS avg_cpu
			FROM sensors.readings WHERE measure_name = 'cpu' AND time BETWEEN @scheduled_runtime - 2h AND @scheduled_runtime
			GROUP BY host, bin(time, 1h)`),
		ScheduledQueryExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/tsq-role"),
		ScheduleConfiguration:          &tqtypes.ScheduleConfiguration{ScheduleExpression: aws.String("rate(1 hour)")},
		NotificationConfiguration: &tqtypes.NotificationConfiguration{
			SnsConfiguration: &tqtypes.SnsConfiguration{TopicArn: aws.String("arn:aws:sns:us-east-1:000000000000:t")},
		},
		ErrorReportConfiguration: &tqtypes.ErrorReportConfiguration{
			S3Configuration: &tqtypes.S3Configuration{BucketName: aws.String("errs")},
		},
		TargetConfiguration: &tqtypes.TargetConfiguration{TimestreamConfiguration: &tqtypes.TimestreamConfiguration{
			DatabaseName: aws.String("sensors"), TableName: aws.String("rollup"), TimeColumn: aws.String("t"),
			DimensionMappings: []tqtypes.DimensionMapping{{Name: aws.String("host"), DimensionValueType: tqtypes.DimensionValueTypeVarchar}},
			MultiMeasureMappings: &tqtypes.MultiMeasureMappings{
				TargetMultiMeasureName: aws.String("cpu_stats"),
				MultiMeasureAttributeMappings: []tqtypes.MultiMeasureAttributeMapping{{
					SourceColumn: aws.String("avg_cpu"), MeasureValueType: tqtypes.ScalarMeasureValueTypeDouble,
				}},
			},
		}},
	})
	require.NoError(t, err)

	_, err = env.query.ExecuteScheduledQuery(t.Context(), &tqsdk.ExecuteScheduledQueryInput{
		ScheduledQueryArn: created.Arn, InvocationTime: aws.Time(t0),
	})
	require.NoError(t, err)

	out, err := env.query.Query(t.Context(), &tqsdk.QueryInput{
		QueryString: aws.String(`SELECT host, measure_name, avg_cpu, time FROM sensors.rollup ORDER BY host, time`),
	})
	require.NoError(t, err)
	assert.Equal(t, [][]string{
		{"a", "cpu_stats", "20.0", "2026-10-31 11:00:00.000000000"},
		{"b", "cpu_stats", "60.25", "2026-10-31 11:00:00.000000000"},
	}, rowsOf(out))

	desc, err := env.query.DescribeScheduledQuery(t.Context(), &tqsdk.DescribeScheduledQueryInput{ScheduledQueryArn: created.Arn})
	require.NoError(t, err)
	require.NotNil(t, desc.ScheduledQuery.LastRunSummary)
	assert.Equal(t, tqtypes.ScheduledQueryRunStatusManualTriggerSuccess, desc.ScheduledQuery.LastRunSummary.RunStatus)
	assert.EqualValues(t, 2, desc.ScheduledQuery.LastRunSummary.ExecutionStats.RecordsIngested)
	assert.EqualValues(t, 2, desc.ScheduledQuery.LastRunSummary.ExecutionStats.QueryResultRows)
}
