package app

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	tqsdk "github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	tqtypes "github.com/aws/aws-sdk-go-v2/service/timestreamquery/types"
	twsdk "github.com/aws/aws-sdk-go-v2/service/timestreamwrite"
	twtypes "github.com/aws/aws-sdk-go-v2/service/timestreamwrite/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
	timestreamquerybackend "github.com/blackbirdworks/gopherstack/services/timestreamquery"
	timestreamwritebackend "github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

const sensorsDB = "sensors"

type tsqStack struct {
	fx    *sfnFixture
	query *tqsdk.Client
	write *twsdk.Client
	s3    *s3.Client
	qb    *timestreamquerybackend.InMemoryBackend
	now   time.Time
}

func newTSQStack(t *testing.T, start time.Time) *tsqStack {
	t.Helper()

	fx := newSFNFixture(t)
	byName := serviceByName(fx.services)

	st := &tsqStack{
		fx:    fx,
		query: tqsdk.NewFromConfig(fx.cfg),
		write: twsdk.NewFromConfig(fx.cfg),
		s3:    s3.NewFromConfig(fx.cfg, func(o *s3.Options) { o.UsePathStyle = true }),
		now:   start,
	}

	qh, ok := byName["TimestreamQuery"].(*timestreamquerybackend.Handler)
	require.True(t, ok)

	st.qb, ok = qh.Backend.(*timestreamquerybackend.InMemoryBackend)
	require.True(t, ok)

	wh, ok := byName["TimestreamWrite"].(*timestreamwritebackend.Handler)
	require.True(t, ok)

	clock := func() time.Time { return st.now }
	st.qb.SetClock(clock)
	wh.Backend.SetClock(clock)

	return st
}

func (s *tsqStack) rows(t *testing.T, q string) [][]string {
	t.Helper()

	out, err := s.query.Query(t.Context(), &tqsdk.QueryInput{QueryString: aws.String(q)})
	require.NoError(t, err, q)

	return rowsOf(out)
}

func (s *tsqStack) seed(
	t *testing.T,
	table string,
	props *twtypes.RetentionProperties,
	magnetic bool,
	recs ...twtypes.Record,
) {
	t.Helper()

	_, err := s.write.CreateDatabase(t.Context(), &twsdk.CreateDatabaseInput{DatabaseName: aws.String(sensorsDB)})
	if err != nil {
		require.ErrorContains(t, err, "ConflictException")
	}

	_, err = s.write.CreateTable(t.Context(), &twsdk.CreateTableInput{
		DatabaseName:        aws.String(sensorsDB),
		TableName:           aws.String(table),
		RetentionProperties: props,
		MagneticStoreWriteProperties: &twtypes.MagneticStoreWriteProperties{
			EnableMagneticStoreWrites: aws.Bool(magnetic),
		},
	})
	require.NoError(t, err)

	if len(recs) > 0 {
		_, err = s.write.WriteRecords(t.Context(), &twsdk.WriteRecordsInput{
			DatabaseName: aws.String(sensorsDB), TableName: aws.String(table), Records: recs,
		})
		require.NoError(t, err)
	}
}

func (s *tsqStack) object(t *testing.T, uri string) []byte {
	t.Helper()

	bucket, key, _ := strings.Cut(strings.TrimPrefix(uri, "s3://"), "/")

	out, err := s.s3.GetObject(t.Context(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	require.NoError(t, err, uri)

	defer func() { _ = out.Body.Close() }()

	b, err := io.ReadAll(out.Body)
	require.NoError(t, err)

	return b
}

func (s *tsqStack) bucket(t *testing.T, name string) {
	t.Helper()

	_, err := s.s3.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: aws.String(name)})
	require.NoError(t, err)
}

func tsqRetention(memHours, magDays int64) *twtypes.RetentionProperties {
	return &twtypes.RetentionProperties{
		MemoryStoreRetentionPeriodInHours: aws.Int64(memHours), MagneticStoreRetentionPeriodInDays: aws.Int64(magDays),
	}
}

func rollupTarget(db, table string) *tqtypes.TargetConfiguration {
	return &tqtypes.TargetConfiguration{TimestreamConfiguration: &tqtypes.TimestreamConfiguration{
		DatabaseName: aws.String(db), TableName: aws.String(table), TimeColumn: aws.String("t"),
		DimensionMappings: []tqtypes.DimensionMapping{
			{Name: aws.String("host"), DimensionValueType: tqtypes.DimensionValueTypeVarchar},
		},
		MultiMeasureMappings: &tqtypes.MultiMeasureMappings{
			TargetMultiMeasureName: aws.String("cpu_stats"),
			MultiMeasureAttributeMappings: []tqtypes.MultiMeasureAttributeMapping{{
				SourceColumn: aws.String("avg_cpu"), MeasureValueType: tqtypes.ScalarMeasureValueTypeDouble,
			}},
		},
	}}
}

func (s *tsqStack) createSchedule(
	t *testing.T, name, topic, bucket string, target *tqtypes.TargetConfiguration,
) string {
	t.Helper()

	out, err := s.query.CreateScheduledQuery(t.Context(), &tqsdk.CreateScheduledQueryInput{
		Name: aws.String(name),
		QueryString: aws.String(
			`SELECT host, bin(time, 1h) AS t, avg(measure_value::double) AS avg_cpu FROM sensors.readings
			WHERE measure_name = 'cpu' AND time BETWEEN @scheduled_runtime - 2h AND @scheduled_runtime
			GROUP BY host, bin(time, 1h)`,
		),
		ScheduledQueryExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/tsq"),
		ScheduleConfiguration:          &tqtypes.ScheduleConfiguration{ScheduleExpression: aws.String("rate(1 hour)")},
		NotificationConfiguration: &tqtypes.NotificationConfiguration{
			SnsConfiguration: &tqtypes.SnsConfiguration{TopicArn: aws.String(topic)},
		},
		ErrorReportConfiguration: &tqtypes.ErrorReportConfiguration{S3Configuration: &tqtypes.S3Configuration{
			BucketName: aws.String(bucket), ObjectKeyPrefix: aws.String("reports"),
		}},
		TargetConfiguration: target,
	})
	require.NoError(t, err)

	return aws.ToString(out.Arn)
}

func TestTimestreamQuery_FullStack_ScheduleNotifyAndErrorReport(t *testing.T) {
	t.Parallel()

	t0 := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	st := newTSQStack(t, t0)
	ctx := t.Context()
	dbl := twtypes.MeasureValueTypeDouble

	q, err := sqs.NewFromConfig(st.fx.cfg).CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("tsq-events")})
	require.NoError(t, err)

	topic, err := sns.NewFromConfig(st.fx.cfg).CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("tsq")})
	require.NoError(t, err)

	_, err = sns.NewFromConfig(st.fx.cfg).Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: topic.TopicArn, Protocol: aws.String("sqs"),
		Endpoint: aws.String("arn:aws:sqs:us-east-1:000000000000:tsq-events"),
	})
	require.NoError(t, err)

	st.bucket(t, "tsq-errors")
	st.seed(t, "readings", tsqRetention(24, 365), true,
		single("cpu", "10", dbl, t0.Add(-30*time.Minute), dim("host", "a")),
		single("cpu", "30", dbl, t0.Add(-10*time.Minute), dim("host", "a")),
	)
	st.seed(t, "rollup", tsqRetention(24, 365), true)
	st.seed(t, "strict", tsqRetention(1, 1), false)

	good := st.createSchedule(t, "good", aws.ToString(topic.TopicArn), "tsq-errors", rollupTarget("sensors", "rollup"))
	bad := st.createSchedule(t, "bad", aws.ToString(topic.TopicArn), "tsq-errors", rollupTarget("sensors", "strict"))

	st.now = t0.Add(time.Hour)
	st.qb.RunDue(ctx)

	assert.Equal(t, [][]string{{"a", "cpu_stats", "20.0", "2026-10-31 11:00:00.000000000"}},
		st.rows(t, `SELECT host, measure_name, avg_cpu, time FROM sensors.rollup`))

	desc, err := st.query.DescribeScheduledQuery(
		ctx,
		&tqsdk.DescribeScheduledQueryInput{ScheduledQueryArn: aws.String(good)},
	)
	require.NoError(t, err)
	assert.Equal(t, tqtypes.ScheduledQueryRunStatusAutoTriggerSuccess, desc.ScheduledQuery.LastRunSummary.RunStatus)
	assert.Equal(t, t0.Add(2*time.Hour), desc.ScheduledQuery.NextInvocationTime.UTC())

	failed, err := st.query.DescribeScheduledQuery(
		ctx,
		&tqsdk.DescribeScheduledQueryInput{ScheduledQueryArn: aws.String(bad)},
	)
	require.NoError(t, err)
	require.NotNil(t, failed.ScheduledQuery.LastRunSummary)
	assert.Equal(t, tqtypes.ScheduledQueryRunStatusAutoTriggerFailure, failed.ScheduledQuery.LastRunSummary.RunStatus)
	require.Len(t, failed.ScheduledQuery.RecentlyFailedRuns, 1)

	loc := failed.ScheduledQuery.LastRunSummary.ErrorReportLocation.S3ReportLocation
	report := st.object(t, "s3://"+aws.ToString(loc.BucketName)+"/"+aws.ToString(loc.ObjectKey))

	var parsed struct {
		Errors []struct {
			Reason string `json:"reason"`
		} `json:"errors"`
	}

	require.NoError(t, json.Unmarshal(report, &parsed))
	require.Len(t, parsed.Errors, 1)
	assert.Contains(t, parsed.Errors[0].Reason, "retention")

	msgs, err := sqs.NewFromConfig(st.fx.cfg).ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl: q.QueueUrl, MaxNumberOfMessages: 10,
	})
	require.NoError(t, err)

	got := make([]string, 0, len(msgs.Messages))

	for _, m := range msgs.Messages {
		var env struct {
			Message string `json:"Message"`
		}

		require.NoError(t, json.Unmarshal([]byte(aws.ToString(m.Body)), &env))

		var inner struct {
			Type string `json:"type"`
		}

		require.NoError(t, json.Unmarshal([]byte(env.Message), &inner))
		got = append(got, inner.Type)
	}

	assert.ElementsMatch(
		t,
		[]string{
			"SCHEDULED_QUERY_CREATING",
			"SCHEDULED_QUERY_CREATING",
			"AUTO_TRIGGER_SUCCESS",
			"AUTO_TRIGGER_FAILURE",
		},
		got,
	)
}

func TestTimestreamQuery_FullStack_UnloadToS3(t *testing.T) {
	t.Parallel()

	t0 := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	st := newTSQStack(t, t0)
	dbl := twtypes.MeasureValueTypeDouble

	st.bucket(t, "unloads")
	st.seed(t, "readings", tsqRetention(24, 365), true,
		single("cpu", "10", dbl, t0.Add(-30*time.Minute), dim("host", "a")),
		single("cpu", "30", dbl, t0.Add(-20*time.Minute), dim("host", "a")),
		single("cpu", "50", dbl, t0.Add(-10*time.Minute), dim("host", "b")),
	)

	run := func(with string) []string {
		out, err := st.query.Query(t.Context(), &tqsdk.QueryInput{QueryString: aws.String(
			`UNLOAD (SELECT host, measure_value::double AS cpu FROM sensors.readings ORDER BY time) ` +
				`TO 's3://unloads/out' WITH (` + with + `)`)})
		require.NoError(t, err)
		require.Len(t, out.Rows, 1)
		assert.Equal(t, "rows", aws.ToString(out.ColumnInfo[0].Name))
		assert.Equal(t, tqtypes.ScalarTypeBigint, out.ColumnInfo[0].Type.ScalarType)

		return rowsOf(out)[0]
	}

	cells := run(`format = 'CSV', include_header = 'true'`)
	assert.Equal(t, "3", cells[0])

	manifest := st.object(t, cells[2])

	var man struct {
		ResultFiles []struct {
			URL string `json:"url"`
		} `json:"result_files"`
	}

	require.NoError(t, json.Unmarshal(manifest, &man))
	require.Len(t, man.ResultFiles, 1)

	zr, err := gzip.NewReader(bytes.NewReader(st.object(t, man.ResultFiles[0].URL)))
	require.NoError(t, err)

	csvBody, err := io.ReadAll(zr)
	require.NoError(t, err)
	assert.Equal(t, "host,cpu\na,10.0\na,30.0\nb,50.0\n", string(csvBody))

	cells = run(`format = 'PARQUET', partitioned_by = ARRAY['host']`)
	assert.Equal(t, "3", cells[0])

	require.NoError(t, json.Unmarshal(st.object(t, cells[2]), &man))
	require.Len(t, man.ResultFiles, 2)

	var total float64

	for _, f := range man.ResultFiles {
		assert.Contains(t, f.URL, "/results/host=")

		pf, perr := parquetio.Open(st.object(t, f.URL))
		require.NoError(t, perr)
		require.NoError(t, pf.ForEach([]string{"cpu"}, func(v []any) error {
			total += v[0].(float64)

			return nil
		}))
	}

	assert.InDelta(t, 90.0, total, 0)

	meta := st.object(t, cells[1])
	assert.Contains(t, string(meta), `"MetadataFileVersion":"1.0"`)

	_, err = st.query.Query(t.Context(), &tqsdk.QueryInput{QueryString: aws.String(
		`UNLOAD (SELECT 1) TO 's3://does-not-exist/x'`)})
	require.ErrorContains(t, err, "ValidationException")
}

func TestTimestreamQuery_FullStack_SchemaOutlivesRetention(t *testing.T) {
	t.Parallel()

	t0 := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	st := newTSQStack(t, t0)
	dbl := twtypes.MeasureValueTypeDouble

	st.seed(t, "short", tsqRetention(1, 1), true,
		single("cpu", "10", dbl, t0.Add(-5*time.Minute), dim("host", "a"), dim("region", "us")),
	)

	require.Equal(t, [][]string{{"1"}}, st.rows(t, `SELECT count(*) FROM sensors.short`))

	st.now = t0.Add(72 * time.Hour)

	assert.Equal(t, [][]string{{"0"}}, st.rows(t, `SELECT count(*) FROM sensors.short`))
	assert.Equal(t, [][]string{
		{"host", "varchar", "DIMENSION"}, {"region", "varchar", "DIMENSION"},
		{"measure_name", "varchar", "MEASURE_NAME"}, {"time", "timestamp", "TIMESTAMP"},
		{"measure_value::double", "double", "MEASURE_VALUE"},
	}, st.rows(t, `DESCRIBE sensors.short`))
	assert.Equal(t, [][]string{
		{
			"cpu",
			"double",
			`[{"data_type":"varchar","dimension_name":"host"},{"data_type":"varchar","dimension_name":"region"}]`,
		},
	}, st.rows(t, `SHOW MEASURES FROM sensors.short`))
	assert.Empty(t, st.rows(t, `SELECT host, region FROM sensors.short`))

	tbl, err := st.write.DescribeTable(t.Context(), &twsdk.DescribeTableInput{
		DatabaseName: aws.String("sensors"), TableName: aws.String("short"),
	})
	require.NoError(t, err)
	assert.Equal(t, "short", aws.ToString(tbl.Table.TableName))
}
