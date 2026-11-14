package timestreamquery_test

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io"
	"net/http/httptest"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	tqsdk "github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	"github.com/aws/aws-sdk-go-v2/service/timestreamquery/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/timestreamquery"
)

var (
	errIngest   = errors.New("the record timestamp is outside the retention period")
	errNotFound = errors.New("NotFound")
)

type recordingNotifier struct {
	msgs   []map[string]any
	topics []string
	mu     sync.Mutex
}

func (n *recordingNotifier) Publish(_ context.Context, topic, message string) error {
	var m map[string]any
	if err := json.Unmarshal([]byte(message), &m, jsonx.V1()); err != nil {
		return err
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	n.msgs = append(n.msgs, m)
	n.topics = append(n.topics, topic)

	return nil
}

func (n *recordingNotifier) types() []string {
	n.mu.Lock()
	defer n.mu.Unlock()

	out := make([]string, 0, len(n.msgs))
	for _, m := range n.msgs {
		out = append(out, m["type"].(string))
	}

	return out
}

func (n *recordingNotifier) last() map[string]any {
	n.mu.Lock()
	defer n.mu.Unlock()

	return n.msgs[len(n.msgs)-1]
}

type memObjects struct {
	buckets map[string]bool
	objects map[string][]byte
	enc     map[string]s3types.ServerSideEncryption
	mu      sync.Mutex
}

func newMemObjects(buckets ...string) *memObjects {
	m := &memObjects{
		buckets: map[string]bool{},
		objects: map[string][]byte{},
		enc:     map[string]s3types.ServerSideEncryption{},
	}
	for _, b := range buckets {
		m.buckets[b] = true
	}

	return m
}

func (m *memObjects) PutObject(_ context.Context, in *sdk_s3.PutObjectInput) (*sdk_s3.PutObjectOutput, error) {
	body, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	k := aws.ToString(in.Bucket) + "/" + aws.ToString(in.Key)
	m.objects[k] = body
	m.enc[k] = in.ServerSideEncryption

	return &sdk_s3.PutObjectOutput{}, nil
}

func (m *memObjects) HeadBucket(_ context.Context, in *sdk_s3.HeadBucketInput) (*sdk_s3.HeadBucketOutput, error) {
	if !m.buckets[aws.ToString(in.Bucket)] {
		return nil, errNotFound
	}

	return &sdk_s3.HeadBucketOutput{}, nil
}

func (m *memObjects) keys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]string, 0, len(m.objects))
	for k := range m.objects {
		out = append(out, k)
	}

	return out
}

// failSource rejects AppendRecords the way Timestream Write rejects out-of-retention records.
type failSource struct {
	*memSource
	fail bool
}

func (f *failSource) AppendRecords(region, db, table string, recs []timestreamquery.SourceRecord) error {
	if f.fail {
		return &timestreamquery.RejectedError{
			Rejected: []timestreamquery.RejectedRecord{{Index: 0, Reason: errIngest.Error()}},
		}
	}

	return f.memSource.AppendRecords(region, db, table, recs)
}

type schedEnv struct {
	backend *timestreamquery.InMemoryBackend
	client  *tqsdk.Client
	src     *failSource
	notify  *recordingNotifier
	objects *memObjects
	now     *time.Time
}

func newSchedEnv(t *testing.T, start time.Time) *schedEnv {
	t.Helper()

	now := start
	src := &failSource{memSource: &memSource{tables: map[string][]timestreamquery.SourceRecord{
		"db.src": {
			{Time: start.Add(10 * time.Minute), MeasureName: "cpu", MeasureValueType: "DOUBLE", MeasureValue: "10",
				Dimensions: []timestreamquery.SourceDimension{{Name: "host", Value: "a"}}},
			{Time: start.Add(50 * time.Minute), MeasureName: "cpu", MeasureValueType: "DOUBLE", MeasureValue: "30",
				Dimensions: []timestreamquery.SourceDimension{{Name: "host", Value: "a"}}},
		},
		"db.rollup": {},
	}}}
	env := &schedEnv{src: src, notify: &recordingNotifier{}, objects: newMemObjects("errs"), now: &now}

	env.backend = timestreamquery.NewInMemoryBackend("000000000000", "us-east-1")
	env.backend.SetClock(func() time.Time { return now })
	env.backend.SetRecordSource(src)
	env.backend.SetNotifier(env.notify)
	env.backend.SetObjectStore(env.objects)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(timestreamquery.NewHandler(env.backend)))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	env.client = tqsdk.NewFromConfig(cfg, func(o *tqsdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	return env
}

func (e *schedEnv) advance(d time.Duration) { *e.now = e.now.Add(d) }

const rollupQuery = `SELECT host, bin(time, 1h) AS t, avg(measure_value::double) AS avg_cpu FROM db.src ` +
	`WHERE measure_name = 'cpu' AND time BETWEEN @scheduled_runtime - 1h AND @scheduled_runtime ` +
	`GROUP BY host, bin(time, 1h)`

func (e *schedEnv) create(t *testing.T, expr, query string) string {
	t.Helper()

	out, err := e.client.CreateScheduledQuery(t.Context(), &tqsdk.CreateScheduledQueryInput{
		Name:                           aws.String("sq"),
		QueryString:                    aws.String(query),
		ScheduledQueryExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/tsq"),
		ScheduleConfiguration:          &types.ScheduleConfiguration{ScheduleExpression: aws.String(expr)},
		NotificationConfiguration: &types.NotificationConfiguration{
			SnsConfiguration: &types.SnsConfiguration{TopicArn: aws.String("arn:aws:sns:us-east-1:000000000000:tsq")},
		},
		ErrorReportConfiguration: &types.ErrorReportConfiguration{S3Configuration: &types.S3Configuration{
			BucketName: aws.String("errs"), ObjectKeyPrefix: aws.String("reports"),
		}},
		TargetConfiguration: &types.TargetConfiguration{TimestreamConfiguration: &types.TimestreamConfiguration{
			DatabaseName: aws.String("db"), TableName: aws.String("rollup"), TimeColumn: aws.String("t"),
			DimensionMappings: []types.DimensionMapping{
				{Name: aws.String("host"), DimensionValueType: types.DimensionValueTypeVarchar},
			},
			MultiMeasureMappings: &types.MultiMeasureMappings{
				TargetMultiMeasureName: aws.String("stats"),
				MultiMeasureAttributeMappings: []types.MultiMeasureAttributeMapping{{
					SourceColumn: aws.String("avg_cpu"), MeasureValueType: types.ScalarMeasureValueTypeDouble,
				}},
			},
		}},
	})
	require.NoError(t, err)

	return aws.ToString(out.Arn)
}

func (e *schedEnv) describe(t *testing.T, arn string) *types.ScheduledQueryDescription {
	t.Helper()

	out, err := e.client.DescribeScheduledQuery(t.Context(), &tqsdk.DescribeScheduledQueryInput{
		ScheduledQueryArn: aws.String(arn),
	})
	require.NoError(t, err)

	return out.ScheduledQuery
}

func (n *recordingNotifier) runs(kind string) []int64 {
	n.mu.Lock()
	defer n.mu.Unlock()

	var out []int64

	for _, m := range n.msgs {
		if m["type"] != kind {
			continue
		}

		sum := m["runSummary"].(map[string]any)
		out = append(out, int64(sum["invocationTime"].(float64)))
	}

	return out
}

func TestScheduledQuery_RunsOnSchedule(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	at := func(d ...time.Duration) []int64 {
		out := make([]int64, 0, len(d))
		for _, x := range d {
			out = append(out, start.Add(x).Unix())
		}

		return out
	}

	tests := []struct {
		name     string
		expr     string
		wantRuns []int64
		advance  time.Duration
		wantNext time.Duration
	}{
		{
			name: "rate hours", expr: "rate(1 hour)", advance: 3*time.Hour + 30*time.Minute,
			wantRuns: at(time.Hour, 2*time.Hour, 3*time.Hour), wantNext: 4 * time.Hour,
		},
		{
			name: "rate minutes", expr: "rate(30 minutes)", advance: time.Hour,
			wantRuns: at(30*time.Minute, time.Hour), wantNext: 90 * time.Minute,
		},
		{
			name: "cron half hourly", expr: "cron(0/30 * * * ? *)", advance: 65 * time.Minute,
			wantRuns: at(30*time.Minute, time.Hour), wantNext: 90 * time.Minute,
		},
		{
			name: "cron daily at 18:00", expr: "cron(0 18 * * ? *)", advance: 7 * time.Hour,
			wantRuns: at(6 * time.Hour), wantNext: 30 * time.Hour,
		},
		{name: "not yet due", expr: "rate(1 day)", advance: 23 * time.Hour, wantNext: 24 * time.Hour},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newSchedEnv(t, start)
			arn := env.create(t, tt.expr, rollupQuery)
			assert.Equal(t, []string{"SCHEDULED_QUERY_CREATING"}, env.notify.types())

			env.advance(tt.advance)
			env.backend.RunDue(t.Context())

			d := env.describe(t, arn)
			require.NotNil(t, d.NextInvocationTime)
			assert.Equal(t, start.Add(tt.wantNext), d.NextInvocationTime.UTC())
			assert.Equal(t, tt.wantRuns, env.notify.runs("AUTO_TRIGGER_SUCCESS"))

			if len(tt.wantRuns) == 0 {
				assert.Nil(t, d.LastRunSummary)

				return
			}

			require.NotNil(t, d.LastRunSummary)
			assert.Equal(t, types.ScheduledQueryRunStatusAutoTriggerSuccess, d.LastRunSummary.RunStatus)
			assert.Equal(t, time.Unix(tt.wantRuns[len(tt.wantRuns)-1], 0).UTC(), d.LastRunSummary.InvocationTime.UTC())
		})
	}
}

func TestScheduledQuery_WritesTargetAndNotifies(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	env := newSchedEnv(t, start)
	arn := env.create(t, "rate(1 hour)", rollupQuery)

	env.advance(time.Hour)
	env.backend.RunDue(t.Context())

	rows := env.src.tables["db.rollup"]
	require.Len(t, rows, 1)
	assert.Equal(t, "stats", rows[0].MeasureName)
	assert.Equal(t, "MULTI", rows[0].MeasureValueType)
	assert.Equal(t, []timestreamquery.SourceMeasure{{Name: "avg_cpu", Value: "20.0", Type: "DOUBLE"}}, rows[0].Measures)

	msg := env.notify.last()
	assert.Equal(t, "AUTO_TRIGGER_SUCCESS", msg["type"])
	assert.Equal(t, arn, msg["arn"])
	assert.EqualValues(t, start.Add(2*time.Hour).Unix(), msg["nextInvocationEpochSecond"])

	sum := msg["runSummary"].(map[string]any)
	assert.Equal(t, "AUTO_TRIGGER_SUCCESS", sum["runStatus"])
	assert.EqualValues(t, 1, sum["executionStats"].(map[string]any)["recordsIngested"])
	assert.Equal(t, "arn:aws:sns:us-east-1:000000000000:tsq", env.notify.topics[len(env.notify.topics)-1])

	d := env.describe(t, arn)
	assert.EqualValues(t, 1, d.LastRunSummary.ExecutionStats.RecordsIngested)
	assert.Empty(t, d.RecentlyFailedRuns)
}

func TestScheduledQuery_StopsWhenDisabledOrDeleted(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	env := newSchedEnv(t, start)
	arn := env.create(t, "rate(1 hour)", rollupQuery)

	_, err := env.client.UpdateScheduledQuery(t.Context(), &tqsdk.UpdateScheduledQueryInput{
		ScheduledQueryArn: aws.String(arn), State: types.ScheduledQueryStateDisabled,
	})
	require.NoError(t, err)

	env.advance(5 * time.Hour)
	env.backend.RunDue(t.Context())
	assert.Empty(t, env.notify.runs("AUTO_TRIGGER_SUCCESS"))
	assert.Nil(t, env.describe(t, arn).NextInvocationTime)

	_, err = env.client.UpdateScheduledQuery(t.Context(), &tqsdk.UpdateScheduledQueryInput{
		ScheduledQueryArn: aws.String(arn), State: types.ScheduledQueryStateEnabled,
	})
	require.NoError(t, err)

	re := env.describe(t, arn)
	require.NotNil(t, re.NextInvocationTime)
	assert.Equal(t, env.now.Add(time.Hour), re.NextInvocationTime.UTC(), "re-enabling restarts the schedule from now")

	env.advance(time.Hour)
	env.backend.RunDue(t.Context())
	assert.Len(t, env.notify.runs("AUTO_TRIGGER_SUCCESS"), 1)

	_, err = env.client.DeleteScheduledQuery(
		t.Context(),
		&tqsdk.DeleteScheduledQueryInput{ScheduledQueryArn: aws.String(arn)},
	)
	require.NoError(t, err)

	env.advance(5 * time.Hour)
	env.backend.RunDue(t.Context())
	assert.Len(t, env.notify.runs("AUTO_TRIGGER_SUCCESS"), 1)
	assert.Equal(t, []string{
		"SCHEDULED_QUERY_CREATING", "SCHEDULED_QUERY_UPDATE", "SCHEDULED_QUERY_UPDATE",
		"AUTO_TRIGGER_SUCCESS", "SCHEDULED_QUERY_DELETED",
	}, env.notify.types())
}

func TestScheduledQuery_IngestionFailureWritesErrorReport(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	env := newSchedEnv(t, start)
	env.src.fail = true
	arn := env.create(t, "rate(1 hour)", rollupQuery)

	env.advance(time.Hour)
	env.backend.RunDue(t.Context())

	d := env.describe(t, arn)
	require.NotNil(t, d.LastRunSummary)
	assert.Equal(t, types.ScheduledQueryRunStatusAutoTriggerFailure, d.LastRunSummary.RunStatus)
	assert.Contains(t, aws.ToString(d.LastRunSummary.FailureReason), "retention period")
	assert.Equal(t, types.ScheduledQueryStateEnabled, d.State, "recoverable failures keep the query enabled")
	require.Len(t, d.RecentlyFailedRuns, 1)

	loc := d.LastRunSummary.ErrorReportLocation.S3ReportLocation
	assert.Equal(t, "errs", aws.ToString(loc.BucketName))
	assert.Regexp(t, `^reports/sq/2026-10-31T13:00:00Z/Auto/2026-10-31T13:00:00Z$`, aws.ToString(loc.ObjectKey))
	assert.Equal(t, loc, d.RecentlyFailedRuns[0].ErrorReportLocation.S3ReportLocation)

	body := env.objects.objects["errs/"+aws.ToString(loc.ObjectKey)]
	require.NotEmpty(t, body)
	assert.Equal(t, s3types.ServerSideEncryptionAes256, env.objects.enc["errs/"+aws.ToString(loc.ObjectKey)])

	var report struct {
		ReportID string `json:"reportId"`
		Errors   []struct {
			Reason  string `json:"reason"`
			Records []struct {
				MeasureName string `json:"measureName"`
				TimeUnit    string `json:"timeUnit"`
			} `json:"records"`
		} `json:"errors"`
	}

	require.NoError(t, json.UnmarshalDecode(jsontext.NewDecoder(bytes.NewReader(body)), &report, jsonx.V1()))
	assert.Len(t, report.ReportID, 32)
	require.Len(t, report.Errors, 1)
	assert.Equal(t, errIngest.Error(), report.Errors[0].Reason)
	require.Len(t, report.Errors[0].Records, 1)
	assert.Equal(t, "stats", report.Errors[0].Records[0].MeasureName)
	assert.Equal(t, "NANOSECONDS", report.Errors[0].Records[0].TimeUnit)

	msg := env.notify.last()
	assert.Equal(t, "AUTO_TRIGGER_FAILURE", msg["type"])
	s3loc := msg["runSummary"].(map[string]any)["errorReportLocation"].(map[string]any)["s3ReportLocation"]
	assert.Equal(t, aws.ToString(loc.ObjectKey), s3loc.(map[string]any)["objectKey"])
}

func TestScheduledQuery_QueryFailureDisablesQuery(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	env := newSchedEnv(t, start)
	arn := env.create(t, "rate(1 hour)", `SELECT host, time AS t, nonexistent AS avg_cpu FROM db.src`)

	env.advance(time.Hour)
	env.backend.RunDue(t.Context())

	d := env.describe(t, arn)
	assert.Equal(t, types.ScheduledQueryStateDisabled, d.State)
	assert.Equal(t, types.ScheduledQueryRunStatusAutoTriggerFailure, d.LastRunSummary.RunStatus)
	assert.Nil(t, d.LastRunSummary.ErrorReportLocation, "non-recoverable errors produce no report")
	assert.Empty(t, env.objects.keys())
	assert.Equal(
		t,
		[]string{"SCHEDULED_QUERY_CREATING", "AUTO_TRIGGER_FAILURE", "SCHEDULED_QUERY_UPDATE"},
		env.notify.types(),
	)
	assert.Equal(t, "DISABLED", env.notify.last()["state"])

	env.advance(5 * time.Hour)
	env.backend.RunDue(t.Context())
	assert.Len(t, env.notify.runs("AUTO_TRIGGER_FAILURE"), 1, "disabled queries do not run")
}

func TestScheduledQuery_ManualRunKeepsSemantics(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	env := newSchedEnv(t, start)
	arn := env.create(t, "rate(1 day)", rollupQuery)

	_, err := env.client.ExecuteScheduledQuery(t.Context(), &tqsdk.ExecuteScheduledQueryInput{
		ScheduledQueryArn: aws.String(arn), InvocationTime: aws.Time(start),
	})
	require.NoError(t, err)

	d := env.describe(t, arn)
	assert.Equal(t, types.ScheduledQueryRunStatusManualTriggerSuccess, d.LastRunSummary.RunStatus)
	assert.Equal(t, start.Add(24*time.Hour), d.NextInvocationTime.UTC(), "a manual run does not move the schedule")
	assert.Equal(t, "MANUAL_TRIGGER_SUCCESS", env.notify.last()["type"])
}

func TestScheduledQuery_SnapshotRestoreResumesSchedule(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	env := newSchedEnv(t, start)
	env.create(t, "rate(1 hour)", rollupQuery)
	snap := env.backend.Snapshot(t.Context())

	restored := newSchedEnv(t, start)
	require.NoError(t, restored.backend.Restore(t.Context(), snap))

	restored.advance(2 * time.Hour)
	restored.backend.RunDue(t.Context())
	assert.Equal(
		t,
		[]int64{start.Add(time.Hour).Unix(), start.Add(2 * time.Hour).Unix()},
		restored.notify.runs("AUTO_TRIGGER_SUCCESS"),
	)
}

func TestScheduledQuery_WorkerRunsOnFakeClock(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := timestreamquery.NewInMemoryBackend("000000000000", "us-east-1")
		src := &memSource{tables: map[string][]timestreamquery.SourceRecord{"db.src": {}}}
		b.SetRecordSource(src)

		n := &recordingNotifier{}
		b.SetNotifier(n)

		_, err := b.CreateScheduledQuery(
			t.Context(),
			"sq",
			"SELECT 1",
			"rate(1 minute)",
			"arn:aws:iam::000000000000:role/x",
			"arn:aws:sns:us-east-1:000000000000:tsq",
			"errs",
			"",
			"",
			"",
			"",
			"",
			"",
			nil,
			nil,
		)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(t.Context())
		h := timestreamquery.NewHandler(b)
		require.NoError(t, h.StartWorker(ctx))

		require.Eventually(t, func() bool { return len(n.runs("AUTO_TRIGGER_SUCCESS")) >= 3 }, time.Hour, time.Second)

		cancel()
		synctest.Wait()
	})
}
