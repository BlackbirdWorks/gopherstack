package timestreamquery_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	tqsdk "github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	"github.com/aws/aws-sdk-go-v2/service/timestreamquery/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tableARN(table string) string {
	return "arn:aws:timestream:" + rtTestRegion + ":" + rtTestAccountID + ":database/db/table/" + table
}

func TestSDKQuery_InsightsTemporalAndSpatial(t *testing.T) {
	t.Parallel()

	const hour = int64(time.Hour)

	tests := []struct {
		name         string
		query        string
		wantTable    string
		wantKey      []string
		wantRange    int64
		wantCoverage float64
	}{
		{
			"ago bound", `SELECT * FROM db.t WHERE time > ago(1h)`,
			"t", []string{"measure_name"}, hour, 1,
		},
		{
			"between", `SELECT * FROM db.t WHERE time BETWEEN ago(2h) AND ago(1h)`,
			"t", []string{"measure_name"}, hour, 1,
		},
		{
			"unbounded uses data extent", `SELECT * FROM db.t`,
			"t", []string{"measure_name"}, 3 * hour, 1,
		},
		{
			"timestamp literal to now",
			`SELECT * FROM db.t WHERE time >= TIMESTAMP '2026-10-31 10:00:00' AND time < now()`,
			"t", []string{"measure_name"}, 2 * hour, 1,
		},
		{
			"bin and interval arithmetic",
			`SELECT * FROM db.t WHERE time >= bin(now(), 1h) - 1h AND time < bin(now(), 1h)`,
			"t", []string{"measure_name"}, hour, 1,
		},
		{
			"reversed comparison", `SELECT * FROM db.t WHERE ago(30m) <= time`,
			"t", []string{"measure_name"}, hour / 2, 1,
		},
		{
			"measure name equality", `SELECT * FROM db.t WHERE time > ago(1h) AND measure_name = 'cpu'`,
			"t", []string{"measure_name"}, hour, 0.5,
		},
		{
			"measure name in all", `SELECT * FROM db.t WHERE time > ago(1h) AND measure_name IN ('cpu', 'n')`,
			"t", []string{"measure_name"}, hour, 1,
		},
		{
			"unknown partition value", `SELECT * FROM db.t WHERE time > ago(1h) AND measure_name = 'nope'`,
			"t", []string{"measure_name"}, hour, 0,
		},
		{
			"customer partition key", `SELECT * FROM db.m WHERE time > ago(1h) AND host = 'h1'`,
			"m", []string{"host"}, hour, 0.25,
		},
		{
			"customer partition key in", `SELECT * FROM db.m WHERE time > ago(1h) AND host IN ('h1', 'h2')`,
			"m", []string{"host"}, hour, 0.5,
		},
		{
			"customer partition key or", `SELECT * FROM db.m WHERE time > ago(1h) AND (host = 'h1' OR host = 'h3')`,
			"m", []string{"host"}, hour, 0.5,
		},
		{
			"function around key does not prune", `SELECT * FROM db.m WHERE time > ago(1h) AND upper(host) = 'H1'`,
			"m", []string{"host"}, hour, 1,
		},
		{
			"join reports widest table",
			`SELECT a.host FROM db.t a JOIN db.m b ON a.host = b.host WHERE b.time > ago(1h) AND b.host = 'h1'`,
			"t", []string{"measure_name"}, 3 * hour, 1,
		},
		{
			"join with bounded sides",
			`SELECT a.host FROM db.t a JOIN db.m b ON a.host = b.host WHERE a.time > ago(2h) AND b.time > ago(1h)`,
			"t", []string{"measure_name"}, 2 * hour, 1,
		},
		{
			"subquery bounds its own table",
			`SELECT * FROM (SELECT * FROM db.m WHERE time > ago(1h) AND host = 'h4') x`,
			"m", []string{"host"}, hour, 0.25,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newSDKQueryEnv(t)

			out, err := client.Query(t.Context(), &tqsdk.QueryInput{
				QueryString:   aws.String(tc.query),
				QueryInsights: &types.QueryInsights{Mode: types.QueryInsightsModeEnabledWithRateControl},
			})
			require.NoError(t, err)
			require.NotNil(t, out.QueryInsightsResponse)

			temporal := out.QueryInsightsResponse.QueryTemporalRange
			require.NotNil(t, temporal)
			require.NotNil(t, temporal.Max)
			assert.Equal(t, tc.wantRange, temporal.Max.Value)
			assert.Equal(t, tableARN(tc.wantTable), aws.ToString(temporal.Max.TableArn))

			spatial := out.QueryInsightsResponse.QuerySpatialCoverage
			require.NotNil(t, spatial)
			require.NotNil(t, spatial.Max)
			assert.InDelta(t, tc.wantCoverage, spatial.Max.Value, 1e-9)
			assert.Equal(t, tc.wantKey, spatial.Max.PartitionKey)
		})
	}
}

func TestSDKQuery_InsightsModes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		insights    *types.QueryInsights
		name        string
		wantErr     string
		wantPresent bool
	}{
		{&types.QueryInsights{Mode: types.QueryInsightsModeEnabledWithRateControl}, "enabled", "", true},
		{&types.QueryInsights{Mode: types.QueryInsightsModeDisabled}, "disabled", "", false},
		{nil, "absent", "", false},
		{&types.QueryInsights{Mode: "BOGUS"}, "unknown", "ValidationException", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newSDKQueryEnv(t)

			out, err := client.Query(t.Context(), &tqsdk.QueryInput{
				QueryString: aws.String("SELECT * FROM db.t"), QueryInsights: tc.insights,
			})
			if tc.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.wantPresent, out.QueryInsightsResponse != nil)
		})
	}
}

func TestSDKQuery_InsightsRateLimit(t *testing.T) {
	t.Parallel()

	client, clock := newSDKQueryEnv(t)
	enabled := &types.QueryInsights{Mode: types.QueryInsightsModeEnabledWithRateControl}
	run := func(in *types.QueryInsights) error {
		_, err := client.Query(t.Context(), &tqsdk.QueryInput{QueryString: aws.String("SELECT 1"), QueryInsights: in})

		return err
	}

	require.NoError(t, run(enabled))

	err := run(enabled)
	require.Error(t, err)

	var throttled *types.ThrottlingException

	require.ErrorAs(t, err, &throttled, "second insights query within a second must be throttled")
	require.NoError(t, run(nil))
	require.NoError(t, run(&types.QueryInsights{Mode: types.QueryInsightsModeDisabled}))

	clock.advance(time.Second)
	require.NoError(t, run(enabled))
}

func TestSDKQuery_InsightsOutputBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  int64
	}{
		{"scalar", `SELECT 'abc', 12`, 5},
		{"array", `SELECT ARRAY[1, 22, 333]`, 6},
		{"row", `SELECT ROW(1, 'ab')`, 3},
		{"array of rows", `SELECT ARRAY[ROW(1, 'a'), ROW(2, 'b')]`, 4},
		{
			"series",
			`SELECT create_time_series(time, measure_value::double) FROM db.m`,
			int64(4 * (len("2026-10-31 11:50:00.000000000") + 3)),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newSDKQueryEnv(t)

			out, err := client.Query(t.Context(), &tqsdk.QueryInput{
				QueryString:   aws.String(tc.query),
				QueryInsights: &types.QueryInsights{Mode: types.QueryInsightsModeEnabledWithRateControl},
			})
			require.NoError(t, err)
			assert.Equal(t, tc.want, aws.ToInt64(out.QueryInsightsResponse.OutputBytes))
		})
	}
}

func TestSDKScheduledQuery_InsightsRun(t *testing.T) {
	t.Parallel()

	client, _ := newSDKQueryEnv(t)

	created, err := client.CreateScheduledQuery(t.Context(), &tqsdk.CreateScheduledQueryInput{
		Name:                           aws.String("insights-run"),
		QueryString:                    aws.String(`SELECT count(*) FROM db.m WHERE time > ago(1h) AND host = 'h1'`),
		ScheduledQueryExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/tsq-role"),
		ScheduleConfiguration:          &types.ScheduleConfiguration{ScheduleExpression: aws.String("rate(1 hour)")},
		NotificationConfiguration: &types.NotificationConfiguration{
			SnsConfiguration: &types.SnsConfiguration{TopicArn: aws.String("arn:aws:sns:us-east-1:000000000000:tsq")},
		},
		ErrorReportConfiguration: &types.ErrorReportConfiguration{
			S3Configuration: &types.S3Configuration{BucketName: aws.String("tsq-errors")},
		},
	})
	require.NoError(t, err)

	_, err = client.ExecuteScheduledQuery(t.Context(), &tqsdk.ExecuteScheduledQueryInput{
		ScheduledQueryArn: created.Arn,
		InvocationTime:    aws.Time(sdkNow()),
		QueryInsights:     &types.ScheduledQueryInsights{Mode: types.ScheduledQueryInsightsModeEnabledWithRateControl},
	})
	require.NoError(t, err)

	desc, err := client.DescribeScheduledQuery(t.Context(), &tqsdk.DescribeScheduledQueryInput{
		ScheduledQueryArn: created.Arn,
	})
	require.NoError(t, err)

	ins := desc.ScheduledQuery.LastRunSummary.QueryInsightsResponse
	require.NotNil(t, ins)
	require.NotNil(t, ins.QueryTemporalRange)
	assert.Equal(t, int64(time.Hour), ins.QueryTemporalRange.Max.Value)
	assert.Equal(t, tableARN("m"), aws.ToString(ins.QueryTemporalRange.Max.TableArn))
	require.NotNil(t, ins.QuerySpatialCoverage)
	assert.InDelta(t, 0.25, ins.QuerySpatialCoverage.Max.Value, 1e-9)
	assert.Equal(t, []string{"host"}, ins.QuerySpatialCoverage.Max.PartitionKey)
	assert.EqualValues(t, 1, aws.ToInt64(ins.QueryTableCount))
}

func TestSDKPrepareQuery_StaticColumns(t *testing.T) {
	t.Parallel()

	const series = `create_time_series(time, measure_value::double)`

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			"scalars and series",
			`SELECT host, count(*) AS c, ` + series + ` FROM db.t GROUP BY host`,
			"host:VARCHAR,c:BIGINT,_col2:TIMESERIES(DOUBLE)",
		},
		{
			"nested",
			`SELECT ARRAY[1], ROW(1, 'a') AS r, typeof(time) AS ty, now() FROM db.t`,
			"_col0:ARRAY(INTEGER),r:ROW(:INTEGER,:VARCHAR),ty:VARCHAR,_col3:TIMESTAMP",
		},
		{
			"star with comment", "SELECT * FROM db.m -- all columns",
			"host:VARCHAR,measure_name:VARCHAR,time:TIMESTAMP,measure_value::double:DOUBLE",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newSDKQueryClient(t)

			out, err := client.PrepareQuery(t.Context(), &tqsdk.PrepareQueryInput{QueryString: aws.String(tc.query)})
			require.NoError(t, err)

			cols := make([]string, len(out.Columns))
			for i, c := range out.Columns {
				cols[i] = aws.ToString(c.Name) + ":" + typeLabel(c.Type)
			}

			assert.Equal(t, tc.want, strings.Join(cols, ","))
		})
	}
}
