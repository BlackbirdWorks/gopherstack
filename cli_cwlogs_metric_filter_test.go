package main

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	logssdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	logstypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cwbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func TestInitializeServices_MetricFilterEmitsCloudWatchMetrics(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)

	cwH, ok := byName["CloudWatch"].(*cwbackend.Handler)
	require.True(t, ok)

	logsH, ok := byName["CloudWatchLogs"].(*cwlogsbackend.Handler)
	require.True(t, ok)

	logsBk, ok := logsH.Backend.(*cwlogsbackend.InMemoryBackend)
	require.True(t, ok)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(cwH))
	require.NoError(t, registry.Register(logsH))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion(crossHome),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	logs := logssdk.NewFromConfig(cfg, func(o *logssdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })
	cw := cwsdk.NewFromConfig(cfg, func(o *cwsdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })

	_, err = logs.CreateLogGroup(t.Context(), &logssdk.CreateLogGroupInput{LogGroupName: aws.String("/mf/app")})
	require.NoError(t, err)
	_, err = logs.CreateLogStream(t.Context(), &logssdk.CreateLogStreamInput{
		LogGroupName: aws.String("/mf/app"), LogStreamName: aws.String("s"),
	})
	require.NoError(t, err)

	put := func(name, pattern string, mt logstypes.MetricTransformation) {
		_, perr := logs.PutMetricFilter(t.Context(), &logssdk.PutMetricFilterInput{
			LogGroupName: aws.String("/mf/app"), FilterName: aws.String(name), FilterPattern: aws.String(pattern),
			MetricTransformations: []logstypes.MetricTransformation{mt},
		})
		require.NoError(t, perr)
	}

	put("errors", `{ $.level = "ERROR" }`, logstypes.MetricTransformation{
		MetricName: aws.String("Errors"), MetricNamespace: aws.String("Mf/App"), MetricValue: aws.String("1"),
		Unit: logstypes.StandardUnitCount,
	})
	put("bytes", `{ $.level = "ERROR" }`, logstypes.MetricTransformation{
		MetricName: aws.String("Bytes"), MetricNamespace: aws.String("Mf/App"), MetricValue: aws.String("$.bytes"),
		Dimensions: map[string]string{"svc": "$.svc"},
	})
	put("idle", `{ $.level = "FATAL" }`, logstypes.MetricTransformation{
		MetricName: aws.String("Idle"), MetricNamespace: aws.String("Mf/App"), MetricValue: aws.String("1"),
		DefaultValue: aws.Float64(0),
	})

	now := time.Now().UnixMilli()
	ev := func(msg string) logstypes.InputLogEvent {
		return logstypes.InputLogEvent{Message: aws.String(msg), Timestamp: aws.Int64(now)}
	}

	_, err = logs.PutLogEvents(t.Context(), &logssdk.PutLogEventsInput{
		LogGroupName: aws.String("/mf/app"), LogStreamName: aws.String("s"),
		LogEvents: []logstypes.InputLogEvent{
			ev(`{"level":"ERROR","bytes":100,"svc":"api"}`),
			ev(`{"level":"ERROR","bytes":50,"svc":"api"}`),
			ev(`{"level":"INFO","bytes":999,"svc":"api"}`),
		},
	})
	require.NoError(t, err)

	logsBk.EmitMetricFilterDefaults()

	sum := func(id, metric string, dims []cwtypes.Dimension) float64 {
		t.Helper()

		out, gerr := cw.GetMetricData(t.Context(), &cwsdk.GetMetricDataInput{
			StartTime: aws.Time(time.Now().Add(-time.Hour)), EndTime: aws.Time(time.Now().Add(time.Hour)),
			MetricDataQueries: []cwtypes.MetricDataQuery{{
				Id: aws.String(id),
				MetricStat: &cwtypes.MetricStat{
					Metric: &cwtypes.Metric{
						Namespace: aws.String("Mf/App"), MetricName: aws.String(metric), Dimensions: dims,
					},
					Period: aws.Int32(3600), Stat: aws.String("Sum"),
				},
			}},
		})
		require.NoError(t, gerr)

		total := 0.0
		for _, r := range out.MetricDataResults {
			for _, v := range r.Values {
				total += v
			}
		}

		return total
	}

	tests := []struct {
		name   string
		metric string
		dims   []cwtypes.Dimension
		want   float64
	}{
		{"literal_count", "Errors", nil, 2},
		{
			"extracted_value_dimension", "Bytes",
			[]cwtypes.Dimension{{Name: aws.String("svc"), Value: aws.String("api")}}, 150,
		},
		{"default_value", "Idle", nil, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.InDelta(t, tc.want, sum("q", tc.metric, tc.dims), 0.001)
		})
	}

	lm, err := cw.ListMetrics(t.Context(), &cwsdk.ListMetricsInput{Namespace: aws.String("Mf/App")})
	require.NoError(t, err)

	names := map[string]bool{}
	for _, m := range lm.Metrics {
		names[aws.ToString(m.MetricName)] = true
	}

	assert.Equal(t, map[string]bool{"Errors": true, "Bytes": true, "Idle": true}, names)
}
