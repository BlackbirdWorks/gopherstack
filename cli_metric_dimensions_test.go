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
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cwbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
)

func newMetricDimensionsClient(t *testing.T, h *cwbackend.Handler) *cwsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion(crossHome),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	return cwsdk.NewFromConfig(cfg, func(o *cwsdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })
}

func TestInitializeServices_InProcessMetricsCarryDocumentedDimensions(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)

	cwH, ok := byName["CloudWatch"].(*cwbackend.Handler)
	require.True(t, ok)

	sqsH, ok := byName["SQS"].(*sqsbackend.Handler)
	require.True(t, ok)

	sqsBk, ok := sqsH.Backend.(*sqsbackend.InMemoryBackend)
	require.True(t, ok)

	logsH, ok := byName["CloudWatchLogs"].(*cwlogsbackend.Handler)
	require.True(t, ok)

	logsBk, ok := logsH.Backend.(*cwlogsbackend.InMemoryBackend)
	require.True(t, ok)

	client := newMetricDimensionsClient(t, cwH)

	send := func(t *testing.T, queue string, bodies ...string) string {
		t.Helper()

		q, err := sqsBk.CreateQueue(&sqsbackend.CreateQueueInput{QueueName: queue, Region: crossHome})
		require.NoError(t, err)

		for _, body := range bodies {
			_, err = sqsBk.SendMessage(&sqsbackend.SendMessageInput{
				QueueURL: q.QueueURL, Region: crossHome, MessageBody: body,
			})
			require.NoError(t, err)
		}

		return q.QueueURL
	}

	tests := []struct {
		emit       func(t *testing.T)
		name       string
		namespace  string
		metric     string
		dimName    string
		dimValue   string
		otherValue string
		wantUnit   cwtypes.StandardUnit
		wantSum    float64
	}{
		{
			name: "sqs-sent-per-queue", namespace: "AWS/SQS", metric: "NumberOfMessagesSent",
			dimName: "QueueName", dimValue: "dim-q-a", otherValue: "dim-q-b",
			wantUnit: cwtypes.StandardUnitCount, wantSum: 2,
			emit: func(t *testing.T) {
				t.Helper()
				send(t, "dim-q-a", "one", "two")
				send(t, "dim-q-b", "three")
			},
		},
		{
			name: "sqs-sent-size-bytes", namespace: "AWS/SQS", metric: "SentMessageSize",
			dimName: "QueueName", dimValue: "dim-size-a", otherValue: "dim-size-b",
			wantUnit: cwtypes.StandardUnitBytes, wantSum: 8,
			emit: func(t *testing.T) {
				t.Helper()
				send(t, "dim-size-a", "abcd", "efgh")
				send(t, "dim-size-b", "x")
			},
		},
		{
			name: "sqs-empty-receives", namespace: "AWS/SQS", metric: "NumberOfEmptyReceives",
			dimName: "QueueName", dimValue: "dim-empty-a", otherValue: "dim-empty-b",
			wantUnit: cwtypes.StandardUnitCount, wantSum: 1,
			emit: func(t *testing.T) {
				t.Helper()
				url := send(t, "dim-empty-a")
				send(t, "dim-empty-b")

				_, err := sqsBk.ReceiveMessage(&sqsbackend.ReceiveMessageInput{
					QueueURL: url, Region: crossHome,
				})
				require.NoError(t, err)
			},
		},
		{
			name: "logs-metric-filter-dimension", namespace: "Custom/Dim", metric: "Hits",
			dimName: "level", dimValue: "ERROR", otherValue: "WARN",
			wantUnit: cwtypes.StandardUnitCount, wantSum: 2,
			emit: func(t *testing.T) {
				t.Helper()

				ctx := cwlogsbackend.WithRegion(t.Context(), crossHome)
				_, err := logsBk.CreateLogGroup(ctx, "/dim/app", "", "")
				require.NoError(t, err)
				_, err = logsBk.CreateLogStream(ctx, "/dim/app", "s")
				require.NoError(t, err)

				mt := []cwlogsbackend.MetricTransformation{{
					MetricNamespace: "Custom/Dim", MetricName: "Hits", MetricValue: "1", Unit: "Count",
					Dimensions: map[string]string{"level": "$.level"},
				}}
				require.NoError(t, logsBk.PutMetricFilter(ctx, "/dim/app", "f", `{ $.level = "*" }`, mt))

				now := time.Now().UnixMilli()
				_, err = logsBk.PutLogEvents(ctx, "/dim/app", "s", "", []cwlogsbackend.InputLogEvent{
					{Message: `{"level":"ERROR"}`, Timestamp: now},
					{Message: `{"level":"ERROR"}`, Timestamp: now},
					{Message: `{"level":"WARN"}`, Timestamp: now},
				})
				require.NoError(t, err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.emit(t)

			dim := func(v string) []cwtypes.Dimension {
				return []cwtypes.Dimension{{Name: aws.String(tc.dimName), Value: aws.String(v)}}
			}

			require.Eventually(t, func() bool {
				out, err := client.ListMetrics(t.Context(), &cwsdk.ListMetricsInput{
					Namespace: aws.String(tc.namespace), MetricName: aws.String(tc.metric),
					Dimensions: []cwtypes.DimensionFilter{
						{Name: aws.String(tc.dimName), Value: aws.String(tc.dimValue)},
					},
				})

				return err == nil && len(out.Metrics) == 1
			}, 5*time.Second, 10*time.Millisecond)

			now := time.Now()

			var units []cwtypes.StandardUnit

			require.Eventually(t, func() bool {
				stats, err := client.GetMetricStatistics(t.Context(), &cwsdk.GetMetricStatisticsInput{
					Namespace: aws.String(tc.namespace), MetricName: aws.String(tc.metric),
					Dimensions: dim(tc.dimValue),
					StartTime:  aws.Time(now.Add(-time.Hour)), EndTime: aws.Time(now.Add(time.Hour)),
					Period: aws.Int32(3600), Statistics: []cwtypes.Statistic{cwtypes.StatisticSum},
				})
				if err != nil {
					return false
				}

				total := 0.0
				units = units[:0]

				for _, dp := range stats.Datapoints {
					total += aws.ToFloat64(dp.Sum)
					units = append(units, dp.Unit)
				}

				return total == tc.wantSum
			}, 5*time.Second, 10*time.Millisecond)

			for _, u := range units {
				assert.Equal(t, tc.wantUnit, u)
			}

			data, err := client.GetMetricData(t.Context(), &cwsdk.GetMetricDataInput{
				StartTime: aws.Time(now.Add(-time.Hour)), EndTime: aws.Time(now.Add(time.Hour)),
				MetricDataQueries: []cwtypes.MetricDataQuery{{
					Id: aws.String("m1"),
					MetricStat: &cwtypes.MetricStat{
						Metric: &cwtypes.Metric{
							Namespace: aws.String(tc.namespace), MetricName: aws.String(tc.metric),
							Dimensions: dim(tc.otherValue),
						},
						Period: aws.Int32(3600), Stat: aws.String("Sum"),
					},
				}},
			})
			require.NoError(t, err)
			require.Len(t, data.MetricDataResults, 1)

			other := 0.0
			for _, v := range data.MetricDataResults[0].Values {
				other += v
			}

			assert.NotEqual(t, tc.wantSum, other, "other resource must not share the first resource's series")
		})
	}
}
