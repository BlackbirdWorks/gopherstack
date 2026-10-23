package app

import (
	"testing"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcploggingbackend "github.com/blackbirdworks/gopherstack/services/gcplogging"
	gcpmonitoringbackend "github.com/blackbirdworks/gopherstack/services/gcpmonitoring"
	gcppubsubbackend "github.com/blackbirdworks/gopherstack/services/gcppubsub"
)

type monitoringFixture struct {
	monitoring *gcpmonitoringbackend.Handler
	logging    *gcploggingbackend.Handler
	pubsub     *gcppubsubbackend.Handler
	now        time.Time
}

func newMonitoringFixture(t *testing.T) *monitoringFixture {
	t.Helper()

	f := &monitoringFixture{
		monitoring: gcpmonitoringbackend.NewHandler(gcpmonitoringbackend.NewInMemoryBackend()),
		logging:    gcploggingbackend.NewHandler(gcploggingbackend.NewInMemoryBackend()),
		pubsub:     gcppubsubbackend.NewHandler(gcppubsubbackend.NewInMemoryBackend()),
		now:        time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC),
	}
	f.monitoring.Backend.SetClock(func() time.Time { return f.now })

	wireGCPMonitoring(map[string]service.Registerable{
		"GCPMonitoring": f.monitoring, "GCPLogging": f.logging, "GCPPubSub": f.pubsub,
	})

	return f
}

func TestMonitoringReadsLogBasedMetrics(t *testing.T) {
	t.Parallel()

	f := newMonitoringFixture(t)

	_, err := f.logging.Backend.CreateLogMetric(&loggingpb.CreateLogMetricRequest{
		Parent: "projects/p",
		Metric: &loggingpb.LogMetric{Name: "errors", Filter: "severity>=ERROR"},
	})
	require.NoError(t, err)

	err = f.logging.Backend.WriteLogEntries(t.Context(), &loggingpb.WriteLogEntriesRequest{
		LogName:  "projects/p/logs/app",
		Resource: &monitoredres.MonitoredResource{Type: "global"},
		Entries: []*loggingpb.LogEntry{
			{Severity: 500, Payload: &loggingpb.LogEntry_TextPayload{TextPayload: "boom"}},
			{Severity: 500, Payload: &loggingpb.LogEntry_TextPayload{TextPayload: "bang"}},
			{Severity: 200, Payload: &loggingpb.LogEntry_TextPayload{TextPayload: "fine"}},
		},
	})
	require.NoError(t, err)

	resp, err := f.monitoring.Backend.ListTimeSeries(&monitoringpb.ListTimeSeriesRequest{
		Name:   "projects/p",
		Filter: `metric.type = "logging.googleapis.com/user/errors"`,
		Interval: &monitoringpb.TimeInterval{
			StartTime: timestamppb.New(f.now.Add(-time.Hour)), EndTime: timestamppb.New(f.now),
		},
	})
	require.NoError(t, err)
	require.Len(t, resp.GetTimeSeries(), 1)
	assert.EqualValues(t, 2, resp.GetTimeSeries()[0].GetPoints()[0].GetValue().GetInt64Value())

	d, err := f.monitoring.Backend.GetMetricDescriptor(
		"projects/p/metricDescriptors/logging.googleapis.com/user/errors",
	)
	require.NoError(t, err)
	assert.Equal(t, metricpb.MetricDescriptor_DELTA, d.GetMetricKind())
}

func TestMonitoringAlertPublishesToPubSub(t *testing.T) {
	t.Parallel()

	f := newMonitoringFixture(t)
	topic := "projects/p/topics/alerts"
	sub := "projects/p/subscriptions/alerts-sub"

	_, err := f.pubsub.Backend.CreateTopic(&pubsubpb.Topic{Name: topic})
	require.NoError(t, err)
	_, err = f.pubsub.Backend.CreateSubscription(&pubsubpb.Subscription{Name: sub, Topic: topic})
	require.NoError(t, err)

	b := f.monitoring.Backend
	ch, err := b.CreateNotificationChannel(&monitoringpb.CreateNotificationChannelRequest{
		Name: "projects/p",
		NotificationChannel: &monitoringpb.NotificationChannel{
			Type: "pubsub", Labels: map[string]string{"topic": topic},
		},
	})
	require.NoError(t, err)

	const metric = "custom.googleapis.com/app/depth"

	_, err = b.CreateAlertPolicy(&monitoringpb.CreateAlertPolicyRequest{
		Name: "projects/p",
		AlertPolicy: &monitoringpb.AlertPolicy{
			DisplayName: "deep", Combiner: monitoringpb.AlertPolicy_OR, NotificationChannels: []string{ch.GetName()},
			Conditions: []*monitoringpb.AlertPolicy_Condition{{
				DisplayName: "depth",
				Condition: &monitoringpb.AlertPolicy_Condition_ConditionThreshold{
					ConditionThreshold: &monitoringpb.AlertPolicy_Condition_MetricThreshold{
						Filter:     `metric.type = "` + metric + `"`,
						Comparison: monitoringpb.ComparisonType_COMPARISON_GT, ThresholdValue: 3,
						Duration: durationpb.New(0),
						Aggregations: []*monitoringpb.Aggregation{{
							AlignmentPeriod:  durationpb.New(time.Minute),
							PerSeriesAligner: monitoringpb.Aggregation_ALIGN_MEAN,
						}},
					},
				},
			}},
		},
	})
	require.NoError(t, err)

	err = b.CreateTimeSeries(&monitoringpb.CreateTimeSeriesRequest{
		Name: "projects/p",
		TimeSeries: []*monitoringpb.TimeSeries{{
			Metric:   &metricpb.Metric{Type: metric},
			Resource: &monitoredres.MonitoredResource{Type: "global"},
			Points: []*monitoringpb.Point{{
				Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(f.now)},
				Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: 8}},
			}},
		}},
	})
	require.NoError(t, err)

	b.Tick(t.Context())

	resp, err := f.pubsub.SubscriberAPI().Pull(t.Context(), &pubsubpb.PullRequest{
		Subscription:      sub,
		MaxMessages:       5,
		ReturnImmediately: true, //nolint:staticcheck // immediate pull
	})
	require.NoError(t, err)
	require.Len(t, resp.GetReceivedMessages(), 1)
	assert.Contains(t, string(resp.GetReceivedMessages()[0].GetMessage().GetData()), `"state":"open"`)
}
