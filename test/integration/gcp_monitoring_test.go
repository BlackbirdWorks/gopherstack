package integration_test

import (
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/logging"
	"cloud.google.com/go/logging/logadmin"
	monitoring "cloud.google.com/go/monitoring/apiv3/v2"
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const monitoringPort = "8132"

func monitoringProject() string { return "projects/" + gcpProject }

func newMetricClient(t *testing.T) *monitoring.MetricClient {
	t.Helper()

	c, err := monitoring.NewMetricClient(t.Context(), gcpGRPCOpts(t, monitoringPort)...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	return c
}

func gaugeSeries(metric string, labels map[string]string, id string, at time.Time, v float64) *monitoringpb.TimeSeries {
	return &monitoringpb.TimeSeries{
		Metric: &metricpb.Metric{Type: metric, Labels: labels},
		Resource: &monitoredres.MonitoredResource{
			Type: "gce_instance", Labels: map[string]string{"instance_id": id, "zone": "us-central1-a"},
		},
		Points: []*monitoringpb.Point{{
			Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(at)},
			Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_DoubleValue{DoubleValue: v}},
		}},
	}
}

func listSeries(
	t *testing.T,
	c *monitoring.MetricClient,
	req *monitoringpb.ListTimeSeriesRequest,
) []*monitoringpb.TimeSeries {
	t.Helper()

	req.Name = monitoringProject()

	var out []*monitoringpb.TimeSeries

	it := c.ListTimeSeries(t.Context(), req)

	for {
		ts, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out
		}

		require.NoError(t, err)

		out = append(out, ts)
	}
}

func TestIntegration_GCP_Monitoring_WriteAndAggregate(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	c := newMetricClient(t)
	ctx := t.Context()
	typ := "custom.googleapis.com/" + gcpName("it-depth")
	id := gcpName("it-inst")
	now := time.Now().Truncate(time.Second)

	for i, v := range []float64{2, 4, 6} {
		at := now.Add(time.Duration(i-3) * time.Second)
		require.NoError(t, c.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
			Name:       monitoringProject(),
			TimeSeries: []*monitoringpb.TimeSeries{gaugeSeries(typ, map[string]string{"tier": "gold"}, id, at, v)},
		}))
	}

	interval := &monitoringpb.TimeInterval{
		StartTime: timestamppb.New(now.Add(-time.Hour)), EndTime: timestamppb.New(now.Add(time.Minute)),
	}
	filter := `metric.type = "` + typ + `" AND resource.labels.instance_id = "` + id + `"`

	raw := listSeries(t, c, &monitoringpb.ListTimeSeriesRequest{Filter: filter, Interval: interval})
	require.Len(t, raw, 1)
	assert.Len(t, raw[0].GetPoints(), 3)

	mean := listSeries(t, c, &monitoringpb.ListTimeSeriesRequest{
		Filter: filter, Interval: interval,
		Aggregation: &monitoringpb.Aggregation{
			AlignmentPeriod: durationpb.New(2 * time.Hour), PerSeriesAligner: monitoringpb.Aggregation_ALIGN_MEAN,
		},
	})
	require.Len(t, mean, 1)
	require.Len(t, mean[0].GetPoints(), 1)
	assert.InDelta(t, 4.0, mean[0].GetPoints()[0].GetValue().GetDoubleValue(), 0.0001)

	err := c.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name: monitoringProject(),
		TimeSeries: []*monitoringpb.TimeSeries{
			gaugeSeries(typ, map[string]string{"tier": "gold"}, id, now.Add(-10*time.Second), 1),
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "in order")
}

func TestIntegration_GCP_Monitoring_AlertPublishesToPubSub(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	mc := newMetricClient(t)
	ps := newPubSubClient(t)
	ctx := t.Context()

	nc, err := monitoring.NewNotificationChannelClient(ctx, gcpGRPCOpts(t, monitoringPort)...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = nc.Close() })

	ac, err := monitoring.NewAlertPolicyClient(ctx, gcpGRPCOpts(t, monitoringPort)...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = ac.Close() })

	topic := monitoringProject() + "/topics/" + gcpName("it-alert")
	sub := monitoringProject() + "/subscriptions/" + gcpName("it-alertsub")

	_, err = ps.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: topic})
	require.NoError(t, err)
	_, err = ps.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{Name: sub, Topic: topic})
	require.NoError(t, err)

	ch, err := nc.CreateNotificationChannel(ctx, &monitoringpb.CreateNotificationChannelRequest{
		Name: monitoringProject(),
		NotificationChannel: &monitoringpb.NotificationChannel{
			Type: "pubsub", DisplayName: "it", Labels: map[string]string{"topic": topic},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = nc.DeleteNotificationChannel(
			t.Context(),
			&monitoringpb.DeleteNotificationChannelRequest{Name: ch.GetName(), Force: true},
		)
	})

	typ := "custom.googleapis.com/" + gcpName("it-hot")
	id := gcpName("it-host")

	policy, err := ac.CreateAlertPolicy(ctx, &monitoringpb.CreateAlertPolicyRequest{
		Name: monitoringProject(),
		AlertPolicy: &monitoringpb.AlertPolicy{
			DisplayName:          "hot " + id,
			Combiner:             monitoringpb.AlertPolicy_OR,
			NotificationChannels: []string{ch.GetName()},
			Conditions: []*monitoringpb.AlertPolicy_Condition{{
				DisplayName: "hot",
				Condition: &monitoringpb.AlertPolicy_Condition_ConditionThreshold{
					ConditionThreshold: &monitoringpb.AlertPolicy_Condition_MetricThreshold{
						Filter: `metric.type = "` + typ + `"`,
						Aggregations: []*monitoringpb.Aggregation{
							{
								AlignmentPeriod: durationpb.New(
									time.Minute,
								),
								PerSeriesAligner: monitoringpb.Aggregation_ALIGN_MEAN,
							},
						},
						Comparison: monitoringpb.ComparisonType_COMPARISON_GT, ThresholdValue: 3,
						Duration: durationpb.New(0),
					},
				},
			}},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = ac.DeleteAlertPolicy(t.Context(), &monitoringpb.DeleteAlertPolicyRequest{Name: policy.GetName()})
	})

	require.NoError(t, mc.CreateTimeSeries(ctx, &monitoringpb.CreateTimeSeriesRequest{
		Name:       monitoringProject(),
		TimeSeries: []*monitoringpb.TimeSeries{gaugeSeries(typ, nil, id, time.Now(), 9)},
	}))

	msg := receiveOne(t, ps, sub)
	assert.Contains(t, string(msg.Data), `"state":"open"`)
	assert.Contains(t, string(msg.Data), typ)
}

func TestIntegration_GCP_Monitoring_UptimeCheckProbesItself(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	mc := newMetricClient(t)
	uc, err := monitoring.NewUptimeCheckClient(t.Context(), gcpGRPCOpts(t, monitoringPort)...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = uc.Close() })

	cfg, err := uc.CreateUptimeCheckConfig(t.Context(), &monitoringpb.CreateUptimeCheckConfigRequest{
		Parent: monitoringProject(),
		UptimeCheckConfig: &monitoringpb.UptimeCheckConfig{
			DisplayName: gcpName("it-uptime"),
			Resource: &monitoringpb.UptimeCheckConfig_MonitoredResource{
				MonitoredResource: &monitoredres.MonitoredResource{
					Type: "uptime_url", Labels: map[string]string{"host": "localhost"},
				},
			},
			CheckRequestType: &monitoringpb.UptimeCheckConfig_HttpCheck_{
				HttpCheck: &monitoringpb.UptimeCheckConfig_HttpCheck{
					Path: "/v3/projects/it/monitoredResourceDescriptors", Port: 8132,
				},
			},
			ContentMatchers: []*monitoringpb.UptimeCheckConfig_ContentMatcher{{Content: "gce_instance"}},
			Period:          durationpb.New(time.Minute),
			Timeout:         durationpb.New(10 * time.Second),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = uc.DeleteUptimeCheckConfig(t.Context(), &monitoringpb.DeleteUptimeCheckConfigRequest{Name: cfg.GetName()})
	})

	checkID := cfg.GetName()[len(monitoringProject()+"/uptimeCheckConfigs/"):]

	require.Eventually(t, func() bool {
		got := listSeries(t, mc, &monitoringpb.ListTimeSeriesRequest{
			Filter: `metric.type = "monitoring.googleapis.com/uptime_check/check_passed" AND ` +
				`metric.labels.check_id = "` + checkID + `"`,
			Interval: &monitoringpb.TimeInterval{
				StartTime: timestamppb.New(
					time.Now().Add(-time.Hour),
				),
				EndTime: timestamppb.New(time.Now().Add(time.Minute)),
			},
		})

		return len(got) > 0 && got[0].GetPoints()[0].GetValue().GetBoolValue()
	}, 30*time.Second, time.Second)
}

func TestIntegration_GCP_Monitoring_LogBasedMetricVisible(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	mc := newMetricClient(t)
	lc, ac := newLoggingClients(t)
	ctx := t.Context()
	metricID := gcpName("it-logmetric")
	logID := gcpName("it-monlog")

	require.NoError(t, ac.CreateMetric(ctx, &logadmin.Metric{
		ID: metricID, Filter: `logName="projects/` + gcpProject + `/logs/` + logID + `" AND severity>=ERROR`,
	}))
	t.Cleanup(func() { _ = ac.DeleteMetric(t.Context(), metricID) })

	lg := lc.Logger(logID)
	lg.Log(logging.Entry{Payload: "boom", Severity: logging.Error})
	lg.Log(logging.Entry{Payload: "boom again", Severity: logging.Critical})
	lg.Log(logging.Entry{Payload: "fine", Severity: logging.Info})
	require.NoError(t, lg.Flush())

	require.Eventually(t, func() bool {
		got := listSeries(t, mc, &monitoringpb.ListTimeSeriesRequest{
			Filter: `metric.type = "logging.googleapis.com/user/` + metricID + `"`,
			Interval: &monitoringpb.TimeInterval{
				StartTime: timestamppb.New(
					time.Now().Add(-time.Hour),
				),
				EndTime: timestamppb.New(time.Now().Add(time.Minute)),
			},
		})

		return len(got) == 1 && got[0].GetPoints()[0].GetValue().GetInt64Value() == 2
	}, 30*time.Second, time.Second)
}
