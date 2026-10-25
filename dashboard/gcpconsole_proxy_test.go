package dashboard_test

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"cloud.google.com/go/redis/apiv1/redispb"
	"github.com/stretchr/testify/require"
	metricpb "google.golang.org/genproto/googleapis/api/metric"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/gcpcloudbuild"
	"github.com/blackbirdworks/gopherstack/services/gcpmonitoring"
	"github.com/blackbirdworks/gopherstack/services/gcpredis"
)

func TestGCPCloudBuildProxy(t *testing.T) {
	t.Parallel()

	const (
		loc     = "/dashboard/api/v1/gcp/cloudbuild/v1/projects/p/locations/global"
		trigger = loc + "/triggers/"
	)

	seed := func(t *testing.T) (*dashboard.DashboardHandler, string, string) {
		t.Helper()

		cb := gcpcloudbuild.NewHandler(gcpcloudbuild.NewInMemoryBackend())
		t.Cleanup(cb.Backend.Close)

		_, err := cb.Backend.CreateBuild(&cloudbuildpb.CreateBuildRequest{
			Parent: "projects/p/locations/global",
			Build: &cloudbuildpb.Build{
				Steps: []*cloudbuildpb.BuildStep{{Name: "alpine", Args: []string{"echo", "hi"}}},
			},
		})
		require.NoError(t, err)

		trig, err := cb.Backend.CreateBuildTrigger(&cloudbuildpb.CreateBuildTriggerRequest{
			Parent: "projects/p/locations/global",
			Trigger: &cloudbuildpb.BuildTrigger{
				Name: "nightly",
				BuildTemplate: &cloudbuildpb.BuildTrigger_Build{Build: &cloudbuildpb.Build{
					Steps: []*cloudbuildpb.BuildStep{{Name: "alpine"}},
				}},
			},
		})
		require.NoError(t, err)

		builds, _, err := cb.Backend.ListBuilds(&cloudbuildpb.ListBuildsRequest{Parent: "projects/p/locations/global"})
		require.NoError(t, err)
		require.Len(t, builds, 1)

		return dashboard.NewHandler(dashboard.Config{GCPCloudBuildOps: cb}), builds[0].GetId(), trig.GetId()
	}

	tests := []struct {
		name  string
		steps func(build, trig string) []gcpStep
	}{
		{"browse", func(build, trig string) []gcpStep {
			b := loc + "/builds/" + build

			return []gcpStep{
				{http.MethodGet, loc + "/builds", "", `"steps"`, 200},
				{http.MethodGet, loc + "/builds?pageSize=1", "", build, 200},
				{http.MethodGet, b, "", `"id":"` + build + `"`, 200},
				{http.MethodGet, b + "/log", "", `"available"`, 200},
				{http.MethodGet, loc + "/triggers", "", `"nightly"`, 200},
				{http.MethodGet, trigger + trig, "", `"name":"nightly"`, 200},
				{http.MethodGet, loc + "/builds/nope", "", "NOT_FOUND", http.StatusNotFound},
			}
		}},
		{"actions", func(build, trig string) []gcpStep {
			return []gcpStep{
				{http.MethodPost, trigger + trig + ":run", `{}`, `"name"`, 200},
				{http.MethodPost, loc + "/builds/" + build + ":retry", `{}`, `"name"`, 200},
				{http.MethodDelete, trigger + trig, "", "{}", 200},
				{http.MethodGet, trigger + trig, "", "NOT_FOUND", http.StatusNotFound},
			}
		}},
		{"rejects", func(build, trig string) []gcpStep {
			return []gcpStep{
				{http.MethodGet, loc + "/other", "", "unsupported", http.StatusNotFound},
				{http.MethodPost, loc + "/builds", "{}", "unsupported", http.StatusNotFound},
				{http.MethodDelete, loc + "/builds/" + build, "", "unsupported", http.StatusNotFound},
				{http.MethodPost, loc + "/builds/" + build + ":bogus", "", "unsupported", http.StatusNotFound},
				{http.MethodPut, trigger + trig, "{}", "unsupported", http.StatusNotFound},
				{http.MethodGet, trigger + trig + "/x", "", "unsupported", http.StatusNotFound},
				{http.MethodPost, trigger + trig + ":run", `{bad`, "invalid request body", http.StatusBadRequest},
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, build, trig := seed(t)
			runGCPSteps(t, h, tt.steps(build, trig))
		})
	}
}

func TestGCPRedisProxy(t *testing.T) {
	t.Parallel()

	const (
		loc  = "/dashboard/api/v1/gcp/redis/v1/projects/p/locations/us-central1"
		inst = loc + "/instances/cache1"
	)

	seed := func(t *testing.T) *dashboard.DashboardHandler {
		t.Helper()

		r := gcpredis.NewHandler(gcpredis.NewInMemoryBackend())
		t.Cleanup(r.Backend.Close)

		_, err := r.Backend.CreateInstance(&redispb.CreateInstanceRequest{
			Parent: "projects/p/locations/us-central1", InstanceId: "cache1",
			Instance: &redispb.Instance{Tier: redispb.Instance_BASIC, MemorySizeGb: 1, AuthEnabled: true},
		})
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			got, gErr := r.Backend.GetInstance("projects/p/locations/us-central1/instances/cache1")

			return gErr == nil && got.GetState() == redispb.Instance_READY
		}, 10*time.Second, 10*time.Millisecond)

		return dashboard.NewHandler(dashboard.Config{GCPRedisOps: r})
	}

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"browse", []gcpStep{
			{http.MethodGet, loc + "/instances", "", `"authEnabled":true`, 200},
			{http.MethodGet, inst, "", `"host"`, 200},
			{http.MethodGet, inst + "/authString", "", `"authString"`, 200},
			{http.MethodGet, loc + "/instances/nope", "", "NOT_FOUND", http.StatusNotFound},
		}},
		{"actions", []gcpStep{
			{http.MethodDelete, inst, "", `"name"`, 200},
		}},
		{"rejects", []gcpStep{
			{http.MethodGet, loc + "/other", "", "unsupported", http.StatusNotFound},
			{http.MethodPost, loc + "/instances", "{}", "unsupported", http.StatusNotFound},
			{http.MethodPut, inst, "{}", "unsupported", http.StatusNotFound},
			{http.MethodPost, inst + ":failover", "{}", "unsupported", http.StatusNotFound},
			{http.MethodGet, inst + "/other", "", "unsupported", http.StatusNotFound},
			{http.MethodPost, inst + "/authString", "{}", "unsupported", http.StatusNotFound},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runGCPSteps(t, seed(t), tt.steps)
		})
	}
}

func TestGCPMonitoringProxy(t *testing.T) {
	t.Parallel()

	const root = "/dashboard/api/v1/gcp/monitoring/v3/projects/p"

	end := time.Now().UTC().Truncate(time.Second)
	start := end.Add(-time.Hour)

	seed := func(t *testing.T) (*dashboard.DashboardHandler, string, string, string) {
		t.Helper()

		m := gcpmonitoring.NewHandler(gcpmonitoring.NewInMemoryBackend())
		b := m.Backend

		_, err := b.CreateMetricDescriptor(&monitoringpb.CreateMetricDescriptorRequest{
			Name: "projects/p",
			MetricDescriptor: &metricpb.MetricDescriptor{
				Type: "custom.googleapis.com/queue_depth", MetricKind: metricpb.MetricDescriptor_GAUGE,
				ValueType: metricpb.MetricDescriptor_INT64, Description: "depth",
			},
		})
		require.NoError(t, err)

		require.NoError(t, b.CreateTimeSeries(&monitoringpb.CreateTimeSeriesRequest{
			Name: "projects/p",
			TimeSeries: []*monitoringpb.TimeSeries{{
				Metric:   &metricpb.Metric{Type: "custom.googleapis.com/queue_depth"},
				Resource: &monitoredres.MonitoredResource{Type: "global", Labels: map[string]string{"project_id": "p"}},
				Points: []*monitoringpb.Point{{
					Interval: &monitoringpb.TimeInterval{EndTime: timestamppb.New(end.Add(-time.Minute))},
					Value:    &monitoringpb.TypedValue{Value: &monitoringpb.TypedValue_Int64Value{Int64Value: 7}},
				}},
			}},
		}))

		policy, err := b.CreateAlertPolicy(&monitoringpb.CreateAlertPolicyRequest{
			Name: "projects/p",
			AlertPolicy: &monitoringpb.AlertPolicy{
				DisplayName: "deep queue", Combiner: monitoringpb.AlertPolicy_OR,
				Conditions: []*monitoringpb.AlertPolicy_Condition{{
					DisplayName: "gt3",
					Condition: &monitoringpb.AlertPolicy_Condition_ConditionThreshold{
						ConditionThreshold: &monitoringpb.AlertPolicy_Condition_MetricThreshold{
							Filter:         `metric.type="custom.googleapis.com/queue_depth" AND resource.type="global"`,
							Comparison:     monitoringpb.ComparisonType_COMPARISON_GT,
							ThresholdValue: 3,
							Duration:       durationpb.New(time.Minute),
						},
					},
				}},
			},
		})
		require.NoError(t, err)

		check, err := b.CreateUptimeCheckConfig(&monitoringpb.CreateUptimeCheckConfigRequest{
			Parent: "projects/p",
			UptimeCheckConfig: &monitoringpb.UptimeCheckConfig{
				DisplayName: "home", Period: durationpb.New(time.Minute), Timeout: durationpb.New(10 * time.Second),
				Resource: &monitoringpb.UptimeCheckConfig_MonitoredResource{MonitoredResource: &monitoredres.MonitoredResource{
					Type: "uptime_url", Labels: map[string]string{"host": "example.com", "project_id": "p"},
				}},
				CheckRequestType: &monitoringpb.UptimeCheckConfig_HttpCheck_{
					HttpCheck: &monitoringpb.UptimeCheckConfig_HttpCheck{Path: "/", Port: 80},
				},
			},
		})
		require.NoError(t, err)

		snooze, err := b.CreateSnooze(&monitoringpb.CreateSnoozeRequest{
			Parent: "projects/p",
			Snooze: &monitoringpb.Snooze{
				DisplayName: "maintenance",
				Criteria:    &monitoringpb.Snooze_Criteria{Policies: []string{policy.GetName()}},
				Interval: &monitoringpb.TimeInterval{
					StartTime: timestamppb.New(time.Now().Add(time.Hour)),
					EndTime:   timestamppb.New(time.Now().Add(2 * time.Hour)),
				},
			},
		})
		require.NoError(t, err)

		return dashboard.NewHandler(dashboard.Config{GCPMonitoringOps: m}), policy.GetName(), check.GetName(), snooze.GetName()
	}

	series := func(extra string) string {
		q := url.Values{}
		q.Set("filter", `metric.type="custom.googleapis.com/queue_depth"`)
		q.Set("interval.startTime", start.Format(time.RFC3339))
		q.Set("interval.endTime", end.Format(time.RFC3339))

		return root + "/timeSeries?" + q.Encode() + extra
	}

	tests := []struct {
		name  string
		steps func(policy, check, snooze string) []gcpStep
	}{
		{"browse", func(policy, check, snooze string) []gcpStep {
			const prefix = "/dashboard/api/v1/gcp/monitoring/v3/"

			return []gcpStep{
				{http.MethodGet, root + "/metricDescriptors", "", "custom.googleapis.com/queue_depth", 200},
				{http.MethodGet, root + "/metricDescriptors/custom.googleapis.com/queue_depth", "", `"description":"depth"`, 200},
				{http.MethodGet, root + "/metricDescriptors/custom.googleapis.com/nope", "", "NOT_FOUND", http.StatusNotFound},
				{http.MethodGet, series(""), "", `"int64Value":"7"`, 200},
				{http.MethodGet, series("&aggregation.alignmentPeriod=60s&aggregation.perSeriesAligner=ALIGN_MAX"), "", `"points"`, 200},
				{http.MethodGet, root + "/alertPolicies", "", "deep queue", 200},
				{http.MethodGet, prefix + policy, "", "deep queue", 200},
				{http.MethodGet, root + "/uptimeCheckConfigs", "", `"home"`, 200},
				{http.MethodGet, prefix + check, "", "example.com", 200},
				{http.MethodGet, root + "/snoozes", "", "maintenance", 200},
				{http.MethodGet, prefix + snooze, "", "maintenance", 200},
				{http.MethodGet, root + "/notificationChannels", "", "{", 200},
			}
		}},
		{"actions", func(policy, check, _ string) []gcpStep {
			const prefix = "/dashboard/api/v1/gcp/monitoring/v3/"

			return []gcpStep{
				{http.MethodDelete, prefix + policy, "", "{}", 200},
				{http.MethodGet, prefix + policy, "", "NOT_FOUND", http.StatusNotFound},
				{http.MethodDelete, prefix + check, "", "{}", 200},
				{http.MethodGet, prefix + check, "", "NOT_FOUND", http.StatusNotFound},
			}
		}},
		{"rejects", func(policy, _, _ string) []gcpStep {
			const prefix = "/dashboard/api/v1/gcp/monitoring/v3/"

			return []gcpStep{
				{http.MethodGet, root + "/other", "", "unsupported", http.StatusNotFound},
				{http.MethodGet, "/dashboard/api/v1/gcp/monitoring/v1/projects/p/alertPolicies", "", "unsupported", http.StatusBadRequest},
				{http.MethodPost, root + "/timeSeries", "{}", "unsupported", http.StatusNotFound},
				{http.MethodPost, root + "/alertPolicies", "{}", "unsupported", http.StatusNotFound},
				{http.MethodPut, prefix + policy, "{}", "unsupported", http.StatusNotFound},
				{http.MethodPost, prefix + policy + ":bogus", "", "unsupported", http.StatusNotFound},
				{http.MethodDelete, root + "/metricDescriptors/custom.googleapis.com/queue_depth", "", "unsupported", http.StatusNotFound},
				{http.MethodGet, root + "/timeSeries?interval.endTime=bad", "", "invalid timestamp", http.StatusBadRequest},
				{http.MethodGet, series("&aggregation.alignmentPeriod=bad"), "", "alignmentPeriod", http.StatusBadRequest},
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, policy, check, snooze := seed(t)
			runGCPSteps(t, h, tt.steps(policy, check, snooze))
		})
	}
}

func TestGCPConsoleProxiesUnavailable(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})

	for _, path := range []string{
		"/dashboard/api/v1/gcp/cloudbuild/v1/projects/p/locations/global/builds",
		"/dashboard/api/v1/gcp/redis/v1/projects/p/locations/l/instances",
		"/dashboard/api/v1/gcp/monitoring/v3/projects/p/alertPolicies",
	} {
		runGCPSteps(t, h, []gcpStep{{http.MethodGet, path, "", "", http.StatusServiceUnavailable}})
	}
}
