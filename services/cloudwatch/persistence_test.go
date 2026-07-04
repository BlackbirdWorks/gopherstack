package cloudwatch_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *cloudwatch.InMemoryBackend) string
		verify func(t *testing.T, b *cloudwatch.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *cloudwatch.InMemoryBackend) string {
				err := b.PutMetricAlarm(&cloudwatch.MetricAlarm{
					AlarmName:          "test-alarm",
					ComparisonOperator: "GreaterThanThreshold",
					MetricName:         "CPUUtilization",
					Namespace:          "AWS/EC2",
					Statistic:          "Average",
				})
				if err != nil {
					return ""
				}

				return "test-alarm"
			},
			verify: func(t *testing.T, b *cloudwatch.InMemoryBackend, id string) {
				t.Helper()

				alarms, _, err := b.DescribeAlarms([]string{id}, nil, "", "", "", 0)
				require.NoError(t, err)
				require.Len(t, alarms.Data, 1)
				assert.Equal(t, id, alarms.Data[0].AlarmName)
				assert.Equal(t, "CPUUtilization", alarms.Data[0].MetricName)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *cloudwatch.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *cloudwatch.InMemoryBackend, _ string) {
				t.Helper()

				alarms, _, _ := b.DescribeAlarms(nil, nil, "", "", "", 0)
				assert.Empty(t, alarms.Data)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := cloudwatch.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := cloudwatch.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

func TestInMemoryBackend_SnapshotRestore_CompositeAndHistory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *cloudwatch.InMemoryBackend)
		verify func(t *testing.T, b *cloudwatch.InMemoryBackend)
		name   string
	}{
		{
			name: "composite_alarm_round_trip",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				require.NoError(
					t,
					b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "child-persist", StateValue: "OK"}),
				)
				require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
					AlarmName: "parent-persist",
					AlarmRule: `ALARM("child-persist")`,
				}))
			},
			verify: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()

				_, composites, err := b.DescribeAlarms(nil, []string{"CompositeAlarm"}, "", "", "", 0)
				require.NoError(t, err)
				require.Len(t, composites.Data, 1)
				assert.Equal(t, "parent-persist", composites.Data[0].AlarmName)
				assert.Equal(t, `ALARM("child-persist")`, composites.Data[0].AlarmRule)
			},
		},
		{
			name: "alarm_history_round_trip",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				require.NoError(
					t,
					b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "hist-persist", StateValue: "OK"}),
				)
				require.NoError(t, b.SetAlarmState(t.Context(), "hist-persist", "ALARM", "test reason", ""))
			},
			verify: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()

				p, err := b.DescribeAlarmHistory("hist-persist", "", "", "", time.Time{}, time.Time{}, 0)
				require.NoError(t, err)
				assert.NotEmpty(t, p.Data)
				assert.Equal(t, "hist-persist", p.Data[0].AlarmName)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := cloudwatch.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			tt.setup(t, original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := cloudwatch.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh)
		})
	}
}

func TestHandler_SnapshotRestore(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	h := cloudwatch.NewHandler(b)

	// Put an alarm so there is some state to snapshot.
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:  "snap-alarm",
		MetricName: "CPU",
		Namespace:  "AWS/EC2",
	}))

	// Handler.Snapshot delegates to the backend.
	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	// Create a fresh handler and restore into it.
	b2 := cloudwatch.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	h2 := cloudwatch.NewHandler(b2)
	require.NoError(t, h2.Restore(t.Context(), snap))

	alarms, _, err := b2.DescribeAlarms([]string{"snap-alarm"}, nil, "", "", "", 0)
	require.NoError(t, err)
	require.Len(t, alarms.Data, 1)
	assert.Equal(t, "snap-alarm", alarms.Data[0].AlarmName)
}

func TestInMemoryBackend_SnapshotRestore_NewResourceTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, b *cloudwatch.InMemoryBackend)
		verify func(t *testing.T, b *cloudwatch.InMemoryBackend)
		name   string
	}{
		{
			name: "anomaly_detector_round_trip",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				b.PutAnomalyDetectorInternal(&cloudwatch.AnomalyDetector{
					Namespace:  "AWS/EC2",
					MetricName: "CPUUtilization",
					Stat:       "Average",
				})
			},
			verify: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				p, err := b.DescribeAnomalyDetectors("AWS/EC2", "", "", 0)
				require.NoError(t, err)
				require.Len(t, p.Data, 1)
				assert.Equal(t, "CPUUtilization", p.Data[0].MetricName)
				assert.Equal(t, "TRAINED_INSUFFICIENT_DATA", p.Data[0].StateValue)
			},
		},
		{
			name: "insight_rule_round_trip",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				b.PutInsightRuleInternal(&cloudwatch.InsightRule{Name: "persist-rule", State: "ENABLED"})
			},
			verify: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				p, err := b.DescribeInsightRules("", 0)
				require.NoError(t, err)
				require.Len(t, p.Data, 1)
				assert.Equal(t, "persist-rule", p.Data[0].Name)
				assert.Equal(t, "ENABLED", p.Data[0].State)
				assert.NotEmpty(t, p.Data[0].Arn)
				assert.False(t, p.Data[0].CreatedAt.IsZero())
			},
		},
		{
			name: "metric_stream_round_trip",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				b.PutMetricStreamInternal(&cloudwatch.MetricStream{
					Name:         "persist-stream",
					FirehoseArn:  "arn:aws:firehose:us-east-1:000000000000:deliverystream/my-stream",
					OutputFormat: "json",
				})
			},
			verify: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				// Delete succeeds -> the stream was persisted and restored.
				err := b.DeleteMetricStream("persist-stream")
				require.NoError(t, err)
			},
		},
		{
			name: "alarm_mute_rule_round_trip",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				b.PutAlarmMuteRuleInternal(&cloudwatch.AlarmMuteRule{
					MuteName:    "persist-mute",
					Description: "test mute",
					AlarmNames:  []string{"alarm-a", "alarm-b"},
				})
			},
			verify: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				rule, err := b.GetAlarmMuteRule("persist-mute")
				require.NoError(t, err)
				assert.Equal(t, "test mute", rule.Description)
				assert.Equal(t, []string{"alarm-a", "alarm-b"}, rule.AlarmNames)
				assert.False(t, rule.CreationTime.IsZero())
			},
		},
		{
			name: "dashboard_round_trip",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutDashboard("persist-dash", `{"widgets":[]}`))
			},
			verify: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				entry, body, err := b.GetDashboard("persist-dash")
				require.NoError(t, err)
				assert.Equal(t, "persist-dash", entry.DashboardName)
				assert.JSONEq(t, `{"widgets":[]}`, body)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := cloudwatch.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			tt.setup(t, original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := cloudwatch.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh)
		})
	}
}

func TestInMemoryBackend_Reset_ClearsNewMaps(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("000000000000", "us-east-1")

	b.PutAnomalyDetectorInternal(&cloudwatch.AnomalyDetector{Namespace: "NS", MetricName: "M", Stat: "Sum"})
	b.PutInsightRuleInternal(&cloudwatch.InsightRule{Name: "rule-to-clear"})
	b.PutMetricStreamInternal(&cloudwatch.MetricStream{Name: "stream-to-clear"})
	b.PutAlarmMuteRuleInternal(&cloudwatch.AlarmMuteRule{MuteName: "mute-to-clear"})

	b.Reset()

	p1, err := b.DescribeAnomalyDetectors("", "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, p1.Data, "anomaly detectors should be empty after reset")

	p2, err := b.DescribeInsightRules("", 0)
	require.NoError(t, err)
	assert.Empty(t, p2.Data, "insight rules should be empty after reset")

	err = b.DeleteMetricStream("stream-to-clear")
	require.Error(t, err, "metric stream should not exist after reset")

	_, err = b.GetAlarmMuteRule("mute-to-clear")
	require.Error(t, err, "mute rule should not exist after reset")
}

func TestHandler_Reset_ClearsTags(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	h := cloudwatch.NewHandler(b)

	// Tag a resource.
	const alarmARN = "arn:aws:cloudwatch:us-east-1:000000000000:alarm:tagged-alarm-reset"
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:  "tagged-alarm-reset",
		AlarmArn:   alarmARN,
		MetricName: "CPU",
		Namespace:  "NS",
	}))

	rec := postForm(t, h,
		"Action=TagResource&ResourceARN="+alarmARN+
			"&Tags.member.1.Key=env&Tags.member.1.Value=prod")
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify tag exists.
	rec = postForm(t, h, "Action=ListTagsForResource&ResourceARN="+alarmARN)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "env")

	// Reset.
	h.Reset()

	// Tag should be gone after reset.
	rec = postForm(t, h, "Action=ListTagsForResource&ResourceARN="+alarmARN)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "env")
}

func TestHandler_SnapshotRestore_IncludesTags(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	h := cloudwatch.NewHandler(b)

	const alarmARN = "arn:aws:cloudwatch:us-east-1:000000000000:alarm:tagged-alarm-snap2"
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:  "tagged-alarm-snap2",
		AlarmArn:   alarmARN,
		MetricName: "CPU",
		Namespace:  "NS",
	}))

	rec := postForm(t, h, "Action=TagResource&ResourceARN="+alarmARN+
		"&Tags.member.1.Key=env&Tags.member.1.Value=staging")
	require.Equal(t, http.StatusOK, rec.Code)

	// Snapshot + restore into a fresh handler.
	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := cloudwatch.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	h2 := cloudwatch.NewHandler(b2)
	require.NoError(t, h2.Restore(t.Context(), snap))

	// Tags should have been restored.
	rec = postForm(t, h2, "Action=ListTagsForResource&ResourceARN="+alarmARN)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "staging")
}
