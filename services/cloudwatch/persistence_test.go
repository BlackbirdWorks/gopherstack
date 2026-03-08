package cloudwatch_test

import (
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

				alarms, _, err := b.DescribeAlarms([]string{id}, nil, "", "", 0)
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

				alarms, _, _ := b.DescribeAlarms(nil, nil, "", "", 0)
				assert.Empty(t, alarms.Data)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := cloudwatch.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := cloudwatch.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}

func TestInMemoryBackend_SnapshotRestore_CompositeAndHistory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *cloudwatch.InMemoryBackend)
		verify func(t *testing.T, b *cloudwatch.InMemoryBackend)
		name   string
	}{
		{
			name: "composite_alarm_round_trip",
			setup: func(b *cloudwatch.InMemoryBackend) {
				_ = b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "child-persist", StateValue: "OK"})
				_ = b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
					AlarmName: "parent-persist",
					AlarmRule: `ALARM("child-persist")`,
				})
			},
			verify: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()

				_, composites, err := b.DescribeAlarms(nil, []string{"CompositeAlarm"}, "", "", 0)
				require.NoError(t, err)
				require.Len(t, composites.Data, 1)
				assert.Equal(t, "parent-persist", composites.Data[0].AlarmName)
				assert.Equal(t, `ALARM("child-persist")`, composites.Data[0].AlarmRule)
			},
		},
		{
			name: "alarm_history_round_trip",
			setup: func(b *cloudwatch.InMemoryBackend) {
				_ = b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "hist-persist", StateValue: "OK"})
				_ = b.SetAlarmState("hist-persist", "ALARM", "test reason")
			},
			verify: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()

				p, err := b.DescribeAlarmHistory("hist-persist", "", "", time.Time{}, time.Time{}, 0)
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
			tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := cloudwatch.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(snap))

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
	snap := h.Snapshot()
	require.NotNil(t, snap)

	// Create a fresh handler and restore into it.
	b2 := cloudwatch.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	h2 := cloudwatch.NewHandler(b2)
	require.NoError(t, h2.Restore(snap))

	alarms, _, err := b2.DescribeAlarms([]string{"snap-alarm"}, nil, "", "", 0)
	require.NoError(t, err)
	require.Len(t, alarms.Data, 1)
	assert.Equal(t, "snap-alarm", alarms.Data[0].AlarmName)
}
