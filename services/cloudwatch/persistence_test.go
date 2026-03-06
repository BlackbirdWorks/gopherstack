package cloudwatch_test

import (
	"testing"

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

				alarms, err := b.DescribeAlarms([]string{id}, "", "", 0)
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

				alarms, _ := b.DescribeAlarms(nil, "", "", 0)
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
