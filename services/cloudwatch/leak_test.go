package cloudwatch_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// TestDeleteAlarms_ReleasesAlarmHistory verifies that deleting an alarm also
// drops its retained history, so alarmHistory cannot grow unbounded across the
// lifetime of the backend as alarms are created and deleted.
func TestDeleteAlarms_ReleasesAlarmHistory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		alarmNames []string
	}{
		{name: "single alarm", alarmNames: []string{"alarm-0"}},
		{name: "many alarms", alarmNames: []string{"alarm-0", "alarm-1", "alarm-2", "alarm-3", "alarm-4"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			ctx := context.Background()

			baseline := b.AlarmHistoryKeyCountForTest()

			for _, name := range tc.alarmNames {
				require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
					AlarmName:  name,
					StateValue: "OK",
				}))
				// Force several state transitions so each alarm accrues history.
				require.NoError(t, b.SetAlarmState(ctx, name, "ALARM", "r1", ""))
				require.NoError(t, b.SetAlarmState(ctx, name, "OK", "r2", ""))
				require.NoError(t, b.SetAlarmState(ctx, name, "ALARM", "r3", ""))
			}

			require.Greater(t, b.AlarmHistoryKeyCountForTest(), baseline,
				"history should have accumulated before delete")

			require.NoError(t, b.DeleteAlarms(tc.alarmNames))

			require.Equal(t, baseline, b.AlarmHistoryKeyCountForTest(),
				"alarm history must return to baseline after all alarms are deleted")
		})
	}
}

// TestPutMetricData_DatapointsBackingArrayBounded verifies that repeatedly writing
// datapoints beyond cwMaxMetricDataPoints copies the tail into a fresh slice so that
// the backing array capacity stays at the cap rather than growing without bound.
func TestPutMetricData_DatapointsBackingArrayBounded(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	const (
		namespace  = "Test/Leak"
		metricName = "Requests"
		writes     = cloudwatch.CwMaxMetricDataPointsForTest + 50
	)

	now := time.Now().UTC()
	for i := range writes {
		err := b.PutMetricData(namespace, []cloudwatch.MetricDatum{{
			MetricName: metricName,
			Timestamp:  now.Add(time.Duration(i) * time.Second),
			Value:      float64(i),
		}})
		require.NoError(t, err)
	}

	gotCap := b.MetricPointsCapForTest(namespace, metricName)
	require.NotEqual(t, -1, gotCap, "metric series must exist")
	require.LessOrEqual(t, gotCap, cloudwatch.CwMaxMetricDataPointsForTest,
		"backing array capacity must not exceed the data-point cap after cap-and-copy")
}
