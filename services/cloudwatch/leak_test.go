package cloudwatch_test

import (
	"context"
	"testing"

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
