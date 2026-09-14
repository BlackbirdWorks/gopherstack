package cloudwatch_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// TestAlarmEvaluator_UnitFilter covers gopherstack-l4ywn: real CloudWatch only
// evaluates datapoints published with the alarm's configured Unit
// (PutMetricAlarm API doc, "Unit" parameter: "the unit that you want to use
// when the comparison operation compares the actual metric value against the
// threshold"). A breaching datapoint published under a different unit must not
// trigger the alarm; the same value published under the matching unit must.
func TestAlarmEvaluator_UnitFilter(t *testing.T) {
	t.Parallel()

	const (
		namespace  = "UnitFilterNS"
		metricName = "Latency"
		alarmName  = "unit-filtered-alarm"
		threshold  = 100.0
	)

	now := time.Now().UTC()

	tests := []struct {
		name      string
		alarmUnit string
		datumUnit string
		wantState string
	}{
		{
			name:      "matching unit evaluates and breaches",
			alarmUnit: "Milliseconds",
			datumUnit: "Milliseconds",
			wantState: "ALARM",
		},
		{
			name:      "mismatched unit is excluded, stays insufficient data",
			alarmUnit: "Milliseconds",
			datumUnit: "Seconds",
			wantState: "INSUFFICIENT_DATA",
		},
		{
			name:      "no alarm unit matches any datum unit",
			alarmUnit: "",
			datumUnit: "Seconds",
			wantState: "ALARM",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackend()

			err := b.PutMetricData(namespace, []cloudwatch.MetricDatum{
				{
					MetricName: metricName,
					Value:      150.0,
					Count:      1,
					Sum:        150.0,
					Min:        150.0,
					Max:        150.0,
					Timestamp:  now.Add(-30 * time.Second),
					Unit:       tc.datumUnit,
				},
			})
			require.NoError(t, err)

			err = b.PutMetricAlarm(&cloudwatch.MetricAlarm{
				AlarmName:          alarmName + "-" + tc.name,
				Namespace:          namespace,
				MetricName:         metricName,
				Statistic:          "Average",
				Period:             60,
				EvaluationPeriods:  1,
				Threshold:          threshold,
				ComparisonOperator: "GreaterThanThreshold",
				StateValue:         "INSUFFICIENT_DATA",
				Unit:               tc.alarmUnit,
			})
			require.NoError(t, err)

			b.EvaluateAlarms(t.Context(), now)

			pages, _, _, err := b.DescribeAlarms(
				[]string{alarmName + "-" + tc.name}, nil, "", "", "",
				100, "", "", "")
			require.NoError(t, err)
			require.Len(t, pages.Data, 1)
			assert.Equal(t, tc.wantState, pages.Data[0].StateValue)
		})
	}
}
