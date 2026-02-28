package cloudwatch_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/cloudwatch"
)

func TestCloudWatchBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T, b *cloudwatch.InMemoryBackend)
	}{
		{name: "PutMetricData/basic", run: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
			data := []cloudwatch.MetricDatum{
				{MetricName: "Requests", Value: 42, Count: 1, Sum: 42, Min: 42, Max: 42, Timestamp: time.Now()},
			}
			require.NoError(t, b.PutMetricData("AWS/EC2", data))
		}},
		{name: "PutMetricData/multiple", run: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
			data := []cloudwatch.MetricDatum{
				{MetricName: "CPU", Value: 10, Count: 1, Sum: 10, Min: 10, Max: 10, Timestamp: time.Now()},
				{MetricName: "CPU", Value: 20, Count: 1, Sum: 20, Min: 20, Max: 20, Timestamp: time.Now()},
			}
			require.NoError(t, b.PutMetricData("AWS/EC2", data))
			metrics, err := b.ListMetrics("AWS/EC2", "CPU")
			require.NoError(t, err)
			assert.Len(t, metrics, 1)
		}},
		{name: "ListMetrics/filter_namespace", run: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
			_ = b.PutMetricData("NS1", []cloudwatch.MetricDatum{
				{MetricName: "M1", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1, Timestamp: time.Now()},
			})
			_ = b.PutMetricData("NS2", []cloudwatch.MetricDatum{
				{MetricName: "M2", Value: 2, Count: 1, Sum: 2, Min: 2, Max: 2, Timestamp: time.Now()},
			})

			all, err := b.ListMetrics("", "")
			require.NoError(t, err)
			assert.Len(t, all, 2)

			ns1, err := b.ListMetrics("NS1", "")
			require.NoError(t, err)
			assert.Len(t, ns1, 1)
			assert.Equal(t, "M1", ns1[0].MetricName)

			byName, err := b.ListMetrics("", "M2")
			require.NoError(t, err)
			assert.Len(t, byName, 1)
		}},
		{name: "GetMetricStatistics/average", run: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
			now := time.Now().UTC().Truncate(time.Minute)
			data := []cloudwatch.MetricDatum{
				{MetricName: "CPU", Value: 10, Count: 1, Sum: 10, Min: 10, Max: 10, Timestamp: now},
				{MetricName: "CPU", Value: 20, Count: 1, Sum: 20, Min: 20, Max: 20, Timestamp: now.Add(5 * time.Second)},
			}
			require.NoError(t, b.PutMetricData("AWS/EC2", data))

			dps, err := b.GetMetricStatistics(
				"AWS/EC2", "CPU",
				now.Add(-time.Second), now.Add(time.Minute),
				60, []string{"Average", "Sum", "Minimum", "Maximum", "SampleCount"},
			)
			require.NoError(t, err)
			require.NotEmpty(t, dps)
			assert.NotNil(t, dps[0].Average)
			assert.InDelta(t, 15.0, *dps[0].Average, 0.01)
			assert.NotNil(t, dps[0].Sum)
			assert.InDelta(t, 30.0, *dps[0].Sum, 0.01)
			assert.NotNil(t, dps[0].Minimum)
			assert.InDelta(t, 10.0, *dps[0].Minimum, 0.01)
			assert.NotNil(t, dps[0].Maximum)
			assert.InDelta(t, 20.0, *dps[0].Maximum, 0.01)
			assert.NotNil(t, dps[0].SampleCount)
			assert.InDelta(t, 2.0, *dps[0].SampleCount, 0.01)
		}},
		{name: "GetMetricStatistics/outside_range", run: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
			old := time.Now().Add(-24 * time.Hour)
			data := []cloudwatch.MetricDatum{
				{MetricName: "CPU", Value: 10, Count: 1, Sum: 10, Min: 10, Max: 10, Timestamp: old},
			}
			require.NoError(t, b.PutMetricData("AWS/EC2", data))

			start := time.Now().Add(-time.Hour)
			end := time.Now()
			dps, err := b.GetMetricStatistics("AWS/EC2", "CPU", start, end, 60, []string{"Sum"})
			require.NoError(t, err)
			assert.Empty(t, dps)
		}},
		{name: "GetMetricStatistics/no_data", run: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
			start := time.Now().Add(-time.Hour)
			end := time.Now()
			dps, err := b.GetMetricStatistics("NS", "Missing", start, end, 60, []string{"Average"})
			require.NoError(t, err)
			assert.Empty(t, dps)
		}},
		{name: "PutAndDescribeAlarms/success", run: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
			alarm := &cloudwatch.MetricAlarm{
				AlarmName:          "high-cpu",
				Namespace:          "AWS/EC2",
				MetricName:         "CPUUtilization",
				ComparisonOperator: "GreaterThanThreshold",
				Threshold:          80.0,
				EvaluationPeriods:  1,
				Period:             60,
				Statistic:          "Average",
			}
			require.NoError(t, b.PutMetricAlarm(alarm))

			alarms, err := b.DescribeAlarms(nil, "")
			require.NoError(t, err)
			require.Len(t, alarms, 1)
			assert.Equal(t, "high-cpu", alarms[0].AlarmName)
			assert.Contains(t, alarms[0].AlarmArn, "high-cpu")
			assert.Equal(t, "INSUFFICIENT_DATA", alarms[0].StateValue)
		}},
		{name: "DescribeAlarms/filter_by_name", run: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
			for _, name := range []string{"alarm-a", "alarm-b", "alarm-c"} {
				require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: name}))
			}
			alarms, err := b.DescribeAlarms([]string{"alarm-a", "alarm-c"}, "")
			require.NoError(t, err)
			assert.Len(t, alarms, 2)
		}},
		{name: "DescribeAlarms/filter_by_state", run: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
			require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "a1", StateValue: "OK"}))
			require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "a2", StateValue: "ALARM"}))

			alarms, err := b.DescribeAlarms(nil, "OK")
			require.NoError(t, err)
			require.Len(t, alarms, 1)
			assert.Equal(t, "a1", alarms[0].AlarmName)
		}},
		{name: "DeleteAlarms/success", run: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
			require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "to-delete"}))
			require.NoError(t, b.DeleteAlarms([]string{"to-delete"}))

			alarms, err := b.DescribeAlarms(nil, "")
			require.NoError(t, err)
			assert.Empty(t, alarms)
		}},
		{name: "DeleteAlarms/nonexistent", run: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
			// Deleting a nonexistent alarm should not error.
			require.NoError(t, b.DeleteAlarms([]string{"no-such-alarm"}))
		}},
		{name: "PutMetricAlarm/missing_name", run: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
			err := b.PutMetricAlarm(&cloudwatch.MetricAlarm{})
			require.Error(t, err)
		}},
		{name: "PutMetricAlarm/update_existing", run: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
			require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "upd", Threshold: 10}))
			require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "upd", Threshold: 20}))
			alarms, err := b.DescribeAlarms(nil, "")
			require.NoError(t, err)
			assert.Len(t, alarms, 1)
			assert.InDelta(t, 20.0, alarms[0].Threshold, 0.01)
		}},
		{name: "NewInMemoryBackend/defaults", run: func(t *testing.T, _ *cloudwatch.InMemoryBackend) {
			b := cloudwatch.NewInMemoryBackend()
			require.NotNil(t, b)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			tt.run(t, b)
		})
	}
}
