package cloudwatch_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/cloudwatch"
)

func TestCloudWatchBackend_PutMetricData(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	data := []cloudwatch.MetricDatum{
		{MetricName: "Requests", Value: 42, Count: 1, Sum: 42, Min: 42, Max: 42, Timestamp: time.Now()},
	}
	require.NoError(t, b.PutMetricData("AWS/EC2", data))
}

func TestCloudWatchBackend_PutMetricData_Multiple(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	data := []cloudwatch.MetricDatum{
		{MetricName: "CPU", Value: 10, Count: 1, Sum: 10, Min: 10, Max: 10, Timestamp: time.Now()},
		{MetricName: "CPU", Value: 20, Count: 1, Sum: 20, Min: 20, Max: 20, Timestamp: time.Now()},
	}
	require.NoError(t, b.PutMetricData("AWS/EC2", data))
	metrics, err := b.ListMetrics("AWS/EC2", "CPU", "", 0)
	require.NoError(t, err)
	assert.Len(t, metrics.Data, 1)
}

func TestCloudWatchBackend_ListMetrics(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	_ = b.PutMetricData("NS1", []cloudwatch.MetricDatum{
		{MetricName: "M1", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1, Timestamp: time.Now()},
	})
	_ = b.PutMetricData("NS2", []cloudwatch.MetricDatum{
		{MetricName: "M2", Value: 2, Count: 1, Sum: 2, Min: 2, Max: 2, Timestamp: time.Now()},
	})

	all, err := b.ListMetrics("", "", "", 0)
	require.NoError(t, err)
	assert.Len(t, all.Data, 2)

	ns1, err := b.ListMetrics("NS1", "", "", 0)
	require.NoError(t, err)
	assert.Len(t, ns1.Data, 1)
	assert.Equal(t, "M1", ns1.Data[0].MetricName)

	byName, err := b.ListMetrics("", "M2", "", 0)
	require.NoError(t, err)
	assert.Len(t, byName.Data, 1)
}

func TestCloudWatchBackend_GetMetricStatistics(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Minute)

	tests := []struct {
		start           time.Time
		end             time.Time
		wantAverage     *float64
		setup           func(t *testing.T, b *cloudwatch.InMemoryBackend)
		wantSampleCount *float64
		wantMaximum     *float64
		wantMinimum     *float64
		wantSum         *float64
		metricName      string
		name            string
		namespace       string
		statistics      []string
		period          int32
		wantEmpty       bool
	}{
		{
			name: "average",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				data := []cloudwatch.MetricDatum{
					{MetricName: "CPU", Value: 10, Count: 1, Sum: 10, Min: 10, Max: 10, Timestamp: now},
					{
						MetricName: "CPU",
						Value:      20,
						Count:      1,
						Sum:        20,
						Min:        20,
						Max:        20,
						Timestamp:  now.Add(5 * time.Second),
					},
				}
				require.NoError(t, b.PutMetricData("AWS/EC2", data))
			},
			namespace:       "AWS/EC2",
			metricName:      "CPU",
			start:           now.Add(-time.Second),
			end:             now.Add(time.Minute),
			period:          60,
			statistics:      []string{"Average", "Sum", "Minimum", "Maximum", "SampleCount"},
			wantEmpty:       false,
			wantAverage:     new(15.0),
			wantSum:         new(30.0),
			wantMinimum:     new(10.0),
			wantMaximum:     new(20.0),
			wantSampleCount: new(2.0),
		},
		{
			name: "outside_range",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				old := time.Now().Add(-24 * time.Hour)
				data := []cloudwatch.MetricDatum{
					{MetricName: "CPU", Value: 10, Count: 1, Sum: 10, Min: 10, Max: 10, Timestamp: old},
				}
				require.NoError(t, b.PutMetricData("AWS/EC2", data))
			},
			namespace:  "AWS/EC2",
			metricName: "CPU",
			start:      time.Now().Add(-time.Hour),
			end:        time.Now(),
			period:     60,
			statistics: []string{"Sum"},
			wantEmpty:  true,
		},
		{
			name:       "no_data",
			namespace:  "NS",
			metricName: "Missing",
			start:      time.Now().Add(-time.Hour),
			end:        time.Now(),
			period:     60,
			statistics: []string{"Average"},
			wantEmpty:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			dps, err := b.GetMetricStatistics(tt.namespace, tt.metricName, tt.start, tt.end, tt.period, tt.statistics)
			require.NoError(t, err)

			if tt.wantEmpty {
				assert.Empty(t, dps)

				return
			}

			require.NotEmpty(t, dps)

			if tt.wantAverage != nil {
				assert.NotNil(t, dps[0].Average)
				assert.InDelta(t, *tt.wantAverage, *dps[0].Average, 0.01)
			}

			if tt.wantSum != nil {
				assert.NotNil(t, dps[0].Sum)
				assert.InDelta(t, *tt.wantSum, *dps[0].Sum, 0.01)
			}

			if tt.wantMinimum != nil {
				assert.NotNil(t, dps[0].Minimum)
				assert.InDelta(t, *tt.wantMinimum, *dps[0].Minimum, 0.01)
			}

			if tt.wantMaximum != nil {
				assert.NotNil(t, dps[0].Maximum)
				assert.InDelta(t, *tt.wantMaximum, *dps[0].Maximum, 0.01)
			}

			if tt.wantSampleCount != nil {
				assert.NotNil(t, dps[0].SampleCount)
				assert.InDelta(t, *tt.wantSampleCount, *dps[0].SampleCount, 0.01)
			}
		})
	}
}

func TestCloudWatchBackend_PutAndDescribeAlarms(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
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

	alarms, err := b.DescribeAlarms(nil, "", "", 0)
	require.NoError(t, err)
	require.Len(t, alarms.Data, 1)
	assert.Equal(t, "high-cpu", alarms.Data[0].AlarmName)
	assert.Contains(t, alarms.Data[0].AlarmArn, "high-cpu")
	assert.Equal(t, "INSUFFICIENT_DATA", alarms.Data[0].StateValue)
}

func TestCloudWatchBackend_DescribeAlarms(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, b *cloudwatch.InMemoryBackend)
		name       string
		stateValue string
		alarmNames []string
		wantCount  int
	}{
		{
			name: "filter_by_name",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				for _, name := range []string{"alarm-a", "alarm-b", "alarm-c"} {
					require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: name}))
				}
			},
			alarmNames: []string{"alarm-a", "alarm-c"},
			wantCount:  2,
		},
		{
			name: "filter_by_state",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "a1", StateValue: "OK"}))
				require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "a2", StateValue: "ALARM"}))
			},
			stateValue: "OK",
			wantCount:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			alarms, err := b.DescribeAlarms(tt.alarmNames, tt.stateValue, "", 0)
			require.NoError(t, err)
			assert.Len(t, alarms.Data, tt.wantCount)
		})
	}
}

func TestCloudWatchBackend_DeleteAlarms(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		setup         func(t *testing.T, b *cloudwatch.InMemoryBackend)
		names         []string
		wantRemaining int
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "to-delete"}))
			},
			names:         []string{"to-delete"},
			wantRemaining: 0,
		},
		{
			name:          "nonexistent",
			names:         []string{"no-such-alarm"},
			wantRemaining: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			require.NoError(t, b.DeleteAlarms(tt.names))

			alarms, err := b.DescribeAlarms(nil, "", "", 0)
			require.NoError(t, err)
			assert.Len(t, alarms.Data, tt.wantRemaining)
		})
	}
}

func TestCloudWatchBackend_PutMetricAlarm_MissingName(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	err := b.PutMetricAlarm(&cloudwatch.MetricAlarm{})
	require.Error(t, err)
}

func TestCloudWatchBackend_PutMetricAlarm_UpdateExisting(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "upd", Threshold: 10}))
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "upd", Threshold: 20}))
	alarms, err := b.DescribeAlarms(nil, "", "", 0)
	require.NoError(t, err)
	assert.Len(t, alarms.Data, 1)
	assert.InDelta(t, 20.0, alarms.Data[0].Threshold, 0.01)
}

func TestCloudWatchBackend_NewInMemoryBackend(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	require.NotNil(t, b)
}
