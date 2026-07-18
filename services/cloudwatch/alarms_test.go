package cloudwatch_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// ---------------------------------------------------------------------------
// Alarm: DatapointsToAlarm validation
// ---------------------------------------------------------------------------

func TestBackend_PutMetricAlarm_DatapointsToAlarmExceedsEvalPeriods(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	err := b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:          "bad",
		Namespace:          "NS",
		MetricName:         "M",
		EvaluationPeriods:  3,
		DatapointsToAlarm:  5, // > EvaluationPeriods
		ComparisonOperator: "GreaterThanThreshold",
		Threshold:          80,
	})
	assert.Error(t, err, "DatapointsToAlarm > EvaluationPeriods should be rejected")
}

func TestBackend_PutMetricAlarm_DatapointsToAlarmValid(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	err := b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:          "ok",
		Namespace:          "NS",
		MetricName:         "M",
		EvaluationPeriods:  5,
		DatapointsToAlarm:  3,
		ComparisonOperator: "GreaterThanThreshold",
		Threshold:          80,
	})
	assert.NoError(t, err, "DatapointsToAlarm <= EvaluationPeriods is valid")
}

// ---------------------------------------------------------------------------
// Alarm: TreatMissingData field round-trip
// ---------------------------------------------------------------------------

func TestBackend_PutMetricAlarm_TreatMissingData(t *testing.T) {
	t.Parallel()

	for _, tmd := range []string{"missing", "notBreaching", "breaching", "ignore"} {
		t.Run(tmd, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackend()
			err := b.PutMetricAlarm(&cloudwatch.MetricAlarm{
				AlarmName:          "a",
				Namespace:          "NS",
				MetricName:         "M",
				EvaluationPeriods:  1,
				ComparisonOperator: "GreaterThanThreshold",
				Threshold:          50,
				TreatMissingData:   tmd,
			})
			require.NoError(t, err)

			alarms, _, err := b.DescribeAlarms([]string{"a"}, nil, "", "", "", 0)
			require.NoError(t, err)
			require.Len(t, alarms.Data, 1)
			assert.Equal(t, tmd, alarms.Data[0].TreatMissingData)
		})
	}
}

// ---------------------------------------------------------------------------
// Alarm: Statistic / ExtendedStatistic mutual exclusion
// ---------------------------------------------------------------------------

func TestBackend_PutMetricAlarm_StatAndExtendedStat_Rejected(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	err := b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:          "a",
		Namespace:          "NS",
		MetricName:         "M",
		EvaluationPeriods:  1,
		ComparisonOperator: "GreaterThanThreshold",
		Threshold:          50,
		Statistic:          "Average",
		ExtendedStatistic:  "p99",
	})
	assert.Error(t, err, "Statistic and ExtendedStatistic are mutually exclusive")
}

// ---------------------------------------------------------------------------
// DescribeAlarms: filters
// ---------------------------------------------------------------------------

func TestBackend_DescribeAlarms_ByNamePrefix(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	for _, name := range []string{"prod-cpu", "prod-mem", "staging-cpu"} {
		require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
			AlarmName: name, Namespace: "NS", MetricName: "M",
			ComparisonOperator: "GreaterThanThreshold", Threshold: 80, EvaluationPeriods: 1,
		}))
	}

	p, _, err := b.DescribeAlarms(nil, nil, "prod-", "", "", 0)
	require.NoError(t, err)
	assert.Len(t, p.Data, 2)
	for _, a := range p.Data {
		assert.True(t, strings.HasPrefix(a.AlarmName, "prod-"))
	}
}

func TestBackend_DescribeAlarms_ByState(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName: "a1", Namespace: "NS", MetricName: "M",
		ComparisonOperator: "GreaterThanThreshold", Threshold: 80, EvaluationPeriods: 1,
	}))
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName: "a2", Namespace: "NS", MetricName: "M",
		ComparisonOperator: "GreaterThanThreshold", Threshold: 80, EvaluationPeriods: 1,
	}))
	require.NoError(t, b.SetAlarmState(t.Context(), "a1", "ALARM", "test", ""))

	p, _, err := b.DescribeAlarms(nil, nil, "", "ALARM", "", 0)
	require.NoError(t, err)
	assert.Len(t, p.Data, 1)
	assert.Equal(t, "a1", p.Data[0].AlarmName)
}

func TestBackend_DescribeAlarmsForMetric_Filters(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	for _, mn := range []string{"CPU", "Memory"} {
		require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
			AlarmName: mn + "-alarm", Namespace: "AWS/EC2", MetricName: mn,
			ComparisonOperator: "GreaterThanThreshold", Threshold: 80, EvaluationPeriods: 1,
		}))
	}

	p, err := b.DescribeAlarmsForMetric("AWS/EC2", "CPU", nil, nil, "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "CPU-alarm", p.Data[0].AlarmName)
}

// ---------------------------------------------------------------------------
// Fix 6: DescribeAlarmsForMetric ignores Dimensions filter
// ---------------------------------------------------------------------------

func TestDescribeAlarmsForMetric_DimensionFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		filterDims []cloudwatch.Dimension
		wantNames  []string
	}{
		{
			name:       "filter by prod dimension — returns only prod alarm",
			filterDims: []cloudwatch.Dimension{{Name: "Env", Value: "prod"}},
			wantNames:  []string{"prod-alarm"},
		},
		{
			name:       "filter by staging dimension — returns only staging alarm",
			filterDims: []cloudwatch.Dimension{{Name: "Env", Value: "staging"}},
			wantNames:  []string{"staging-alarm"},
		},
		{
			name:       "no dimension filter — returns both alarms",
			filterDims: nil,
			wantNames:  []string{"prod-alarm", "staging-alarm"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackend()
			require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
				AlarmName:          "prod-alarm",
				Namespace:          "NS",
				MetricName:         "M",
				ComparisonOperator: "GreaterThanThreshold",
				EvaluationPeriods:  1,
				Period:             60,
				Dimensions:         []cloudwatch.Dimension{{Name: "Env", Value: "prod"}},
			}))
			require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
				AlarmName:          "staging-alarm",
				Namespace:          "NS",
				MetricName:         "M",
				ComparisonOperator: "GreaterThanThreshold",
				EvaluationPeriods:  1,
				Period:             60,
				Dimensions:         []cloudwatch.Dimension{{Name: "Env", Value: "staging"}},
			}))

			p, err := b.DescribeAlarmsForMetric("NS", "M", tc.filterDims, nil, "", 0)
			require.NoError(t, err)

			gotNames := make([]string, 0, len(p.Data))
			for _, a := range p.Data {
				gotNames = append(gotNames, a.AlarmName)
			}
			assert.ElementsMatch(t, tc.wantNames, gotNames,
				"DescribeAlarmsForMetric must filter by Dimensions when provided")
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

	alarms, _, err := b.DescribeAlarms(nil, nil, "", "", "", 0)
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
				require.NoError(
					t,
					b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "a1", StateValue: "OK"}),
				)
				require.NoError(
					t,
					b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "a2", StateValue: "ALARM"}),
				)
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

			alarms, _, err := b.DescribeAlarms(tt.alarmNames, nil, "", tt.stateValue, "", 0)
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
				require.NoError(
					t,
					b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "to-delete"}),
				)
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

			alarms, _, err := b.DescribeAlarms(nil, nil, "", "", "", 0)
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
	alarms, _, err := b.DescribeAlarms(nil, nil, "", "", "", 0)
	require.NoError(t, err)
	assert.Len(t, alarms.Data, 1)
	assert.InDelta(t, 20.0, alarms.Data[0].Threshold, 0.01)
}

func TestCloudWatchBackend_EnableDisableAlarmActions(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	require.NoError(
		t,
		b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "test", ActionsEnabled: true}),
	)

	require.NoError(t, b.DisableAlarmActions([]string{"test"}))
	alarms, _, err := b.DescribeAlarms([]string{"test"}, nil, "", "", "", 0)
	require.NoError(t, err)
	require.Len(t, alarms.Data, 1)
	assert.False(t, alarms.Data[0].ActionsEnabled)

	require.NoError(t, b.EnableAlarmActions([]string{"test"}))
	alarms2, _, err2 := b.DescribeAlarms([]string{"test"}, nil, "", "", "", 0)
	require.NoError(t, err2)
	require.Len(t, alarms2.Data, 1)
	assert.True(t, alarms2.Data[0].ActionsEnabled)
}

func TestCloudWatchBackend_DescribeAlarmsForMetric(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName: "cpu-alarm", Namespace: "AWS/EC2", MetricName: "CPUUtilization",
	}))
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName: "mem-alarm", Namespace: "AWS/EC2", MetricName: "MemoryUtilization",
	}))

	p, err := b.DescribeAlarmsForMetric("AWS/EC2", "CPUUtilization", nil, nil, "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "cpu-alarm", p.Data[0].AlarmName)
}

func TestCloudWatchBackend_DescribeAlarms_WithComposite(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	require.NoError(
		t,
		b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "metric1", StateValue: "ALARM"}),
	)
	require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
		AlarmName: "comp1", AlarmRule: `ALARM("metric1")`,
	}))

	metricPage, compositePage, err := b.DescribeAlarms(nil, nil, "", "", "", 0)
	require.NoError(t, err)
	assert.Len(t, metricPage.Data, 1)
	assert.Len(t, compositePage.Data, 1)
	assert.Equal(t, "ALARM", compositePage.Data[0].StateValue)
}

func TestCloudWatchBackend_DescribeAlarmsForMetric_WithAlarmNames(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:  "match-name",
		Namespace:  "NS",
		MetricName: "M",
	}))
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:  "other-name",
		Namespace:  "NS",
		MetricName: "M",
	}))

	// Filter by both namespace+metric AND alarm name.
	p, err := b.DescribeAlarmsForMetric("NS", "M", nil, []string{"match-name"}, "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "match-name", p.Data[0].AlarmName)
}

func TestCloudWatchBackend_DescribeAlarms_StateFilter(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	require.NoError(
		t,
		b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "a-ok", StateValue: "OK"}),
	)
	require.NoError(
		t,
		b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "a-alarm", StateValue: "ALARM"}),
	)

	p, _, err := b.DescribeAlarms(nil, nil, "", "OK", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "a-ok", p.Data[0].AlarmName)
}

func TestCloudWatchBackend_DescribeAlarms_AlarmNamePrefix(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	for _, name := range []string{"prod-cpu", "prod-mem", "staging-cpu"} {
		require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
			AlarmName:          name,
			ComparisonOperator: "GreaterThanThreshold",
			EvaluationPeriods:  1,
			Period:             60,
		}))
	}

	p, _, err := b.DescribeAlarms(nil, nil, "prod-", "", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 2)
	for _, a := range p.Data {
		assert.Contains(t, a.AlarmName, "prod-")
	}
}

func TestCloudWatchBackend_StateTransitionedTimestamp(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:          "ts-alarm",
		ComparisonOperator: "GreaterThanThreshold",
		EvaluationPeriods:  1,
		Period:             60,
	}))

	p, _, err := b.DescribeAlarms([]string{"ts-alarm"}, nil, "", "", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.False(
		t,
		p.Data[0].StateTransitionedTimestamp.IsZero(),
		"StateTransitionedTimestamp should be set on creation",
	)

	// Change state — timestamp should update.
	prevTS := p.Data[0].StateTransitionedTimestamp
	require.NoError(t, b.SetAlarmState(t.Context(), "ts-alarm", "ALARM", "manual", ""))

	p2, _, err2 := b.DescribeAlarms([]string{"ts-alarm"}, nil, "", "", "", 0)
	require.NoError(t, err2)
	require.Len(t, p2.Data, 1)

	newTS := p2.Data[0].StateTransitionedTimestamp
	assert.True(
		t,
		newTS.After(prevTS) || newTS.Equal(prevTS),
		"timestamp should not go backwards",
	)
}

func TestCloudWatchBackend_GetAlarmARNs(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:          "arn-alarm",
		ComparisonOperator: "GreaterThanThreshold",
		EvaluationPeriods:  1,
		Period:             60,
	}))

	arns := b.GetAlarmARNs([]string{"arn-alarm", "nonexistent"})
	require.Len(t, arns, 1)
	assert.Contains(t, arns[0], "arn-alarm")
}
