package cloudwatch_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
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

	alarms, _, err := b.DescribeAlarms(nil, nil, "", "", 0)
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

			alarms, _, err := b.DescribeAlarms(tt.alarmNames, nil, tt.stateValue, "", 0)
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

			alarms, _, err := b.DescribeAlarms(nil, nil, "", "", 0)
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
	alarms, _, err := b.DescribeAlarms(nil, nil, "", "", 0)
	require.NoError(t, err)
	assert.Len(t, alarms.Data, 1)
	assert.InDelta(t, 20.0, alarms.Data[0].Threshold, 0.01)
}

func TestCloudWatchBackend_NewInMemoryBackend(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	require.NotNil(t, b)
}

func TestCloudWatchBackend_PutCompositeAlarm(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(t *testing.T, b *cloudwatch.InMemoryBackend)
		alarm     *cloudwatch.CompositeAlarm
		wantState string
		wantErr   bool
	}{
		{
			name: "alarm_in_alarm_state",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "child", StateValue: "ALARM"}))
			},
			alarm:     &cloudwatch.CompositeAlarm{AlarmName: "composite", AlarmRule: `ALARM("child")`},
			wantState: "ALARM",
		},
		{
			name: "alarm_in_ok_state",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "child", StateValue: "OK"}))
			},
			alarm:     &cloudwatch.CompositeAlarm{AlarmName: "composite", AlarmRule: `ALARM("child")`},
			wantState: "OK",
		},
		{
			name: "and_rule",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "a", StateValue: "ALARM"}))
				require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "b", StateValue: "ALARM"}))
			},
			alarm:     &cloudwatch.CompositeAlarm{AlarmName: "composite", AlarmRule: `ALARM("a") AND ALARM("b")`},
			wantState: "ALARM",
		},
		{
			name: "or_rule_one_ok",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "a", StateValue: "ALARM"}))
				require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "b", StateValue: "OK"}))
			},
			alarm:     &cloudwatch.CompositeAlarm{AlarmName: "composite", AlarmRule: `ALARM("a") OR ALARM("b")`},
			wantState: "ALARM",
		},
		{
			name:    "missing_name",
			alarm:   &cloudwatch.CompositeAlarm{AlarmRule: `ALARM("x")`},
			wantErr: true,
		},
		{
			name:    "missing_rule",
			alarm:   &cloudwatch.CompositeAlarm{AlarmName: "c"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.PutCompositeAlarm(tt.alarm)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			_, compositeAlarms, err2 := b.DescribeAlarms(
				[]string{tt.alarm.AlarmName},
				[]string{"CompositeAlarm"},
				"",
				"",
				0,
			)
			require.NoError(t, err2)
			require.Len(t, compositeAlarms.Data, 1)
			assert.Equal(t, tt.wantState, compositeAlarms.Data[0].StateValue)
		})
	}
}

func TestCloudWatchBackend_SetAlarmState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setup       func(t *testing.T, b *cloudwatch.InMemoryBackend)
		alarmName   string
		stateValue  string
		stateReason string
		wantErr     bool
	}{
		{
			name: "metric_alarm_state_change",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "test-alarm"}))
			},
			alarmName:   "test-alarm",
			stateValue:  "ALARM",
			stateReason: "Test triggered",
		},
		{
			name: "composite_alarm_state_change",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "child", StateValue: "OK"}))
				require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
					AlarmName: "comp", AlarmRule: `ALARM("child")`,
				}))
			},
			alarmName:   "comp",
			stateValue:  "ALARM",
			stateReason: "Manual override",
		},
		{
			name:       "nonexistent_alarm",
			alarmName:  "no-alarm",
			stateValue: "ALARM",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.SetAlarmState(tt.alarmName, tt.stateValue, tt.stateReason)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestCloudWatchBackend_EnableDisableAlarmActions(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "test", ActionsEnabled: true}))

	require.NoError(t, b.DisableAlarmActions([]string{"test"}))
	alarms, _, err := b.DescribeAlarms([]string{"test"}, nil, "", "", 0)
	require.NoError(t, err)
	require.Len(t, alarms.Data, 1)
	assert.False(t, alarms.Data[0].ActionsEnabled)

	require.NoError(t, b.EnableAlarmActions([]string{"test"}))
	alarms2, _, err2 := b.DescribeAlarms([]string{"test"}, nil, "", "", 0)
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

	p, err := b.DescribeAlarmsForMetric("AWS/EC2", "CPUUtilization", nil, "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "cpu-alarm", p.Data[0].AlarmName)
}

func TestCloudWatchBackend_DescribeAlarmHistory(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "hist-alarm", ActionsEnabled: true}))
	require.NoError(t, b.SetAlarmState("hist-alarm", "ALARM", "test trigger"))

	p, err := b.DescribeAlarmHistory("hist-alarm", "", "", time.Time{}, time.Time{}, 0)
	require.NoError(t, err)
	assert.NotEmpty(t, p.Data)
}

func TestCloudWatchBackend_DescribeAlarms_WithComposite(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "metric1", StateValue: "ALARM"}))
	require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
		AlarmName: "comp1", AlarmRule: `ALARM("metric1")`,
	}))

	metricPage, compositePage, err := b.DescribeAlarms(nil, nil, "", "", 0)
	require.NoError(t, err)
	assert.Len(t, metricPage.Data, 1)
	assert.Len(t, compositePage.Data, 1)
	assert.Equal(t, "ALARM", compositePage.Data[0].StateValue)
}

func TestCloudWatchBackend_CompositeAlarmReevalOnChildChange(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "child", StateValue: "OK"}))
	require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
		AlarmName: "parent", AlarmRule: `ALARM("child")`,
	}))

	// Initially composite should be OK since child is OK
	_, compositeAlarms, err := b.DescribeAlarms([]string{"parent"}, nil, "", "", 0)
	require.NoError(t, err)
	assert.Equal(t, "OK", compositeAlarms.Data[0].StateValue)

	// Change child to ALARM; composite should re-evaluate
	require.NoError(t, b.SetAlarmState("child", "ALARM", "test"))
	_, compositeAlarms2, err2 := b.DescribeAlarms([]string{"parent"}, nil, "", "", 0)
	require.NoError(t, err2)
	assert.Equal(t, "ALARM", compositeAlarms2.Data[0].StateValue)
}

// mockSNSPublisher captures published messages for assertions.
type mockSNSPublisher struct {
	messages []string
}

func (m *mockSNSPublisher) PublishToTopic(_ string, message string) error {
	m.messages = append(m.messages, message)

	return nil
}

func TestCloudWatchBackend_CompositeAlarmActionsFireOnChildChange(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	pub := &mockSNSPublisher{}
	b.SetSNSPublisher(pub)

	topicARN := "arn:aws:sns:us-east-1:123456789012:test-topic"

	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "child2", StateValue: "OK"}))
	require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
		AlarmName:      "parent2",
		AlarmRule:      `ALARM("child2")`,
		ActionsEnabled: true,
		AlarmActions:   []string{topicARN},
	}))

	// No SNS publish yet — child is OK, composite is OK.
	assert.Empty(t, pub.messages)

	// Transition child to ALARM; composite should re-evaluate and fire its AlarmActions.
	require.NoError(t, b.SetAlarmState("child2", "ALARM", "test trigger"))

	assert.Len(t, pub.messages, 1, "composite alarm action should have been fired")
	assert.Contains(t, pub.messages[0], "parent2")
}

// mockLambdaInvoker captures lambda invocations for assertions.
type mockLambdaInvoker struct {
	invocations []string
}

func (m *mockLambdaInvoker) InvokeFunction(_ context.Context, name string, _ string, _ []byte) ([]byte, int, error) {
	m.invocations = append(m.invocations, name)

	return nil, 200, nil
}

func TestCloudWatchBackend_LambdaActionFires(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	inv := &mockLambdaInvoker{}
	b.SetLambdaInvoker(inv)

	lambdaARN := "arn:aws:lambda:us-east-1:123456789012:function:my-fn"

	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:      "lambda-alarm",
		StateValue:     "OK",
		ActionsEnabled: true,
		AlarmActions:   []string{lambdaARN},
	}))

	require.NoError(t, b.SetAlarmState("lambda-alarm", "ALARM", "test"))

	assert.Len(t, inv.invocations, 1)
	assert.Equal(t, lambdaARN, inv.invocations[0])
}

func TestCloudWatchBackend_ExecuteActions_NoInvoker(t *testing.T) {
	t.Parallel()

	// Ensures executeActions does not panic when publisher/invoker is nil.
	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:      "no-invoker-alarm",
		StateValue:     "OK",
		ActionsEnabled: true,
		AlarmActions: []string{
			"arn:aws:sns:us-east-1:123:topic",
			"arn:aws:lambda:us-east-1:123:function:fn",
			"arn:aws:ec2:us-east-1:123:action/stop",
		},
	}))

	// Should not panic even with nil publisher/invoker.
	require.NoError(t, b.SetAlarmState("no-invoker-alarm", "ALARM", "test"))
}

func TestCloudWatchBackend_EvalCompositeRule_NestedComposite(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "leaf", StateValue: "ALARM"}))
	require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
		AlarmName: "mid",
		AlarmRule: `ALARM("leaf")`,
	}))
	require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
		AlarmName: "top",
		AlarmRule: `ALARM("mid")`,
	}))

	_, composites, err := b.DescribeAlarms([]string{"top"}, nil, "", "", 0)
	require.NoError(t, err)
	require.Len(t, composites.Data, 1)
	// top should be ALARM since leaf is ALARM -> mid is ALARM -> top is ALARM
	assert.Equal(t, "ALARM", composites.Data[0].StateValue)
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
	p, err := b.DescribeAlarmsForMetric("NS", "M", []string{"match-name"}, "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "match-name", p.Data[0].AlarmName)
}

func TestCloudWatchBackend_DescribeAlarmHistory_TypeFilter(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "type-filter", StateValue: "OK"}))
	require.NoError(t, b.SetAlarmState("type-filter", "ALARM", "transition"))

	// Filter by StateUpdate type — should find the state transition.
	p, err := b.DescribeAlarmHistory("type-filter", "StateUpdate", "", time.Time{}, time.Time{}, 0)
	require.NoError(t, err)
	assert.NotEmpty(t, p.Data)
	for _, item := range p.Data {
		assert.Equal(t, "StateUpdate", item.HistoryItemType)
	}

	// PutMetricAlarm creates ConfigurationUpdate items.
	p2, err2 := b.DescribeAlarmHistory("type-filter", "ConfigurationUpdate", "", time.Time{}, time.Time{}, 0)
	require.NoError(t, err2)
	assert.NotEmpty(t, p2.Data)
	for _, item := range p2.Data {
		assert.Equal(t, "ConfigurationUpdate", item.HistoryItemType)
	}
}

func TestCloudWatchBackend_PutCompositeAlarm_UpdateExisting(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "child-update", StateValue: "ALARM"}))

	require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
		AlarmName: "comp-update",
		AlarmRule: `ALARM("child-update")`,
	}))

	// Update with a new description.
	require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
		AlarmName:        "comp-update",
		AlarmRule:        `ALARM("child-update")`,
		AlarmDescription: "updated",
	}))

	_, composites, err := b.DescribeAlarms([]string{"comp-update"}, nil, "", "", 0)
	require.NoError(t, err)
	require.Len(t, composites.Data, 1)
	assert.Equal(t, "updated", composites.Data[0].AlarmDescription)
}

func TestCloudWatchBackend_DescribeAlarms_StateFilter(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "a-ok", StateValue: "OK"}))
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "a-alarm", StateValue: "ALARM"}))

	p, _, err := b.DescribeAlarms(nil, nil, "OK", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "a-ok", p.Data[0].AlarmName)
}

func TestCloudWatchBackend_SetAlarmState_ChildTriggersCompositeReevaluation(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	// Child alarm starts in ALARM state, composite rule evaluates to ALARM.
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "child-direct", StateValue: "ALARM"}))
	require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
		AlarmName: "direct-composite",
		AlarmRule: `ALARM("child-direct")`,
	}))

	// Composite should be ALARM since child is ALARM.
	_, composites0, err0 := b.DescribeAlarms([]string{"direct-composite"}, nil, "", "", 0)
	require.NoError(t, err0)
	require.Len(t, composites0.Data, 1)
	assert.Equal(t, "ALARM", composites0.Data[0].StateValue)

	// SetAlarmState on child to OK; composite should re-evaluate to OK.
	require.NoError(t, b.SetAlarmState("child-direct", "OK", "recovered"))

	_, composites, err := b.DescribeAlarms([]string{"direct-composite"}, nil, "", "", 0)
	require.NoError(t, err)
	require.Len(t, composites.Data, 1)
	assert.Equal(t, "OK", composites.Data[0].StateValue)
}

func TestCloudWatchBackend_SetAlarmState_OKAndInsufficientData(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	pub := &mockSNSPublisher{}
	b.SetSNSPublisher(pub)

	topicARN := "arn:aws:sns:us-east-1:123456789012:test-topic-2"

	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:               "state-cycle",
		StateValue:              "ALARM",
		ActionsEnabled:          true,
		OKActions:               []string{topicARN},
		InsufficientDataActions: []string{topicARN},
	}))

	// Transition to OK — should fire OKActions.
	require.NoError(t, b.SetAlarmState("state-cycle", "OK", "recovered"))
	assert.Len(t, pub.messages, 1)

	// Transition to INSUFFICIENT_DATA — should fire InsufficientDataActions.
	require.NoError(t, b.SetAlarmState("state-cycle", "INSUFFICIENT_DATA", "no data"))
	assert.Len(t, pub.messages, 2)
}

func TestCloudWatchBackend_EnableDisableAlarmActions_CompositeAlarm(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
		AlarmName:      "comp-enable-disable",
		AlarmRule:      `ALARM("x")`,
		ActionsEnabled: false,
	}))

	require.NoError(t, b.EnableAlarmActions([]string{"comp-enable-disable"}))

	_, composites, err := b.DescribeAlarms([]string{"comp-enable-disable"}, nil, "", "", 0)
	require.NoError(t, err)
	require.Len(t, composites.Data, 1)
	assert.True(t, composites.Data[0].ActionsEnabled)

	require.NoError(t, b.DisableAlarmActions([]string{"comp-enable-disable"}))

	_, composites2, err2 := b.DescribeAlarms([]string{"comp-enable-disable"}, nil, "", "", 0)
	require.NoError(t, err2)
	require.Len(t, composites2.Data, 1)
	assert.False(t, composites2.Data[0].ActionsEnabled)
}

func TestCloudWatchBackend_Dashboards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, b *cloudwatch.InMemoryBackend)
		name         string
		wantNames    []string
		wantNotNames []string
		wantErr      bool
	}{
		{
			name: "PutDashboard/creates",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutDashboard("MyDash", `{"widgets":[]}`))
			},
			wantNames: []string{"MyDash"},
		},
		{
			name: "PutDashboard/updates existing",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutDashboard("UpdateDash", `{"widgets":[]}`))
				require.NoError(t, b.PutDashboard("UpdateDash", `{"widgets":[{"type":"text"}]}`))
			},
			wantNames: []string{"UpdateDash"},
		},
		{
			name: "PutDashboard/missing name returns error",
			setup: func(t *testing.T, _ *cloudwatch.InMemoryBackend) {
				t.Helper()
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

			if tt.name == "PutDashboard/missing name returns error" {
				err := b.PutDashboard("", `{}`)
				require.Error(t, err)

				return
			}

			tt.setup(t, b)

			page, err := b.ListDashboards("", "")
			require.NoError(t, err)

			names := make([]string, 0, len(page.Data))
			for _, e := range page.Data {
				names = append(names, e.DashboardName)
			}

			for _, wn := range tt.wantNames {
				assert.Contains(t, names, wn)
			}
			for _, wn := range tt.wantNotNames {
				assert.NotContains(t, names, wn)
			}
		})
	}
}

func TestCloudWatchBackend_GetDashboard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		dashName  string
		body      string
		fetchName string
		wantErr   bool
	}{
		{
			name:      "found",
			dashName:  "MyDash",
			body:      `{"widgets":[]}`,
			fetchName: "MyDash",
		},
		{
			name:      "not found",
			dashName:  "MyDash",
			body:      `{"widgets":[]}`,
			fetchName: "OtherDash",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			require.NoError(t, b.PutDashboard(tt.dashName, tt.body))

			entry, body, err := b.GetDashboard(tt.fetchName)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.fetchName, entry.DashboardName)
			assert.Equal(t, tt.body, body)
			assert.NotEmpty(t, entry.DashboardArn)
			assert.Equal(t, int64(len(tt.body)), entry.Size)
		})
	}
}

func TestCloudWatchBackend_ListDashboards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		prefix    string
		dashNames []string
		wantCount int
	}{
		{
			name:      "list all",
			dashNames: []string{"alpha", "beta", "gamma"},
			wantCount: 3,
		},
		{
			name:      "filter by prefix",
			dashNames: []string{"prod-web", "prod-api", "staging-web"},
			prefix:    "prod-",
			wantCount: 2,
		},
		{
			name:      "no match prefix",
			dashNames: []string{"alpha", "beta"},
			prefix:    "xyz-",
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

			for _, n := range tt.dashNames {
				require.NoError(t, b.PutDashboard(n, `{}`))
			}

			page, err := b.ListDashboards(tt.prefix, "")
			require.NoError(t, err)
			assert.Len(t, page.Data, tt.wantCount)
		})
	}
}

func TestCloudWatchBackend_DeleteDashboards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		create     []string
		deleteName []string
		wantCount  int
	}{
		{
			name:       "delete existing",
			create:     []string{"dash1", "dash2", "dash3"},
			deleteName: []string{"dash1", "dash3"},
			wantCount:  1,
		},
		{
			name:       "delete non-existent is no-op",
			create:     []string{"dash1"},
			deleteName: []string{"nonexistent"},
			wantCount:  1,
		},
		{
			name:       "delete all",
			create:     []string{"a", "b"},
			deleteName: []string{"a", "b"},
			wantCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

			for _, n := range tt.create {
				require.NoError(t, b.PutDashboard(n, `{}`))
			}

			require.NoError(t, b.DeleteDashboards(tt.deleteName))

			page, err := b.ListDashboards("", "")
			require.NoError(t, err)
			assert.Len(t, page.Data, tt.wantCount)
		})
	}
}
