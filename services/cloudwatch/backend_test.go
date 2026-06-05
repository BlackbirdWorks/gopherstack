package cloudwatch_test

import (
	"context"
	"fmt"
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
		{
			MetricName: "Requests",
			Value:      42,
			Count:      1,
			Sum:        42,
			Min:        42,
			Max:        42,
			Timestamp:  time.Now(),
		},
	}
	_, err := b.PutMetricData("AWS/EC2", data)
	require.NoError(t, err)
}

func TestCloudWatchBackend_PutMetricData_Multiple(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	data := []cloudwatch.MetricDatum{
		{MetricName: "CPU", Value: 10, Count: 1, Sum: 10, Min: 10, Max: 10, Timestamp: time.Now()},
		{MetricName: "CPU", Value: 20, Count: 1, Sum: 20, Min: 20, Max: 20, Timestamp: time.Now()},
	}
	_, err := b.PutMetricData("AWS/EC2", data)
	require.NoError(t, err)
	metrics, err := b.ListMetrics("AWS/EC2", "CPU", nil, "", 0)
	require.NoError(t, err)
	assert.Len(t, metrics.Data, 1)
}

func TestCloudWatchBackend_ListMetrics(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	_, _ = b.PutMetricData("NS1", []cloudwatch.MetricDatum{
		{MetricName: "M1", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1, Timestamp: time.Now()},
	})
	_, _ = b.PutMetricData("NS2", []cloudwatch.MetricDatum{
		{MetricName: "M2", Value: 2, Count: 1, Sum: 2, Min: 2, Max: 2, Timestamp: time.Now()},
	})

	all, err := b.ListMetrics("", "", nil, "", 0)
	require.NoError(t, err)
	assert.Len(t, all.Data, 2)

	ns1, err := b.ListMetrics("NS1", "", nil, "", 0)
	require.NoError(t, err)
	assert.Len(t, ns1.Data, 1)
	assert.Equal(t, "M1", ns1.Data[0].MetricName)

	byName, err := b.ListMetrics("", "M2", nil, "", 0)
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
					{
						MetricName: "CPU",
						Value:      10,
						Count:      1,
						Sum:        10,
						Min:        10,
						Max:        10,
						Timestamp:  now,
					},
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
				_, err := b.PutMetricData("AWS/EC2", data)
				require.NoError(t, err)
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
					{
						MetricName: "CPU",
						Value:      10,
						Count:      1,
						Sum:        10,
						Min:        10,
						Max:        10,
						Timestamp:  old,
					},
				}
				_, err := b.PutMetricData("AWS/EC2", data)
				require.NoError(t, err)
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

			dps, err := b.GetMetricStatistics(
				tt.namespace,
				tt.metricName,
				nil,
				tt.start,
				tt.end,
				tt.period,
				tt.statistics,
				nil,
			)
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
				require.NoError(
					t,
					b.PutMetricAlarm(
						&cloudwatch.MetricAlarm{AlarmName: "child", StateValue: "ALARM"},
					),
				)
			},
			alarm: &cloudwatch.CompositeAlarm{
				AlarmName: "composite",
				AlarmRule: `ALARM("child")`,
			},
			wantState: "ALARM",
		},
		{
			name: "alarm_in_ok_state",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				require.NoError(
					t,
					b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "child", StateValue: "OK"}),
				)
			},
			alarm: &cloudwatch.CompositeAlarm{
				AlarmName: "composite",
				AlarmRule: `ALARM("child")`,
			},
			wantState: "OK",
		},
		{
			name: "and_rule",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				require.NoError(
					t,
					b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "a", StateValue: "ALARM"}),
				)
				require.NoError(
					t,
					b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "b", StateValue: "ALARM"}),
				)
			},
			alarm: &cloudwatch.CompositeAlarm{
				AlarmName: "composite",
				AlarmRule: `ALARM("a") AND ALARM("b")`,
			},
			wantState: "ALARM",
		},
		{
			name: "or_rule_one_ok",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				require.NoError(
					t,
					b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "a", StateValue: "ALARM"}),
				)
				require.NoError(
					t,
					b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "b", StateValue: "OK"}),
				)
			},
			alarm: &cloudwatch.CompositeAlarm{
				AlarmName: "composite",
				AlarmRule: `ALARM("a") OR ALARM("b")`,
			},
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
				require.NoError(
					t,
					b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "test-alarm"}),
				)
			},
			alarmName:   "test-alarm",
			stateValue:  "ALARM",
			stateReason: "Test triggered",
		},
		{
			name: "composite_alarm_state_change",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				require.NoError(
					t,
					b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "child", StateValue: "OK"}),
				)
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

			err := b.SetAlarmState(t.Context(), tt.alarmName, tt.stateValue, tt.stateReason, "")
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

func TestCloudWatchBackend_DescribeAlarmHistory(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	require.NoError(
		t,
		b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "hist-alarm", ActionsEnabled: true}),
	)
	require.NoError(t, b.SetAlarmState(t.Context(), "hist-alarm", "ALARM", "test trigger", ""))

	p, err := b.DescribeAlarmHistory("hist-alarm", "", "", "", time.Time{}, time.Time{}, 0)
	require.NoError(t, err)
	assert.NotEmpty(t, p.Data)
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

func TestCloudWatchBackend_CompositeAlarmReevalOnChildChange(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	require.NoError(
		t,
		b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "child", StateValue: "OK"}),
	)
	require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
		AlarmName: "parent", AlarmRule: `ALARM("child")`,
	}))

	// Initially composite should be OK since child is OK
	_, compositeAlarms, err := b.DescribeAlarms([]string{"parent"}, nil, "", "", "", 0)
	require.NoError(t, err)
	assert.Equal(t, "OK", compositeAlarms.Data[0].StateValue)

	// Change child to ALARM; composite should re-evaluate
	require.NoError(t, b.SetAlarmState(t.Context(), "child", "ALARM", "test", ""))
	_, compositeAlarms2, err2 := b.DescribeAlarms([]string{"parent"}, nil, "", "", "", 0)
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

	require.NoError(
		t,
		b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "child2", StateValue: "OK"}),
	)
	require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
		AlarmName:      "parent2",
		AlarmRule:      `ALARM("child2")`,
		ActionsEnabled: true,
		AlarmActions:   []string{topicARN},
	}))

	// No SNS publish yet — child is OK, composite is OK.
	assert.Empty(t, pub.messages)

	// Transition child to ALARM; composite should re-evaluate and fire its AlarmActions.
	require.NoError(t, b.SetAlarmState(t.Context(), "child2", "ALARM", "test trigger", ""))

	assert.Len(t, pub.messages, 1, "composite alarm action should have been fired")
	assert.Contains(t, pub.messages[0], "parent2")
}

// mockLambdaInvoker captures lambda invocations for assertions.
type mockLambdaInvoker struct {
	invocations []string
}

func (m *mockLambdaInvoker) InvokeFunction(
	_ context.Context,
	name string,
	_ string,
	_ []byte,
) ([]byte, int, error) {
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

	require.NoError(t, b.SetAlarmState(t.Context(), "lambda-alarm", "ALARM", "test", ""))

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
	require.NoError(t, b.SetAlarmState(t.Context(), "no-invoker-alarm", "ALARM", "test", ""))
}

func TestCloudWatchBackend_EvalCompositeRule_NestedComposite(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	require.NoError(
		t,
		b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "leaf", StateValue: "ALARM"}),
	)
	require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
		AlarmName: "mid",
		AlarmRule: `ALARM("leaf")`,
	}))
	require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
		AlarmName: "top",
		AlarmRule: `ALARM("mid")`,
	}))

	_, composites, err := b.DescribeAlarms([]string{"top"}, nil, "", "", "", 0)
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
	p, err := b.DescribeAlarmsForMetric("NS", "M", nil, []string{"match-name"}, "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "match-name", p.Data[0].AlarmName)
}

func TestCloudWatchBackend_DescribeAlarmHistory_TypeFilter(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	require.NoError(
		t,
		b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "type-filter", StateValue: "OK"}),
	)
	require.NoError(t, b.SetAlarmState(t.Context(), "type-filter", "ALARM", "transition", ""))

	// Filter by StateUpdate type — should find the state transition.
	p, err := b.DescribeAlarmHistory(
		"type-filter",
		"",
		"StateUpdate",
		"",
		time.Time{},
		time.Time{},
		0,
	)
	require.NoError(t, err)
	assert.NotEmpty(t, p.Data)
	for _, item := range p.Data {
		assert.Equal(t, "StateUpdate", item.HistoryItemType)
	}

	// PutMetricAlarm creates ConfigurationUpdate items.
	p2, err2 := b.DescribeAlarmHistory(
		"type-filter",
		"",
		"ConfigurationUpdate",
		"",
		time.Time{},
		time.Time{},
		0,
	)
	require.NoError(t, err2)
	assert.NotEmpty(t, p2.Data)
	for _, item := range p2.Data {
		assert.Equal(t, "ConfigurationUpdate", item.HistoryItemType)
	}
}

func TestCloudWatchBackend_PutCompositeAlarm_UpdateExisting(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	require.NoError(
		t,
		b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "child-update", StateValue: "ALARM"}),
	)

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

	_, composites, err := b.DescribeAlarms([]string{"comp-update"}, nil, "", "", "", 0)
	require.NoError(t, err)
	require.Len(t, composites.Data, 1)
	assert.Equal(t, "updated", composites.Data[0].AlarmDescription)
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

func TestCloudWatchBackend_SetAlarmState_ChildTriggersCompositeReevaluation(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	// Child alarm starts in ALARM state, composite rule evaluates to ALARM.
	require.NoError(
		t,
		b.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "child-direct", StateValue: "ALARM"}),
	)
	require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
		AlarmName: "direct-composite",
		AlarmRule: `ALARM("child-direct")`,
	}))

	// Composite should be ALARM since child is ALARM.
	_, composites0, err0 := b.DescribeAlarms([]string{"direct-composite"}, nil, "", "", "", 0)
	require.NoError(t, err0)
	require.Len(t, composites0.Data, 1)
	assert.Equal(t, "ALARM", composites0.Data[0].StateValue)

	// SetAlarmState on child to OK; composite should re-evaluate to OK.
	require.NoError(t, b.SetAlarmState(t.Context(), "child-direct", "OK", "recovered", ""))

	_, composites, err := b.DescribeAlarms([]string{"direct-composite"}, nil, "", "", "", 0)
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
	require.NoError(t, b.SetAlarmState(t.Context(), "state-cycle", "OK", "recovered", ""))
	assert.Len(t, pub.messages, 1)

	// Transition to INSUFFICIENT_DATA — should fire InsufficientDataActions.
	require.NoError(t, b.SetAlarmState(t.Context(), "state-cycle", "INSUFFICIENT_DATA", "no data", ""))
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

	_, composites, err := b.DescribeAlarms([]string{"comp-enable-disable"}, nil, "", "", "", 0)
	require.NoError(t, err)
	require.Len(t, composites.Data, 1)
	assert.True(t, composites.Data[0].ActionsEnabled)

	require.NoError(t, b.DisableAlarmActions([]string{"comp-enable-disable"}))

	_, composites2, err2 := b.DescribeAlarms([]string{"comp-enable-disable"}, nil, "", "", "", 0)
	require.NoError(t, err2)
	require.Len(t, composites2.Data, 1)
	assert.False(t, composites2.Data[0].ActionsEnabled)
}

func TestCloudWatchBackend_Dashboards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, b *cloudwatch.InMemoryBackend)
		name         string
		putName      string
		putBody      string
		wantNames    []string
		wantNotNames []string
		wantPutErr   bool
	}{
		{
			name:      "PutDashboard/creates",
			putName:   "MyDash",
			putBody:   `{"widgets":[]}`,
			wantNames: []string{"MyDash"},
		},
		{
			name: "PutDashboard/updates existing",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.PutDashboard("UpdateDash", `{"widgets":[]}`))
			},
			putName:   "UpdateDash",
			putBody:   `{"widgets":[{"type":"text"}]}`,
			wantNames: []string{"UpdateDash"},
		},
		{
			name:       "PutDashboard/missing name returns error",
			putName:    "",
			putBody:    `{}`,
			wantPutErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

			if tt.setup != nil {
				tt.setup(t, b)
			}

			err := b.PutDashboard(tt.putName, tt.putBody)

			if tt.wantPutErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			page, listErr := b.ListDashboards("", "")
			require.NoError(t, listErr)

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

func TestCloudWatchBackend_MetricDataCap(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	// Insert more than cwMaxMetricDataPoints data points.
	const total = 1100
	for range total {
		_, err := b.PutMetricData("AWS/EC2", []cloudwatch.MetricDatum{
			{
				MetricName: "CPUUtilization",
				Value:      42.0,
				Unit:       "Percent",
				Timestamp:  time.Now(),
			},
		})
		require.NoError(t, err)
	}

	// At least one metric entry should still exist after capping.
	page, err := b.ListMetrics("AWS/EC2", "CPUUtilization", nil, "", 0)
	require.NoError(t, err)
	assert.NotEmpty(t, page.Data)
}

func TestCloudWatchBackend_AlarmHistoryCap(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:          "cap-alarm",
		ComparisonOperator: "GreaterThanThreshold",
		EvaluationPeriods:  1,
		Threshold:          50,
		Namespace:          "AWS/EC2",
		MetricName:         "CPUUtilization",
		Period:             60,
		Statistic:          "Average",
		ActionsEnabled:     false,
	}))

	// Toggle state more than 100 times to exceed the history cap.
	for i := range 110 {
		state := "OK"
		if i%2 == 0 {
			state = "ALARM"
		}

		require.NoError(t, b.SetAlarmState(t.Context(), "cap-alarm", state, "test reason", ""))
	}

	// History should be capped at 100 entries.
	page, err := b.DescribeAlarmHistory("cap-alarm", "", "", "", time.Time{}, time.Time{}, 0)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(page.Data), 100)
}

// mockCancelledLambdaInvoker blocks until the context is cancelled, then returns an error.
type mockCancelledLambdaInvoker struct {
	called chan struct{}
}

func (m *mockCancelledLambdaInvoker) InvokeFunction(
	ctx context.Context,
	_ string,
	_ string,
	_ []byte,
) ([]byte, int, error) {
	close(m.called)
	<-ctx.Done()

	return nil, 0, ctx.Err()
}

func TestCloudWatchBackend_ExecuteActions_ContextPropagated(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	inv := &mockCancelledLambdaInvoker{called: make(chan struct{})}
	b.SetLambdaInvoker(inv)

	lambdaARN := "arn:aws:lambda:us-east-1:123456789012:function:ctx-fn"

	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:      "ctx-alarm",
		StateValue:     "OK",
		ActionsEnabled: true,
		AlarmActions:   []string{lambdaARN},
	}))

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan error, 1)
	go func() {
		done <- b.SetAlarmState(ctx, "ctx-alarm", "ALARM", "test", "")
	}()

	// Wait for the invoker to be called, then cancel the context.
	<-inv.called
	cancel()

	err := <-done
	require.NoError(t, err, "SetAlarmState itself should not propagate Lambda delivery errors")
}

func TestCloudWatchBackend_CompositeAlarm_CircularDependency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(t *testing.T, b *cloudwatch.InMemoryBackend)
		alarmName string
		wantState string
	}{
		{
			name: "self_reference",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				// Composite alarm referencing itself: A -> ALARM("A")
				require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
					AlarmName: "self-ref",
					AlarmRule: `ALARM("self-ref")`,
				}))
			},
			alarmName: "self-ref",
			// Self-reference: the alarm doesn't exist when first evaluated, so rule
			// evaluates to OK. The cycle guard prevents infinite recursion on reevaluation.
			wantState: "OK",
		},
		{
			name: "mutual_dependency",
			setup: func(t *testing.T, b *cloudwatch.InMemoryBackend) {
				t.Helper()
				// A -> ALARM("B"), B -> ALARM("A")
				require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
					AlarmName: "cycle-a",
					AlarmRule: `ALARM("cycle-b")`,
				}))
				require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
					AlarmName: "cycle-b",
					AlarmRule: `ALARM("cycle-a")`,
				}))
			},
			alarmName: "cycle-a",
			// Mutual dependency: cycle-b doesn't exist when cycle-a is first evaluated,
			// so the rule evaluates to OK. The cycle guard prevents infinite recursion.
			wantState: "OK",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			tt.setup(t, b)

			_, composites, err := b.DescribeAlarms([]string{tt.alarmName}, nil, "", "", "", 0)
			require.NoError(t, err)
			require.Len(t, composites.Data, 1)
			assert.Equal(t, tt.wantState, composites.Data[0].StateValue)
		})
	}
}

func TestCloudWatchBackend_CompositeAlarm_CircularDependency_ReevaluationNoPanic(t *testing.T) {
	t.Parallel()

	// Verify that reevaluateCompositeAlarms triggered by SetAlarmState does not
	// panic or deadlock when composite alarms contain circular references.
	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:  "trigger",
		StateValue: "OK",
	}))
	require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
		AlarmName: "cycle-x",
		AlarmRule: `ALARM("cycle-y")`,
	}))
	require.NoError(t, b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
		AlarmName: "cycle-y",
		AlarmRule: `ALARM("cycle-x")`,
	}))

	// SetAlarmState triggers reevaluateCompositeAlarms; must not hang.
	require.NoError(t, b.SetAlarmState(t.Context(), "trigger", "ALARM", "test", ""))
}

func TestCloudWatchBackend_PutMetricData_NamespaceCapEnforced(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()

	// Fill the namespace to the cap by putting one data point per unique metric name.
	for i := range cloudwatch.CwMaxMetricNamesPerNamespace {
		name := fmt.Sprintf("Metric%d", i)
		datum := cloudwatch.MetricDatum{
			MetricName: name,
			Value:      1,
			Count:      1,
			Sum:        1,
			Min:        1,
			Max:        1,
			Timestamp:  time.Now(),
		}
		_, err := b.PutMetricData("NS/Cap", []cloudwatch.MetricDatum{datum})
		require.NoError(t, err)
	}

	// Attempt to add one more unique metric; it should be silently dropped.
	extra := cloudwatch.MetricDatum{
		MetricName: "ExtraMetric",
		Value:      1,
		Count:      1,
		Sum:        1,
		Min:        1,
		Max:        1,
		Timestamp:  time.Now(),
	}
	_, err := b.PutMetricData("NS/Cap", []cloudwatch.MetricDatum{extra})
	require.NoError(t, err)

	metrics, err2 := b.ListMetrics("NS/Cap", "", nil, "", 0)
	require.NoError(t, err2)
	assert.LessOrEqual(t, len(metrics.Data), cloudwatch.CwMaxMetricNamesPerNamespace,
		"namespace metric count should not exceed the cap")
	assert.Len(t, metrics.Data, cloudwatch.CwMaxMetricNamesPerNamespace,
		"exactly cap metrics should be present")
}

func TestCloudWatchBackend_SweepExpiredMetrics(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()

	// Use a timestamp outside the retention window by a safe margin.
	oldTimestamp := time.Now().UTC().AddDate(0, 0, -(cloudwatch.CwMetricRetentionDays + 5))
	recentTimestamp := time.Now().UTC()

	oldDatum := cloudwatch.MetricDatum{
		MetricName: "OldMetric",
		Value:      1, Count: 1, Sum: 1, Min: 1, Max: 1,
		Timestamp: oldTimestamp,
	}
	recentDatum := cloudwatch.MetricDatum{
		MetricName: "RecentMetric",
		Value:      2, Count: 1, Sum: 2, Min: 2, Max: 2,
		Timestamp: recentTimestamp,
	}

	_, err := b.PutMetricData("NS/Sweep", []cloudwatch.MetricDatum{oldDatum, recentDatum})
	require.NoError(t, err)

	b.SweepExpiredMetrics()

	// OldMetric should be evicted; RecentMetric should remain.
	all, err := b.ListMetrics("NS/Sweep", "", nil, "", 0)
	require.NoError(t, err)

	names := make(map[string]bool, len(all.Data))
	for _, m := range all.Data {
		names[m.MetricName] = true
	}

	assert.False(t, names["OldMetric"], "expired metric should have been swept")
	assert.True(t, names["RecentMetric"], "recent metric should remain after sweep")
}

func TestCloudWatchBackend_SweepExpiredMetrics_OutOfOrder(t *testing.T) {
	t.Parallel()

	// Verify that SweepExpiredMetrics correctly handles out-of-order data points
	// (i.e. it uses a linear filter, not binary search).
	b := cloudwatch.NewInMemoryBackend()

	old := time.Now().UTC().AddDate(0, 0, -(cloudwatch.CwMetricRetentionDays + 5))
	recent := time.Now().UTC()

	// Intentionally store points out of order: recent first, then old.
	pts := []cloudwatch.MetricDatum{
		{MetricName: "Mixed", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1, Timestamp: recent},
		{MetricName: "Mixed", Value: 2, Count: 1, Sum: 2, Min: 2, Max: 2, Timestamp: old},
	}
	_, err := b.PutMetricData("NS/OutOfOrder", pts)
	require.NoError(t, err)

	b.SweepExpiredMetrics()

	// The metric still has the recent point; old point should be gone.
	stats, err := b.GetMetricStatistics(
		"NS/OutOfOrder", "Mixed",
		nil,
		recent.Add(-time.Minute), recent.Add(time.Minute),
		60, []string{"Sum"}, nil,
	)
	require.NoError(t, err)
	require.Len(t, stats, 1)
	assert.NotNil(t, stats[0].Sum)
	assert.InDelta(t, 1.0, *stats[0].Sum, 1e-9, "only the recent data point should remain")
}

func TestCloudWatchBackend_PutAnomalyDetector(t *testing.T) {
	t.Parallel()

	tests := []struct {
		det     *cloudwatch.AnomalyDetector
		name    string
		wantErr bool
	}{
		{
			name: "valid",
			det: &cloudwatch.AnomalyDetector{
				Namespace:  "AWS/EC2",
				MetricName: "CPUUtilization",
				Stat:       "Average",
			},
		},
		{
			name:    "missing_namespace",
			det:     &cloudwatch.AnomalyDetector{MetricName: "CPUUtilization"},
			wantErr: true,
		},
		{
			name:    "missing_metric_name",
			det:     &cloudwatch.AnomalyDetector{Namespace: "AWS/EC2"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackend()
			err := b.PutAnomalyDetector(tt.det)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			p, err2 := b.DescribeAnomalyDetectors(tt.det.Namespace, tt.det.MetricName, "", 0)
			require.NoError(t, err2)
			require.Len(t, p.Data, 1)
			assert.Equal(t, tt.det.Namespace, p.Data[0].Namespace)
			assert.Equal(t, tt.det.MetricName, p.Data[0].MetricName)
		})
	}
}

func TestCloudWatchBackend_ListMetricStreams(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	b.PutMetricStreamInternal(&cloudwatch.MetricStream{
		Name:         "stream-a",
		FirehoseArn:  "arn:aws:firehose:us-east-1:123456789012:deliverystream/a",
		OutputFormat: "json",
		State:        "running",
	})
	b.PutMetricStreamInternal(&cloudwatch.MetricStream{
		Name:         "stream-b",
		FirehoseArn:  "arn:aws:firehose:us-east-1:123456789012:deliverystream/b",
		OutputFormat: "opentelemetry0.7",
		State:        "running",
	})

	p, err := b.ListMetricStreams("", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 2)
	assert.Equal(t, "stream-a", p.Data[0].Name)
	assert.Equal(t, "stream-b", p.Data[1].Name)
}

func TestCloudWatchBackend_PutMetricFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		filter  *cloudwatch.MetricFilter
		name    string
		wantErr bool
	}{
		{
			name: "valid",
			filter: &cloudwatch.MetricFilter{
				FilterName:    "my-filter",
				LogGroupName:  "/aws/lambda/fn",
				FilterPattern: "[host, ident, authuser, date, request, status]",
				MetricTransformations: []cloudwatch.MetricTransformation{
					{MetricName: "ReqCount", MetricNamespace: "MyApp", MetricValue: "1"},
				},
			},
		},
		{
			name:    "missing_filter_name",
			filter:  &cloudwatch.MetricFilter{LogGroupName: "/aws/lambda/fn"},
			wantErr: true,
		},
		{
			name:    "missing_log_group",
			filter:  &cloudwatch.MetricFilter{FilterName: "my-filter"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackend()
			err := b.PutMetricFilter(tt.filter)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestCloudWatchBackend_DescribeMetricFilters(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()

	filters := []cloudwatch.MetricFilter{
		{FilterName: "alpha", LogGroupName: "/aws/lambda/fn1", FilterPattern: "[a]"},
		{FilterName: "beta", LogGroupName: "/aws/lambda/fn1", FilterPattern: "[b]"},
		{FilterName: "gamma", LogGroupName: "/aws/ec2", FilterPattern: "[c]"},
	}
	for i := range filters {
		require.NoError(t, b.PutMetricFilter(&filters[i]))
	}

	tests := []struct {
		name             string
		filterNamePrefix string
		logGroupName     string
		wantCount        int
	}{
		{name: "all", wantCount: 3},
		{name: "by_log_group", logGroupName: "/aws/lambda/fn1", wantCount: 2},
		{name: "by_prefix", filterNamePrefix: "al", wantCount: 1},
		{
			name:             "prefix_and_group",
			filterNamePrefix: "b",
			logGroupName:     "/aws/lambda/fn1",
			wantCount:        1,
		},
		{name: "no_match", logGroupName: "/aws/nonexistent", wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p, err := b.DescribeMetricFilters(tt.filterNamePrefix, tt.logGroupName, "", 0)
			require.NoError(t, err)
			assert.Len(t, p.Data, tt.wantCount)
		})
	}
}

func TestCloudWatchBackend_DeleteMetricFilter(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	require.NoError(t, b.PutMetricFilter(&cloudwatch.MetricFilter{
		FilterName:   "del-filter",
		LogGroupName: "/aws/lambda/fn",
	}))

	// delete should succeed
	require.NoError(t, b.DeleteMetricFilter("del-filter", "/aws/lambda/fn"))

	// second delete should fail
	require.ErrorIs(
		t,
		b.DeleteMetricFilter("del-filter", "/aws/lambda/fn"),
		cloudwatch.ErrMetricFilterNotFound,
	)
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

func TestCloudWatchBackend_GetDashboardARNs(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	require.NoError(t, b.PutDashboard("my-dash", `{"widgets":[]}`))

	arns := b.GetDashboardARNs([]string{"my-dash", "nonexistent"})
	require.Len(t, arns, 1)
	assert.Contains(t, arns[0], "my-dash")
}

// ---------------------------------------------------------------------------
// Accuracy audit tests — gaps from issue #1686
// ---------------------------------------------------------------------------

func TestCloudWatchBackend_DimensionAwareStorage(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	now := time.Now().UTC()

	dims1 := []cloudwatch.Dimension{{Name: "Host", Value: "host-1"}}
	dims2 := []cloudwatch.Dimension{{Name: "Host", Value: "host-2"}}

	_, err := b.PutMetricData("AWS/EC2", []cloudwatch.MetricDatum{
		{
			MetricName: "CPUUtilization", Value: 10, Count: 1, Sum: 10, Min: 10, Max: 10,
			Timestamp: now, Dimensions: dims1,
		},
		{
			MetricName: "CPUUtilization", Value: 90, Count: 1, Sum: 90, Min: 90, Max: 90,
			Timestamp: now, Dimensions: dims2,
		},
	})
	require.NoError(t, err)

	// ListMetrics should return two separate series, not one.
	all, err := b.ListMetrics("AWS/EC2", "CPUUtilization", nil, "", 0)
	require.NoError(t, err)
	assert.Len(t, all.Data, 2, "each dimension set is a distinct metric series")

	// Filter by dims1 should return exactly one series.
	filtered, err := b.ListMetrics("AWS/EC2", "CPUUtilization", dims1, "", 0)
	require.NoError(t, err)
	require.Len(t, filtered.Data, 1)
	assert.Equal(t, "host-1", filtered.Data[0].Dimensions[0].Value)
}

func TestCloudWatchBackend_DimensionAwareGetMetricStatistics(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	start := time.Now().UTC().Add(-2 * time.Minute)
	mid := time.Now().UTC().Add(-time.Minute)

	dimsA := []cloudwatch.Dimension{{Name: "Service", Value: "A"}}
	dimsB := []cloudwatch.Dimension{{Name: "Service", Value: "B"}}

	_, err := b.PutMetricData("App/Metrics", []cloudwatch.MetricDatum{
		{MetricName: "Latency", Value: 100, Count: 1, Sum: 100, Min: 100, Max: 100, Timestamp: mid, Dimensions: dimsA},
		{MetricName: "Latency", Value: 200, Count: 1, Sum: 200, Min: 200, Max: 200, Timestamp: mid, Dimensions: dimsB},
	})
	require.NoError(t, err)

	dpA, err := b.GetMetricStatistics(
		"App/Metrics", "Latency", dimsA, start, time.Now().UTC(), 60, []string{"Average"}, nil,
	)
	require.NoError(t, err)
	require.Len(t, dpA, 1)
	assert.InDelta(t, 100.0, *dpA[0].Average, 1e-9, "should return series A data only")

	dpB, err := b.GetMetricStatistics(
		"App/Metrics", "Latency", dimsB, start, time.Now().UTC(), 60, []string{"Average"}, nil,
	)
	require.NoError(t, err)
	require.Len(t, dpB, 1)
	assert.InDelta(t, 200.0, *dpB[0].Average, 1e-9, "should return series B data only")
}

func TestCloudWatchBackend_DimensionAwareGetMetricData(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	mid := time.Now().UTC().Add(-time.Minute)

	dimsX := []cloudwatch.Dimension{{Name: "Shard", Value: "x"}}

	_, err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "Errors", Value: 5, Count: 1, Sum: 5, Min: 5, Max: 5, Timestamp: mid, Dimensions: dimsX},
		{MetricName: "Errors", Value: 50, Count: 1, Sum: 50, Min: 50, Max: 50, Timestamp: mid},
	})
	require.NoError(t, err)

	queries := []cloudwatch.MetricDataQuery{
		{
			ID:         "m1",
			ReturnData: true,
			MetricStat: cloudwatch.MetricStat{
				Namespace: "NS", MetricName: "Errors",
				Stat: "Sum", Period: 60,
				Dimensions: dimsX,
			},
		},
	}

	results, err := b.GetMetricData(queries, time.Now().UTC().Add(-2*time.Minute), time.Now().UTC())
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Len(t, results[0].Values, 1)
	assert.InDelta(t, 5.0, results[0].Values[0], 1e-9, "should return only the shard-x series sum")
}

func TestCloudWatchBackend_StatisticSet(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	start := time.Now().UTC().Add(-2 * time.Minute)
	ts := time.Now().UTC().Add(-time.Minute)

	// Pre-aggregated StatisticSet datum.
	_, err := b.PutMetricData("App", []cloudwatch.MetricDatum{
		{
			MetricName: "RequestCount",
			Timestamp:  ts,
			Count:      10,
			Sum:        250,
			Min:        20,
			Max:        35,
		},
	})
	require.NoError(t, err)

	dps, err := b.GetMetricStatistics(
		"App", "RequestCount", nil,
		start, time.Now().UTC(), 60,
		[]string{"Sum", "SampleCount", "Minimum", "Maximum"}, nil,
	)
	require.NoError(t, err)
	require.Len(t, dps, 1)
	assert.InDelta(t, 250.0, *dps[0].Sum, 1e-9)
	assert.InDelta(t, 10.0, *dps[0].SampleCount, 1e-9)
	assert.InDelta(t, 20.0, *dps[0].Minimum, 1e-9)
	assert.InDelta(t, 35.0, *dps[0].Maximum, 1e-9)
}

func TestCloudWatchBackend_DimensionlessVsDimensioned(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	_, err := b.PutMetricData("MyNS", []cloudwatch.MetricDatum{
		{MetricName: "M", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1, Timestamp: ts},
		{MetricName: "M", Value: 2, Count: 1, Sum: 2, Min: 2, Max: 2, Timestamp: ts,
			Dimensions: []cloudwatch.Dimension{{Name: "D", Value: "v"}}},
	})
	require.NoError(t, err)

	// No-dim query should return only the dimensionless series.
	all, err := b.ListMetrics("MyNS", "M", nil, "", 0)
	require.NoError(t, err)
	assert.Len(t, all.Data, 2, "dimensionless and dimensioned are separate series")

	noDim, err := b.ListMetrics("MyNS", "M", []cloudwatch.Dimension{}, "", 0)
	require.NoError(t, err)
	assert.Len(t, noDim.Data, 2, "empty dimension filter matches all")
}

func TestCloudWatchBackend_ScanByDescending(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	now := time.Now().UTC()
	t1 := now.Add(-3 * time.Minute)
	t2 := now.Add(-2 * time.Minute)
	t3 := now.Add(-time.Minute)

	_, err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "Counter", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1, Timestamp: t1},
		{MetricName: "Counter", Value: 2, Count: 1, Sum: 2, Min: 2, Max: 2, Timestamp: t2},
		{MetricName: "Counter", Value: 3, Count: 1, Sum: 3, Min: 3, Max: 3, Timestamp: t3},
	})
	require.NoError(t, err)

	queries := []cloudwatch.MetricDataQuery{
		{
			ID:         "m1",
			ReturnData: true,
			MetricStat: cloudwatch.MetricStat{
				Namespace: "NS", MetricName: "Counter",
				Stat: "Sum", Period: 60,
			},
		},
	}

	asc, err := b.GetMetricDataWithOptions(queries, now.Add(-5*time.Minute), now, "TimestampAscending")
	require.NoError(t, err)
	require.Len(t, asc, 1)
	require.Len(t, asc[0].Values, 3)
	assert.True(t, asc[0].Timestamps[0].Before(asc[0].Timestamps[1]), "ascending order")

	desc, err := b.GetMetricDataWithOptions(queries, now.Add(-5*time.Minute), now, "TimestampDescending")
	require.NoError(t, err)
	require.Len(t, desc, 1)
	require.Len(t, desc[0].Values, 3)
	assert.True(t, desc[0].Timestamps[0].After(desc[0].Timestamps[1]), "descending order")
}

func TestCloudWatchBackend_ReturnDataFalse_SuppressesExpressionResult(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	_, err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "Val", Value: 7, Count: 1, Sum: 7, Min: 7, Max: 7, Timestamp: ts},
	})
	require.NoError(t, err)

	queries := []cloudwatch.MetricDataQuery{
		{
			ID:         "m1",
			ReturnData: true,
			MetricStat: cloudwatch.MetricStat{
				Namespace: "NS", MetricName: "Val", Stat: "Sum", Period: 60,
			},
		},
		{
			ID:         "e1",
			Expression: "m1 * 2",
			ReturnData: false,
		},
	}

	results, err := b.GetMetricDataWithOptions(queries, time.Now().UTC().Add(-2*time.Minute), time.Now().UTC(), "")
	require.NoError(t, err)

	ids := make([]string, 0, len(results))
	for _, r := range results {
		ids = append(ids, r.ID)
	}
	assert.Contains(t, ids, "m1", "m1 should be present")
	assert.NotContains(t, ids, "e1", "e1 ReturnData=false should be suppressed")
}

func TestCloudWatchBackend_UnprocessedMetricData_NamespaceCap(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	// Fill the namespace to the cap.
	for i := range cloudwatch.CwMaxMetricNamesPerNamespace {
		_, err := b.PutMetricData("NS/Full", []cloudwatch.MetricDatum{
			{
				MetricName: fmt.Sprintf("M%d", i),
				Value:      float64(i),
				Count:      1, Sum: float64(i), Min: float64(i), Max: float64(i),
				Timestamp: time.Now(),
			},
		})
		require.NoError(t, err)
	}

	// One more new metric should come back as unprocessed.
	unprocessed, err := b.PutMetricData("NS/Full", []cloudwatch.MetricDatum{
		{MetricName: "OverflowMetric", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1, Timestamp: time.Now()},
	})
	require.NoError(t, err)
	require.Len(t, unprocessed, 1)
	assert.Equal(t, "OverflowMetric", unprocessed[0].MetricName)
	assert.Equal(t, "LimitExceeded", unprocessed[0].ErrorCode)
}

func TestCloudWatchBackend_MetricStream_IncludeFilter(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	b.PutMetricStreamInternal(&cloudwatch.MetricStream{
		Name:        "stream-include",
		FirehoseArn: "arn:aws:firehose:us-east-1:123:deliverystream/s",
		State:       "RUNNING",
		IncludeFilters: []cloudwatch.MetricStreamFilter{
			{Namespace: "AWS/EC2", MetricNames: []string{"CPUUtilization"}},
		},
	})

	before, err := b.GetMetricStream("stream-include")
	require.NoError(t, err)

	ts := time.Now().UTC()
	// This metric matches the include filter.
	_, err = b.PutMetricData("AWS/EC2", []cloudwatch.MetricDatum{
		{MetricName: "CPUUtilization", Value: 50, Count: 1, Sum: 50, Min: 50, Max: 50, Timestamp: ts},
	})
	require.NoError(t, err)

	after, err := b.GetMetricStream("stream-include")
	require.NoError(t, err)
	assert.True(t, after.LastUpdateDate.After(before.LastUpdateDate),
		"matching metric should bump stream last-update date")

	// Record baseline after previous update.
	baseline := after.LastUpdateDate

	// Non-matching namespace should NOT change LastUpdateDate.
	b.PutMetricStreamInternal(&cloudwatch.MetricStream{
		Name:  "stream-include2",
		State: "RUNNING",
		IncludeFilters: []cloudwatch.MetricStreamFilter{
			{Namespace: "AWS/EC2"},
		},
	})
	beforeNonMatch, _ := b.GetMetricStream("stream-include2")
	_, err = b.PutMetricData("AWS/RDS", []cloudwatch.MetricDatum{
		{MetricName: "FreeStorageSpace", Value: 100, Count: 1, Sum: 100, Min: 100, Max: 100, Timestamp: ts},
	})
	require.NoError(t, err)
	afterNonMatch, err := b.GetMetricStream("stream-include2")
	require.NoError(t, err)
	assert.Equal(t, beforeNonMatch.LastUpdateDate, afterNonMatch.LastUpdateDate,
		"non-matching namespace should not update stream")
	_ = baseline
}

func TestCloudWatchBackend_MetricStream_ExcludeFilter(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	b.PutMetricStreamInternal(&cloudwatch.MetricStream{
		Name:  "stream-exclude",
		State: "RUNNING",
		ExcludeFilters: []cloudwatch.MetricStreamFilter{
			{Namespace: "AWS/EC2"},
		},
	})

	ts := time.Now().UTC()
	baseline, _ := b.GetMetricStream("stream-exclude")

	// Excluded namespace: should not update stream.
	_, err := b.PutMetricData("AWS/EC2", []cloudwatch.MetricDatum{
		{MetricName: "CPUUtilization", Value: 50, Count: 1, Sum: 50, Min: 50, Max: 50, Timestamp: ts},
	})
	require.NoError(t, err)
	afterExcluded, err := b.GetMetricStream("stream-exclude")
	require.NoError(t, err)
	assert.Equal(t, baseline.LastUpdateDate, afterExcluded.LastUpdateDate,
		"excluded namespace should not update stream")

	// Non-excluded namespace: should update.
	_, err = b.PutMetricData("AWS/RDS", []cloudwatch.MetricDatum{
		{MetricName: "FreeStorageSpace", Value: 99, Count: 1, Sum: 99, Min: 99, Max: 99, Timestamp: ts},
	})
	require.NoError(t, err)
	afterAllowed, err := b.GetMetricStream("stream-exclude")
	require.NoError(t, err)
	assert.True(t, afterAllowed.LastUpdateDate.After(baseline.LastUpdateDate),
		"non-excluded namespace should update stream")
}

func TestCloudWatchBackend_ListMetrics_DimensionFilter(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC()

	dimsA := []cloudwatch.Dimension{{Name: "Env", Value: "prod"}}
	dimsB := []cloudwatch.Dimension{{Name: "Env", Value: "staging"}}

	_, err := b.PutMetricData("Custom", []cloudwatch.MetricDatum{
		{MetricName: "RPM", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1, Timestamp: ts, Dimensions: dimsA},
		{MetricName: "RPM", Value: 2, Count: 1, Sum: 2, Min: 2, Max: 2, Timestamp: ts, Dimensions: dimsB},
		{MetricName: "RPM", Value: 3, Count: 1, Sum: 3, Min: 3, Max: 3, Timestamp: ts},
	})
	require.NoError(t, err)

	// Filter to prod only.
	prod, err := b.ListMetrics("Custom", "RPM", dimsA, "", 0)
	require.NoError(t, err)
	require.Len(t, prod.Data, 1)
	assert.Equal(t, "prod", prod.Data[0].Dimensions[0].Value)
}

func TestCloudWatchBackend_ListMetrics_ReturnsDimensions(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	dims := []cloudwatch.Dimension{
		{Name: "Region", Value: "us-east-1"},
		{Name: "Service", Value: "web"},
	}

	_, err := b.PutMetricData("Infra", []cloudwatch.MetricDatum{
		{MetricName: "Errors", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1,
			Timestamp: time.Now(), Dimensions: dims},
	})
	require.NoError(t, err)

	p, err := b.ListMetrics("Infra", "Errors", nil, "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Len(t, p.Data[0].Dimensions, 2, "dimensions should be returned in ListMetrics")
}

func TestCloudWatchBackend_StorageResolution_StoredOnDatum(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC()

	_, err := b.PutMetricData("App", []cloudwatch.MetricDatum{
		{MetricName: "Ticks", Value: 1, Count: 1, Sum: 1, Min: 1, Max: 1,
			Timestamp: ts, StorageResolution: 1},
	})
	require.NoError(t, err)

	// Metric should be stored and queryable.
	p, err := b.ListMetrics("App", "Ticks", nil, "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
}

func TestCloudWatchBackend_GetInsightRuleContributors(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	require.NoError(t, b.PutInsightRule(&cloudwatch.InsightRule{
		Name:       "rule-1",
		Definition: `{}`,
		Schema:     "CloudWatchLogRule",
	}))

	ts := time.Now().UTC().Add(-30 * time.Second)
	_, err := b.PutMetricData("App", []cloudwatch.MetricDatum{
		{MetricName: "Hits", Value: 10, Count: 10, Sum: 100, Min: 8, Max: 12, Timestamp: ts,
			Dimensions: []cloudwatch.Dimension{{Name: "Host", Value: "h1"}}},
		{MetricName: "Hits", Value: 5, Count: 5, Sum: 50, Min: 9, Max: 11, Timestamp: ts,
			Dimensions: []cloudwatch.Dimension{{Name: "Host", Value: "h2"}}},
	})
	require.NoError(t, err)

	contributors, err := b.GetInsightRuleContributorsForTest(
		"rule-1",
		time.Now().UTC().Add(-2*time.Minute),
		time.Now().UTC(),
		10,
		"Sum",
	)
	require.NoError(t, err)
	require.Len(t, contributors, 2, "should return contributors for each dimension set")
	// h1 has higher sum so should be first.
	assert.Equal(t, []string{"h1"}, contributors[0].Keys)
}

func TestCloudWatchBackend_DimensionOrderNormalized(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	ts := time.Now().UTC().Add(-30 * time.Second)

	// Store with dims in one order.
	dims1 := []cloudwatch.Dimension{{Name: "B", Value: "2"}, {Name: "A", Value: "1"}}
	// Query with dims in different order.
	dims2 := []cloudwatch.Dimension{{Name: "A", Value: "1"}, {Name: "B", Value: "2"}}

	_, err := b.PutMetricData("NS", []cloudwatch.MetricDatum{
		{MetricName: "M", Value: 42, Count: 1, Sum: 42, Min: 42, Max: 42,
			Timestamp: ts, Dimensions: dims1},
	})
	require.NoError(t, err)

	// Should find with reordered dims.
	dps, err := b.GetMetricStatistics("NS", "M", dims2,
		time.Now().UTC().Add(-time.Minute), time.Now().UTC(),
		60, []string{"Sum"}, nil)
	require.NoError(t, err)
	require.Len(t, dps, 1)
	assert.InDelta(t, 42.0, *dps[0].Sum, 1e-9, "dimension order should be normalized")
}
