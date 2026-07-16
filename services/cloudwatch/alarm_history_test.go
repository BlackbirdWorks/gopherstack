package cloudwatch_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// ---------------------------------------------------------------------------
// AlarmHistory: filtering
// ---------------------------------------------------------------------------

func TestBackend_DescribeAlarmHistory_FilterByType(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName: "a1", Namespace: "NS", MetricName: "M",
		ComparisonOperator: "GreaterThanThreshold", Threshold: 80, EvaluationPeriods: 1,
	}))
	require.NoError(t, b.SetAlarmState(t.Context(), "a1", "ALARM", "breach", ""))

	hist, err := b.DescribeAlarmHistory("a1", "", "StateUpdate", "", time.Time{}, time.Time{}, 0)
	require.NoError(t, err)
	for _, item := range hist.Data {
		assert.Equal(t, "StateUpdate", item.HistoryItemType)
	}
}

func TestBackend_DescribeAlarmHistory_AllAlarms(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	for _, name := range []string{"a1", "a2"} {
		require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
			AlarmName: name, Namespace: "NS", MetricName: "M",
			ComparisonOperator: "GreaterThanThreshold", Threshold: 80, EvaluationPeriods: 1,
		}))
	}

	hist, err := b.DescribeAlarmHistory("", "", "", "", time.Time{}, time.Time{}, 0)
	require.NoError(t, err)
	// Should have history for both alarms (creation events).
	alarmNames := make(map[string]bool)
	for _, item := range hist.Data {
		alarmNames[item.AlarmName] = true
	}
	assert.True(t, alarmNames["a1"])
	assert.True(t, alarmNames["a2"])
}

// ---------------------------------------------------------------------------
// Alarm history — action entries recorded on ALARM transition
// ---------------------------------------------------------------------------

func TestAlarmHistory_RecordsActionOnTransition(t *testing.T) {
	t.Parallel()

	const (
		ns     = "ActNS"
		metric = "CPU"
		alarm  = "act-alarm"
		action = "arn:aws:sns:us-east-1:123456789012:my-topic"
	)

	b := cloudwatch.NewInMemoryBackend()
	now := time.Now().UTC()

	putMetric(t, b, cloudwatch.MetricDatum{
		Namespace: ns, MetricName: metric,
		Timestamp: now.Add(-30 * time.Second), Value: 200.0,
	})

	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:          alarm,
		Namespace:          ns,
		MetricName:         metric,
		Statistic:          "Average",
		ComparisonOperator: "GreaterThanThreshold",
		EvaluationPeriods:  1,
		DatapointsToAlarm:  1,
		Period:             60,
		Threshold:          100.0,
		ActionsEnabled:     true,
		AlarmActions:       []string{action},
	}))

	b.EvaluateAlarms(context.Background(), now)

	page, err := b.DescribeAlarmHistory(alarm, "", "", "", time.Time{}, time.Time{}, 0)
	require.NoError(t, err)

	var hasAction bool

	for _, h := range page.Data {
		if h.HistoryItemType == "Action" {
			hasAction = true

			break
		}
	}

	assert.True(t, hasAction, "expected Action history entry after ALARM transition; got %+v", page.Data)
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
