package cloudwatch_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// stubSNS records SNS publish calls for assertion.
type stubSNS struct {
	records []snsRecord
	mu      sync.Mutex
}

type snsRecord struct {
	topicARN string
	message  string
}

func (s *stubSNS) PublishToTopic(topicARN, message string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, snsRecord{topicARN: topicARN, message: message})

	return nil
}

func (s *stubSNS) calls() []snsRecord {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]snsRecord(nil), s.records...)
}

// TestAlarmEvaluator_StateTransitions verifies that EvaluateAlarms transitions
// OK↔ALARM↔INSUFFICIENT_DATA based on metric data and alarm configuration.
func TestAlarmEvaluator_StateTransitions(t *testing.T) {
	t.Parallel()

	const (
		namespace  = "TestNS"
		metricName = "Requests"
		alarmName  = "high-requests"
		threshold  = 100.0
	)

	now := time.Now().UTC()

	tests := []struct {
		name              string
		operator          string
		statistic         string
		treatMissing      string
		initialState      string
		wantState         string
		points            []cloudwatch.MetricDatum
		period            int32
		evalPeriods       int32
		datapointsToAlarm int32
	}{
		{
			name:         "breaching_data_transitions_to_alarm",
			operator:     "GreaterThanThreshold",
			statistic:    "Average",
			period:       60,
			evalPeriods:  1,
			initialState: "INSUFFICIENT_DATA",
			wantState:    "ALARM",
			points: []cloudwatch.MetricDatum{
				{Timestamp: now.Add(-30 * time.Second), Value: 150.0},
			},
		},
		{
			name:         "non_breaching_data_transitions_to_ok",
			operator:     "GreaterThanThreshold",
			statistic:    "Average",
			period:       60,
			evalPeriods:  1,
			initialState: "INSUFFICIENT_DATA",
			wantState:    "OK",
			points: []cloudwatch.MetricDatum{
				{Timestamp: now.Add(-30 * time.Second), Value: 50.0},
			},
		},
		{
			name:              "m_of_n_alarm_requires_enough_breaching_periods",
			operator:          "GreaterThanThreshold",
			statistic:         "Average",
			period:            60,
			evalPeriods:       3,
			datapointsToAlarm: 2,
			initialState:      "INSUFFICIENT_DATA",
			wantState:         "ALARM",
			// 3 periods: oldest (not breaching), middle (breaching), newest (breaching).
			points: []cloudwatch.MetricDatum{
				{Timestamp: now.Add(-150 * time.Second), Value: 50.0}, // period 0: not breach
				{Timestamp: now.Add(-90 * time.Second), Value: 200.0}, // period 1: breach
				{Timestamp: now.Add(-30 * time.Second), Value: 200.0}, // period 2: breach
			},
		},
		{
			name:              "m_of_n_not_enough_breaching_stays_ok",
			operator:          "GreaterThanThreshold",
			statistic:         "Average",
			period:            60,
			evalPeriods:       3,
			datapointsToAlarm: 2,
			initialState:      "INSUFFICIENT_DATA",
			wantState:         "OK",
			points: []cloudwatch.MetricDatum{
				{Timestamp: now.Add(-150 * time.Second), Value: 50.0},
				{Timestamp: now.Add(-90 * time.Second), Value: 50.0},
				{Timestamp: now.Add(-30 * time.Second), Value: 200.0}, // only 1 breach, need 2
			},
		},
		{
			name:         "no_data_treat_missing_breaching_alarms",
			operator:     "GreaterThanThreshold",
			statistic:    "Average",
			period:       60,
			evalPeriods:  2,
			treatMissing: "breaching",
			initialState: "INSUFFICIENT_DATA",
			wantState:    "ALARM",
			points:       nil, // no data
		},
		{
			name:         "no_data_treat_missing_not_breaching_ok",
			operator:     "GreaterThanThreshold",
			statistic:    "Average",
			period:       60,
			evalPeriods:  1,
			treatMissing: "notBreaching",
			initialState: "INSUFFICIENT_DATA",
			wantState:    "OK",
			points:       nil, // no data → not breaching
		},
		{
			name:         "no_data_treat_missing_default_stays_insufficient",
			operator:     "GreaterThanThreshold",
			statistic:    "Average",
			period:       60,
			evalPeriods:  2,
			treatMissing: "", // default: "missing"
			initialState: "INSUFFICIENT_DATA",
			wantState:    "INSUFFICIENT_DATA",
			points:       nil,
		},
		{
			name:         "less_than_operator_breaches_when_below_threshold",
			operator:     "LessThanThreshold",
			statistic:    "Average",
			period:       60,
			evalPeriods:  1,
			initialState: "OK",
			wantState:    "ALARM",
			points: []cloudwatch.MetricDatum{
				{Timestamp: now.Add(-30 * time.Second), Value: 50.0},
			},
		},
		{
			name:         "alarm_transitions_back_to_ok_when_data_normalizes",
			operator:     "GreaterThanThreshold",
			statistic:    "Sum",
			period:       60,
			evalPeriods:  1,
			initialState: "ALARM",
			wantState:    "OK",
			points: []cloudwatch.MetricDatum{
				{Timestamp: now.Add(-30 * time.Second), Value: 10.0},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackend()

			// Put metric data — normalize Value to StatisticSet fields for backend.
			if len(tt.points) > 0 {
				normalized := make([]cloudwatch.MetricDatum, len(tt.points))
				for i, p := range tt.points {
					v := p.Value
					normalized[i] = cloudwatch.MetricDatum{
						MetricName: metricName,
						Value:      v,
						Count:      1,
						Sum:        v,
						Min:        v,
						Max:        v,
						Timestamp:  p.Timestamp,
						Unit:       "Count",
					}
				}

				_, err := b.PutMetricData(namespace, normalized)
				require.NoError(t, err)
			}

			// Create the alarm in initial state.
			datapointsToAlarm := tt.datapointsToAlarm
			err := b.PutMetricAlarm(&cloudwatch.MetricAlarm{
				AlarmName:          alarmName,
				Namespace:          namespace,
				MetricName:         metricName,
				Statistic:          tt.statistic,
				Period:             tt.period,
				EvaluationPeriods:  tt.evalPeriods,
				DatapointsToAlarm:  datapointsToAlarm,
				Threshold:          threshold,
				ComparisonOperator: tt.operator,
				TreatMissingData:   tt.treatMissing,
				StateValue:         tt.initialState,
				ActionsEnabled:     false,
			})
			require.NoError(t, err)

			// Force a single evaluation at `now`.
			b.EvaluateAlarms(t.Context(), now)

			// Read back the alarm state.
			pages, _, err := b.DescribeAlarms(
				[]string{alarmName}, nil, "", "", "",
				100,
			)
			require.NoError(t, err)
			require.Len(t, pages.Data, 1)
			assert.Equal(t, tt.wantState, pages.Data[0].StateValue,
				"alarm state after evaluation")
		})
	}
}

// TestAlarmEvaluator_SNSActionFiredOnStateChange verifies that SNS alarm actions
// are fired when the alarm transitions to a new state.
func TestAlarmEvaluator_SNSActionFiredOnStateChange(t *testing.T) {
	t.Parallel()

	const (
		namespace   = "Prod"
		metricName  = "Errors"
		alarmName   = "error-alarm"
		snsTopicARN = "arn:aws:sns:us-east-1:000000000000:error-alerts"
		threshold   = 0.0
	)

	tests := []struct {
		name         string
		initialState string
		wantState    string
		dataValue    float64
		wantSNSCalls int
	}{
		{
			name:         "alarm_action_fires_on_ok_to_alarm",
			initialState: "OK",
			dataValue:    5.0,
			wantState:    "ALARM",
			wantSNSCalls: 1,
		},
		{
			name:         "ok_action_fires_on_alarm_to_ok",
			initialState: "ALARM",
			dataValue:    0.0,
			wantState:    "OK",
			wantSNSCalls: 1,
		},
		{
			name:         "no_sns_when_state_unchanged",
			initialState: "OK",
			dataValue:    0.0, // still not breaching
			wantState:    "OK",
			wantSNSCalls: 0,
		},
	}

	now := time.Now().UTC()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackend()
			sns := &stubSNS{}
			b.SetSNSPublisher(sns)

			// Put metric datum — normalize Value to StatisticSet fields.
			v := tt.dataValue
			_, err := b.PutMetricData(namespace, []cloudwatch.MetricDatum{
				{
					MetricName: metricName,
					Value:      v,
					Count:      1,
					Sum:        v,
					Min:        v,
					Max:        v,
					Timestamp:  now.Add(-30 * time.Second),
					Unit:       "Count",
				},
			})
			require.NoError(t, err)

			err = b.PutMetricAlarm(&cloudwatch.MetricAlarm{
				AlarmName:          alarmName,
				Namespace:          namespace,
				MetricName:         metricName,
				Statistic:          "Sum",
				Period:             60,
				EvaluationPeriods:  1,
				Threshold:          threshold,
				ComparisonOperator: "GreaterThanThreshold",
				StateValue:         tt.initialState,
				ActionsEnabled:     true,
				AlarmActions:       []string{snsTopicARN},
				OKActions:          []string{snsTopicARN},
			})
			require.NoError(t, err)

			b.EvaluateAlarms(t.Context(), now)

			pages, _, err := b.DescribeAlarms([]string{alarmName}, nil, "", "", "", 100)
			require.NoError(t, err)
			require.Len(t, pages.Data, 1)
			assert.Equal(t, tt.wantState, pages.Data[0].StateValue)
			assert.Len(t, sns.calls(), tt.wantSNSCalls,
				"SNS call count after evaluation")
		})
	}
}

// TestAlarmEvaluator_BackgroundJanitor verifies that the janitor's alarm evaluator
// goroutine automatically transitions alarms over time.
func TestAlarmEvaluator_BackgroundJanitor(t *testing.T) {
	t.Parallel()

	const (
		namespace  = "BgNS"
		metricName = "CPU"
		alarmName  = "bg-alarm"
	)

	b := cloudwatch.NewInMemoryBackend()

	now := time.Now().UTC()

	_, err := b.PutMetricData(namespace, []cloudwatch.MetricDatum{
		{
			MetricName: metricName,
			Value:      200.0,
			Count:      1,
			Sum:        200.0,
			Min:        200.0,
			Max:        200.0,
			Timestamp:  now.Add(-5 * time.Second),
			Unit:       "Percent",
		},
	})
	require.NoError(t, err)

	err = b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:          alarmName,
		Namespace:          namespace,
		MetricName:         metricName,
		Statistic:          "Average",
		Period:             60,
		EvaluationPeriods:  1,
		Threshold:          100.0,
		ComparisonOperator: "GreaterThanThreshold",
		StateValue:         "INSUFFICIENT_DATA",
		ActionsEnabled:     false,
	})
	require.NoError(t, err)

	j := cloudwatch.NewJanitor(b)
	j.AlarmEvalInterval = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	go j.Run(ctx)

	require.Eventually(t, func() bool {
		pages, _, listErr := b.DescribeAlarms([]string{alarmName}, nil, "", "", "", 100)

		return listErr == nil && len(pages.Data) > 0 && pages.Data[0].StateValue == "ALARM"
	}, 500*time.Millisecond, 10*time.Millisecond, "background janitor must evaluate alarm to ALARM")
}
