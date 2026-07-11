package cloudwatch_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// putMetric is a test helper that normalizes Value into Sum/Count/Min/Max and stores the datum.
// Backends aggregate via Sum+Count, not Value directly.
func putMetric(t *testing.T, b *cloudwatch.InMemoryBackend, datum cloudwatch.MetricDatum) {
	t.Helper()
	if !datum.HasStatisticSet {
		datum.Sum = datum.Value
		datum.Count = 1
		datum.Min = datum.Value
		datum.Max = datum.Value
	}
	err := b.PutMetricData(datum.Namespace, []cloudwatch.MetricDatum{datum})
	require.NoError(t, err)
}

// describeMetricAlarm returns the single named metric alarm or fails.
func describeMetricAlarm(t *testing.T, b *cloudwatch.InMemoryBackend, name string) cloudwatch.MetricAlarm {
	t.Helper()
	metric, _, err := b.DescribeAlarms([]string{name}, nil, "", "", "", 0)
	require.NoError(t, err)
	require.Len(t, metric.Data, 1, "expected exactly one alarm named %q", name)

	return metric.Data[0]
}

// ---------------------------------------------------------------------------
// Alarm state evaluation — band-aware comparison operators
// ---------------------------------------------------------------------------

func TestAuditCW_AnomalyBandAlarmEval(t *testing.T) {
	t.Parallel()

	const (
		ns     = "BandNS"
		metric = "Latency"
	)

	now := time.Now().UTC()

	tests := []struct {
		name      string
		operator  string
		wantState string
		value     float64
	}{
		{
			name:      "GreaterThanUpperThreshold_above_band_is_alarm",
			operator:  "GreaterThanUpperThreshold",
			value:     999.0,
			wantState: "ALARM",
		},
		{
			name:      "GreaterThanUpperThreshold_within_band_is_ok",
			operator:  "GreaterThanUpperThreshold",
			value:     10.0,
			wantState: "OK",
		},
		{
			name:      "LessThanLowerOrGreaterThanUpperThreshold_above_band_is_alarm",
			operator:  "LessThanLowerOrGreaterThanUpperThreshold",
			value:     999.0,
			wantState: "ALARM",
		},
		{
			name:      "LessThanLowerOrGreaterThanUpperThreshold_within_band_is_ok",
			operator:  "LessThanLowerOrGreaterThanUpperThreshold",
			value:     10.0,
			wantState: "OK",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackend()
			aName := "band-alarm-" + tc.name

			// Seed 20 stable points to build a meaningful anomaly band.
			for i := range 20 {
				putMetric(t, b, cloudwatch.MetricDatum{
					Namespace:  ns,
					MetricName: metric,
					Timestamp:  now.Add(-time.Duration(20-i) * time.Minute),
					Value:      10.0,
				})
			}

			require.NoError(t, b.PutAnomalyDetector(&cloudwatch.AnomalyDetector{
				Namespace:  ns,
				MetricName: metric,
				Stat:       "Average",
			}))

			putMetric(t, b, cloudwatch.MetricDatum{
				Namespace:  ns,
				MetricName: metric,
				Timestamp:  now.Add(-30 * time.Second),
				Value:      tc.value,
			})

			require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
				AlarmName:          aName,
				Namespace:          ns,
				MetricName:         metric,
				Statistic:          "Average",
				ComparisonOperator: tc.operator,
				EvaluationPeriods:  1,
				DatapointsToAlarm:  1,
				Period:             60,
				Threshold:          0,
				ActionsEnabled:     false,
			}))

			b.EvaluateAlarms(context.Background(), now)

			got := describeMetricAlarm(t, b, aName)
			assert.Equal(t, tc.wantState, got.StateValue, "operator=%s value=%v", tc.operator, tc.value)
		})
	}
}

// ---------------------------------------------------------------------------
// Anomaly detector key isolation by dimensions
// ---------------------------------------------------------------------------

func TestAuditCW_AnomalyDetectorDimensionIsolation(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()

	dimA := []cloudwatch.Dimension{{Name: "Host", Value: "a"}}
	dimB := []cloudwatch.Dimension{{Name: "Host", Value: "b"}}

	require.NoError(t, b.PutAnomalyDetector(&cloudwatch.AnomalyDetector{
		Namespace:  "NS",
		MetricName: "CPU",
		Stat:       "Average",
		Dimensions: dimA,
	}))

	require.NoError(t, b.PutAnomalyDetector(&cloudwatch.AnomalyDetector{
		Namespace:  "NS",
		MetricName: "CPU",
		Stat:       "Average",
		Dimensions: dimB,
	}))

	// Delete only dimA detector; dimB must remain.
	require.NoError(t, b.DeleteAnomalyDetector("NS", "CPU", "Average", dimA))

	page, err := b.DescribeAnomalyDetectors("NS", "CPU", "", 0)
	require.NoError(t, err)
	require.Len(t, page.Data, 1, "only dimB detector should remain")
	assert.Equal(t, dimB, page.Data[0].Dimensions)
}

// ---------------------------------------------------------------------------
// Multi-metric alarm evaluation (metric math)
// ---------------------------------------------------------------------------

func TestAuditCW_MultiMetricAlarmEval(t *testing.T) {
	t.Parallel()

	const (
		ns1   = "MMAlarmNS"
		errM  = "Errors"
		reqM  = "Requests"
		aName = "mm-alarm"
	)

	now := time.Now().UTC()

	tests := []struct {
		name      string
		wantState string
		errors    float64
		requests  float64
	}{
		{
			name:      "ratio_above_threshold_is_alarm",
			errors:    50.0,
			requests:  100.0,
			wantState: "ALARM",
		},
		{
			name:      "ratio_below_threshold_is_ok",
			errors:    5.0,
			requests:  100.0,
			wantState: "OK",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackend()
			alarmName := aName + "-" + tc.name

			putMetric(t, b, cloudwatch.MetricDatum{
				Namespace: ns1, MetricName: errM,
				Timestamp: now.Add(-30 * time.Second), Value: tc.errors,
			})
			putMetric(t, b, cloudwatch.MetricDatum{
				Namespace: ns1, MetricName: reqM,
				Timestamp: now.Add(-30 * time.Second), Value: tc.requests,
			})

			require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
				AlarmName:          alarmName,
				ComparisonOperator: "GreaterThanThreshold",
				EvaluationPeriods:  1,
				DatapointsToAlarm:  1,
				Threshold:          0.1,
				ActionsEnabled:     false,
				Metrics: []cloudwatch.MetricDataQuery{
					{
						ID: "m1",
						MetricStat: cloudwatch.MetricStat{
							Namespace: ns1, MetricName: errM,
							Stat: "Sum", Period: 60,
						},
					},
					{
						ID: "m2",
						MetricStat: cloudwatch.MetricStat{
							Namespace: ns1, MetricName: reqM,
							Stat: "Sum", Period: 60,
						},
					},
					{
						ID:         "ratio",
						Expression: "m1/m2",
						ReturnData: true,
					},
				},
			}))

			b.EvaluateAlarms(context.Background(), now)

			got := describeMetricAlarm(t, b, alarmName)
			assert.Equal(t, tc.wantState, got.StateValue, "errors=%v requests=%v", tc.errors, tc.requests)
		})
	}
}

// ---------------------------------------------------------------------------
// ListMetrics — name-only dimension filter (empty Value = match any)
// ---------------------------------------------------------------------------

func TestAuditCW_ListMetrics_NameOnlyDimensionFilter(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	now := time.Now().UTC()

	for _, datum := range []cloudwatch.MetricDatum{
		{Namespace: "SvcNS", MetricName: "Req", Timestamp: now.Add(-10 * time.Second), Value: 1,
			Dimensions: []cloudwatch.Dimension{{Name: "Host", Value: "alpha"}}},
		{Namespace: "SvcNS", MetricName: "Req", Timestamp: now.Add(-10 * time.Second), Value: 1,
			Dimensions: []cloudwatch.Dimension{{Name: "Host", Value: "beta"}}},
		{Namespace: "SvcNS", MetricName: "Req", Timestamp: now.Add(-10 * time.Second), Value: 1,
			Dimensions: []cloudwatch.Dimension{{Name: "Region", Value: "us-east-1"}}},
	} {
		putMetric(t, b, datum)
	}

	tests := []struct {
		name      string
		dimFilter []cloudwatch.Dimension
		wantLen   int
	}{
		{
			name:      "name_only_filter_matches_all_host_values",
			dimFilter: []cloudwatch.Dimension{{Name: "Host", Value: ""}},
			wantLen:   2,
		},
		{
			name:      "name_value_filter_matches_exact",
			dimFilter: []cloudwatch.Dimension{{Name: "Host", Value: "alpha"}},
			wantLen:   1,
		},
		{
			name:      "name_filter_non_matching_dim_returns_empty",
			dimFilter: []cloudwatch.Dimension{{Name: "AZ", Value: ""}},
			wantLen:   0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			page, err := b.ListMetrics("SvcNS", "Req", tc.dimFilter, "", "", 0)
			require.NoError(t, err)
			assert.Len(t, page.Data, tc.wantLen, "filter=%v", tc.dimFilter)
		})
	}
}

// ---------------------------------------------------------------------------
// Alarm history — action entries recorded on ALARM transition
// ---------------------------------------------------------------------------

func TestAuditCW_AlarmHistory_RecordsActionOnTransition(t *testing.T) {
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

// ---------------------------------------------------------------------------
// SetAlarmState — manual override persists
// ---------------------------------------------------------------------------

func TestAuditCW_SetAlarmState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setState  string
		wantState string
	}{
		{name: "set_alarm", setState: "ALARM", wantState: "ALARM"},
		{name: "set_ok", setState: "OK", wantState: "OK"},
		{name: "set_insufficient", setState: "INSUFFICIENT_DATA", wantState: "INSUFFICIENT_DATA"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackend()
			aName := "sa-alarm-" + tc.name

			require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
				AlarmName:          aName,
				Namespace:          "NS",
				MetricName:         "M",
				Statistic:          "Average",
				ComparisonOperator: "GreaterThanThreshold",
				EvaluationPeriods:  1,
				Period:             60,
				Threshold:          50,
			}))

			require.NoError(t, b.SetAlarmState(context.Background(), aName, tc.setState, "manual override", ""))

			got := describeMetricAlarm(t, b, aName)
			assert.Equal(t, tc.wantState, got.StateValue)
		})
	}
}
