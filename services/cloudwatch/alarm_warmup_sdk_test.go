package cloudwatch_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

func TestPutMetricAlarm_WarmUpEvaluation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantState types.StateValue
		onlyAfter bool
		datapoint bool
		warm      bool
	}{
		{
			name: "inside_warmup_waits_full_period", onlyAfter: true, datapoint: true, warm: true,
			wantState: types.StateValueInsufficientData,
		},
		{
			name: "inside_warmup_no_data_holds", warm: true,
			wantState: types.StateValueInsufficientData,
		},
		{
			name: "inside_warmup_early_end_with_data", datapoint: true, warm: true,
			wantState: types.StateValueAlarm,
		},
		{
			name: "after_warmup_evaluates", onlyAfter: true, datapoint: true,
			wantState: types.StateValueAlarm,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, backend := newTestHandlerAndClientWithBackend(t)
			now := time.Now().UTC()

			if tt.datapoint {
				require.NoError(t, backend.PutMetricData("NS", []cloudwatch.MetricDatum{{
					MetricName: "M", Value: 10, Count: 1, Sum: 10, Min: 10, Max: 10,
					Timestamp: now.Add(-30 * time.Second), Unit: "Count",
				}}))
			}

			_, err := client.PutMetricAlarm(t.Context(), &cwsdk.PutMetricAlarmInput{
				AlarmName: aws.String("warm"), Namespace: aws.String("NS"), MetricName: aws.String("M"),
				ComparisonOperator: types.ComparisonOperatorGreaterThanThreshold,
				EvaluationPeriods:  aws.Int32(1), Period: aws.Int32(60), Statistic: types.StatisticSum,
				Threshold: aws.Float64(1),
				WarmUpConfiguration: &types.WarmUpConfiguration{
					WarmUpPeriodDurationInMinutes:            aws.Int32(30),
					OnlyStartEvaluatingAfterWarmUpPeriodEnds: aws.Bool(tt.onlyAfter),
				},
				EvaluateLowSampleCountPercentile: aws.String("ignore"),
			})
			require.NoError(t, err)

			evalAt := now
			if !tt.warm {
				evalAt = now.Add(31 * time.Minute)
				require.NoError(t, backend.PutMetricData("NS", []cloudwatch.MetricDatum{{
					MetricName: "M", Value: 10, Count: 1, Sum: 10, Min: 10, Max: 10,
					Timestamp: evalAt.Add(-30 * time.Second), Unit: "Count",
				}}))
			}

			backend.EvaluateAlarms(t.Context(), evalAt)

			out, err := client.DescribeAlarms(t.Context(), &cwsdk.DescribeAlarmsInput{AlarmNames: []string{"warm"}})
			require.NoError(t, err)
			require.Len(t, out.MetricAlarms, 1)

			a := out.MetricAlarms[0]
			assert.Equal(t, tt.wantState, a.StateValue)
			require.NotNil(t, a.WarmUpConfiguration)
			assert.Equal(t, int32(30), aws.ToInt32(a.WarmUpConfiguration.WarmUpPeriodDurationInMinutes))
			assert.Equal(t, tt.onlyAfter, aws.ToBool(a.WarmUpConfiguration.OnlyStartEvaluatingAfterWarmUpPeriodEnds))
			assert.Equal(t, "ignore", aws.ToString(a.EvaluateLowSampleCountPercentile))
		})
	}
}

func TestPutMetricAlarm_RejectedMembers(t *testing.T) {
	t.Parallel()

	base := func() *cwsdk.PutMetricAlarmInput {
		return &cwsdk.PutMetricAlarmInput{
			AlarmName: aws.String("bad"), Namespace: aws.String("NS"), MetricName: aws.String("M"),
			ComparisonOperator: types.ComparisonOperatorGreaterThanThreshold,
			EvaluationPeriods:  aws.Int32(1), Period: aws.Int32(60), Statistic: types.StatisticSum,
			Threshold: aws.Float64(1),
		}
	}

	tests := []struct {
		mutate func(*cwsdk.PutMetricAlarmInput)
		name   string
	}{
		{
			name:   "low_sample_value",
			mutate: func(in *cwsdk.PutMetricAlarmInput) { in.EvaluateLowSampleCountPercentile = aws.String("maybe") },
		},
		{name: "warmup_zero", mutate: func(in *cwsdk.PutMetricAlarmInput) {
			in.WarmUpConfiguration = &types.WarmUpConfiguration{WarmUpPeriodDurationInMinutes: aws.Int32(0)}
		}},
		{
			name:   "interval_with_metric_name",
			mutate: func(in *cwsdk.PutMetricAlarmInput) { in.EvaluationInterval = aws.Int32(30) },
		},
		{name: "promql_criteria", mutate: func(in *cwsdk.PutMetricAlarmInput) {
			in.EvaluationCriteria = &types.EvaluationCriteriaMemberPromQLCriteria{
				Value: types.AlarmPromQLCriteria{Query: aws.String("up")},
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			in := base()
			tt.mutate(in)

			_, err := client.PutMetricAlarm(t.Context(), in)
			require.Error(t, err)

			out, descErr := client.DescribeAlarms(t.Context(), &cwsdk.DescribeAlarmsInput{})
			require.NoError(t, descErr)
			assert.Empty(t, out.MetricAlarms)
		})
	}
}
