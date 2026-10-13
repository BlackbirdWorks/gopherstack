package cloudwatch_test

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

func putBreachAlarm(t *testing.T, client *cwsdk.Client, name string, threshold float64) {
	t.Helper()

	_, err := client.PutMetricAlarm(t.Context(), &cwsdk.PutMetricAlarmInput{
		AlarmName: aws.String(name), Namespace: aws.String("NS"), MetricName: aws.String("M"),
		ComparisonOperator: types.ComparisonOperatorGreaterThanThreshold,
		EvaluationPeriods:  aws.Int32(1), Period: aws.Int32(60), Statistic: types.StatisticAverage,
		Threshold: aws.Float64(threshold),
	})
	require.NoError(t, err)
}

func TestSDK_AlarmStateTransitionRealism(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantState   types.StateValue
		wantReason  string
		wantSummary string
		value       float64
	}{
		{
			name: "breach", value: 10, wantState: types.StateValueAlarm,
			wantReason:  "Threshold Crossed: 1 out of the last 1 datapoints [10.0 (",
			wantSummary: "Alarm updated from INSUFFICIENT_DATA to ALARM",
		},
		{
			name: "recover", value: 2, wantState: types.StateValueOk,
			wantReason:  "Threshold Crossed: 1 out of the last 1 datapoints [2.0 (",
			wantSummary: "Alarm updated from INSUFFICIENT_DATA to OK",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, backend := newTestHandlerAndClientWithBackend(t)
			putBreachAlarm(t, client, "a", 5)

			out, err := client.DescribeAlarms(t.Context(), &cwsdk.DescribeAlarmsInput{})
			require.NoError(t, err)
			require.Len(t, out.MetricAlarms, 1)
			assert.Equal(t, types.StateValueInsufficientData, out.MetricAlarms[0].StateValue)
			assert.Equal(t, "Unchecked: Initial alarm creation", aws.ToString(out.MetricAlarms[0].StateReason))

			now := time.Now().UTC()
			require.NoError(t, backend.PutMetricData("NS", []cloudwatch.MetricDatum{{
				MetricName: "M", Value: tt.value, HasValue: true, Count: 1, Sum: tt.value, Min: tt.value, Max: tt.value,
				Timestamp: now.Add(-30 * time.Second),
			}}))
			backend.EvaluateAlarms(t.Context(), now)

			out, err = client.DescribeAlarms(t.Context(), &cwsdk.DescribeAlarmsInput{})
			require.NoError(t, err)
			got := out.MetricAlarms[0]
			assert.Equal(t, tt.wantState, got.StateValue)
			assert.Contains(t, aws.ToString(got.StateReason), tt.wantReason)
			assert.True(t, json.Valid([]byte(aws.ToString(got.StateReasonData))))

			putBreachAlarm(t, client, "a", 6)
			out, err = client.DescribeAlarms(t.Context(), &cwsdk.DescribeAlarmsInput{})
			require.NoError(t, err)
			assert.Equal(t, tt.wantState, out.MetricAlarms[0].StateValue, "update keeps state")
			assert.Contains(t, aws.ToString(out.MetricAlarms[0].StateReason), tt.wantReason)

			hist, err := client.DescribeAlarmHistory(t.Context(), &cwsdk.DescribeAlarmHistoryInput{
				HistoryItemType: types.HistoryItemTypeStateUpdate,
			})
			require.NoError(t, err)
			require.Len(t, hist.AlarmHistoryItems, 1)
			assert.Equal(t, tt.wantSummary, aws.ToString(hist.AlarmHistoryItems[0].HistorySummary))

			var data struct {
				NewState struct {
					StateValue string `json:"stateValue"`
				} `json:"newState"`
			}
			require.NoError(t, json.Unmarshal([]byte(aws.ToString(hist.AlarmHistoryItems[0].HistoryData)), &data))
			assert.Equal(t, string(tt.wantState), data.NewState.StateValue)
		})
	}
}

func TestSDK_RequestValidationErrors(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	statInput := func(period int32, from, to time.Time, stats ...types.Statistic) *cwsdk.GetMetricStatisticsInput {
		return &cwsdk.GetMetricStatisticsInput{
			Namespace: aws.String("NS"), MetricName: aws.String("M"), Period: aws.Int32(period),
			StartTime: aws.Time(from), EndTime: aws.Time(to), Statistics: stats,
		}
	}

	tests := []struct {
		call     func(*cwsdk.Client) error
		name     string
		wantCode string
	}{
		{
			name: "set_state_bad_enum", wantCode: "ValidationError",
			call: func(c *cwsdk.Client) error {
				_, err := c.SetAlarmState(t.Context(), &cwsdk.SetAlarmStateInput{
					AlarmName: aws.String("x"), StateValue: types.StateValue("Bogus"), StateReason: aws.String("r"),
				})

				return err
			},
		},
		{
			name: "set_state_unknown_alarm", wantCode: "ResourceNotFound",
			call: func(c *cwsdk.Client) error {
				_, err := c.SetAlarmState(t.Context(), &cwsdk.SetAlarmStateInput{
					AlarmName: aws.String("x"), StateValue: types.StateValueOk, StateReason: aws.String("r"),
				})

				return err
			},
		},
		{
			name: "describe_alarms_bad_token", wantCode: "InvalidNextToken",
			call: func(c *cwsdk.Client) error {
				_, err := c.DescribeAlarms(t.Context(), &cwsdk.DescribeAlarmsInput{NextToken: aws.String("garbage")})

				return err
			},
		},
		{
			name: "describe_alarms_max_records", wantCode: "ValidationError",
			call: func(c *cwsdk.Client) error {
				_, err := c.DescribeAlarms(t.Context(), &cwsdk.DescribeAlarmsInput{MaxRecords: aws.Int32(101)})

				return err
			},
		},
		{
			name: "history_bad_token", wantCode: "InvalidNextToken",
			call: func(c *cwsdk.Client) error {
				_, err := c.DescribeAlarmHistory(
					t.Context(),
					&cwsdk.DescribeAlarmHistoryInput{NextToken: aws.String("x")},
				)

				return err
			},
		},
		{
			name: "list_metrics_bad_token", wantCode: "InvalidNextToken",
			call: func(c *cwsdk.Client) error {
				_, err := c.ListMetrics(t.Context(), &cwsdk.ListMetricsInput{NextToken: aws.String("x")})

				return err
			},
		},
		{
			name: "get_metric_data_reversed_window", wantCode: "InvalidParameterValue",
			call: func(c *cwsdk.Client) error {
				_, err := c.GetMetricData(t.Context(), &cwsdk.GetMetricDataInput{
					StartTime: aws.Time(now), EndTime: aws.Time(now.Add(-time.Hour)),
					MetricDataQueries: []types.MetricDataQuery{{
						Id: aws.String("q1"),
						MetricStat: &types.MetricStat{
							Metric: &types.Metric{Namespace: aws.String("NS"), MetricName: aws.String("M")},
							Period: aws.Int32(60), Stat: aws.String("Average"),
						},
					}},
				})

				return err
			},
		},
		{
			name: "get_metric_data_bad_token", wantCode: "InvalidNextToken",
			call: func(c *cwsdk.Client) error {
				_, err := c.GetMetricData(t.Context(), &cwsdk.GetMetricDataInput{
					StartTime: aws.Time(now.Add(-time.Hour)), EndTime: aws.Time(now), NextToken: aws.String("zz"),
					MetricDataQueries: []types.MetricDataQuery{{
						Id: aws.String("q1"),
						MetricStat: &types.MetricStat{
							Metric: &types.Metric{Namespace: aws.String("NS"), MetricName: aws.String("M")},
							Period: aws.Int32(60), Stat: aws.String("Average"),
						},
					}},
				})

				return err
			},
		},
		{
			name: "stats_period_not_multiple", wantCode: "InvalidParameterValue",
			call: func(c *cwsdk.Client) error {
				_, err := c.GetMetricStatistics(
					t.Context(),
					statInput(45, now.Add(-time.Hour), now, types.StatisticAverage),
				)

				return err
			},
		},
		{
			name: "stats_none_requested", wantCode: "InvalidParameterCombination",
			call: func(c *cwsdk.Client) error {
				_, err := c.GetMetricStatistics(t.Context(), statInput(60, now.Add(-time.Hour), now))

				return err
			},
		},
		{
			name: "stats_too_many_datapoints", wantCode: "InvalidParameterCombination",
			call: func(c *cwsdk.Client) error {
				_, err := c.GetMetricStatistics(t.Context(),
					statInput(60, now.Add(-72*time.Hour), now, types.StatisticAverage))

				return err
			},
		},
		{
			name: "put_data_bad_unit", wantCode: "InvalidParameterValue",
			call: func(c *cwsdk.Client) error {
				_, err := c.PutMetricData(t.Context(), &cwsdk.PutMetricDataInput{
					Namespace: aws.String("NS"),
					MetricData: []types.MetricDatum{{
						MetricName: aws.String("M"), Value: aws.Float64(1), Unit: types.StandardUnit("Bogus"),
					}},
				})

				return err
			},
		},
		{
			name: "put_data_too_many_dimensions", wantCode: "InvalidParameterValue",
			call: func(c *cwsdk.Client) error {
				dims := make([]types.Dimension, 31)
				for i := range dims {
					dims[i] = types.Dimension{Name: aws.String("d" + strconv.Itoa(i)), Value: aws.String("v")}
				}
				_, err := c.PutMetricData(t.Context(), &cwsdk.PutMetricDataInput{
					Namespace: aws.String("NS"),
					MetricData: []types.MetricDatum{
						{MetricName: aws.String("M"), Value: aws.Float64(1), Dimensions: dims},
					},
				})

				return err
			},
		},
		{
			name: "put_composite_unparseable_rule", wantCode: "ValidationError",
			call: func(c *cwsdk.Client) error {
				_, err := c.PutCompositeAlarm(t.Context(), &cwsdk.PutCompositeAlarmInput{
					AlarmName: aws.String("c"), AlarmRule: aws.String("ALARM(a) AND"),
				})

				return err
			},
		},
		{
			name: "put_alarm_bad_operator", wantCode: "ValidationError",
			call: func(c *cwsdk.Client) error {
				_, err := c.PutMetricAlarm(t.Context(), &cwsdk.PutMetricAlarmInput{
					AlarmName: aws.String("x"), Namespace: aws.String("NS"), MetricName: aws.String("M"),
					ComparisonOperator: types.ComparisonOperator("Bogus"), EvaluationPeriods: aws.Int32(1),
					Period: aws.Int32(60), Statistic: types.StatisticAverage, Threshold: aws.Float64(1),
				})

				return err
			},
		},
		{
			name: "put_alarm_bad_period", wantCode: "InvalidParameterValue",
			call: func(c *cwsdk.Client) error {
				_, err := c.PutMetricAlarm(t.Context(), &cwsdk.PutMetricAlarmInput{
					AlarmName: aws.String("x"), Namespace: aws.String("NS"), MetricName: aws.String("M"),
					ComparisonOperator: types.ComparisonOperatorGreaterThanThreshold, EvaluationPeriods: aws.Int32(1),
					Period: aws.Int32(7), Statistic: types.StatisticAverage, Threshold: aws.Float64(1),
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)
			err := tt.call(client)
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr, "got %T: %v", err, err)
			assert.Equal(t, tt.wantCode, strings.TrimSuffix(apiErr.ErrorCode(), "Exception"))
			assert.NotContains(t, apiErr.ErrorMessage(), tt.wantCode+":")
			assert.NotEmpty(t, apiErr.ErrorMessage())
		})
	}
}
