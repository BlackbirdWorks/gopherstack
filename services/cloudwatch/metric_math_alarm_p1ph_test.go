package cloudwatch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/stretchr/testify/require"
)

// TestPutMetricAlarm_Metrics_RealClient_RoundTrip covers gopherstack-p1ph:
// cborPutMetricAlarm never read the "Metrics" member (metric-math alarms),
// unlike the dead legacy XML handlePutMetricAlarm which parses it via
// parseMetricDataQueriesFromForm. A real aws-sdk-go-v2 client only speaks
// rpc-v2-cbor for this service (cloudwatch@v1.66.3 api_client.go's
// rpcv2.NewCBOR), so the XML path is unreachable and this drives the live
// path a real client uses, then reads back through DescribeAlarms to prove
// the structure survives a write-then-read round trip.
func TestPutMetricAlarm_Metrics_RealClient_RoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	_, err := client.PutMetricAlarm(ctx, &cwsdk.PutMetricAlarmInput{
		AlarmName:          aws.String("metric-math-alarm"),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
		EvaluationPeriods:  aws.Int32(1),
		Threshold:          aws.Float64(10),
		Metrics: []cwtypes.MetricDataQuery{
			{
				Id: aws.String("m1"),
				MetricStat: &cwtypes.MetricStat{
					Metric: &cwtypes.Metric{
						Namespace:  aws.String("AWS/EC2"),
						MetricName: aws.String("CPUUtilization"),
						Dimensions: []cwtypes.Dimension{
							{Name: aws.String("InstanceId"), Value: aws.String("i-abc123")},
						},
					},
					Period: aws.Int32(60),
					Stat:   aws.String("Average"),
				},
				ReturnData: aws.Bool(false),
			},
			{
				Id:         aws.String("e1"),
				Expression: aws.String("m1*2"),
				Label:      aws.String("Doubled CPU"),
				ReturnData: aws.Bool(true),
			},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeAlarms(ctx, &cwsdk.DescribeAlarmsInput{
		AlarmNames: []string{"metric-math-alarm"},
	})
	require.NoError(t, err)
	require.Len(t, out.MetricAlarms, 1)

	alarm := out.MetricAlarms[0]
	require.Len(t, alarm.Metrics, 2, "PutMetricAlarm Metrics must survive a write-then-read round trip")

	byID := make(map[string]cwtypes.MetricDataQuery, len(alarm.Metrics))
	for _, m := range alarm.Metrics {
		require.NotNil(t, m.Id)
		byID[*m.Id] = m
	}

	m1, ok := byID["m1"]
	require.True(t, ok, "expected metric query m1 to round-trip")
	require.NotNil(t, m1.MetricStat)
	require.NotNil(t, m1.MetricStat.Metric)
	require.Equal(t, "AWS/EC2", aws.ToString(m1.MetricStat.Metric.Namespace))
	require.Equal(t, "CPUUtilization", aws.ToString(m1.MetricStat.Metric.MetricName))
	require.Len(t, m1.MetricStat.Metric.Dimensions, 1)
	require.Equal(t, "InstanceId", aws.ToString(m1.MetricStat.Metric.Dimensions[0].Name))
	require.Equal(t, "i-abc123", aws.ToString(m1.MetricStat.Metric.Dimensions[0].Value))
	require.Equal(t, int32(60), aws.ToInt32(m1.MetricStat.Period))
	require.Equal(t, "Average", aws.ToString(m1.MetricStat.Stat))
	require.False(t, aws.ToBool(m1.ReturnData))

	e1, ok := byID["e1"]
	require.True(t, ok, "expected metric query e1 to round-trip")
	require.Equal(t, "m1*2", aws.ToString(e1.Expression))
	require.Equal(t, "Doubled CPU", aws.ToString(e1.Label))
	require.True(t, aws.ToBool(e1.ReturnData))
}
