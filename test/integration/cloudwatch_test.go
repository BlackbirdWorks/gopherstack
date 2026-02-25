package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudwatchsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_CloudWatch_MetricsAndAlarms(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createCloudWatchClient(t)
	ctx := t.Context()

	ns := "TestNS/" + uuid.NewString()[:8]
	metricName := "Latency"
	alarmName := "test-alarm-" + uuid.NewString()[:8]

	// PutMetricData
	_, err := client.PutMetricData(ctx, &cloudwatchsdk.PutMetricDataInput{
		Namespace: aws.String(ns),
		MetricData: []cwtypes.MetricDatum{
			{MetricName: aws.String(metricName), Value: aws.Float64(100.0), Timestamp: aws.Time(time.Now().UTC())},
			{MetricName: aws.String(metricName), Value: aws.Float64(200.0), Timestamp: aws.Time(time.Now().UTC().Add(-time.Minute))},
		},
	})
	require.NoError(t, err)

	// GetMetricStatistics
	statsOut, err := client.GetMetricStatistics(ctx, &cloudwatchsdk.GetMetricStatisticsInput{
		Namespace:  aws.String(ns),
		MetricName: aws.String(metricName),
		StartTime:  aws.Time(time.Now().UTC().Add(-time.Hour)),
		EndTime:    aws.Time(time.Now().UTC().Add(time.Minute)),
		Period:     aws.Int32(3600),
		Statistics: []cwtypes.Statistic{cwtypes.StatisticSum},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, statsOut.Datapoints)

	// PutMetricAlarm
	_, err = client.PutMetricAlarm(ctx, &cloudwatchsdk.PutMetricAlarmInput{
		AlarmName:          aws.String(alarmName),
		Namespace:          aws.String(ns),
		MetricName:         aws.String(metricName),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
		Threshold:          aws.Float64(500.0),
		EvaluationPeriods:  aws.Int32(1),
		Period:             aws.Int32(60),
		Statistic:          cwtypes.StatisticAverage,
	})
	require.NoError(t, err)

	// DescribeAlarms
	descOut, err := client.DescribeAlarms(ctx, &cloudwatchsdk.DescribeAlarmsInput{
		AlarmNames: []string{alarmName},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, descOut.MetricAlarms)

	// DeleteAlarms
	_, err = client.DeleteAlarms(ctx, &cloudwatchsdk.DeleteAlarmsInput{
		AlarmNames: []string{alarmName},
	})
	require.NoError(t, err)
}
