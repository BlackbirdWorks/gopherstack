package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudwatchsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
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
			{
				MetricName: aws.String(metricName),
				Value:      aws.Float64(200.0),
				Timestamp:  aws.Time(time.Now().UTC().Add(-time.Minute)),
			},
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

func TestIntegration_CloudWatch_CompositeAlarms(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createCloudWatchClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	childAlarm := "child-" + suffix
	compositeAlarm := "composite-" + suffix

	// Create a child metric alarm.
	_, err := client.PutMetricAlarm(ctx, &cloudwatchsdk.PutMetricAlarmInput{
		AlarmName:          aws.String(childAlarm),
		Namespace:          aws.String("AWS/EC2"),
		MetricName:         aws.String("CPUUtilization"),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
		Threshold:          aws.Float64(80.0),
		EvaluationPeriods:  aws.Int32(1),
		Period:             aws.Int32(60),
		Statistic:          cwtypes.StatisticAverage,
	})
	require.NoError(t, err)

	// Create a composite alarm referencing the child.
	_, err = client.PutCompositeAlarm(ctx, &cloudwatchsdk.PutCompositeAlarmInput{
		AlarmName: aws.String(compositeAlarm),
		AlarmRule: aws.String(`ALARM("` + childAlarm + `")`),
	})
	require.NoError(t, err)

	// DescribeAlarms returns both metric and composite alarms.
	descOut, err := client.DescribeAlarms(ctx, &cloudwatchsdk.DescribeAlarmsInput{
		AlarmNames: []string{childAlarm, compositeAlarm},
	})
	require.NoError(t, err)
	assert.Len(t, descOut.MetricAlarms, 1)
	assert.Len(t, descOut.CompositeAlarms, 1)
	assert.Equal(t, compositeAlarm, aws.ToString(descOut.CompositeAlarms[0].AlarmName))

	// Cleanup.
	_, err = client.DeleteAlarms(ctx, &cloudwatchsdk.DeleteAlarmsInput{
		AlarmNames: []string{childAlarm, compositeAlarm},
	})
	require.NoError(t, err)
}

func TestIntegration_CloudWatch_SetAlarmState(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createCloudWatchClient(t)
	ctx := t.Context()

	alarmName := "state-alarm-" + uuid.NewString()[:8]

	_, err := client.PutMetricAlarm(ctx, &cloudwatchsdk.PutMetricAlarmInput{
		AlarmName:          aws.String(alarmName),
		Namespace:          aws.String("AWS/EC2"),
		MetricName:         aws.String("CPUUtilization"),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
		Threshold:          aws.Float64(80.0),
		EvaluationPeriods:  aws.Int32(1),
		Period:             aws.Int32(60),
		Statistic:          cwtypes.StatisticAverage,
	})
	require.NoError(t, err)

	// Set alarm state manually.
	_, err = client.SetAlarmState(ctx, &cloudwatchsdk.SetAlarmStateInput{
		AlarmName:   aws.String(alarmName),
		StateValue:  cwtypes.StateValueAlarm,
		StateReason: aws.String("manual test trigger"),
	})
	require.NoError(t, err)

	// Verify state changed.
	descOut, err := client.DescribeAlarms(ctx, &cloudwatchsdk.DescribeAlarmsInput{
		AlarmNames: []string{alarmName},
	})
	require.NoError(t, err)
	require.Len(t, descOut.MetricAlarms, 1)
	assert.Equal(t, cwtypes.StateValueAlarm, descOut.MetricAlarms[0].StateValue)

	// Cleanup.
	_, err = client.DeleteAlarms(ctx, &cloudwatchsdk.DeleteAlarmsInput{
		AlarmNames: []string{alarmName},
	})
	require.NoError(t, err)
}

func TestIntegration_CloudWatch_EnableDisableAlarmActions(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createCloudWatchClient(t)
	ctx := t.Context()

	alarmName := "actions-alarm-" + uuid.NewString()[:8]

	_, err := client.PutMetricAlarm(ctx, &cloudwatchsdk.PutMetricAlarmInput{
		AlarmName:          aws.String(alarmName),
		Namespace:          aws.String("AWS/EC2"),
		MetricName:         aws.String("CPUUtilization"),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
		Threshold:          aws.Float64(80.0),
		EvaluationPeriods:  aws.Int32(1),
		Period:             aws.Int32(60),
		Statistic:          cwtypes.StatisticAverage,
		ActionsEnabled:     aws.Bool(true),
	})
	require.NoError(t, err)

	// Disable actions.
	_, err = client.DisableAlarmActions(ctx, &cloudwatchsdk.DisableAlarmActionsInput{
		AlarmNames: []string{alarmName},
	})
	require.NoError(t, err)

	// Verify disabled.
	descOut, err := client.DescribeAlarms(ctx, &cloudwatchsdk.DescribeAlarmsInput{
		AlarmNames: []string{alarmName},
	})
	require.NoError(t, err)
	require.Len(t, descOut.MetricAlarms, 1)
	assert.False(t, aws.ToBool(descOut.MetricAlarms[0].ActionsEnabled))

	// Enable actions.
	_, err = client.EnableAlarmActions(ctx, &cloudwatchsdk.EnableAlarmActionsInput{
		AlarmNames: []string{alarmName},
	})
	require.NoError(t, err)

	// Verify enabled.
	descOut2, err := client.DescribeAlarms(ctx, &cloudwatchsdk.DescribeAlarmsInput{
		AlarmNames: []string{alarmName},
	})
	require.NoError(t, err)
	require.Len(t, descOut2.MetricAlarms, 1)
	assert.True(t, aws.ToBool(descOut2.MetricAlarms[0].ActionsEnabled))

	// Cleanup.
	_, err = client.DeleteAlarms(ctx, &cloudwatchsdk.DeleteAlarmsInput{
		AlarmNames: []string{alarmName},
	})
	require.NoError(t, err)
}

func TestIntegration_CloudWatch_DescribeAlarmsForMetric(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createCloudWatchClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	ns := "Test/DescForMetric/" + suffix
	metricName := "Requests"
	alarmName := "metric-alarm-" + suffix

	_, err := client.PutMetricAlarm(ctx, &cloudwatchsdk.PutMetricAlarmInput{
		AlarmName:          aws.String(alarmName),
		Namespace:          aws.String(ns),
		MetricName:         aws.String(metricName),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
		Threshold:          aws.Float64(100.0),
		EvaluationPeriods:  aws.Int32(1),
		Period:             aws.Int32(60),
		Statistic:          cwtypes.StatisticSum,
	})
	require.NoError(t, err)

	descOut, err := client.DescribeAlarmsForMetric(ctx, &cloudwatchsdk.DescribeAlarmsForMetricInput{
		Namespace:  aws.String(ns),
		MetricName: aws.String(metricName),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, descOut.MetricAlarms)
	assert.Equal(t, alarmName, aws.ToString(descOut.MetricAlarms[0].AlarmName))

	// Cleanup.
	_, err = client.DeleteAlarms(ctx, &cloudwatchsdk.DeleteAlarmsInput{
		AlarmNames: []string{alarmName},
	})
	require.NoError(t, err)
}

func TestIntegration_CloudWatch_DescribeAlarmHistory(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createCloudWatchClient(t)
	ctx := t.Context()

	alarmName := "hist-alarm-" + uuid.NewString()[:8]

	_, err := client.PutMetricAlarm(ctx, &cloudwatchsdk.PutMetricAlarmInput{
		AlarmName:          aws.String(alarmName),
		Namespace:          aws.String("AWS/EC2"),
		MetricName:         aws.String("CPUUtilization"),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
		Threshold:          aws.Float64(80.0),
		EvaluationPeriods:  aws.Int32(1),
		Period:             aws.Int32(60),
		Statistic:          cwtypes.StatisticAverage,
	})
	require.NoError(t, err)

	_, err = client.SetAlarmState(ctx, &cloudwatchsdk.SetAlarmStateInput{
		AlarmName:   aws.String(alarmName),
		StateValue:  cwtypes.StateValueAlarm,
		StateReason: aws.String("test trigger for history"),
	})
	require.NoError(t, err)

	histOut, err := client.DescribeAlarmHistory(ctx, &cloudwatchsdk.DescribeAlarmHistoryInput{
		AlarmName: aws.String(alarmName),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, histOut.AlarmHistoryItems)

	// Cleanup.
	_, err = client.DeleteAlarms(ctx, &cloudwatchsdk.DeleteAlarmsInput{
		AlarmNames: []string{alarmName},
	})
	require.NoError(t, err)
}

func TestIntegration_CloudWatch_AlarmActions_SNS(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	cwClient := createCloudWatchClient(t)
	snsClient := createSNSClient(t)
	sqsClient := createSQSClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	topicName := "cw-alarm-topic-" + suffix
	queueName := "cw-alarm-queue-" + suffix
	alarmName := "sns-action-alarm-" + suffix

	// Create an SNS topic for the alarm action.
	topicOut, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String(topicName)})
	require.NoError(t, err)
	topicARN := aws.ToString(topicOut.TopicArn)
	t.Cleanup(func() {
		_, _ = snsClient.DeleteTopic(ctx, &sns.DeleteTopicInput{TopicArn: aws.String(topicARN)})
	})

	// Create an SQS queue and subscribe it to the topic to capture published messages.
	queueOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(queueName)})
	require.NoError(t, err)
	queueURL := aws.ToString(queueOut.QueueUrl)
	t.Cleanup(func() {
		_, _ = sqsClient.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: aws.String(queueURL)})
	})

	attrOut, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []sqstypes.QueueAttributeName{"QueueArn"},
	})
	require.NoError(t, err)
	queueARN := attrOut.Attributes["QueueArn"]

	_, err = snsClient.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: aws.String(topicARN),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueARN),
	})
	require.NoError(t, err)

	// Create a metric alarm with an SNS AlarmAction.
	_, err = cwClient.PutMetricAlarm(ctx, &cloudwatchsdk.PutMetricAlarmInput{
		AlarmName:          aws.String(alarmName),
		Namespace:          aws.String("AWS/EC2"),
		MetricName:         aws.String("CPUUtilization"),
		ComparisonOperator: cwtypes.ComparisonOperatorGreaterThanThreshold,
		Threshold:          aws.Float64(80.0),
		EvaluationPeriods:  aws.Int32(1),
		Period:             aws.Int32(60),
		Statistic:          cwtypes.StatisticAverage,
		ActionsEnabled:     aws.Bool(true),
		AlarmActions:       []string{topicARN},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = cwClient.DeleteAlarms(ctx, &cloudwatchsdk.DeleteAlarmsInput{AlarmNames: []string{alarmName}})
	})

	// Trigger the alarm by setting state to ALARM — should invoke SNS action.
	_, err = cwClient.SetAlarmState(ctx, &cloudwatchsdk.SetAlarmStateInput{
		AlarmName:   aws.String(alarmName),
		StateValue:  cwtypes.StateValueAlarm,
		StateReason: aws.String("integration test trigger"),
	})
	require.NoError(t, err)

	// Verify alarm is in ALARM state.
	descOut, err := cwClient.DescribeAlarms(ctx, &cloudwatchsdk.DescribeAlarmsInput{
		AlarmNames: []string{alarmName},
	})
	require.NoError(t, err)
	require.Len(t, descOut.MetricAlarms, 1)
	assert.Equal(t, cwtypes.StateValueAlarm, descOut.MetricAlarms[0].StateValue)

	// Verify the SNS action fired — SQS queue should have received the alarm notification.
	recvOut, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     5,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 1, "alarm action should have published a message to SNS/SQS")
	assert.Contains(t, aws.ToString(recvOut.Messages[0].Body), alarmName)
}

func TestIntegration_CloudWatch_Dashboards(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createCloudWatchClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	dashName := "test-dashboard-" + suffix
	dashBody := `{"widgets":[{"type":"text","x":0,"y":0,"width":6,"height":3}]}`

	// PutDashboard
	_, err := client.PutDashboard(ctx, &cloudwatchsdk.PutDashboardInput{
		DashboardName: aws.String(dashName),
		DashboardBody: aws.String(dashBody),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = client.DeleteDashboards(ctx, &cloudwatchsdk.DeleteDashboardsInput{
			DashboardNames: []string{dashName},
		})
	})

	// GetDashboard
	getOut, err := client.GetDashboard(ctx, &cloudwatchsdk.GetDashboardInput{
		DashboardName: aws.String(dashName),
	})
	require.NoError(t, err)
	assert.Equal(t, dashName, aws.ToString(getOut.DashboardName))
	assert.Equal(t, dashBody, aws.ToString(getOut.DashboardBody))

	// ListDashboards - should find our dashboard
	listOut, err := client.ListDashboards(ctx, &cloudwatchsdk.ListDashboardsInput{
		DashboardNamePrefix: aws.String("test-dashboard-"),
	})
	require.NoError(t, err)
	found := false
	for _, e := range listOut.DashboardEntries {
		if aws.ToString(e.DashboardName) == dashName {
			found = true
			assert.NotNil(t, e.Size)
			assert.Positive(t, aws.ToInt64(e.Size))
			assert.NotNil(t, e.LastModified)
		}
	}
	assert.True(t, found, "expected dashboard %s in ListDashboards response", dashName)

	// DeleteDashboards
	_, err = client.DeleteDashboards(ctx, &cloudwatchsdk.DeleteDashboardsInput{
		DashboardNames: []string{dashName},
	})
	require.NoError(t, err)

	// ListDashboards - should no longer find it
	listAfter, err := client.ListDashboards(ctx, &cloudwatchsdk.ListDashboardsInput{
		DashboardNamePrefix: aws.String(dashName),
	})
	require.NoError(t, err)
	assert.Empty(t, listAfter.DashboardEntries)
}
