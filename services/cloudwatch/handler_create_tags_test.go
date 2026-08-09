package cloudwatch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPutOpsWithTags_RoundTrip drives every cloudwatch Put* op whose real
// Input struct accepts Tags (cloudwatch@v1.66.3: api_op_PutMetricAlarm.go,
// api_op_PutCompositeAlarm.go, api_op_PutDashboard.go,
// api_op_PutInsightRule.go, api_op_PutLogAlarm.go,
// api_op_PutMetricStream.go, all `Tags []types.Tag`) through the real SDK
// client and asserts ListTagsForResource sees what was supplied at creation
// (gopherstack-2mwl). PutAlarmMuteRule is excluded: its wire shape has
// diverged entirely from this SDK version (Name/Rule/MuteTargets, not the
// MuteName/AlarmNames/MuteDuration gopherstack implements), tracked
// separately from this issue's decode-drop class.
func TestPutOpsWithTags_RoundTrip(t *testing.T) {
	t.Parallel()

	wantTags := []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}}

	t.Run("metric alarm", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		_, err := client.PutMetricAlarm(t.Context(), &cwsdk.PutMetricAlarmInput{
			AlarmName:          aws.String("tagged-metric-alarm"),
			Namespace:          aws.String("NS"),
			MetricName:         aws.String("M"),
			ComparisonOperator: types.ComparisonOperatorGreaterThanThreshold,
			EvaluationPeriods:  aws.Int32(1),
			Period:             aws.Int32(60),
			Statistic:          types.StatisticAverage,
			Threshold:          aws.Float64(1),
			Tags:               wantTags,
		})
		require.NoError(t, err)

		out, err := client.DescribeAlarms(t.Context(), &cwsdk.DescribeAlarmsInput{
			AlarmNames: []string{"tagged-metric-alarm"},
		})
		require.NoError(t, err)
		require.Len(t, out.MetricAlarms, 1)

		got, err := client.ListTagsForResource(t.Context(), &cwsdk.ListTagsForResourceInput{
			ResourceARN: out.MetricAlarms[0].AlarmArn,
		})
		require.NoError(t, err)
		assert.Equal(t, wantTags, got.Tags)
	})

	t.Run("composite alarm", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		_, err := client.PutCompositeAlarm(t.Context(), &cwsdk.PutCompositeAlarmInput{
			AlarmName: aws.String("tagged-composite-alarm"),
			AlarmRule: aws.String(`ALARM("some-child-alarm")`),
			Tags:      wantTags,
		})
		require.NoError(t, err)

		out, err := client.DescribeAlarms(t.Context(), &cwsdk.DescribeAlarmsInput{
			AlarmNames: []string{"tagged-composite-alarm"},
			AlarmTypes: []types.AlarmType{types.AlarmTypeCompositeAlarm},
		})
		require.NoError(t, err)
		require.Len(t, out.CompositeAlarms, 1)

		got, err := client.ListTagsForResource(t.Context(), &cwsdk.ListTagsForResourceInput{
			ResourceARN: out.CompositeAlarms[0].AlarmArn,
		})
		require.NoError(t, err)
		assert.Equal(t, wantTags, got.Tags)
	})

	t.Run("dashboard", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		_, err := client.PutDashboard(t.Context(), &cwsdk.PutDashboardInput{
			DashboardName: aws.String("tagged-dashboard"),
			DashboardBody: aws.String(`{"widgets":[]}`),
			Tags:          wantTags,
		})
		require.NoError(t, err)

		out, err := client.GetDashboard(t.Context(), &cwsdk.GetDashboardInput{
			DashboardName: aws.String("tagged-dashboard"),
		})
		require.NoError(t, err)

		got, err := client.ListTagsForResource(t.Context(), &cwsdk.ListTagsForResourceInput{
			ResourceARN: out.DashboardArn,
		})
		require.NoError(t, err)
		assert.Equal(t, wantTags, got.Tags)
	})

	t.Run("insight rule", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		ruleDef := `{"Schema":{"Name":"CloudWatchLogRule","Version":1},"LogFormat":"JSON",` +
			`"LogGroupNames":["lg"],"Fields":{"@message":"Message"},"Contribution":{"Keys":["@message"]},"AggregateOn":"Count"}`

		_, err := client.PutInsightRule(t.Context(), &cwsdk.PutInsightRuleInput{
			RuleName:       aws.String("tagged-insight-rule"),
			RuleDefinition: aws.String(ruleDef),
			Tags:           wantTags,
		})
		require.NoError(t, err)

		// DescribeInsightRules' real response has no ARN member (rules are
		// identified by RuleName everywhere except Tag* ops); gopherstack
		// builds the ARN deterministically (insight_rules.go:
		// arn.Build(...,"insight-rule/"+name)), so it's reconstructed here.
		ruleARN := "arn:aws:cloudwatch:" + rtTestRegion + ":000000000000:insight-rule/tagged-insight-rule"

		got, err := client.ListTagsForResource(t.Context(), &cwsdk.ListTagsForResourceInput{
			ResourceARN: aws.String(ruleARN),
		})
		require.NoError(t, err)
		assert.Equal(t, wantTags, got.Tags)
	})

	t.Run("log alarm", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		_, err := client.PutLogAlarm(t.Context(), &cwsdk.PutLogAlarmInput{
			AlarmName:              aws.String("tagged-log-alarm"),
			ComparisonOperator:     types.ComparisonOperatorGreaterThanThreshold,
			QueryResultsToAlarm:    aws.Int32(1),
			QueryResultsToEvaluate: aws.Int32(1),
			Threshold:              aws.Float64(1),
			ScheduledQueryConfiguration: &types.ScheduledQueryConfiguration{
				AggregationExpression: aws.String("count(*)"),
				QueryString:           aws.String("fields @message"),
				ScheduledQueryRoleARN: aws.String("arn:aws:iam::000000000000:role/cw-role"),
				ScheduleConfiguration: &types.ScheduleConfiguration{
					ScheduleExpression: aws.String("rate(5 minutes)"),
					StartTimeOffset:    aws.Int64(300),
				},
			},
			Tags: wantTags,
		})
		require.NoError(t, err)

		out, err := client.DescribeAlarms(t.Context(), &cwsdk.DescribeAlarmsInput{
			AlarmNames: []string{"tagged-log-alarm"},
			AlarmTypes: []types.AlarmType{types.AlarmTypeLogAlarm},
		})
		require.NoError(t, err)
		require.Len(t, out.LogAlarms, 1)

		got, err := client.ListTagsForResource(t.Context(), &cwsdk.ListTagsForResourceInput{
			ResourceARN: out.LogAlarms[0].AlarmArn,
		})
		require.NoError(t, err)
		assert.Equal(t, wantTags, got.Tags)
	})

	t.Run("metric stream", func(t *testing.T) {
		t.Parallel()

		client := newTestHandlerAndClient(t)

		_, err := client.PutMetricStream(t.Context(), &cwsdk.PutMetricStreamInput{
			Name:         aws.String("tagged-metric-stream"),
			FirehoseArn:  aws.String("arn:aws:firehose:us-east-1:000000000000:deliverystream/ds"),
			RoleArn:      aws.String("arn:aws:iam::000000000000:role/cw-role"),
			OutputFormat: types.MetricStreamOutputFormatJson,
			Tags:         wantTags,
		})
		require.NoError(t, err)

		out, err := client.GetMetricStream(t.Context(), &cwsdk.GetMetricStreamInput{
			Name: aws.String("tagged-metric-stream"),
		})
		require.NoError(t, err)

		got, err := client.ListTagsForResource(t.Context(), &cwsdk.ListTagsForResourceInput{
			ResourceARN: out.Arn,
		})
		require.NoError(t, err)
		assert.Equal(t, wantTags, got.Tags)
	})
}
