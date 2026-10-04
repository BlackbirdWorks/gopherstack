package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudwatchsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	cloudwatchlogssdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwlogstypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The aws-sdk-go-v2 cloudwatch client uses the rpc-v2-cbor protocol by default,
// so each call below exercises the CBOR dispatch path in services/cloudwatch.
// These tests guard against the parity gaps where a working form/XML handler
// existed but the CBOR dispatch case was missing or broken (which surfaced as an
// "unknown operation" error or a hanging request through the SDK).

func TestIntegration_CloudWatchAudit_ManagedInsightRules(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createCloudWatchClient(t)
	ctx := t.Context()

	resourceARN := "arn:aws:dynamodb:us-east-1:000000000000:table/audit-" + uuid.NewString()[:8]

	// PutManagedInsightRules must dispatch through the CBOR path without erroring.
	_, err := client.PutManagedInsightRules(ctx, &cloudwatchsdk.PutManagedInsightRulesInput{
		ManagedRules: []cwtypes.ManagedRule{
			{
				ResourceARN:  aws.String(resourceARN),
				TemplateName: aws.String("DynamoDBContributorInsights-Account"),
			},
		},
	})
	require.NoError(t, err, "PutManagedInsightRules should not return 'unknown operation'")

	// ListManagedInsightRules must dispatch through the CBOR path without erroring.
	out, err := client.ListManagedInsightRules(ctx, &cloudwatchsdk.ListManagedInsightRulesInput{
		ResourceARN: aws.String(resourceARN),
	})
	require.NoError(t, err, "ListManagedInsightRules should not return 'unknown operation'")
	assert.NotNil(t, out)
}

func TestIntegration_CloudWatchAudit_GetMetricWidgetImage(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createCloudWatchClient(t)
	ctx := t.Context()

	widget := `{"metrics":[["AWS/EC2","CPUUtilization"]],"width":600,"height":400}`

	out, err := client.GetMetricWidgetImage(ctx, &cloudwatchsdk.GetMetricWidgetImageInput{
		MetricWidget: aws.String(widget),
	})
	require.NoError(t, err, "GetMetricWidgetImage should not return 'unknown operation'")
	require.NotNil(t, out)
	// The emulator returns a minimal but valid PNG as a raw blob over CBOR.
	assert.NotEmpty(t, out.MetricWidgetImage)
}

func TestIntegration_CloudWatchAudit_ListAlarmMuteRules(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createCloudWatchClient(t)
	ctx := t.Context()

	out, err := client.ListAlarmMuteRules(ctx, &cloudwatchsdk.ListAlarmMuteRulesInput{})
	require.NoError(t, err, "ListAlarmMuteRules should not return 'unknown operation'")
	assert.NotNil(t, out)
}

func TestIntegration_CloudWatchAudit_GetInsightRuleReport(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createCloudWatchClient(t)
	logs := createCloudWatchLogsClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	group := "/aws/audit/" + suffix
	ruleName := "audit-rule-" + suffix
	ruleDef := `{"Schema":{"Name":"CloudWatchLogRule","Version":1},` +
		`"LogGroupNames":["` + group + `"],` +
		`"LogFormat":"JSON","Contribution":{"Keys":["$.requestId"]},` +
		`"AggregateOn":"Count"}`

	_, err := logs.CreateLogGroup(ctx, &cloudwatchlogssdk.CreateLogGroupInput{LogGroupName: aws.String(group)})
	require.NoError(t, err)
	_, err = logs.CreateLogStream(ctx, &cloudwatchlogssdk.CreateLogStreamInput{
		LogGroupName: aws.String(group), LogStreamName: aws.String("s"),
	})
	require.NoError(t, err)

	now := time.Now().UTC()
	events := make([]cwlogstypes.InputLogEvent, 0, 3)

	for _, id := range []string{"req-a", "req-a", "req-b"} {
		events = append(events, cwlogstypes.InputLogEvent{
			Message: aws.String(`{"requestId":"` + id + `"}`), Timestamp: aws.Int64(now.UnixMilli()),
		})
	}

	_, err = logs.PutLogEvents(ctx, &cloudwatchlogssdk.PutLogEventsInput{
		LogGroupName: aws.String(group), LogStreamName: aws.String("s"), LogEvents: events,
	})
	require.NoError(t, err)

	_, err = client.PutInsightRule(ctx, &cloudwatchsdk.PutInsightRuleInput{
		RuleName:       aws.String(ruleName),
		RuleDefinition: aws.String(ruleDef),
	})
	require.NoError(t, err)

	out, err := client.GetInsightRuleReport(ctx, &cloudwatchsdk.GetInsightRuleReportInput{
		RuleName:  aws.String(ruleName),
		StartTime: aws.Time(now.Add(-time.Hour)),
		EndTime:   aws.Time(now.Add(time.Hour)),
		Period:    aws.Int32(60),
	})
	require.NoError(t, err)
	require.Len(t, out.Contributors, 2)
	assert.Equal(t, []string{"req-a"}, out.Contributors[0].Keys)
	assert.InDelta(t, 2, aws.ToFloat64(out.Contributors[0].ApproximateAggregateValue), 0)
	assert.Equal(t, []string{"req-b"}, out.Contributors[1].Keys)
	assert.InDelta(t, 3, aws.ToFloat64(out.AggregateValue), 0)
	assert.EqualValues(t, 2, aws.ToInt64(out.ApproximateUniqueCount))
	assert.Equal(t, "COUNT", aws.ToString(out.AggregationStatistic))
	assert.Equal(t, []string{"$.requestId"}, out.KeyLabels)
}
