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
	ctx := t.Context()

	ruleName := "audit-rule-" + uuid.NewString()[:8]
	ruleDef := `{"Schema":{"Name":"CloudWatchLogRule","Version":1},` +
		`"LogGroupNames":["/aws/audit/` + ruleName + `"],` +
		`"LogFormat":"JSON","Contribution":{"Keys":["$.requestId"]},` +
		`"AggregateOn":"Count"}`

	// Seed an insight rule so GetInsightRuleReport has a target to report on.
	_, err := client.PutInsightRule(ctx, &cloudwatchsdk.PutInsightRuleInput{
		RuleName:       aws.String(ruleName),
		RuleDefinition: aws.String(ruleDef),
	})
	require.NoError(t, err)

	// GetInsightRuleReport must call the backend via the CBOR path and return a
	// (possibly empty) Contributors list rather than erroring.
	out, err := client.GetInsightRuleReport(ctx, &cloudwatchsdk.GetInsightRuleReportInput{
		RuleName:  aws.String(ruleName),
		StartTime: aws.Time(time.Now().UTC().Add(-time.Hour)),
		EndTime:   aws.Time(time.Now().UTC()),
		Period:    aws.Int32(60),
	})
	require.NoError(t, err, "GetInsightRuleReport should not return 'unknown operation'")
	require.NotNil(t, out)
	// No log events were ingested so Contributors is empty.  The SDK deserialiser
	// uses var-append which leaves an empty CBOR list as a nil Go slice; assert
	// Empty (not NotNil) to accept both nil and [].
	assert.Empty(t, out.Contributors)
	assert.NotNil(t, out.AggregateValue, "AggregateValue must be present in response")
	assert.NotNil(t, out.AggregationStatistic, "AggregationStatistic must be present in response")
}
