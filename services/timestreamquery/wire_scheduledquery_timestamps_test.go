package timestreamquery_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	tqsdk "github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	"github.com/aws/aws-sdk-go-v2/service/timestreamquery/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test_SDKRoundTrip_ScheduledQuery_Timestamps drives CreateScheduledQuery,
// DescribeScheduledQuery, and ListScheduledQueries through the real SDK
// client and asserts every epoch-seconds timestamp member of
// ScheduledQueryDescription/ScheduledQuery decodes non-nil
// (aws-sdk-go-v2/service/timestreamquery@v1.39.4/types/types.go
// CreationTime/NextInvocationTime; the deserializer's "CreationTime"/
// "NextInvocationTime" cases in deserializers.go parse a JSON Number via
// smithytime.ParseEpochSeconds). gopherstack-1ai8's field-by-field audit
// found this family already correctly emitted as float64 epoch seconds
// (models.go ScheduledQueryListEntry/scheduledQueryToView's "CreationTime"
// key via the epochSeconds helper) -- this test exists to verify that
// finding against the real client rather than leave it asserted only by
// eyeballing the source.
func Test_SDKRoundTrip_ScheduledQuery_Timestamps(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)
	ctx := t.Context()

	created, err := client.CreateScheduledQuery(ctx, &tqsdk.CreateScheduledQueryInput{
		Name:                           aws.String("sq-" + uuid.NewString()[:8]),
		QueryString:                    aws.String("SELECT 1"),
		ScheduledQueryExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/tsq-role"),
		ScheduleConfiguration: &types.ScheduleConfiguration{
			ScheduleExpression: aws.String("rate(1 hour)"),
		},
		NotificationConfiguration: &types.NotificationConfiguration{
			SnsConfiguration: &types.SnsConfiguration{
				TopicArn: aws.String("arn:aws:sns:us-east-1:000000000000:tsq-topic"),
			},
		},
		ErrorReportConfiguration: &types.ErrorReportConfiguration{
			S3Configuration: &types.S3Configuration{
				BucketName: aws.String("tsq-error-bucket"),
			},
		},
	})
	require.NoError(t, err)

	described, err := client.DescribeScheduledQuery(ctx, &tqsdk.DescribeScheduledQueryInput{
		ScheduledQueryArn: created.Arn,
	})
	require.NoError(t, err)
	require.NotNil(t, described.ScheduledQuery)
	require.NotNil(
		t,
		described.ScheduledQuery.CreationTime,
		"DescribeScheduledQuery must decode a non-nil CreationTime",
	)
	assert.NotZero(t, *described.ScheduledQuery.CreationTime)
	require.NotNil(
		t, described.ScheduledQuery.NextInvocationTime,
		"DescribeScheduledQuery must decode a non-nil NextInvocationTime",
	)
	assert.NotZero(t, *described.ScheduledQuery.NextInvocationTime)

	listed, err := client.ListScheduledQueries(ctx, &tqsdk.ListScheduledQueriesInput{})
	require.NoError(t, err)
	require.Len(t, listed.ScheduledQueries, 1)
	require.NotNil(
		t,
		listed.ScheduledQueries[0].CreationTime,
		"ListScheduledQueries must decode a non-nil CreationTime",
	)
	assert.NotZero(t, *listed.ScheduledQueries[0].CreationTime)

	_, err = client.ExecuteScheduledQuery(ctx, &tqsdk.ExecuteScheduledQueryInput{
		ScheduledQueryArn: created.Arn,
		InvocationTime:    aws.Time(*described.ScheduledQuery.NextInvocationTime),
	})
	require.NoError(t, err)

	afterRun, err := client.DescribeScheduledQuery(ctx, &tqsdk.DescribeScheduledQueryInput{
		ScheduledQueryArn: created.Arn,
	})
	require.NoError(t, err)
	require.NotNil(t, afterRun.ScheduledQuery.LastRunSummary)
	require.NotNil(
		t, afterRun.ScheduledQuery.LastRunSummary.InvocationTime,
		"LastRunSummary must decode a non-nil InvocationTime after a run",
	)
	assert.NotZero(t, *afterRun.ScheduledQuery.LastRunSummary.InvocationTime)
	require.NotNil(
		t, afterRun.ScheduledQuery.LastRunSummary.TriggerTime,
		"LastRunSummary must decode a non-nil TriggerTime after a run",
	)
	assert.NotZero(t, *afterRun.ScheduledQuery.LastRunSummary.TriggerTime)
	require.NotNil(
		t, afterRun.ScheduledQuery.PreviousInvocationTime,
		"DescribeScheduledQuery must decode a non-nil PreviousInvocationTime after a run",
	)
	assert.NotZero(t, *afterRun.ScheduledQuery.PreviousInvocationTime)
}
