package cloudwatchlogs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwlsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// TestScheduledQuery_IdentifierRealClient covers gopherstack-wksweep-cwl-1:
// DeleteScheduledQuery, UpdateScheduledQuery, GetScheduledQuery, and
// GetScheduledQueryHistory all take an Identifier member on the real SDK
// (cloudwatchlogs@v1.81.1 api_op_{Delete,Update,Get,GetHistory}ScheduledQuery.go),
// not ScheduledQueryArn. A real client only ever sends "identifier" on the
// wire; before the fix, gopherstack read "scheduledQueryArn" instead, so
// these four ops could never resolve the query a real client asked for.
func TestScheduledQuery_IdentifierRealClient(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateScheduledQuery(ctx, &cwlsdk.CreateScheduledQueryInput{
		Name:               aws.String("sq-identifier-rt"),
		QueryString:        aws.String("fields @message | limit 100"),
		QueryLanguage:      types.QueryLanguageCwli,
		ScheduleExpression: aws.String("cron(0 * * * ? *)"),
		ExecutionRoleArn:   aws.String("arn:aws:iam::123456789012:role/r"),
	})
	require.NoError(t, err)
	arn := aws.ToString(created.ScheduledQueryArn)
	require.NotEmpty(t, arn)

	get, err := client.GetScheduledQuery(ctx, &cwlsdk.GetScheduledQueryInput{
		Identifier: aws.String(arn),
	})
	require.NoError(t, err, "GetScheduledQuery must resolve the query by the real Identifier member")
	assert.Equal(t, arn, aws.ToString(get.ScheduledQueryArn))
	assert.Equal(t, "sq-identifier-rt", aws.ToString(get.Name))

	_, err = client.UpdateScheduledQuery(ctx, &cwlsdk.UpdateScheduledQueryInput{
		Identifier:         aws.String(arn),
		ExecutionRoleArn:   aws.String("arn:aws:iam::123456789012:role/r"),
		QueryLanguage:      types.QueryLanguageCwli,
		QueryString:        aws.String("fields @message | limit 100"),
		ScheduleExpression: aws.String("cron(0 * * * ? *)"),
		State:              types.ScheduledQueryStateDisabled,
	})
	require.NoError(t, err, "UpdateScheduledQuery must resolve the query by the real Identifier member")

	updated, err := client.GetScheduledQuery(ctx, &cwlsdk.GetScheduledQueryInput{Identifier: aws.String(arn)})
	require.NoError(t, err)
	assert.Equal(t, types.ScheduledQueryStateDisabled, updated.State)

	_, err = client.GetScheduledQueryHistory(ctx, &cwlsdk.GetScheduledQueryHistoryInput{
		Identifier: aws.String(arn),
		StartTime:  aws.Int64(0),
		EndTime:    aws.Int64(9999999999),
	})
	require.NoError(t, err, "GetScheduledQueryHistory must resolve the query by the real Identifier member")

	_, err = client.DeleteScheduledQuery(ctx, &cwlsdk.DeleteScheduledQueryInput{Identifier: aws.String(arn)})
	require.NoError(t, err, "DeleteScheduledQuery must resolve the query by the real Identifier member")

	_, err = client.GetScheduledQuery(ctx, &cwlsdk.GetScheduledQueryInput{Identifier: aws.String(arn)})
	require.Error(t, err, "scheduled query must actually be gone after DeleteScheduledQuery")
}

// TestListLogAnomalyDetectors_FilterLogGroupArnRealClient covers
// gopherstack-wksweep-cwl-2: ListLogAnomalyDetectorsInput's real filter
// member is the singular FilterLogGroupArn *string (cloudwatchlogs@v1.81.1
// api_op_ListLogAnomalyDetectors.go), not a list. Before the fix, gopherstack
// read a nonexistent "filterLogGroupArnList" field, so a real client's
// filter was always silently dropped and every detector was returned
// regardless of the requested log group.
func TestListLogAnomalyDetectors_FilterLogGroupArnRealClient(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))
	ctx := t.Context()

	lg1, err := client.CreateLogGroup(ctx, &cwlsdk.CreateLogGroupInput{LogGroupName: aws.String("/flt/one")})
	require.NoError(t, err)
	_ = lg1

	lg2, err := client.CreateLogGroup(ctx, &cwlsdk.CreateLogGroupInput{LogGroupName: aws.String("/flt/two")})
	require.NoError(t, err)
	_ = lg2

	desc1, err := client.DescribeLogGroups(ctx, &cwlsdk.DescribeLogGroupsInput{
		LogGroupNamePrefix: aws.String("/flt/one"),
	})
	require.NoError(t, err)
	require.Len(t, desc1.LogGroups, 1)
	arn1 := aws.ToString(desc1.LogGroups[0].Arn)

	desc2, err := client.DescribeLogGroups(ctx, &cwlsdk.DescribeLogGroupsInput{
		LogGroupNamePrefix: aws.String("/flt/two"),
	})
	require.NoError(t, err)
	require.Len(t, desc2.LogGroups, 1)
	arn2 := aws.ToString(desc2.LogGroups[0].Arn)

	_, err = client.CreateLogAnomalyDetector(ctx, &cwlsdk.CreateLogAnomalyDetectorInput{
		LogGroupArnList: []string{arn1},
		DetectorName:    aws.String("det-one"),
	})
	require.NoError(t, err)

	_, err = client.CreateLogAnomalyDetector(ctx, &cwlsdk.CreateLogAnomalyDetectorInput{
		LogGroupArnList: []string{arn2},
		DetectorName:    aws.String("det-two"),
	})
	require.NoError(t, err)

	all, err := client.ListLogAnomalyDetectors(ctx, &cwlsdk.ListLogAnomalyDetectorsInput{})
	require.NoError(t, err)
	require.Len(t, all.AnomalyDetectors, 2, "sanity: both detectors exist before filtering")

	filtered, err := client.ListLogAnomalyDetectors(ctx, &cwlsdk.ListLogAnomalyDetectorsInput{
		FilterLogGroupArn: aws.String(arn1),
	})
	require.NoError(t, err)
	require.Len(t, filtered.AnomalyDetectors, 1,
		"FilterLogGroupArn must actually filter; pre-fix it was silently ignored and returned both")
	assert.Equal(t, "det-one", aws.ToString(filtered.AnomalyDetectors[0].DetectorName))
}
