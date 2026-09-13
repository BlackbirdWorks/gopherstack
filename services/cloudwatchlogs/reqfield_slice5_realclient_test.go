package cloudwatchlogs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwlsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

// TestCreateLogGroup_DeletionProtectionEnabled_SurvivesWireConversion proves
// CreateLogGroupInput's DeletionProtectionEnabled (reqfielddiff slice 5,
// gopherstack-xhu2t) is honoured at creation time by reusing the same
// enforcement PutLogGroupDeletionProtection already provides: DeleteLogGroup
// must refuse until protection is explicitly disabled.
func TestCreateLogGroup_DeletionProtectionEnabled_SurvivesWireConversion(t *testing.T) {
	t.Parallel()

	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend()))
	ctx := t.Context()

	_, err := client.CreateLogGroup(ctx, &cwlsdk.CreateLogGroupInput{
		LogGroupName:              aws.String("dp-at-create"),
		DeletionProtectionEnabled: aws.Bool(true),
	})
	require.NoError(t, err)

	_, err = client.DeleteLogGroup(ctx, &cwlsdk.DeleteLogGroupInput{
		LogGroupName: aws.String("dp-at-create"),
	})
	require.Error(t, err, "DeletionProtectionEnabled at CreateLogGroup time must block deletion")

	_, err = client.PutLogGroupDeletionProtection(ctx, &cwlsdk.PutLogGroupDeletionProtectionInput{
		LogGroupIdentifier:        aws.String("dp-at-create"),
		DeletionProtectionEnabled: aws.Bool(false),
	})
	require.NoError(t, err)

	_, err = client.DeleteLogGroup(ctx, &cwlsdk.DeleteLogGroupInput{
		LogGroupName: aws.String("dp-at-create"),
	})
	require.NoError(t, err, "deletion must succeed once protection is disabled")
}

// TestDescribeAndListLogGroups_LogGroupClass_FiltersRealState proves
// DescribeLogGroupsInput/ListLogGroupsInput's LogGroupClass filter
// (reqfielddiff slice 5) is applied against each log group's real,
// already-tracked class rather than ignored.
func TestDescribeAndListLogGroups_LogGroupClass_FiltersRealState(t *testing.T) {
	t.Parallel()

	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend()))
	ctx := t.Context()

	_, err := client.CreateLogGroup(ctx, &cwlsdk.CreateLogGroupInput{
		LogGroupName:  aws.String("lgc-standard"),
		LogGroupClass: cwltypes.LogGroupClassStandard,
	})
	require.NoError(t, err)

	_, err = client.CreateLogGroup(ctx, &cwlsdk.CreateLogGroupInput{
		LogGroupName:  aws.String("lgc-ia"),
		LogGroupClass: cwltypes.LogGroupClassInfrequentAccess,
	})
	require.NoError(t, err)

	descOut, err := client.DescribeLogGroups(ctx, &cwlsdk.DescribeLogGroupsInput{
		LogGroupClass: cwltypes.LogGroupClassInfrequentAccess,
	})
	require.NoError(t, err)
	require.Len(t, descOut.LogGroups, 1)
	assert.Equal(t, "lgc-ia", aws.ToString(descOut.LogGroups[0].LogGroupName))

	listOut, err := client.ListLogGroups(ctx, &cwlsdk.ListLogGroupsInput{
		LogGroupClass: cwltypes.LogGroupClassStandard,
	})
	require.NoError(t, err)
	require.Len(t, listOut.LogGroups, 1)
	assert.Equal(t, "lgc-standard", aws.ToString(listOut.LogGroups[0].LogGroupName))
}

// TestListScheduledQueries_ScheduleTypeAndState_FilterRealState proves
// ListScheduledQueriesInput's ScheduleType and State filters (reqfielddiff
// slice 5) apply against each scheduled query's real tracked state.
func TestListScheduledQueries_ScheduleTypeAndState_FilterRealState(t *testing.T) {
	t.Parallel()

	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend()))
	ctx := t.Context()

	mk := func(name string) *cwlsdk.CreateScheduledQueryOutput {
		out, err := client.CreateScheduledQuery(ctx, &cwlsdk.CreateScheduledQueryInput{
			Name:               aws.String(name),
			QueryString:        aws.String("fields @message"),
			QueryLanguage:      cwltypes.QueryLanguageCwli,
			ScheduleExpression: aws.String("cron(0 * * * ? *)"),
			ExecutionRoleArn:   aws.String("arn:aws:iam::123456789012:role/scheduled-query-role"),
		})
		require.NoError(t, err)

		return out
	}

	enabled := mk("sq-enabled")
	disabled := mk("sq-disabled")

	updateInput := func(identifier *string, state cwltypes.ScheduledQueryState) *cwlsdk.UpdateScheduledQueryInput {
		return &cwlsdk.UpdateScheduledQueryInput{
			Identifier:         identifier,
			State:              state,
			QueryString:        aws.String("fields @message"),
			QueryLanguage:      cwltypes.QueryLanguageCwli,
			ScheduleExpression: aws.String("cron(0 * * * ? *)"),
			ExecutionRoleArn:   aws.String("arn:aws:iam::123456789012:role/scheduled-query-role"),
		}
	}

	_, err := client.UpdateScheduledQuery(
		ctx, updateInput(enabled.ScheduledQueryArn, cwltypes.ScheduledQueryStateEnabled),
	)
	require.NoError(t, err)

	_, err = client.UpdateScheduledQuery(
		ctx, updateInput(disabled.ScheduledQueryArn, cwltypes.ScheduledQueryStateDisabled),
	)
	require.NoError(t, err)

	enabledOut, err := client.ListScheduledQueries(ctx, &cwlsdk.ListScheduledQueriesInput{
		State: cwltypes.ScheduledQueryStateEnabled,
	})
	require.NoError(t, err)
	require.Len(t, enabledOut.ScheduledQueries, 1)
	assert.Equal(t, "sq-enabled", aws.ToString(enabledOut.ScheduledQueries[0].Name))

	awsManagedOut, err := client.ListScheduledQueries(ctx, &cwlsdk.ListScheduledQueriesInput{
		ScheduleType: cwltypes.ScheduleTypeAwsManaged,
	})
	require.NoError(t, err)
	assert.Empty(
		t, awsManagedOut.ScheduledQueries,
		"this backend never creates AWS_MANAGED scheduled queries, so the filter must honestly return none",
	)

	customerManagedOut, err := client.ListScheduledQueries(ctx, &cwlsdk.ListScheduledQueriesInput{
		ScheduleType: cwltypes.ScheduleTypeCustomerManaged,
	})
	require.NoError(t, err)
	assert.Len(t, customerManagedOut.ScheduledQueries, 2)
}

// TestUpdateAnomaly_Baseline_SetsRealState proves UpdateAnomalyInput's
// Baseline field (reqfielddiff slice 5) actually moves the anomaly's State
// to Baseline, distinct from Active/Suppressed -- previously read nowhere.
func TestUpdateAnomaly_Baseline_SetsRealState(t *testing.T) {
	t.Parallel()

	backend := cloudwatchlogs.NewInMemoryBackend()
	client := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(backend))
	ctx := t.Context()

	detOut, err := client.CreateLogAnomalyDetector(ctx, &cwlsdk.CreateLogAnomalyDetectorInput{
		LogGroupArnList: []string{"arn:aws:logs:us-east-1:000000000000:log-group:baseline-src"},
	})
	require.NoError(t, err)

	backend.AddAnomalyInternal(cloudwatchlogs.Anomaly{
		AnomalyDetectorArn: *detOut.AnomalyDetectorArn,
		AnomalyID:          "anomaly-baseline",
		Active:             true,
	})

	_, err = client.UpdateAnomaly(ctx, &cwlsdk.UpdateAnomalyInput{
		AnomalyDetectorArn: detOut.AnomalyDetectorArn,
		AnomalyId:          aws.String("anomaly-baseline"),
		Baseline:           aws.Bool(true),
	})
	require.NoError(t, err)

	listOut, err := client.ListAnomalies(ctx, &cwlsdk.ListAnomaliesInput{
		AnomalyDetectorArn: detOut.AnomalyDetectorArn,
	})
	require.NoError(t, err)
	require.Len(t, listOut.Anomalies, 1)
	assert.Equal(t, cwltypes.StateBaseline, listOut.Anomalies[0].State)
}
