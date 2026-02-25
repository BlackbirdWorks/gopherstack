package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudwatchlogssdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwlogstypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_CloudWatchLogs_Lifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createCloudWatchLogsClient(t)
	ctx := t.Context()

	groupName := "/test/log-group-" + uuid.NewString()[:8]
	streamName := "test-stream-" + uuid.NewString()[:8]

	// CreateLogGroup
	_, err := client.CreateLogGroup(ctx, &cloudwatchlogssdk.CreateLogGroupInput{LogGroupName: aws.String(groupName)})
	require.NoError(t, err)

	// DescribeLogGroups
	descGroups, err := client.DescribeLogGroups(
		ctx,
		&cloudwatchlogssdk.DescribeLogGroupsInput{LogGroupNamePrefix: aws.String(groupName)},
	)
	require.NoError(t, err)
	require.NotEmpty(t, descGroups.LogGroups)

	// CreateLogStream
	_, err = client.CreateLogStream(ctx, &cloudwatchlogssdk.CreateLogStreamInput{
		LogGroupName:  aws.String(groupName),
		LogStreamName: aws.String(streamName),
	})
	require.NoError(t, err)

	// PutLogEvents
	_, err = client.PutLogEvents(ctx, &cloudwatchlogssdk.PutLogEventsInput{
		LogGroupName:  aws.String(groupName),
		LogStreamName: aws.String(streamName),
		LogEvents: []cwlogstypes.InputLogEvent{
			{Message: aws.String("test message 1"), Timestamp: aws.Int64(time.Now().UnixMilli())},
			{Message: aws.String("test message 2"), Timestamp: aws.Int64(time.Now().UnixMilli())},
		},
	})
	require.NoError(t, err)

	// GetLogEvents
	getOut, err := client.GetLogEvents(ctx, &cloudwatchlogssdk.GetLogEventsInput{
		LogGroupName:  aws.String(groupName),
		LogStreamName: aws.String(streamName),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, getOut.Events)

	// FilterLogEvents
	filterOut, err := client.FilterLogEvents(ctx, &cloudwatchlogssdk.FilterLogEventsInput{
		LogGroupName: aws.String(groupName),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, filterOut.Events)

	// DeleteLogGroup
	_, err = client.DeleteLogGroup(ctx, &cloudwatchlogssdk.DeleteLogGroupInput{LogGroupName: aws.String(groupName)})
	require.NoError(t, err)
}
