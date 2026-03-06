package integration_test

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudwatchlogssdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwlogstypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	kinesissdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// decompressGzip decompresses gzip-compressed bytes.
func decompressGzip(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return io.ReadAll(r)
}

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

// TestIntegration_CloudWatchLogs_SubscriptionFilter_CRUD verifies that subscription
// filters can be created, listed, and deleted via the AWS SDK.
func TestIntegration_CloudWatchLogs_SubscriptionFilter_CRUD(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	cwlClient := createCloudWatchLogsClient(t)
	ctx := t.Context()

	groupName := "/test/sub-filter-grp-" + uuid.NewString()[:8]
	filterName := "my-filter-" + uuid.NewString()[:8]
	destARN := "arn:aws:lambda:us-east-1:123456789012:function:dummy-fn"

	// Create log group.
	_, err := cwlClient.CreateLogGroup(ctx, &cloudwatchlogssdk.CreateLogGroupInput{
		LogGroupName: aws.String(groupName),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = cwlClient.DeleteLogGroup(ctx, &cloudwatchlogssdk.DeleteLogGroupInput{
			LogGroupName: aws.String(groupName),
		})
	})

	// PutSubscriptionFilter — create.
	_, err = cwlClient.PutSubscriptionFilter(ctx, &cloudwatchlogssdk.PutSubscriptionFilterInput{
		LogGroupName:   aws.String(groupName),
		FilterName:     aws.String(filterName),
		FilterPattern:  aws.String(""),
		DestinationArn: aws.String(destARN),
	})
	require.NoError(t, err)

	// DescribeSubscriptionFilters — verify the filter exists.
	descOut, err := cwlClient.DescribeSubscriptionFilters(ctx, &cloudwatchlogssdk.DescribeSubscriptionFiltersInput{
		LogGroupName: aws.String(groupName),
	})
	require.NoError(t, err)
	require.Len(t, descOut.SubscriptionFilters, 1)
	assert.Equal(t, filterName, aws.ToString(descOut.SubscriptionFilters[0].FilterName))
	assert.Equal(t, destARN, aws.ToString(descOut.SubscriptionFilters[0].DestinationArn))
	assert.Equal(t, groupName, aws.ToString(descOut.SubscriptionFilters[0].LogGroupName))

	// PutSubscriptionFilter — update (same name, new pattern).
	_, err = cwlClient.PutSubscriptionFilter(ctx, &cloudwatchlogssdk.PutSubscriptionFilterInput{
		LogGroupName:   aws.String(groupName),
		FilterName:     aws.String(filterName),
		FilterPattern:  aws.String("ERROR"),
		DestinationArn: aws.String(destARN),
	})
	require.NoError(t, err)

	// Verify the update is reflected.
	descAfterUpdate, err := cwlClient.DescribeSubscriptionFilters(ctx,
		&cloudwatchlogssdk.DescribeSubscriptionFiltersInput{LogGroupName: aws.String(groupName)})
	require.NoError(t, err)
	require.Len(t, descAfterUpdate.SubscriptionFilters, 1)
	assert.Equal(t, "ERROR", aws.ToString(descAfterUpdate.SubscriptionFilters[0].FilterPattern))

	// DescribeSubscriptionFilters with filterNamePrefix — matching.
	descPrefixOut, err := cwlClient.DescribeSubscriptionFilters(ctx,
		&cloudwatchlogssdk.DescribeSubscriptionFiltersInput{
			LogGroupName:     aws.String(groupName),
			FilterNamePrefix: aws.String(filterName[:5]),
		})
	require.NoError(t, err)
	assert.Len(t, descPrefixOut.SubscriptionFilters, 1)

	// DescribeSubscriptionFilters with filterNamePrefix — non-matching.
	descNoMatchOut, err := cwlClient.DescribeSubscriptionFilters(ctx,
		&cloudwatchlogssdk.DescribeSubscriptionFiltersInput{
			LogGroupName:     aws.String(groupName),
			FilterNamePrefix: aws.String("zzz-nomatch"),
		})
	require.NoError(t, err)
	assert.Empty(t, descNoMatchOut.SubscriptionFilters)

	// DeleteSubscriptionFilter.
	_, err = cwlClient.DeleteSubscriptionFilter(ctx, &cloudwatchlogssdk.DeleteSubscriptionFilterInput{
		LogGroupName: aws.String(groupName),
		FilterName:   aws.String(filterName),
	})
	require.NoError(t, err)

	// Verify filter is gone.
	descAfterDelete, err := cwlClient.DescribeSubscriptionFilters(ctx,
		&cloudwatchlogssdk.DescribeSubscriptionFiltersInput{LogGroupName: aws.String(groupName)})
	require.NoError(t, err)
	assert.Empty(t, descAfterDelete.SubscriptionFilters)
}

// TestIntegration_CloudWatchLogs_SubscriptionFilter_LimitEnforced verifies that
// the AWS limit of 2 subscription filters per log group is enforced.
func TestIntegration_CloudWatchLogs_SubscriptionFilter_LimitEnforced(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	cwlClient := createCloudWatchLogsClient(t)
	ctx := t.Context()

	groupName := "/test/sub-filter-limit-" + uuid.NewString()[:8]
	destARN := "arn:aws:lambda:us-east-1:123456789012:function:dummy-fn"

	_, err := cwlClient.CreateLogGroup(ctx, &cloudwatchlogssdk.CreateLogGroupInput{
		LogGroupName: aws.String(groupName),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = cwlClient.DeleteLogGroup(ctx, &cloudwatchlogssdk.DeleteLogGroupInput{
			LogGroupName: aws.String(groupName),
		})
	})

	// Add first filter.
	_, err = cwlClient.PutSubscriptionFilter(ctx, &cloudwatchlogssdk.PutSubscriptionFilterInput{
		LogGroupName:   aws.String(groupName),
		FilterName:     aws.String("filter-1"),
		FilterPattern:  aws.String(""),
		DestinationArn: aws.String(destARN),
	})
	require.NoError(t, err)

	// Add second filter.
	_, err = cwlClient.PutSubscriptionFilter(ctx, &cloudwatchlogssdk.PutSubscriptionFilterInput{
		LogGroupName:   aws.String(groupName),
		FilterName:     aws.String("filter-2"),
		FilterPattern:  aws.String(""),
		DestinationArn: aws.String(destARN),
	})
	require.NoError(t, err)

	// Third filter should fail with LimitExceededException.
	_, err = cwlClient.PutSubscriptionFilter(ctx, &cloudwatchlogssdk.PutSubscriptionFilterInput{
		LogGroupName:   aws.String(groupName),
		FilterName:     aws.String("filter-3"),
		FilterPattern:  aws.String(""),
		DestinationArn: aws.String(destARN),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "LimitExceededException")
}

// TestIntegration_CloudWatchLogs_SubscriptionFilter_KinesisDelivery verifies that
// PutLogEvents with a Kinesis destination subscription filter delivers records to the stream.
func TestIntegration_CloudWatchLogs_SubscriptionFilter_KinesisDelivery(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	cwlClient := createCloudWatchLogsClient(t)
	kinesisClient := createKinesisClient(t)
	ctx := t.Context()

	groupName := "/test/sub-kinesis-grp-" + uuid.NewString()[:8]
	streamName := "sub-kinesis-stream-" + uuid.NewString()[:8]
	logStreamName := "log-stream-" + uuid.NewString()[:8]
	filterName := "kinesis-filter-" + uuid.NewString()[:8]

	// Create Kinesis stream.
	_, err := kinesisClient.CreateStream(ctx, &kinesissdk.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = kinesisClient.DeleteStream(ctx, &kinesissdk.DeleteStreamInput{StreamName: aws.String(streamName)})
	})

	// Get Kinesis stream ARN.
	descKinesis, err := kinesisClient.DescribeStream(ctx, &kinesissdk.DescribeStreamInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
	kinesisARN := aws.ToString(descKinesis.StreamDescription.StreamARN)
	require.NotEmpty(t, kinesisARN)

	// Create CloudWatch Logs log group and stream.
	_, err = cwlClient.CreateLogGroup(ctx, &cloudwatchlogssdk.CreateLogGroupInput{
		LogGroupName: aws.String(groupName),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = cwlClient.DeleteLogGroup(ctx, &cloudwatchlogssdk.DeleteLogGroupInput{
			LogGroupName: aws.String(groupName),
		})
	})

	_, err = cwlClient.CreateLogStream(ctx, &cloudwatchlogssdk.CreateLogStreamInput{
		LogGroupName:  aws.String(groupName),
		LogStreamName: aws.String(logStreamName),
	})
	require.NoError(t, err)

	// Put a subscription filter targeting the Kinesis stream.
	_, err = cwlClient.PutSubscriptionFilter(ctx, &cloudwatchlogssdk.PutSubscriptionFilterInput{
		LogGroupName:   aws.String(groupName),
		FilterName:     aws.String(filterName),
		FilterPattern:  aws.String(""),
		DestinationArn: aws.String(kinesisARN),
	})
	require.NoError(t, err)

	// Put log events — this should trigger delivery to Kinesis.
	_, err = cwlClient.PutLogEvents(ctx, &cloudwatchlogssdk.PutLogEventsInput{
		LogGroupName:  aws.String(groupName),
		LogStreamName: aws.String(logStreamName),
		LogEvents: []cwlogstypes.InputLogEvent{
			{Message: aws.String("delivery test message"), Timestamp: aws.Int64(time.Now().UnixMilli())},
		},
	})
	require.NoError(t, err)

	// Poll Kinesis for the record (delivery is async).
	var recordData []byte
	require.Eventually(t, func() bool {
		iterOut, iterErr := kinesisClient.GetShardIterator(ctx, &kinesissdk.GetShardIteratorInput{
			StreamName:        aws.String(streamName),
			ShardId:           aws.String("shardId-000000000000"),
			ShardIteratorType: "TRIM_HORIZON",
		})
		if iterErr != nil {
			return false
		}

		recOut, recErr := kinesisClient.GetRecords(ctx, &kinesissdk.GetRecordsInput{
			ShardIterator: iterOut.ShardIterator,
			Limit:         aws.Int32(10),
		})
		if recErr != nil || len(recOut.Records) == 0 {
			return false
		}

		recordData = recOut.Records[0].Data

		return true
	}, 5*time.Second, 100*time.Millisecond, "expected Kinesis record from subscription filter delivery")

	// Decode: it's a base64 string (the gzip+base64 payload encoded as JSON string by the delivery).
	payloadStr := string(recordData)

	decoded, decodeErr := base64.StdEncoding.DecodeString(payloadStr)
	require.NoError(t, decodeErr, "expected base64-encoded payload, got: %q", payloadStr)

	// Decompress gzip.
	decompressed, decompErr := decompressGzip(decoded)
	require.NoError(t, decompErr)

	// Verify the payload matches the CloudWatch Logs subscription format.
	var payload map[string]any
	require.NoError(t, json.Unmarshal(decompressed, &payload))
	assert.Equal(t, "DATA_MESSAGE", payload["messageType"])
	assert.Equal(t, groupName, payload["logGroup"])
	assert.Equal(t, logStreamName, payload["logStream"])
	logEvents, ok := payload["logEvents"].([]any)
	require.True(t, ok, "logEvents should be an array")
	require.NotEmpty(t, logEvents)
}
