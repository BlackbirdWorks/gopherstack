package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudwatchlogssdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwlogstypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	firehosesdk "github.com/aws/aws-sdk-go-v2/service/firehose"
	firehosetypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
	s3svc "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_CWLogs_Firehose_SubscriptionReceipt creates a CloudWatch Logs group,
// a Firehose delivery stream (S3 destination), and a subscription filter wiring them
// together. It then puts log events and verifies the Firehose stream received records
// via S3 delivery (receipt at the destination layer).
func TestIntegration_CWLogs_Firehose_SubscriptionReceipt(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	cwlClient := createCloudWatchLogsClient(t)
	fhClient := createFirehoseClient(t)
	s3Client := createS3Client(t)

	id := uuid.NewString()[:8]
	groupName := "/cwlogs-firehose-receipt/" + id
	streamName := "cwlogs-stream-" + id
	bucketName := "cwlogs-fh-bucket-" + id
	fhStreamName := "cwlogs-fh-" + id
	filterName := "fh-filter-" + id

	// Create S3 bucket for Firehose destination.
	_, err := s3Client.CreateBucket(ctx, &s3svc.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	require.NoError(t, err, "CreateBucket should succeed")
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		// Empty the bucket before deleting.
		listOut, listErr := s3Client.ListObjectsV2(cleanupCtx, &s3svc.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
		})
		if listErr == nil {
			for _, obj := range listOut.Contents {
				_, _ = s3Client.DeleteObject(cleanupCtx, &s3svc.DeleteObjectInput{
					Bucket: aws.String(bucketName),
					Key:    obj.Key,
				})
			}
		}

		_, _ = s3Client.DeleteBucket(cleanupCtx, &s3svc.DeleteBucketInput{Bucket: aws.String(bucketName)})
	})

	// Create Firehose delivery stream with S3 destination.
	bucketARN := "arn:aws:s3:::" + bucketName
	roleARN := "arn:aws:iam::000000000000:role/firehose-role"
	createFHOut, err := fhClient.CreateDeliveryStream(ctx, &firehosesdk.CreateDeliveryStreamInput{
		DeliveryStreamName: aws.String(fhStreamName),
		DeliveryStreamType: firehosetypes.DeliveryStreamTypeDirectPut,
		ExtendedS3DestinationConfiguration: &firehosetypes.ExtendedS3DestinationConfiguration{
			BucketARN: aws.String(bucketARN),
			RoleARN:   aws.String(roleARN),
			BufferingHints: &firehosetypes.BufferingHints{
				IntervalInSeconds: aws.Int32(1),
				SizeInMBs:         aws.Int32(128),
			},
		},
	})
	require.NoError(t, err, "CreateDeliveryStream should succeed")
	fhARN := aws.ToString(createFHOut.DeliveryStreamARN)
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = fhClient.DeleteDeliveryStream(cleanupCtx, &firehosesdk.DeleteDeliveryStreamInput{
			DeliveryStreamName: aws.String(fhStreamName),
		})
	})

	// Create CloudWatch Logs group and stream.
	_, err = cwlClient.CreateLogGroup(ctx, &cloudwatchlogssdk.CreateLogGroupInput{
		LogGroupName: aws.String(groupName),
	})
	require.NoError(t, err, "CreateLogGroup should succeed")
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = cwlClient.DeleteLogGroup(cleanupCtx, &cloudwatchlogssdk.DeleteLogGroupInput{
			LogGroupName: aws.String(groupName),
		})
	})

	_, err = cwlClient.CreateLogStream(ctx, &cloudwatchlogssdk.CreateLogStreamInput{
		LogGroupName:  aws.String(groupName),
		LogStreamName: aws.String(streamName),
	})
	require.NoError(t, err, "CreateLogStream should succeed")

	// Wire a subscription filter pointing to the Firehose stream.
	_, err = cwlClient.PutSubscriptionFilter(ctx, &cloudwatchlogssdk.PutSubscriptionFilterInput{
		LogGroupName:   aws.String(groupName),
		FilterName:     aws.String(filterName),
		FilterPattern:  aws.String(""),
		DestinationArn: aws.String(fhARN),
	})
	require.NoError(t, err, "PutSubscriptionFilter should succeed")

	// Verify the subscription filter is registered.
	descFiltersOut, err := cwlClient.DescribeSubscriptionFilters(ctx,
		&cloudwatchlogssdk.DescribeSubscriptionFiltersInput{
			LogGroupName: aws.String(groupName),
		})
	require.NoError(t, err, "DescribeSubscriptionFilters should succeed")
	require.Len(t, descFiltersOut.SubscriptionFilters, 1,
		"exactly one subscription filter should be registered")
	assert.Equal(t, fhARN,
		aws.ToString(descFiltersOut.SubscriptionFilters[0].DestinationArn),
		"subscription filter should target the Firehose stream ARN")

	// Put log events to trigger subscription delivery.
	now := time.Now().UnixMilli()
	_, err = cwlClient.PutLogEvents(ctx, &cloudwatchlogssdk.PutLogEventsInput{
		LogGroupName:  aws.String(groupName),
		LogStreamName: aws.String(streamName),
		LogEvents: []cwlogstypes.InputLogEvent{
			{Timestamp: aws.Int64(now), Message: aws.String("receipt-test-msg-" + id)},
		},
	})
	require.NoError(t, err, "PutLogEvents should succeed")

	// Poll S3 until the Firehose interval flusher delivers at least one object
	// (intervalFlusher ticks every second; with IntervalInSeconds=1 the flush
	// should trigger within ~2s of the record being buffered).
	require.Eventually(t, func() bool {
		listOut, listErr := s3Client.ListObjectsV2(ctx, &s3svc.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
		})
		if listErr != nil {
			return false
		}

		return len(listOut.Contents) > 0
	}, 15*time.Second, 200*time.Millisecond,
		"Firehose should deliver CWLogs subscription records to S3")
}
