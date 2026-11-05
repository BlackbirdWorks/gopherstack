package integration_test

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// TestIntegration_S3_NotificationToSQS verifies that S3 bucket notifications
// are delivered to a configured SQS queue on PutObject and DeleteObject.
func TestIntegration_S3_NotificationToSQS(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	s3Client := createS3Client(t)
	sqsClient := createSQSClient(t)
	ctx := t.Context()

	// Create an SQS queue to receive notifications.
	queueName := "s3-notif-" + uuid.NewString()
	createOut, err := sqsClient.CreateQueue(ctx, &sqssdk.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	require.NoError(t, err)
	queueURL := aws.ToString(createOut.QueueUrl)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = sqsClient.DeleteQueue(cleanupCtx, &sqssdk.DeleteQueueInput{QueueUrl: aws.String(queueURL)})
	})

	// Get the queue ARN.
	attrsOut, err := sqsClient.GetQueueAttributes(ctx, &sqssdk.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []sqstypes.QueueAttributeName{"QueueArn"},
	})
	require.NoError(t, err)
	queueARN := attrsOut.Attributes["QueueArn"]
	require.NotEmpty(t, queueARN)

	// Create S3 bucket.
	bucket := "s3-notif-" + uuid.NewString()
	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		out, _ := s3Client.ListObjects(cleanupCtx, &s3.ListObjectsInput{Bucket: aws.String(bucket)})
		if out != nil {
			for _, obj := range out.Contents {
				_, _ = s3Client.DeleteObject(cleanupCtx, &s3.DeleteObjectInput{
					Bucket: aws.String(bucket), Key: obj.Key,
				})
			}
		}
		_, _ = s3Client.DeleteBucket(cleanupCtx, &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})

	// Configure bucket notifications to send ObjectCreated events to the SQS queue.
	_, err = s3Client.PutBucketNotificationConfiguration(ctx, &s3.PutBucketNotificationConfigurationInput{
		Bucket: aws.String(bucket),
		NotificationConfiguration: &types.NotificationConfiguration{
			QueueConfigurations: []types.QueueConfiguration{
				{
					Id:       aws.String("q1"),
					QueueArn: aws.String(queueARN),
					Events:   []types.Event{"s3:ObjectCreated:*"},
				},
			},
		},
	})
	require.NoError(t, err)

	// Put an object — should trigger a notification.
	objectKey := "hello/world.txt"
	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader("hello world"),
	})
	require.NoError(t, err)

	// Poll the SQS queue until we receive the notification (max 10 s).
	var receivedMsg string
	require.Eventually(t, func() bool {
		msgs, recvErr := sqsClient.ReceiveMessage(ctx, &sqssdk.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: 5,
			WaitTimeSeconds:     1,
		})
		if recvErr != nil || len(msgs.Messages) == 0 {
			return false
		}
		receivedMsg = aws.ToString(msgs.Messages[0].Body)
		for _, m := range msgs.Messages {
			_, _ = sqsClient.DeleteMessage(ctx, &sqssdk.DeleteMessageInput{
				QueueUrl:      aws.String(queueURL),
				ReceiptHandle: m.ReceiptHandle,
			})
		}

		return true
	}, 10*time.Second, 500*time.Millisecond, "should receive SQS notification within 10s")

	// Validate the notification payload format.
	var payload map[string]jsontext.Value
	require.NoError(
		t,
		json.Unmarshal([]byte(receivedMsg), &payload, jsonx.V1()),
		"notification body must be valid JSON",
	)

	records, ok := payload["Records"]
	require.True(t, ok, "notification must contain a Records array")

	var recordList []map[string]jsontext.Value
	require.NoError(t, json.Unmarshal(records, &recordList, jsonx.V1()))
	require.NotEmpty(t, recordList)

	record := recordList[0]

	var eventSource string
	require.NoError(t, json.Unmarshal(record["eventSource"], &eventSource, jsonx.V1()))
	assert.Equal(t, "aws:s3", eventSource)

	var eventName string
	require.NoError(t, json.Unmarshal(record["eventName"], &eventName, jsonx.V1()))
	assert.True(t, strings.HasPrefix(eventName, "s3:ObjectCreated:"), "eventName should be ObjectCreated")

	var s3Block map[string]jsontext.Value
	require.NoError(t, json.Unmarshal(record["s3"], &s3Block, jsonx.V1()))

	var bucketBlock map[string]jsontext.Value
	require.NoError(t, json.Unmarshal(s3Block["bucket"], &bucketBlock, jsonx.V1()))

	var bucketName string
	require.NoError(t, json.Unmarshal(bucketBlock["name"], &bucketName, jsonx.V1()))
	assert.Equal(t, bucket, bucketName)

	var objectBlock map[string]jsontext.Value
	require.NoError(t, json.Unmarshal(s3Block["object"], &objectBlock, jsonx.V1()))

	var key string
	require.NoError(t, json.Unmarshal(objectBlock["key"], &key, jsonx.V1()))
	assert.Equal(t, objectKey, key)

	t.Logf("S3 notification delivered: eventName=%s bucket=%s key=%s", eventName, bucketName, key)
}
