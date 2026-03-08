package integration_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	eventbridgesdk "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_S3_NotificationToEventBridge verifies that when EventBridgeConfiguration
// is enabled on a bucket, S3 events are published to the default EventBridge event bus
// and fan-out delivery to downstream targets (SQS) works end-to-end.
func TestIntegration_S3_NotificationToEventBridge(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	s3Client := createS3Client(t)
	ebClient := createEventBridgeClient(t)
	sqsClient := createSQSClient(t)
	ctx := t.Context()

	// Create an SQS queue to receive events fanned out by EventBridge.
	queueName := "s3-eb-notif-" + uuid.NewString()[:8]
	queueOut, err := sqsClient.CreateQueue(ctx, &sqssdk.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	require.NoError(t, err)
	queueURL := aws.ToString(queueOut.QueueUrl)
	t.Cleanup(func() {
		_, _ = sqsClient.DeleteQueue(t.Context(), &sqssdk.DeleteQueueInput{QueueUrl: aws.String(queueURL)})
	})

	// Get the queue ARN.
	attrOut, err := sqsClient.GetQueueAttributes(ctx, &sqssdk.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	queueARN := attrOut.Attributes["QueueArn"]
	require.NotEmpty(t, queueARN)

	// Create an EventBridge rule on the default bus that matches all aws.s3 events.
	ruleName := "s3-eb-rule-" + uuid.NewString()[:8]
	_, err = ebClient.PutRule(ctx, &eventbridgesdk.PutRuleInput{
		Name:         aws.String(ruleName),
		EventBusName: aws.String("default"),
		EventPattern: aws.String(`{"source":["aws.s3"]}`),
		State:        ebtypes.RuleStateEnabled,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = ebClient.RemoveTargets(t.Context(), &eventbridgesdk.RemoveTargetsInput{
			Rule: aws.String(ruleName), EventBusName: aws.String("default"), Ids: []string{"t1"},
		})
		_, _ = ebClient.DeleteRule(t.Context(), &eventbridgesdk.DeleteRuleInput{
			Name: aws.String(ruleName), EventBusName: aws.String("default"),
		})
	})

	// Add SQS queue as a target for the rule.
	_, err = ebClient.PutTargets(ctx, &eventbridgesdk.PutTargetsInput{
		Rule:         aws.String(ruleName),
		EventBusName: aws.String("default"),
		Targets: []ebtypes.Target{
			{Id: aws.String("t1"), Arn: aws.String(queueARN)},
		},
	})
	require.NoError(t, err)

	// Create S3 bucket.
	bucket := "s3-eb-" + uuid.NewString()[:8]
	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
	t.Cleanup(func() {
		out, _ := s3Client.ListObjects(t.Context(), &s3.ListObjectsInput{Bucket: aws.String(bucket)})
		if out != nil {
			for _, obj := range out.Contents {
				_, _ = s3Client.DeleteObject(t.Context(), &s3.DeleteObjectInput{
					Bucket: aws.String(bucket), Key: obj.Key,
				})
			}
		}
		_, _ = s3Client.DeleteBucket(t.Context(), &s3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})

	// Enable EventBridge notifications on the bucket.
	_, err = s3Client.PutBucketNotificationConfiguration(ctx, &s3.PutBucketNotificationConfigurationInput{
		Bucket: aws.String(bucket),
		NotificationConfiguration: &s3types.NotificationConfiguration{
			EventBridgeConfiguration: &s3types.EventBridgeConfiguration{},
		},
	})
	require.NoError(t, err)

	// Put an object — should trigger an S3 → EventBridge event.
	objectKey := "folder/test-object.txt"
	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
		Body:   strings.NewReader("hello eventbridge"),
	})
	require.NoError(t, err)

	// Poll SQS until we receive an event fanned out from EventBridge (max 10 s).
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
	}, 10*time.Second, 500*time.Millisecond, "should receive EventBridge event in SQS within 10s")

	// Validate the EventBridge event envelope.
	var event map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(receivedMsg), &event), "event must be valid JSON")

	var source string
	require.NoError(t, json.Unmarshal(event["source"], &source))
	assert.Equal(t, "aws.s3", source)

	var detailType string
	require.NoError(t, json.Unmarshal(event["detail-type"], &detailType))
	assert.Equal(t, "Object Created", detailType)

	// Validate the event detail.
	var detail map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(event["detail"], &detail))

	var bucketDetail map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(detail["bucket"], &bucketDetail))

	var bucketName string
	require.NoError(t, json.Unmarshal(bucketDetail["name"], &bucketName))
	assert.Equal(t, bucket, bucketName)

	var objectDetail map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(detail["object"], &objectDetail))

	var key string
	require.NoError(t, json.Unmarshal(objectDetail["key"], &key))
	assert.Equal(t, objectKey, key)

	var reason string
	require.NoError(t, json.Unmarshal(detail["reason"], &reason))
	assert.Equal(t, "PutObject", reason)

	t.Logf("S3→EventBridge event delivered: source=%s detail-type=%s bucket=%s key=%s reason=%s",
		source, detailType, bucketName, key, reason)
}
