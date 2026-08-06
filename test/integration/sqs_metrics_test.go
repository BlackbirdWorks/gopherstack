package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudwatchsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_SQS_MetricEmission verifies that SQS operations emit
// CloudWatch metrics to the AWS/SQS namespace.
func TestIntegration_SQS_MetricEmission(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	sqsClient := createSQSClient(t)
	cwClient := createCloudWatchClient(t)
	ctx := t.Context()

	// Create a queue for this test.
	queueName := "test-metrics-" + uuid.NewString()
	createOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	require.NoError(t, err)

	queueURL := createOut.QueueUrl
	t.Cleanup(func() {
		_, _ = sqsClient.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: queueURL})
	})

	msgBody := "metric-test-body-" + uuid.NewString()

	// --- SendMessage → NumberOfMessagesSent ---
	sendOut, err := sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    queueURL,
		MessageBody: aws.String(msgBody),
	})
	require.NoError(t, err)
	require.NotEmpty(t, *sendOut.MessageId)

	assertMetricExists(t, cwClient, "NumberOfMessagesSent")

	// --- GetQueueAttributes → ApproximateNumberOfMessages reflects queue depth ---
	attrOut, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       queueURL,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameApproximateNumberOfMessages},
	})
	require.NoError(t, err)
	assert.Equal(t, "1", attrOut.Attributes[string(sqstypes.QueueAttributeNameApproximateNumberOfMessages)],
		"queue depth should be 1 after sending one message")

	// --- ReceiveMessage → NumberOfMessagesReceived ---
	receiveOut, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, receiveOut.Messages, 1)

	assertMetricExists(t, cwClient, "NumberOfMessagesReceived")

	// --- DeleteMessage → NumberOfMessagesDeleted ---
	_, err = sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      queueURL,
		ReceiptHandle: receiveOut.Messages[0].ReceiptHandle,
	})
	require.NoError(t, err)

	assertMetricExists(t, cwClient, "NumberOfMessagesDeleted")

	// --- Verify queue is empty after deletion ---
	attrOut2, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       queueURL,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameApproximateNumberOfMessages},
	})
	require.NoError(t, err)
	assert.Equal(t, "0", attrOut2.Attributes[string(sqstypes.QueueAttributeNameApproximateNumberOfMessages)],
		"queue depth should be 0 after deleting the message")
}

// assertMetricExists polls ListMetrics until the async metric-emission goroutine
// has registered the named metric in the AWS/SQS namespace, or the deadline expires.
func assertMetricExists(t *testing.T, cwClient *cloudwatchsdk.Client, metricName string) {
	t.Helper()

	require.Eventually(t, func() bool {
		out, err := cwClient.ListMetrics(t.Context(), &cloudwatchsdk.ListMetricsInput{
			Namespace:  aws.String("AWS/SQS"),
			MetricName: aws.String(metricName),
		})

		return err == nil && len(out.Metrics) > 0
	}, 5*time.Second, 50*time.Millisecond, "expected metric %s to be registered in AWS/SQS namespace", metricName)
}

// TestIntegration_SQS_QueuePolicy verifies that SetQueueAttributes and
// GetQueueAttributes correctly round-trip a queue access Policy document.
func TestIntegration_SQS_QueuePolicy(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createSQSClient(t)
	ctx := t.Context()

	queueName := "test-policy-" + uuid.NewString()
	createOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	require.NoError(t, err)

	queueURL := createOut.QueueUrl
	t.Cleanup(func() {
		_, _ = client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: queueURL})
	})

	// Get the queue ARN first.
	arnOut, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       queueURL,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	queueARN := arnOut.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]
	require.NotEmpty(t, queueARN)

	// Build a minimal IAM policy document.
	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"AWS":"*"},"Action":"sqs:SendMessage","Resource":"` + queueARN + `"}]}`

	// SetQueueAttributes with Policy.
	_, err = client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
		QueueUrl: queueURL,
		Attributes: map[string]string{
			"Policy": policy,
		},
	})
	require.NoError(t, err)

	// GetQueueAttributes should return the Policy we just set.
	attrOut, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       queueURL,
		AttributeNames: []sqstypes.QueueAttributeName{"Policy"},
	})
	require.NoError(t, err)
	assert.Equal(t, policy, attrOut.Attributes["Policy"],
		"Policy attribute should round-trip through SetQueueAttributes/GetQueueAttributes")
}
