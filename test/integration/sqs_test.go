package integration_test

import (
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_SQS_QueueLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSQSClient(t)
	ctx := t.Context()

	queueName := "test-queue-" + uuid.NewString()

	// CreateQueue
	createOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.QueueUrl)

	queueURL := createOut.QueueUrl

	// ListQueues
	listOut, err := client.ListQueues(ctx, &sqs.ListQueuesInput{})
	require.NoError(t, err)
	assert.True(t, slices.Contains(listOut.QueueUrls, *queueURL), "created queue should appear in ListQueues")

	// GetQueueAttributes
	attrOut, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       queueURL,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, attrOut.Attributes)

	// DeleteQueue
	_, err = client.DeleteQueue(ctx, &sqs.DeleteQueueInput{
		QueueUrl: queueURL,
	})
	require.NoError(t, err)

	// Verify gone from list
	listOut2, err := client.ListQueues(ctx, &sqs.ListQueuesInput{})
	require.NoError(t, err)
	for _, u := range listOut2.QueueUrls {
		assert.NotEqual(t, *queueURL, u, "deleted queue should not appear in ListQueues")
	}
}

func TestIntegration_SQS_SendReceiveDelete(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSQSClient(t)
	ctx := t.Context()

	queueName := "test-srq-" + uuid.NewString()
	createOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	require.NoError(t, err)
	queueURL := createOut.QueueUrl

	// SendMessage
	msgBody := "hello-" + uuid.NewString()
	sendOut, err := client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    queueURL,
		MessageBody: aws.String(msgBody),
	})
	require.NoError(t, err)
	require.NotEmpty(t, *sendOut.MessageId)

	// ReceiveMessage
	recvOut, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 1)
	assert.Equal(t, msgBody, *recvOut.Messages[0].Body)
	require.NotEmpty(t, *recvOut.Messages[0].ReceiptHandle)

	// DeleteMessage
	_, err = client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      queueURL,
		ReceiptHandle: recvOut.Messages[0].ReceiptHandle,
	})
	require.NoError(t, err)

	// Verify queue is now empty
	recvOut2, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	assert.Empty(t, recvOut2.Messages, "queue should be empty after delete")
}

func TestIntegration_SQS_VisibilityTimeout(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSQSClient(t)
	ctx := t.Context()

	queueName := "test-vis-" + uuid.NewString()
	createOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
		Attributes: map[string]string{
			"VisibilityTimeout": "1",
		},
	})
	require.NoError(t, err)
	queueURL := createOut.QueueUrl

	// Send a message
	_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    queueURL,
		MessageBody: aws.String("visibility-test"),
	})
	require.NoError(t, err)

	// Receive — makes message invisible
	recvOut, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 1)

	// Immediately receive again — should be empty (message still invisible)
	recvOut2, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	assert.Empty(t, recvOut2.Messages, "message should be invisible immediately after first receive")

	// Wait for visibility timeout to expire (1s + buffer)
	time.Sleep(2 * time.Second)

	// Receive again — message should be visible again
	recvOut3, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	assert.Len(t, recvOut3.Messages, 1, "message should be visible again after visibility timeout")
}

func TestIntegration_SQS_BatchOperations(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSQSClient(t)
	ctx := t.Context()

	queueName := "test-batch-" + uuid.NewString()
	createOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	require.NoError(t, err)
	queueURL := createOut.QueueUrl

	// SendMessageBatch
	batchOut, err := client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: queueURL,
		Entries: []sqstypes.SendMessageBatchRequestEntry{
			{Id: aws.String("msg1"), MessageBody: aws.String("body-1")},
			{Id: aws.String("msg2"), MessageBody: aws.String("body-2")},
			{Id: aws.String("msg3"), MessageBody: aws.String("body-3")},
		},
	})
	require.NoError(t, err)
	assert.Len(t, batchOut.Successful, 3)
	assert.Empty(t, batchOut.Failed)

	// ReceiveMessage with MaxNumberOfMessages
	recvOut, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 3,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	assert.Len(t, recvOut.Messages, 3, "should receive all 3 batch messages")
}

func TestIntegration_SQS_FIFOQueue(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSQSClient(t)
	ctx := t.Context()

	queueName := "test-fifo-" + uuid.NewString() + ".fifo"
	createOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
		Attributes: map[string]string{
			"FifoQueue": "true",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.QueueUrl)

	// Verify the queue exists
	attrOut, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       createOut.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll},
	})
	require.NoError(t, err)
	assert.Equal(t, "true", attrOut.Attributes["FifoQueue"])
}
