package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_SQS_ChangeMessageVisibilityBatchRoundTrip drives
// ChangeMessageVisibilityBatch through a real client -- the only SQS
// mutating op with no prior typed-client coverage at all (every other op in
// sqsDispatchTable is exercised elsewhere in this package). Sends a batch,
// receives it, changes visibility per-message, and confirms both that the
// per-entry Id round-trips into the correct Successful/Failed bucket and
// that the visibility change actually took effect (an immediate re-receive
// sees nothing).
func TestIntegration_SQS_ChangeMessageVisibilityBatchRoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSQSClient(t)
	ctx := t.Context()

	queueName := "test-cmvb-" + uuid.NewString()
	queueOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(queueName)})
	require.NoError(t, err)
	queueURL := queueOut.QueueUrl

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()
		_, _ = client.DeleteQueue(cleanupCtx, &sqs.DeleteQueueInput{QueueUrl: queueURL})
	})

	_, err = client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: queueURL,
		Entries: []sqstypes.SendMessageBatchRequestEntry{
			{Id: aws.String("m1"), MessageBody: aws.String("cmvb-body-1-" + uuid.NewString())},
			{Id: aws.String("m2"), MessageBody: aws.String("cmvb-body-2-" + uuid.NewString())},
		},
	})
	require.NoError(t, err)

	recvOut, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 2,
		WaitTimeSeconds:     2,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 2, "both sent messages should be receivable")

	entries := make([]sqstypes.ChangeMessageVisibilityBatchRequestEntry, len(recvOut.Messages))
	for i, msg := range recvOut.Messages {
		entries[i] = sqstypes.ChangeMessageVisibilityBatchRequestEntry{
			Id:                aws.String("v" + aws.ToString(msg.MessageId)[:6]),
			ReceiptHandle:     msg.ReceiptHandle,
			VisibilityTimeout: 60,
		}
	}

	cmvbOut, err := client.ChangeMessageVisibilityBatch(ctx, &sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: queueURL,
		Entries:  entries,
	})
	require.NoError(t, err)
	require.Empty(t, cmvbOut.Failed)
	require.Len(t, cmvbOut.Successful, 2)

	gotIDs := make(map[string]bool, len(cmvbOut.Successful))
	for _, s := range cmvbOut.Successful {
		gotIDs[aws.ToString(s.Id)] = true
	}

	for _, entry := range entries {
		assert.True(t, gotIDs[aws.ToString(entry.Id)], "each request entry Id should have a matching Successful entry")
	}

	// The messages are now hidden for 60s; an immediate re-receive should see nothing.
	recvOut2, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 2,
		WaitTimeSeconds:     1,
	})
	require.NoError(t, err)
	assert.Empty(
		t, recvOut2.Messages,
		"messages should stay hidden after ChangeMessageVisibilityBatch extended their timeout",
	)
}
