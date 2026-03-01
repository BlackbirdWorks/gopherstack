package sqs_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/sqs"
)

func makeRedrivePolicy(dlqARN string, maxReceiveCount int) string {
	b, _ := json.Marshal(map[string]any{
		"deadLetterTargetArn": dlqARN,
		"maxReceiveCount":     maxReceiveCount,
	})

	return string(b)
}

func TestRedrivePolicy_MessageMovedToDLQ(t *testing.T) {
	t.Parallel()

	b := sqs.NewInMemoryBackend()

	// Create DLQ first.
	_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "my-dlq", Endpoint: "localhost"})
	require.NoError(t, err)

	dlqARN := "arn:aws:sqs:us-east-1:000000000000:my-dlq"

	// Create main queue with redrive policy (maxReceiveCount=2).
	_, err = b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "my-queue",
		Endpoint:  "localhost",
		Attributes: map[string]string{
			"RedrivePolicy": makeRedrivePolicy(dlqARN, 2),
		},
	})
	require.NoError(t, err)

	mainURL := "http://localhost/000000000000/my-queue"
	dlqURL := "http://localhost/000000000000/my-dlq"

	// Send a message.
	_, err = b.SendMessage(&sqs.SendMessageInput{QueueURL: mainURL, MessageBody: "hello"})
	require.NoError(t, err)

	// Receive #1 → count becomes 1, no DLQ move.
	out1, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: mainURL, MaxNumberOfMessages: 1})
	require.NoError(t, err)
	require.Len(t, out1.Messages, 1)

	receipt1 := out1.Messages[0].ReceiptHandle

	// Make message immediately visible again.
	err = b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueURL:          mainURL,
		ReceiptHandle:     receipt1,
		VisibilityTimeout: 0,
	})
	require.NoError(t, err)

	// Receive #2 → count becomes 2 (== maxReceiveCount), still returned.
	out2, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: mainURL, MaxNumberOfMessages: 1})
	require.NoError(t, err)
	require.Len(t, out2.Messages, 1)

	receipt2 := out2.Messages[0].ReceiptHandle

	// Make message immediately visible again.
	err = b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueURL:          mainURL,
		ReceiptHandle:     receipt2,
		VisibilityTimeout: 0,
	})
	require.NoError(t, err)

	// Receive #3 → drainToDLQ should move it before pickMessages runs → empty result.
	out3, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: mainURL, MaxNumberOfMessages: 1})
	require.NoError(t, err)
	assert.Empty(t, out3.Messages, "message should have been moved to DLQ")

	// Receive from DLQ → should return the message.
	dlqOut, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: dlqURL, MaxNumberOfMessages: 1})
	require.NoError(t, err)
	require.Len(t, dlqOut.Messages, 1)
	assert.Equal(t, "hello", dlqOut.Messages[0].Body)
}

func TestRedrivePolicy_NoMovementWithoutDLQ(t *testing.T) {
	t.Parallel()

	b := sqs.NewInMemoryBackend()

	_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "plain-queue", Endpoint: "localhost"})
	require.NoError(t, err)

	qURL := "http://localhost/000000000000/plain-queue"

	_, err = b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "stay"})
	require.NoError(t, err)

	// Receive and re-enqueue 5 times — message must always come back.
	for i := range 5 {
		out, receiveErr := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: qURL, MaxNumberOfMessages: 1})
		require.NoError(t, receiveErr)
		require.Len(t, out.Messages, 1, "iteration %d", i)

		visErr := b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
			QueueURL:          qURL,
			ReceiptHandle:     out.Messages[0].ReceiptHandle,
			VisibilityTimeout: 0,
		})
		require.NoError(t, visErr)
	}
}

func TestRedrivePolicy_SetViaSetQueueAttributes(t *testing.T) {
	t.Parallel()

	b := sqs.NewInMemoryBackend()

	_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "dlq2", Endpoint: "localhost"})
	require.NoError(t, err)

	_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: "main2", Endpoint: "localhost"})
	require.NoError(t, err)

	mainURL := "http://localhost/000000000000/main2"
	dlqURL := "http://localhost/000000000000/dlq2"
	dlqARN := "arn:aws:sqs:us-east-1:000000000000:dlq2"

	// Apply redrive policy after creation.
	err = b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueURL: mainURL,
		Attributes: map[string]string{
			"RedrivePolicy": makeRedrivePolicy(dlqARN, 1),
		},
	})
	require.NoError(t, err)

	_, err = b.SendMessage(&sqs.SendMessageInput{QueueURL: mainURL, MessageBody: "test"})
	require.NoError(t, err)

	// Receive once (count=1 == maxReceiveCount=1, still returned).
	out1, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: mainURL, MaxNumberOfMessages: 1})
	require.NoError(t, err)
	require.Len(t, out1.Messages, 1)

	err = b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueURL:          mainURL,
		ReceiptHandle:     out1.Messages[0].ReceiptHandle,
		VisibilityTimeout: 0,
	})
	require.NoError(t, err)

	// Second receive → drained to DLQ.
	out2, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: mainURL, MaxNumberOfMessages: 1})
	require.NoError(t, err)
	assert.Empty(t, out2.Messages)

	dlqOut, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: dlqURL, MaxNumberOfMessages: 1})
	require.NoError(t, err)
	require.Len(t, dlqOut.Messages, 1)
	assert.Equal(t, "test", dlqOut.Messages[0].Body)
}

func TestRedrivePolicy_InvalidJSONIgnored(t *testing.T) {
	t.Parallel()

	b := sqs.NewInMemoryBackend()

	require.NotPanics(t, func() {
		_, err := b.CreateQueue(&sqs.CreateQueueInput{
			QueueName: "bad-policy",
			Endpoint:  "localhost",
			Attributes: map[string]string{
				"RedrivePolicy": "{not valid json",
			},
		})
		require.NoError(t, err)
	})

	// Queue should still work normally.
	qURL := "http://localhost/000000000000/bad-policy"
	_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "ok"})
	require.NoError(t, err)

	out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: qURL, MaxNumberOfMessages: 1})
	require.NoError(t, err)
	require.Len(t, out.Messages, 1)
	assert.Equal(t, "ok", out.Messages[0].Body)
}
