package sqs_test

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/sqs"
)

func newBackendWithQueue(t *testing.T, queueName string) (*sqs.InMemoryBackend, string) {
	t.Helper()

	backend := sqs.NewInMemoryBackend()
	out, err := backend.CreateQueue(&sqs.CreateQueueInput{
		QueueName: queueName,
		Endpoint:  "localhost",
	})
	require.NoError(t, err)

	return backend, out.QueueURL
}

func TestChangeMessageVisibilityBatch_Success(t *testing.T) {
	t.Parallel()

	_ = logger.NewLogger(slog.LevelDebug)
	backend, queueURL := newBackendWithQueue(t, "test-vis-batch")

	// Send two messages.
	for _, body := range []string{"msg-one", "msg-two"} {
		_, err := backend.SendMessage(&sqs.SendMessageInput{QueueURL: queueURL, MessageBody: body})
		require.NoError(t, err)
	}

	// Receive both messages (they are now in-flight with default visibility timeout).
	rcv, err := backend.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            queueURL,
		MaxNumberOfMessages: 2,
		VisibilityTimeout:   30,
	})
	require.NoError(t, err)
	require.Len(t, rcv.Messages, 2)

	// Batch-change visibility to 0 so they become visible immediately.
	batchInput := &sqs.ChangeMessageVisibilityBatchInput{
		QueueURL: queueURL,
		Entries: []sqs.ChangeMessageVisibilityBatchRequestEntry{
			{ID: "e1", ReceiptHandle: rcv.Messages[0].ReceiptHandle, VisibilityTimeout: 0},
			{ID: "e2", ReceiptHandle: rcv.Messages[1].ReceiptHandle, VisibilityTimeout: 0},
		},
	}

	out, err := backend.ChangeMessageVisibilityBatch(batchInput)
	require.NoError(t, err)
	assert.Len(t, out.Successful, 2)
	assert.Empty(t, out.Failed)

	ids := make([]string, 0, len(out.Successful))
	for _, s := range out.Successful {
		ids = append(ids, s.ID)
	}

	assert.ElementsMatch(t, []string{"e1", "e2"}, ids)

	// Messages should be receivable again now.
	rcv2, err := backend.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            queueURL,
		MaxNumberOfMessages: 2,
		VisibilityTimeout:   30,
	})
	require.NoError(t, err)
	assert.Len(t, rcv2.Messages, 2)
}

func TestChangeMessageVisibilityBatch_PartialFailure(t *testing.T) {
	t.Parallel()

	backend, queueURL := newBackendWithQueue(t, "test-vis-batch-partial")

	_, err := backend.SendMessage(&sqs.SendMessageInput{QueueURL: queueURL, MessageBody: "hello"})
	require.NoError(t, err)

	rcv, err := backend.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            queueURL,
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   30,
	})
	require.NoError(t, err)
	require.Len(t, rcv.Messages, 1)

	out, err := backend.ChangeMessageVisibilityBatch(&sqs.ChangeMessageVisibilityBatchInput{
		QueueURL: queueURL,
		Entries: []sqs.ChangeMessageVisibilityBatchRequestEntry{
			{ID: "good", ReceiptHandle: rcv.Messages[0].ReceiptHandle, VisibilityTimeout: 0},
			{ID: "bad", ReceiptHandle: "invalid-handle", VisibilityTimeout: 0},
		},
	})
	require.NoError(t, err)

	require.Len(t, out.Successful, 1)
	assert.Equal(t, "good", out.Successful[0].ID)

	require.Len(t, out.Failed, 1)
	assert.Equal(t, "bad", out.Failed[0].ID)
	assert.Equal(t, "ReceiptHandleIsInvalid", out.Failed[0].Code)
	assert.True(t, out.Failed[0].SenderFault)
}

func TestChangeMessageVisibilityBatch_QueueNotFound(t *testing.T) {
	t.Parallel()

	backend := sqs.NewInMemoryBackend()

	_, err := backend.ChangeMessageVisibilityBatch(&sqs.ChangeMessageVisibilityBatchInput{
		QueueURL: "http://localhost/000000000000/nonexistent",
		Entries: []sqs.ChangeMessageVisibilityBatchRequestEntry{
			{ID: "e1", ReceiptHandle: "handle", VisibilityTimeout: 0},
		},
	})
	assert.ErrorIs(t, err, sqs.ErrQueueNotFound)
}
