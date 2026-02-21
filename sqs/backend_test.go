package sqs_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/sqs"
)

const testEndpoint = "localhost:4566"

func newBackend() *sqs.InMemoryBackend {
	return sqs.NewInMemoryBackend()
}

func createTestQueue(t *testing.T, b *sqs.InMemoryBackend, name string) string {
	t.Helper()

	out, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: name,
		Endpoint:  testEndpoint,
	})
	require.NoError(t, err)

	return out.QueueURL
}

func queueURL(name string) string {
	return fmt.Sprintf("http://%s/000000000000/%s", testEndpoint, name)
}

func TestCreateQueue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		queueName string
		isFIFO    bool
	}{
		{name: "standard queue", queueName: "my-queue", isFIFO: false},
		{name: "fifo queue", queueName: "my-queue.fifo", isFIFO: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			out, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName: tc.queueName,
				Endpoint:  testEndpoint,
			})

			require.NoError(t, err)
			assert.Equal(t, queueURL(tc.queueName), out.QueueURL)

			queues := b.ListAll()
			require.Len(t, queues, 1)
			assert.Equal(t, tc.isFIFO, queues[0].IsFIFO)
		})
	}
}

func TestCreateQueueDuplicate(t *testing.T) {
	t.Parallel()

	b := newBackend()
	createTestQueue(t, b, "my-queue")

	_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "my-queue", Endpoint: testEndpoint})
	require.ErrorIs(t, err, sqs.ErrQueueAlreadyExists)
}

func TestDeleteQueue(t *testing.T) {
	t.Parallel()

	b := newBackend()
	qURL := createTestQueue(t, b, "my-queue")

	err := b.DeleteQueue(&sqs.DeleteQueueInput{QueueURL: qURL})
	require.NoError(t, err)

	assert.Empty(t, b.ListAll())
}

func TestDeleteQueueNotFound(t *testing.T) {
	t.Parallel()

	b := newBackend()
	err := b.DeleteQueue(&sqs.DeleteQueueInput{QueueURL: queueURL("nonexistent")})
	require.ErrorIs(t, err, sqs.ErrQueueNotFound)
}

func TestListQueues(t *testing.T) {
	t.Parallel()

	b := newBackend()
	createTestQueue(t, b, "alpha-queue")
	createTestQueue(t, b, "beta-queue")
	createTestQueue(t, b, "alpha-other")

	t.Run("no prefix", func(t *testing.T) {
		t.Parallel()

		out, err := b.ListQueues(&sqs.ListQueuesInput{})
		require.NoError(t, err)
		assert.Len(t, out.QueueURLs, 3)
	})

	t.Run("with prefix", func(t *testing.T) {
		t.Parallel()

		out, err := b.ListQueues(&sqs.ListQueuesInput{QueueNamePrefix: "alpha"})
		require.NoError(t, err)
		assert.Len(t, out.QueueURLs, 2)
	})
}

func TestGetQueueURL(t *testing.T) {
	t.Parallel()

	b := newBackend()
	createTestQueue(t, b, "my-queue")

	out, err := b.GetQueueURL(&sqs.GetQueueURLInput{QueueName: "my-queue"})
	require.NoError(t, err)
	assert.Equal(t, queueURL("my-queue"), out.QueueURL)
}

func TestGetQueueURLNotFound(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.GetQueueURL(&sqs.GetQueueURLInput{QueueName: "nonexistent"})
	require.ErrorIs(t, err, sqs.ErrQueueNotFound)
}

func TestGetQueueAttributes(t *testing.T) {
	t.Parallel()

	b := newBackend()
	qURL := createTestQueue(t, b, "my-queue")

	out, err := b.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueURL:       qURL,
		AttributeNames: []string{"All"},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out.Attributes["VisibilityTimeout"])
	assert.NotEmpty(t, out.Attributes["QueueArn"])
	assert.Contains(t, out.Attributes["QueueArn"], "my-queue")
}

func TestSetQueueAttributes(t *testing.T) {
	t.Parallel()

	b := newBackend()
	qURL := createTestQueue(t, b, "my-queue")

	err := b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueURL:   qURL,
		Attributes: map[string]string{"VisibilityTimeout": "60"},
	})
	require.NoError(t, err)

	out, err := b.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueURL:       qURL,
		AttributeNames: []string{"VisibilityTimeout"},
	})
	require.NoError(t, err)
	assert.Equal(t, "60", out.Attributes["VisibilityTimeout"])
}

func TestSendAndReceiveMessage(t *testing.T) {
	t.Parallel()

	b := newBackend()
	qURL := createTestQueue(t, b, "my-queue")

	sendOut, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "hello world",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, sendOut.MessageID)
	assert.NotEmpty(t, sendOut.MD5OfBody)

	recvOut, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   30,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 1)
	assert.Equal(t, "hello world", recvOut.Messages[0].Body)
	assert.Equal(t, sendOut.MessageID, recvOut.Messages[0].MessageID)
	assert.NotEmpty(t, recvOut.Messages[0].ReceiptHandle)
}

func TestDeleteMessage(t *testing.T) {
	t.Parallel()

	b := newBackend()
	qURL := createTestQueue(t, b, "my-queue")

	_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "hello"})
	require.NoError(t, err)

	recvOut, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL: qURL, MaxNumberOfMessages: 1, VisibilityTimeout: 30,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 1)

	err = b.DeleteMessage(&sqs.DeleteMessageInput{
		QueueURL:      qURL,
		ReceiptHandle: recvOut.Messages[0].ReceiptHandle,
	})
	require.NoError(t, err)
}

func TestDeleteMessageInvalidHandle(t *testing.T) {
	t.Parallel()

	b := newBackend()
	qURL := createTestQueue(t, b, "my-queue")

	err := b.DeleteMessage(&sqs.DeleteMessageInput{
		QueueURL:      qURL,
		ReceiptHandle: "invalid-handle",
	})
	require.ErrorIs(t, err, sqs.ErrReceiptHandleInvalid)
}

func TestChangeMessageVisibility(t *testing.T) {
	t.Parallel()

	b := newBackend()
	qURL := createTestQueue(t, b, "my-queue")

	_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "hello"})
	require.NoError(t, err)

	recvOut, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL: qURL, MaxNumberOfMessages: 1, VisibilityTimeout: 30,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 1)

	err = b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueURL:          qURL,
		ReceiptHandle:     recvOut.Messages[0].ReceiptHandle,
		VisibilityTimeout: 0,
	})
	require.NoError(t, err)
}

func TestVisibilityTimeoutExpiry(t *testing.T) {
	t.Parallel()

	b := newBackend()
	qURL := createTestQueue(t, b, "my-queue")

	_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "hello"})
	require.NoError(t, err)

	// Receive with 0-second visibility — message immediately becomes visible again.
	recvOut, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL: qURL, MaxNumberOfMessages: 1, VisibilityTimeout: 0,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 1)

	// Receive again — lazy expiry should re-queue the message.
	recvOut2, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL: qURL, MaxNumberOfMessages: 1, VisibilityTimeout: 30,
	})
	require.NoError(t, err)
	require.Len(t, recvOut2.Messages, 1)
}

func TestSendMessageBatch(t *testing.T) {
	t.Parallel()

	b := newBackend()
	qURL := createTestQueue(t, b, "my-queue")

	out, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueURL: qURL,
		Entries: []sqs.SendMessageBatchEntry{
			{ID: "1", MessageBody: "msg-one"},
			{ID: "2", MessageBody: "msg-two"},
		},
	})
	require.NoError(t, err)
	assert.Len(t, out.Successful, 2)
	assert.Empty(t, out.Failed)
}

func TestDeleteMessageBatch(t *testing.T) {
	t.Parallel()

	b := newBackend()
	qURL := createTestQueue(t, b, "my-queue")

	_, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueURL: qURL,
		Entries: []sqs.SendMessageBatchEntry{
			{ID: "1", MessageBody: "msg-one"},
			{ID: "2", MessageBody: "msg-two"},
		},
	})
	require.NoError(t, err)

	recvOut, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL: qURL, MaxNumberOfMessages: 10, VisibilityTimeout: 30,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 2)

	entries := make([]sqs.DeleteMessageBatchEntry, 0, len(recvOut.Messages))
	for i, msg := range recvOut.Messages {
		entries = append(entries, sqs.DeleteMessageBatchEntry{
			ID:            fmt.Sprintf("%d", i+1),
			ReceiptHandle: msg.ReceiptHandle,
		})
	}

	delOut, err := b.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
		QueueURL: qURL,
		Entries:  entries,
	})
	require.NoError(t, err)
	assert.Len(t, delOut.Successful, 2)
	assert.Empty(t, delOut.Failed)
}

func TestPurgeQueue(t *testing.T) {
	t.Parallel()

	b := newBackend()
	qURL := createTestQueue(t, b, "my-queue")

	for i := range 5 {
		_, err := b.SendMessage(&sqs.SendMessageInput{
			QueueURL:    qURL,
			MessageBody: fmt.Sprintf("msg-%d", i),
		})
		require.NoError(t, err)
	}

	err := b.PurgeQueue(&sqs.PurgeQueueInput{QueueURL: qURL})
	require.NoError(t, err)

	out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL: qURL, MaxNumberOfMessages: 10, WaitTimeSeconds: 0,
	})
	require.NoError(t, err)
	assert.Empty(t, out.Messages)
}

func TestFIFODeduplication(t *testing.T) {
	t.Parallel()

	b := newBackend()
	qURL := createTestQueue(t, b, "my-queue.fifo")

	out1, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "hello",
		MessageGroupID:         "group1",
		MessageDeduplicationID: "dedup-id-1",
	})
	require.NoError(t, err)

	out2, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "hello",
		MessageGroupID:         "group1",
		MessageDeduplicationID: "dedup-id-1",
	})
	require.NoError(t, err)
	assert.Equal(t, out1.MessageID, out2.MessageID)

	// Only one message should be in the queue.
	recvOut, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL: qURL, MaxNumberOfMessages: 10, VisibilityTimeout: 30,
	})
	require.NoError(t, err)
	assert.Len(t, recvOut.Messages, 1)
}

func TestSendMessageBatchEmptyError(t *testing.T) {
	t.Parallel()

	b := newBackend()
	qURL := createTestQueue(t, b, "my-queue")

	_, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueURL: qURL,
		Entries:  []sqs.SendMessageBatchEntry{},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidBatchEntry)
}

func TestSendMessageBatchTooManyEntries(t *testing.T) {
	t.Parallel()

	b := newBackend()
	qURL := createTestQueue(t, b, "my-queue")

	entries := make([]sqs.SendMessageBatchEntry, 11)
	for i := range entries {
		entries[i] = sqs.SendMessageBatchEntry{
			ID:          fmt.Sprintf("%d", i+1),
			MessageBody: "body",
		}
	}

	_, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueURL: qURL,
		Entries:  entries,
	})
	require.ErrorIs(t, err, sqs.ErrTooManyEntriesInBatch)
}

func TestReceiveMessageQueueNotFound(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:        queueURL("nonexistent"),
		WaitTimeSeconds: 0,
	})
	require.ErrorIs(t, err, sqs.ErrQueueNotFound)
}

func TestLongPolling(t *testing.T) {
	t.Parallel()

	b := newBackend()
	qURL := createTestQueue(t, b, "my-queue")

	// Send message before calling receive — should return quickly.
	_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "fast"})
	require.NoError(t, err)

	start := time.Now()

	out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:        qURL,
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   30,
		WaitTimeSeconds:     1,
	})
	require.NoError(t, err)
	require.Len(t, out.Messages, 1)
	assert.Less(t, time.Since(start), 500*time.Millisecond)
}

func TestReceiveMessageDefaultVisibility(t *testing.T) {
	t.Parallel()

	b := newBackend()
	qURL := createTestQueue(t, b, "my-queue")

	_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "hello"})
	require.NoError(t, err)

	// VisibilityTimeout = noVisibilitySet (-1) uses queue default.
	out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   -1,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	require.Len(t, out.Messages, 1)
}

func TestListAll(t *testing.T) {
	t.Parallel()

	b := newBackend()
	createTestQueue(t, b, "q1")
	createTestQueue(t, b, "q2")

	queues := b.ListAll()
	assert.Len(t, queues, 2)
}

func TestQueueNameAttribute(t *testing.T) {
	t.Parallel()

	b := newBackend()
	qURL := createTestQueue(t, b, "test-queue")

	out, err := b.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueURL:       qURL,
		AttributeNames: []string{"QueueArn"},
	})
	require.NoError(t, err)

	arn := out.Attributes["QueueArn"]
	assert.True(t, strings.Contains(arn, "test-queue"), "ARN should contain queue name")
}
