package sqs_test

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

const testEndpoint = "localhost:4566"

func newBackend(t *testing.T) *sqs.InMemoryBackend {
	t.Helper()

	b := sqs.NewInMemoryBackend()
	t.Cleanup(b.Close)

	return b
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

			b := newBackend(t)
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

	b := newBackend(t)
	createTestQueue(t, b, "my-queue")

	tests := []struct {
		wantErr error
		attrs   map[string]string
		name    string
	}{
		{
			name:    "same_attrs_idempotent",
			attrs:   nil,
			wantErr: nil,
		},
		{
			name:    "different_visibility_timeout",
			attrs:   map[string]string{"VisibilityTimeout": "60"},
			wantErr: sqs.ErrQueueAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName:  "my-queue",
				Attributes: tt.attrs,
				Endpoint:   testEndpoint,
			})
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Contains(t, out.QueueURL, "my-queue")
		})
	}
}

func TestCreateQueueNameValidation(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	tests := []struct {
		name    string
		qName   string
		wantErr bool
	}{
		{name: "valid_name", qName: "my-queue-1", wantErr: false},
		{name: "empty_name", qName: "", wantErr: true},
		{name: "too_long", qName: strings.Repeat("a", 81), wantErr: true},
		{name: "invalid_chars", qName: "my queue!", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName: tt.qName,
				Endpoint:  testEndpoint,
			})
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDeleteQueue(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "my-queue")

	err := b.DeleteQueue(&sqs.DeleteQueueInput{QueueURL: qURL})
	require.NoError(t, err)

	assert.Empty(t, b.ListAll())
}

func TestDeleteQueueNotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	err := b.DeleteQueue(&sqs.DeleteQueueInput{QueueURL: queueURL("nonexistent")})
	require.ErrorIs(t, err, sqs.ErrQueueNotFound)
}

func TestListQueues(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
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

	t.Run("pagination first page", func(t *testing.T) {
		t.Parallel()

		out, err := b.ListQueues(&sqs.ListQueuesInput{MaxResults: 2})
		require.NoError(t, err)
		assert.Len(t, out.QueueURLs, 2)
		assert.NotEmpty(t, out.NextToken)
	})

	t.Run("pagination second page", func(t *testing.T) {
		t.Parallel()

		first, err := b.ListQueues(&sqs.ListQueuesInput{MaxResults: 2})
		require.NoError(t, err)
		require.NotEmpty(t, first.NextToken)

		second, err := b.ListQueues(&sqs.ListQueuesInput{MaxResults: 2, NextToken: first.NextToken})
		require.NoError(t, err)
		assert.Len(t, second.QueueURLs, 1)
		assert.Empty(t, second.NextToken)
	})

	t.Run("all pages combined equal full list", func(t *testing.T) {
		t.Parallel()

		var all []string
		var token string

		for {
			out, err := b.ListQueues(&sqs.ListQueuesInput{MaxResults: 2, NextToken: token})
			require.NoError(t, err)
			all = append(all, out.QueueURLs...)
			token = out.NextToken

			if token == "" {
				break
			}
		}

		assert.Len(t, all, 3)
	})
}

func TestGetQueueURL(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	createTestQueue(t, b, "my-queue")

	out, err := b.GetQueueURL(&sqs.GetQueueURLInput{QueueName: "my-queue"})
	require.NoError(t, err)
	assert.Equal(t, queueURL("my-queue"), out.QueueURL)
}

func TestGetQueueURLNotFound(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.GetQueueURL(&sqs.GetQueueURLInput{QueueName: "nonexistent"})
	require.ErrorIs(t, err, sqs.ErrQueueNotFound)
}

func TestGetQueueAttributes(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
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

	b := newBackend(t)
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

	b := newBackend(t)
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

	b := newBackend(t)
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

	b := newBackend(t)
	qURL := createTestQueue(t, b, "my-queue")

	err := b.DeleteMessage(&sqs.DeleteMessageInput{
		QueueURL:      qURL,
		ReceiptHandle: "invalid-handle",
	})
	require.ErrorIs(t, err, sqs.ErrReceiptHandleInvalid)
}

func TestChangeMessageVisibility(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
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

	b := newBackend(t)
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

	b := newBackend(t)
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

	b := newBackend(t)
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
			ID:            strconv.Itoa(i + 1),
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

	b := newBackend(t)
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

	b := newBackend(t)
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

	b := newBackend(t)
	qURL := createTestQueue(t, b, "my-queue")

	_, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueURL: qURL,
		Entries:  []sqs.SendMessageBatchEntry{},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidBatchEntry)
}

func TestSendMessageBatchTooManyEntries(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "my-queue")

	entries := make([]sqs.SendMessageBatchEntry, 11)
	for i := range entries {
		entries[i] = sqs.SendMessageBatchEntry{
			ID:          strconv.Itoa(i + 1),
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

	b := newBackend(t)
	_, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:        queueURL("nonexistent"),
		WaitTimeSeconds: 0,
	})
	require.ErrorIs(t, err, sqs.ErrQueueNotFound)
}

func TestLongPolling(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "my-queue")

	// Send message before calling receive — should return quickly.
	_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "fast"})
	require.NoError(t, err)

	start := time.Now()

	out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   30,
		WaitTimeSeconds:     1,
	})
	require.NoError(t, err)
	require.Len(t, out.Messages, 1)
	assert.Less(t, time.Since(start), 500*time.Millisecond)
}

func TestLongPollingWakesOnMessageArrival(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "wake-queue")

	// Send a message after a short delay while ReceiveMessage is blocking.
	sendErr := make(chan error, 1)
	go func() {
		time.Sleep(150 * time.Millisecond)
		_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "wake"})
		sendErr <- err
	}()

	start := time.Now()

	out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   30,
		WaitTimeSeconds:     5,
	})

	elapsed := time.Since(start)

	require.NoError(t, <-sendErr)
	require.NoError(t, err)
	require.Len(t, out.Messages, 1)
	// Should wake well before the 5-second deadline.
	assert.Less(t, elapsed, 2*time.Second)
}

func TestLongPollingTimesOutWithNoMessages(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "timeout-queue")

	start := time.Now()

	out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   30,
		WaitTimeSeconds:     1,
	})

	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Empty(t, out.Messages)
	// Should have waited approximately WaitTimeSeconds.
	assert.GreaterOrEqual(t, elapsed, 900*time.Millisecond)
}

// TestLongPollingConcurrentReceivers verifies that concurrent long-poll receivers
// do not race or panic due to the old close/recreate channel pattern.
// With the fixed buffered(1) notify channel, receivers are woken without closing
// the channel, so no stale reference can cause a tight loop or panic.
func TestLongPollingConcurrentReceivers(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "concurrent-recv-queue")

	const numReceivers = 3
	const numMessages = 3

	results := make(chan *sqs.ReceiveMessageOutput, numReceivers)
	errs := make(chan error, numReceivers)

	// ready is closed once all receiver goroutines have been launched;
	// each goroutine is guaranteed to start before the first send.
	ready := make(chan struct{})

	var wg sync.WaitGroup

	wg.Add(numReceivers)

	for range numReceivers {
		go func() {
			wg.Done()
			<-ready
			out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            qURL,
				MaxNumberOfMessages: 1,
				VisibilityTimeout:   30,
				WaitTimeSeconds:     5,
			})
			errs <- err
			results <- out
		}()
	}

	// Block until all receiver goroutines have started, signal them to enter
	// ReceiveMessage, then sleep briefly so they reach the long-poll select
	// before the first message is sent.  This matches the approach used in
	// TestLongPollingWakesOnMessageArrival and ensures the test exercises the
	// notify wake-up path rather than the initial receiveOnce fast-path.
	wg.Wait()
	close(ready)
	time.Sleep(50 * time.Millisecond)

	for i := range numMessages {
		_, err := b.SendMessage(&sqs.SendMessageInput{
			QueueURL:    qURL,
			MessageBody: fmt.Sprintf("msg-%d", i),
		})
		require.NoError(t, err)
	}

	for range numReceivers {
		require.NoError(t, <-errs)
		out := <-results
		require.Len(t, out.Messages, 1)
	}
}

func TestReceiveMessageDefaultVisibility(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "my-queue")

	_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "hello"})
	require.NoError(t, err)

	// VisibilityTimeout = sqs.NoVisibilityTimeout uses the queue's default.
	out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   sqs.NoVisibilityTimeout,
		WaitTimeSeconds:     0,
	})
	require.NoError(t, err)
	require.Len(t, out.Messages, 1)
}

func TestListAll(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	createTestQueue(t, b, "q1")
	createTestQueue(t, b, "q2")

	queues := b.ListAll()
	assert.Len(t, queues, 2)
}

func TestQueueNameAttribute(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "test-queue")

	out, err := b.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueURL:       qURL,
		AttributeNames: []string{"QueueArn"},
	})
	require.NoError(t, err)

	arn := out.Attributes["QueueArn"]
	assert.Contains(t, arn, "test-queue", "ARN should contain queue name")
}

func TestSendMessageFIFOContentBasedDedup(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	// Create a FIFO queue with content-based deduplication enabled.
	_, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "dedup.fifo",
		Endpoint:  testEndpoint,
		Attributes: map[string]string{
			"ContentBasedDeduplication": "true",
		},
	})
	require.NoError(t, err)

	qURL := queueURL("dedup.fifo")

	// First send - should succeed.
	out1, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:       qURL,
		MessageBody:    "hello",
		MessageGroupID: "g1",
	})
	require.NoError(t, err)

	// Second send with same body - content-based dedup should return original MessageID.
	out2, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:       qURL,
		MessageBody:    "hello",
		MessageGroupID: "g1",
	})
	require.NoError(t, err)
	assert.Equal(t, out1.MessageID, out2.MessageID, "dedup should return original message ID")
}

func TestSendMessageFIFOExplicitDedup(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "explicitdedup.fifo")

	// First send with explicit deduplication ID.
	out1, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "hello",
		MessageGroupID:         "g1",
		MessageDeduplicationID: "unique-id-1",
	})
	require.NoError(t, err)

	// Second send with same deduplication ID - should return original.
	out2, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "hello again",
		MessageGroupID:         "g1",
		MessageDeduplicationID: "unique-id-1",
	})
	require.NoError(t, err)
	assert.Equal(t, out1.MessageID, out2.MessageID)
}

func TestSendMessageFIFOExpiredDedup(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "expireddedup.fifo")

	// We must set the dedup window in the past by directly manipulating the backend
	// via repeated sends rather than time manipulation. Instead, test that dedup
	// with a *different* dedup ID does NOT deduplicate.
	out1, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "hello",
		MessageGroupID:         "g1",
		MessageDeduplicationID: "id-1",
	})
	require.NoError(t, err)

	out2, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "hello",
		MessageGroupID:         "g1",
		MessageDeduplicationID: "id-2", // different ID - NOT a duplicate
	})
	require.NoError(t, err)
	assert.NotEqual(t, out1.MessageID, out2.MessageID)
}

func TestResolveVisibilityTimeoutInvalidAttr(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "vis-invalid-queue")

	// SetQueueAttributes now validates attribute ranges; a non-integer VisibilityTimeout
	// should be rejected.
	err := b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueURL:   qURL,
		Attributes: map[string]string{"VisibilityTimeout": "notanumber"},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidAttribute)
}

func TestReQueueExpiredMixed(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "requeue-mixed-queue")

	// Send 2 messages.
	_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "a"})
	require.NoError(t, err)
	_, err = b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "b"})
	require.NoError(t, err)

	// Receive both with very short visibility timeout (1 second).
	out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 2,
		VisibilityTimeout:   1,
	})
	require.NoError(t, err)
	require.Len(t, out.Messages, 2)

	// Wait for visibility timeout to expire.
	time.Sleep(1100 * time.Millisecond)

	// Receive again — expired messages should be requeued.
	out2, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 2,
		VisibilityTimeout:   30,
	})
	require.NoError(t, err)
	assert.Len(t, out2.Messages, 2)
}

func TestSendMessageBatchTooManyEntries2(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "batch-too-many-queue")

	entries := make([]sqs.SendMessageBatchEntry, 11)
	for i := range entries {
		entries[i] = sqs.SendMessageBatchEntry{
			ID:          strconv.Itoa(i),
			MessageBody: "msg",
		}
	}

	_, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueURL: qURL,
		Entries:  entries,
	})
	require.ErrorIs(t, err, sqs.ErrTooManyEntriesInBatch)
}

func TestDeleteMessageBatchEmpty(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "del-batch-empty-queue")

	_, err := b.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
		QueueURL: qURL,
		Entries:  []sqs.DeleteMessageBatchEntry{},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidBatchEntry)
}

func TestQueueNameFromInputEmpty(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	// Passing an empty URL to SendMessage triggers queueNameFromInput("") -> ""
	// which means the queue won't be found.
	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    "",
		MessageBody: "hello",
	})
	require.ErrorIs(t, err, sqs.ErrQueueNotFound)
}

func TestApproximateFirstReceiveTimestamp_SetOnFirstReceive(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "first-receive-ts-queue")

	_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "hello"})
	require.NoError(t, err)

	// First receive: ApproximateFirstReceiveTimestamp should be set.
	out1, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: qURL, MaxNumberOfMessages: 1})
	require.NoError(t, err)
	require.Len(t, out1.Messages, 1)

	msg := out1.Messages[0]
	firstTS := msg.Attributes["ApproximateFirstReceiveTimestamp"]
	assert.NotEmpty(t, firstTS, "ApproximateFirstReceiveTimestamp should be set on first receive")

	// The value should be a Unix millisecond timestamp (13-digit integer).
	assert.Len(t, firstTS, 13)

	// Return the message to queue (simulate visibility timeout = 0).
	_ = b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueURL:          qURL,
		ReceiptHandle:     msg.ReceiptHandle,
		VisibilityTimeout: 0,
	})

	// Second receive: ApproximateFirstReceiveTimestamp should NOT change.
	out2, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: qURL, MaxNumberOfMessages: 1})
	require.NoError(t, err)
	require.Len(t, out2.Messages, 1)

	secondTS := out2.Messages[0].Attributes["ApproximateFirstReceiveTimestamp"]
	assert.Equal(t, firstTS, secondTS, "ApproximateFirstReceiveTimestamp must not change on subsequent receives")
}

func TestMaxNumberOfMessages_ClampsAt10(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "clamp-queue")

	// Send 15 messages.
	for i := range 15 {
		_, err := b.SendMessage(&sqs.SendMessageInput{
			QueueURL:    qURL,
			MessageBody: strconv.Itoa(i),
		})
		require.NoError(t, err)
	}

	// MaxNumberOfMessages > 10 should now return an error (matches AWS validation).
	_, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: qURL, MaxNumberOfMessages: 20})
	require.ErrorIs(t, err, sqs.ErrInvalidMaxMessages)
}

func TestMaxNumberOfMessages_AtBoundary(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "boundary-queue")

	for i := range 15 {
		_, err := b.SendMessage(&sqs.SendMessageInput{
			QueueURL:    qURL,
			MessageBody: strconv.Itoa(i),
		})
		require.NoError(t, err)
	}

	// MaxNumberOfMessages=10 (boundary) should succeed and return 10 messages.
	out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: qURL, MaxNumberOfMessages: 10})
	require.NoError(t, err)
	assert.Len(t, out.Messages, 10)
}

func TestSentTimestamp_PresentInAttributes(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "sent-ts-queue")

	beforeSend := time.Now().UnixMilli()

	_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "check"})
	require.NoError(t, err)

	out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: qURL, MaxNumberOfMessages: 1})
	require.NoError(t, err)
	require.Len(t, out.Messages, 1)

	sentTSStr := out.Messages[0].Attributes["SentTimestamp"]
	require.NotEmpty(t, sentTSStr)

	sentTS, err := strconv.ParseInt(sentTSStr, 10, 64)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, sentTS, beforeSend)
}

// TestSendMessage_MD5OfMessageAttributes verifies that sending a message with MessageAttributes
// returns a non-empty MD5OfMessageAttributes that matches the expected AWS algorithm output.
func TestSendMessage_MD5OfMessageAttributes(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "md5-attrs-queue")

	out, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "hello",
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			"MyAttr": {DataType: "String", StringValue: "TestValue"},
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(
		t,
		out.MD5OfMessageAttributes,
		"MD5OfMessageAttributes should be set when MessageAttributes are present",
	)

	// Verify it's a valid 32-char hex string (MD5).
	assert.Len(t, out.MD5OfMessageAttributes, 32)

	// Sending without attributes should produce an empty MD5OfMessageAttributes.
	outNoAttrs, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "hello",
	})
	require.NoError(t, err)
	assert.Empty(t, outNoAttrs.MD5OfMessageAttributes)
}

// Test_FIFORequeueOrdering verifies that a FIFO message returned to the
// visible queue (via ChangeMessageVisibility(0) or automatic visibility-
// timeout expiry) is redelivered before a newer message in the SAME
// MessageGroupId that arrived while the earlier one was in flight — AWS's
// strict per-group ordering guarantee. In-flight messages block further
// delivery from their group but do NOT block further sends to that group, so
// it is possible (and exercised here) for a second message to be sitting in
// the queue behind an in-flight message from the same group. Naively
// appending a re-queued message to the tail of the pending list would let the
// newer message jump ahead of it, violating ordering; the backend must
// reinsert by SequenceNumber instead.
func Test_FIFORequeueOrdering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		requeue func(t *testing.T, b *sqs.InMemoryBackend, queueURL, receiptHandle string)
		name    string
	}{
		{
			name: "ChangeMessageVisibilityZero",
			requeue: func(t *testing.T, b *sqs.InMemoryBackend, queueURL, receiptHandle string) {
				t.Helper()

				err := b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
					QueueURL:          queueURL,
					ReceiptHandle:     receiptHandle,
					VisibilityTimeout: 0,
				})
				require.NoError(t, err)
			},
		},
		{
			name: "VisibilityTimeoutExpiry",
			requeue: func(t *testing.T, b *sqs.InMemoryBackend, _, _ string) {
				t.Helper()

				// The message was received with a 1-second visibility timeout;
				// simulate the janitor sweeping well past that expiry without a
				// real sleep.
				b.RunJanitorOnceForTest(time.Now().Add(2 * time.Second))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			qURL := createTestQueue(t, b, "fifo-requeue-order.fifo")

			// msg1 into group G.
			send1, err := b.SendMessage(&sqs.SendMessageInput{
				QueueURL:               qURL,
				MessageBody:            "msg1",
				MessageGroupID:         "G",
				MessageDeduplicationID: "dedup1",
			})
			require.NoError(t, err)

			// Receive msg1 with a short (1s) visibility timeout so it becomes
			// in-flight, blocking group G from further delivery.
			recv1, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            qURL,
				MaxNumberOfMessages: 1,
				VisibilityTimeout:   1,
			})
			require.NoError(t, err)
			require.Len(t, recv1.Messages, 1)
			require.Equal(t, send1.MessageID, recv1.Messages[0].MessageID)

			// msg2 arrives in the SAME group while msg1 is still in flight —
			// sends are never blocked by an in-flight predecessor, only receives.
			_, err = b.SendMessage(&sqs.SendMessageInput{
				QueueURL:               qURL,
				MessageBody:            "msg2",
				MessageGroupID:         "G",
				MessageDeduplicationID: "dedup2",
			})
			require.NoError(t, err)

			// Return msg1 to the visible queue via whichever mechanism this
			// subtest exercises.
			tt.requeue(t, b, qURL, recv1.Messages[0].ReceiptHandle)

			// The next receive must hand back msg1 (the earlier SequenceNumber),
			// not msg2, even though msg1 was the one just re-queued.
			recv2, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            qURL,
				MaxNumberOfMessages: 1,
				VisibilityTimeout:   sqs.NoVisibilityTimeout,
			})
			require.NoError(t, err)
			require.Len(t, recv2.Messages, 1)
			assert.Equal(t, "msg1", recv2.Messages[0].Body,
				"FIFO group ordering violated: newer same-group message delivered before the re-queued older one")
		})
	}
}
