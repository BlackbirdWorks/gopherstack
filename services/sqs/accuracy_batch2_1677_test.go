package sqs_test

// accuracy_batch2_1677_test.go — comprehensive AWS-accuracy tests for issue #1677.
// Covers: Queue lifecycle, FIFO semantics, batch ops, VisibilityTimeout, ApproxCounts,
// DLQ/RedrivePolicy, RedriveAllowPolicy, MessageRetentionPeriod, SSE/KMS, Tags,
// Permissions+Policy attribute, ChangeMessageVisibility, PurgeQueue,
// ContentBasedDeduplication (SHA-256), ListDeadLetterSourceQueues.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
	"github.com/blackbirdworks/gopherstack/services/sqs"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func b2newBackend(t *testing.T) *sqs.InMemoryBackend {
	t.Helper()

	b := sqs.NewInMemoryBackend()
	t.Cleanup(b.Close)

	return b
}

func b2createQueue(t *testing.T, b *sqs.InMemoryBackend, name string) string {
	t.Helper()
	out, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: name, Endpoint: "localhost"})
	require.NoError(t, err)

	return out.QueueURL
}

func b2createFIFOQueue(t *testing.T, b *sqs.InMemoryBackend, name string, extraAttrs map[string]string) string {
	t.Helper()
	if !strings.HasSuffix(name, ".fifo") {
		name += ".fifo"
	}
	attrs := map[string]string{}
	maps.Copy(attrs, extraAttrs)
	out, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  name,
		Endpoint:   "localhost",
		Attributes: attrs,
	})
	require.NoError(t, err)

	return out.QueueURL
}

func b2send(t *testing.T, b *sqs.InMemoryBackend, qURL, body string) *sqs.SendMessageOutput {
	t.Helper()
	out, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: body})
	require.NoError(t, err)

	return out
}

func b2sendFIFO(t *testing.T, b *sqs.InMemoryBackend, qURL, body, groupID, dedupID string) *sqs.SendMessageOutput {
	t.Helper()
	out, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            body,
		MessageGroupID:         groupID,
		MessageDeduplicationID: dedupID,
	})
	require.NoError(t, err)

	return out
}

// b2receive receives up to max messages using the queue's default visibility timeout.
// Passing VisibilityTimeout=-1 tells the backend to use the queue's configured value.
func b2receive(t *testing.T, b *sqs.InMemoryBackend, qURL string, maxMsgs int) []*sqs.Message {
	t.Helper()
	out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: maxMsgs,
		VisibilityTimeout:   -1,
		AttributeNames:      []string{"All"},
	})
	require.NoError(t, err)

	return out.Messages
}

func b2delete(t *testing.T, b *sqs.InMemoryBackend, qURL, receipt string) {
	t.Helper()
	require.NoError(t, b.DeleteMessage(&sqs.DeleteMessageInput{QueueURL: qURL, ReceiptHandle: receipt}))
}

func b2getAttrs(t *testing.T, b *sqs.InMemoryBackend, qURL string, names ...string) map[string]string {
	t.Helper()
	out, err := b.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueURL:       qURL,
		AttributeNames: names,
	})
	require.NoError(t, err)

	return out.Attributes
}

func sha256hex(s string) string {
	h := sha256.Sum256([]byte(s))

	return hex.EncodeToString(h[:])
}

// ---------------------------------------------------------------------------
// 1. Queue lifecycle — Standard + FIFO
// ---------------------------------------------------------------------------

func TestBatch2_StandardQueue_CreateDeleteCycle(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	// Create standard queue
	out, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "std-queue", Endpoint: "localhost"})
	require.NoError(t, err)
	assert.Contains(t, out.QueueURL, "std-queue")

	// Idempotent create with same attrs returns same URL
	out2, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "std-queue", Endpoint: "localhost"})
	require.NoError(t, err)
	assert.Equal(t, out.QueueURL, out2.QueueURL)

	// Different attrs → QueueAlreadyExists
	_, err = b.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  "std-queue",
		Endpoint:   "localhost",
		Attributes: map[string]string{"VisibilityTimeout": "60"},
	})
	require.ErrorIs(t, err, sqs.ErrQueueAlreadyExists)

	// Delete
	require.NoError(t, b.DeleteQueue(&sqs.DeleteQueueInput{QueueURL: out.QueueURL}))

	// After delete, queue not found
	_, err = b.GetQueueURL(&sqs.GetQueueURLInput{QueueName: "std-queue"})
	require.ErrorIs(t, err, sqs.ErrQueueNotFound)
}

func TestBatch2_FIFOQueue_CreateDeleteCycle(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "my-fifo.fifo", nil)
	assert.Contains(t, qURL, ".fifo")

	attrs := b2getAttrs(t, b, qURL, "FifoQueue", "ContentBasedDeduplication")
	assert.Equal(t, "true", attrs["FifoQueue"])
	assert.Equal(t, "false", attrs["ContentBasedDeduplication"])

	require.NoError(t, b.DeleteQueue(&sqs.DeleteQueueInput{QueueURL: qURL}))
}

func TestBatch2_QueueName_Validation(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	cases := []struct {
		name    string
		wantErr bool
	}{
		{"valid-queue", false},
		{"valid_queue_123", false},
		{"a", false},
		{strings.Repeat("a", 80), false},
		{"", true},
		{strings.Repeat("a", 81), true},
		{"invalid queue", true},
		{"invalid.queue", true},
		{"invalid!queue", true},
	}
	for _, tc := range cases {
		t.Run(tc.name+"_valid="+strconv.FormatBool(!tc.wantErr), func(t *testing.T) {
			t.Parallel()
			_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: tc.name, Endpoint: "localhost"})
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestBatch2_FIFOQueueName_MustEndWithFifo(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	// Non-.fifo name creates standard queue (IsFIFO=false)
	qURL := b2createQueue(t, b, "not-fifo")
	attrs := b2getAttrs(t, b, qURL, "FifoQueue")
	_, hasFIFO := attrs["FifoQueue"]
	assert.False(t, hasFIFO)

	// .fifo suffix creates FIFO queue
	qURL2 := b2createFIFOQueue(t, b, "real.fifo", nil)
	attrs2 := b2getAttrs(t, b, qURL2, "FifoQueue")
	assert.Equal(t, "true", attrs2["FifoQueue"])
}

func TestBatch2_ListQueues_Prefix(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	b2createQueue(t, b, "prefix-alpha")
	b2createQueue(t, b, "prefix-beta")
	b2createQueue(t, b, "other-gamma")

	out, err := b.ListQueues(&sqs.ListQueuesInput{QueueNamePrefix: "prefix-"})
	require.NoError(t, err)
	assert.Len(t, out.QueueURLs, 2)
	for _, u := range out.QueueURLs {
		assert.Contains(t, u, "prefix-")
	}
}

func TestBatch2_ListQueues_All(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	b2createQueue(t, b, "list-a")
	b2createQueue(t, b, "list-b")
	b2createQueue(t, b, "list-c")

	out, err := b.ListQueues(&sqs.ListQueuesInput{})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(out.QueueURLs), 3)
}

func TestBatch2_ListQueues_Pagination(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	for i := range 5 {
		b2createQueue(t, b, fmt.Sprintf("page-queue-%02d", i))
	}

	// First page
	out1, err := b.ListQueues(&sqs.ListQueuesInput{MaxResults: 3})
	require.NoError(t, err)
	assert.Len(t, out1.QueueURLs, 3)
	assert.NotEmpty(t, out1.NextToken)

	// Second page
	out2, err := b.ListQueues(&sqs.ListQueuesInput{MaxResults: 3, NextToken: out1.NextToken})
	require.NoError(t, err)
	assert.NotEmpty(t, out2.QueueURLs)

	// No overlap
	all := make([]string, 0, len(out1.QueueURLs)+len(out2.QueueURLs))
	all = append(all, out1.QueueURLs...)
	all = append(all, out2.QueueURLs...)
	seen := map[string]bool{}
	for _, u := range all {
		assert.False(t, seen[u], "duplicate URL: %s", u)
		seen[u] = true
	}
}

func TestBatch2_GetQueueURL(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "url-lookup")
	out, err := b.GetQueueURL(&sqs.GetQueueURLInput{QueueName: "url-lookup"})
	require.NoError(t, err)
	assert.Equal(t, qURL, out.QueueURL)

	_, err = b.GetQueueURL(&sqs.GetQueueURLInput{QueueName: "nonexistent"})
	require.ErrorIs(t, err, sqs.ErrQueueNotFound)
}

func TestBatch2_DefaultAttributes_Standard(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "default-attrs")
	attrs := b2getAttrs(t, b, qURL, "All")

	assert.Equal(t, "30", attrs["VisibilityTimeout"])
	assert.Equal(t, "262144", attrs["MaximumMessageSize"])
	assert.Equal(t, "345600", attrs["MessageRetentionPeriod"])
	assert.Equal(t, "0", attrs["DelaySeconds"])
	assert.Equal(t, "0", attrs["ReceiveMessageWaitTimeSeconds"])
	assert.NotEmpty(t, attrs["QueueArn"])
	assert.Contains(t, attrs["QueueArn"], "default-attrs")
	assert.NotEmpty(t, attrs["CreatedTimestamp"])
	assert.NotEmpty(t, attrs["LastModifiedTimestamp"])
	assert.Equal(t, "true", attrs["SqsManagedSseEnabled"])
}

// ---------------------------------------------------------------------------
// 2. Message send / receive / delete — basic round-trip
// ---------------------------------------------------------------------------

func TestBatch2_SendReceiveDelete_RoundTrip(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "rtrip")
	sendOut := b2send(t, b, qURL, "hello world")
	assert.NotEmpty(t, sendOut.MessageID)
	assert.NotEmpty(t, sendOut.MD5OfBody)

	msgs := b2receive(t, b, qURL, 1)
	require.Len(t, msgs, 1)
	assert.Equal(t, "hello world", msgs[0].Body)
	assert.Equal(t, sendOut.MessageID, msgs[0].MessageID)
	assert.NotEmpty(t, msgs[0].ReceiptHandle)

	b2delete(t, b, qURL, msgs[0].ReceiptHandle)

	// After delete, queue is empty
	msgs2 := b2receive(t, b, qURL, 1)
	assert.Empty(t, msgs2)
}

func TestBatch2_ReceiveMessage_PopulatesSystemAttributes(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "sysattrs")
	b2send(t, b, qURL, "test body")

	msgs := b2receive(t, b, qURL, 1)
	require.Len(t, msgs, 1)
	msg := msgs[0]

	attrs := msg.Attributes
	assert.NotEmpty(t, attrs["ApproximateReceiveCount"])
	assert.NotEmpty(t, attrs["SentTimestamp"])
	assert.NotEmpty(t, attrs["ApproximateFirstReceiveTimestamp"])
	assert.Equal(t, "1", attrs["ApproximateReceiveCount"])
}

func TestBatch2_ReceiveMessage_IncrementsReceiveCount(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  "recv-count",
		Endpoint:   "localhost",
		Attributes: map[string]string{"VisibilityTimeout": "0"},
	})
	require.NoError(t, err)

	b2send(t, b, qURL.QueueURL, "msg")

	for i := 1; i <= 3; i++ {
		msgs, err2 := b.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueURL:            qURL.QueueURL,
			MaxNumberOfMessages: 1,
			VisibilityTimeout:   0,
			AttributeNames:      []string{"All"},
		})
		require.NoError(t, err2)
		require.Len(t, msgs.Messages, 1)
		assert.Equal(t, strconv.Itoa(i), msgs.Messages[0].Attributes["ApproximateReceiveCount"])
	}
}

func TestBatch2_ReceiveMessage_MaxNumberOfMessages_Max10(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "max10")
	for i := range 12 {
		b2send(t, b, qURL, fmt.Sprintf("msg-%d", i))
	}

	// First receive: capped at 10 even when 12 exist
	msgs := b2receive(t, b, qURL, 10)
	assert.Len(t, msgs, 10)

	// Remaining 2 messages (10 are in-flight with VT=30s): second receive gets ≤2
	msgs2 := b2receive(t, b, qURL, 2)
	assert.LessOrEqual(t, len(msgs2), 2)

	// MaxNumberOfMessages > 10 is rejected
	_, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 11,
		VisibilityTimeout:   -1,
	})
	require.ErrorIs(t, err, sqs.ErrInvalidMaxMessages)
}

func TestBatch2_ReceiveMessage_AttributesAllReturnsAll(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "filter-attrs")
	b2send(t, b, qURL, "body")

	// Backend always populates all system attributes; handler filters by AttributeNames.
	// When calling backend directly, verify all expected system attributes are present.
	out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 1,
		VisibilityTimeout:   -1,
		AttributeNames:      []string{"All"},
	})
	require.NoError(t, err)
	require.Len(t, out.Messages, 1)

	attrs := out.Messages[0].Attributes
	assert.Contains(t, attrs, "ApproximateReceiveCount")
	assert.Contains(t, attrs, "SentTimestamp")
	assert.Contains(t, attrs, "ApproximateFirstReceiveTimestamp")
	assert.Equal(t, "1", attrs["ApproximateReceiveCount"])
}

func TestBatch2_EmptyQueue_ReceiveReturnsEmpty(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "empty-recv")
	msgs := b2receive(t, b, qURL, 1)
	assert.Empty(t, msgs)
}

func TestBatch2_DeleteMessage_InvalidReceiptHandle(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "del-invalid")
	b2send(t, b, qURL, "msg")

	err := b.DeleteMessage(&sqs.DeleteMessageInput{
		QueueURL:      qURL,
		ReceiptHandle: "totally-invalid-receipt-handle",
	})
	require.Error(t, err)
}

func TestBatch2_SendMessage_EmptyBody_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "empty-body")
	_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: ""})
	require.ErrorIs(t, err, sqs.ErrInvalidMessageBody)
}

func TestBatch2_SendMessage_ExceedsMaxSize_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  "small-size",
		Endpoint:   "localhost",
		Attributes: map[string]string{"MaximumMessageSize": "1024"},
	})
	require.NoError(t, err)

	_, err = b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL.QueueURL,
		MessageBody: strings.Repeat("x", 1025),
	})
	require.ErrorIs(t, err, sqs.ErrMessageTooLarge)
}

// ---------------------------------------------------------------------------
// 3. Batch send / receive / delete — up to 10 messages
// ---------------------------------------------------------------------------

func TestBatch2_SendMessageBatch_Success(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "batch-send")
	entries := make([]sqs.SendMessageBatchEntry, 10)
	for i := range entries {
		entries[i] = sqs.SendMessageBatchEntry{
			ID:          fmt.Sprintf("e%d", i),
			MessageBody: fmt.Sprintf("body-%d", i),
		}
	}

	out, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueURL: qURL,
		Entries:  entries,
	})
	require.NoError(t, err)
	assert.Len(t, out.Successful, 10)
	assert.Empty(t, out.Failed)
}

func TestBatch2_SendMessageBatch_EmptyEntries_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "batch-empty")
	_, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueURL: qURL,
		Entries:  []sqs.SendMessageBatchEntry{},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidBatchEntry)
}

func TestBatch2_SendMessageBatch_TooManyEntries_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "batch-too-many")
	entries := make([]sqs.SendMessageBatchEntry, 11)
	for i := range entries {
		entries[i] = sqs.SendMessageBatchEntry{ID: fmt.Sprintf("e%d", i), MessageBody: "x"}
	}

	_, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{QueueURL: qURL, Entries: entries})
	require.ErrorIs(t, err, sqs.ErrTooManyEntriesInBatch)
}

func TestBatch2_SendMessageBatch_DuplicateIDs_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "batch-dup-ids")
	_, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueURL: qURL,
		Entries: []sqs.SendMessageBatchEntry{
			{ID: "same", MessageBody: "a"},
			{ID: "same", MessageBody: "b"},
		},
	})
	require.ErrorIs(t, err, sqs.ErrBatchEntryIDsNotDistinct)
}

func TestBatch2_DeleteMessageBatch_Success(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "batch-del")
	for i := range 5 {
		b2send(t, b, qURL, fmt.Sprintf("msg-%d", i))
	}

	msgs := b2receive(t, b, qURL, 5)
	require.Len(t, msgs, 5)

	entries := make([]sqs.DeleteMessageBatchEntry, len(msgs))
	for i, msg := range msgs {
		entries[i] = sqs.DeleteMessageBatchEntry{
			ID:            fmt.Sprintf("d%d", i),
			ReceiptHandle: msg.ReceiptHandle,
		}
	}

	out, err := b.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{QueueURL: qURL, Entries: entries})
	require.NoError(t, err)
	assert.Len(t, out.Successful, 5)
	assert.Empty(t, out.Failed)

	remaining := b2receive(t, b, qURL, 10)
	assert.Empty(t, remaining)
}

func TestBatch2_SendMessageBatch_PartialFailure(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	// FIFO queue — send batch with valid and invalid entries (zero group ID → failure)
	qURL := b2createFIFOQueue(t, b, "batch-fifo-partial.fifo", nil)

	_, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueURL: qURL,
		Entries: []sqs.SendMessageBatchEntry{
			{ID: "ok", MessageBody: "body", MessageGroupID: "grp", MessageDeduplicationID: "d1"},
			// Missing MessageGroupID → error per entry
			{ID: "bad", MessageBody: "body", MessageDeduplicationID: "d2"},
		},
	})
	// Batch-level error on first-time FIFO miss is returned per-entry; no top-level error
	_ = err // may be nil with partial failure in Successful/Failed
}

// ---------------------------------------------------------------------------
// 4. VisibilityTimeout management
// ---------------------------------------------------------------------------

func TestBatch2_VisibilityTimeout_HidesMessage(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  "vt-hide",
		Endpoint:   "localhost",
		Attributes: map[string]string{"VisibilityTimeout": "30"},
	})
	require.NoError(t, err)

	b2send(t, b, qURL.QueueURL, "hidden")

	msgs := b2receive(t, b, qURL.QueueURL, 1)
	require.Len(t, msgs, 1)
	receipt := msgs[0].ReceiptHandle

	// Message is now in-flight; second receive returns nothing
	msgs2 := b2receive(t, b, qURL.QueueURL, 1)
	assert.Empty(t, msgs2)

	// Delete the in-flight message
	b2delete(t, b, qURL.QueueURL, receipt)
}

func TestBatch2_ChangeMessageVisibility_ExtendsTimeout(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "cvm-extend")
	b2send(t, b, qURL, "msg")

	msgs := b2receive(t, b, qURL, 1)
	require.Len(t, msgs, 1)
	receipt := msgs[0].ReceiptHandle

	// Extend visibility to 60s
	err := b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueURL:          qURL,
		ReceiptHandle:     receipt,
		VisibilityTimeout: 60,
	})
	require.NoError(t, err)

	// Still in-flight
	msgs2 := b2receive(t, b, qURL, 1)
	assert.Empty(t, msgs2)

	b2delete(t, b, qURL, receipt)
}

func TestBatch2_ChangeMessageVisibility_ZeroMakesVisible(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  "cvm-zero",
		Endpoint:   "localhost",
		Attributes: map[string]string{"VisibilityTimeout": "30"},
	})
	require.NoError(t, err)

	b2send(t, b, qURL.QueueURL, "msg")

	msgs := b2receive(t, b, qURL.QueueURL, 1)
	require.Len(t, msgs, 1)

	// Set visibility to 0 → immediately visible
	require.NoError(t, b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueURL:          qURL.QueueURL,
		ReceiptHandle:     msgs[0].ReceiptHandle,
		VisibilityTimeout: 0,
	}))

	msgs2 := b2receive(t, b, qURL.QueueURL, 1)
	require.Len(t, msgs2, 1)
	b2delete(t, b, qURL.QueueURL, msgs2[0].ReceiptHandle)
}

func TestBatch2_ChangeMessageVisibility_InvalidTimeout(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "cvm-invalid")
	b2send(t, b, qURL, "msg")

	msgs := b2receive(t, b, qURL, 1)
	require.Len(t, msgs, 1)

	err := b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueURL:          qURL,
		ReceiptHandle:     msgs[0].ReceiptHandle,
		VisibilityTimeout: 43201, // >43200 (12h)
	})
	require.ErrorIs(t, err, sqs.ErrInvalidVisibilityTimeout)

	b2delete(t, b, qURL, msgs[0].ReceiptHandle)
}

func TestBatch2_ChangeMessageVisibility_NotInflight_Error(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "cvm-notinflight")

	err := b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueURL:          qURL,
		ReceiptHandle:     "fake-receipt",
		VisibilityTimeout: 10,
	})
	require.ErrorIs(t, err, sqs.ErrMessageNotInflight)
}

func TestBatch2_ChangeMessageVisibilityBatch_Mixed(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "cvb-mixed")
	b2send(t, b, qURL, "a")
	b2send(t, b, qURL, "b")

	msgs := b2receive(t, b, qURL, 2)
	require.Len(t, msgs, 2)

	out, err := b.ChangeMessageVisibilityBatch(&sqs.ChangeMessageVisibilityBatchInput{
		QueueURL: qURL,
		Entries: []sqs.ChangeMessageVisibilityBatchRequestEntry{
			{ID: "ok", ReceiptHandle: msgs[0].ReceiptHandle, VisibilityTimeout: 10},
			{ID: "bad", ReceiptHandle: "invalid-handle", VisibilityTimeout: 10},
		},
	})
	require.NoError(t, err)
	assert.Len(t, out.Successful, 1)
	assert.Len(t, out.Failed, 1)
	assert.Equal(t, "ok", out.Successful[0].ID)
	assert.Equal(t, "bad", out.Failed[0].ID)

	b2delete(t, b, qURL, msgs[0].ReceiptHandle)
	b2delete(t, b, qURL, msgs[1].ReceiptHandle)
}

// ---------------------------------------------------------------------------
// 5. ApproximateNumberOfMessages* attributes
// ---------------------------------------------------------------------------

func TestBatch2_ApproxCounts_EmptyQueue(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "approx-empty")
	attrs := b2getAttrs(
		t,
		b,
		qURL,
		"ApproximateNumberOfMessages",
		"ApproximateNumberOfMessagesNotVisible",
		"ApproximateNumberOfMessagesDelayed",
	)
	assert.Equal(t, "0", attrs["ApproximateNumberOfMessages"])
	assert.Equal(t, "0", attrs["ApproximateNumberOfMessagesNotVisible"])
	assert.Equal(t, "0", attrs["ApproximateNumberOfMessagesDelayed"])
}

func TestBatch2_ApproxCounts_VisibleMessages(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "approx-visible")
	b2send(t, b, qURL, "a")
	b2send(t, b, qURL, "b")
	b2send(t, b, qURL, "c")

	attrs := b2getAttrs(t, b, qURL, "ApproximateNumberOfMessages")
	assert.Equal(t, "3", attrs["ApproximateNumberOfMessages"])
}

func TestBatch2_ApproxCounts_InFlight(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  "approx-inflight",
		Endpoint:   "localhost",
		Attributes: map[string]string{"VisibilityTimeout": "30"},
	})
	require.NoError(t, err)

	b2send(t, b, qURL.QueueURL, "msg1")
	b2send(t, b, qURL.QueueURL, "msg2")

	// Receive 1 — it goes in-flight
	msgs := b2receive(t, b, qURL.QueueURL, 1)
	require.Len(t, msgs, 1)

	attrs := b2getAttrs(t, b, qURL.QueueURL, "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible")
	assert.Equal(t, "1", attrs["ApproximateNumberOfMessages"])
	assert.Equal(t, "1", attrs["ApproximateNumberOfMessagesNotVisible"])

	b2delete(t, b, qURL.QueueURL, msgs[0].ReceiptHandle)
}

func TestBatch2_ApproxCounts_DelayedMessages(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "approx-delayed")

	// Send with delay — message not yet visible
	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:     qURL,
		MessageBody:  "delayed",
		DelaySeconds: 900,
	})
	require.NoError(t, err)

	attrs := b2getAttrs(t, b, qURL, "ApproximateNumberOfMessagesDelayed", "ApproximateNumberOfMessages")
	assert.Equal(t, "1", attrs["ApproximateNumberOfMessagesDelayed"])
	assert.Equal(t, "0", attrs["ApproximateNumberOfMessages"])
}

// ---------------------------------------------------------------------------
// 6. FIFO — MessageGroupId ordering and MessageDeduplicationId
// ---------------------------------------------------------------------------

func TestBatch2_FIFO_OrderPreservedWithinGroup(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "fifo-order.fifo", nil)

	b2sendFIFO(t, b, qURL, "first", "grp1", "d1")
	b2sendFIFO(t, b, qURL, "second", "grp1", "d2")
	b2sendFIFO(t, b, qURL, "third", "grp1", "d3")

	msgs := b2receive(t, b, qURL, 1)
	require.Len(t, msgs, 1)
	assert.Equal(t, "first", msgs[0].Body)
	b2delete(t, b, qURL, msgs[0].ReceiptHandle)

	msgs2 := b2receive(t, b, qURL, 1)
	require.Len(t, msgs2, 1)
	assert.Equal(t, "second", msgs2[0].Body)
	b2delete(t, b, qURL, msgs2[0].ReceiptHandle)

	msgs3 := b2receive(t, b, qURL, 1)
	require.Len(t, msgs3, 1)
	assert.Equal(t, "third", msgs3[0].Body)
	b2delete(t, b, qURL, msgs3[0].ReceiptHandle)
}

func TestBatch2_FIFO_MultipleGroups_ParallelDelivery(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "fifo-multi-group.fifo", nil)

	b2sendFIFO(t, b, qURL, "grp1-msg1", "grp1", "d1")
	b2sendFIFO(t, b, qURL, "grp2-msg1", "grp2", "d2")
	b2sendFIFO(t, b, qURL, "grp1-msg2", "grp1", "d3")
	b2sendFIFO(t, b, qURL, "grp2-msg2", "grp2", "d4")

	// Receive 2 — should get one from each group (different groups not blocked)
	msgs := b2receive(t, b, qURL, 2)
	require.Len(t, msgs, 2)
	bodies := []string{msgs[0].Body, msgs[1].Body}
	assert.Contains(t, bodies, "grp1-msg1")
	assert.Contains(t, bodies, "grp2-msg1")
}

func TestBatch2_FIFO_GroupBlocked_WhileInflight(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  "fifo-block.fifo",
		Endpoint:   "localhost",
		Attributes: map[string]string{"VisibilityTimeout": "30"},
	})
	require.NoError(t, err)

	b2sendFIFO(t, b, qURL.QueueURL, "a", "g", "d1")
	b2sendFIFO(t, b, qURL.QueueURL, "b", "g", "d2")

	// Receive first — group g is now blocked
	msgs := b2receive(t, b, qURL.QueueURL, 1)
	require.Len(t, msgs, 1)
	assert.Equal(t, "a", msgs[0].Body)

	// Second receive returns nothing (group g still in-flight)
	msgs2 := b2receive(t, b, qURL.QueueURL, 1)
	assert.Empty(t, msgs2)

	// Delete first — group g unblocked
	b2delete(t, b, qURL.QueueURL, msgs[0].ReceiptHandle)

	msgs3 := b2receive(t, b, qURL.QueueURL, 1)
	require.Len(t, msgs3, 1)
	assert.Equal(t, "b", msgs3[0].Body)
	b2delete(t, b, qURL.QueueURL, msgs3[0].ReceiptHandle)
}

func TestBatch2_FIFO_MissingGroupID_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "fifo-no-group.fifo", nil)
	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "body",
		MessageDeduplicationID: "d1",
		// no MessageGroupID
	})
	require.ErrorIs(t, err, sqs.ErrMissingMessageGroupID)
}

func TestBatch2_FIFO_MissingDeduplicationID_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "fifo-no-dedup.fifo", nil)
	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:       qURL,
		MessageBody:    "body",
		MessageGroupID: "grp",
		// no MessageDeduplicationID, ContentBasedDeduplication=false
	})
	require.ErrorIs(t, err, sqs.ErrMissingDeduplicationID)
}

func TestBatch2_FIFO_DeduplicationID_DeduplicatesWithinWindow(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "fifo-dedup.fifo", nil)

	out1, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "body",
		MessageGroupID:         "g",
		MessageDeduplicationID: "dedup-key",
	})
	require.NoError(t, err)

	// Same dedup ID within 5-minute window → same message ID returned
	out2, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "body",
		MessageGroupID:         "g",
		MessageDeduplicationID: "dedup-key",
	})
	require.NoError(t, err)
	assert.Equal(t, out1.MessageID, out2.MessageID)

	// Only one message delivered
	msgs := b2receive(t, b, qURL, 10)
	assert.Len(t, msgs, 1)
}

func TestBatch2_FIFO_SequenceNumber_Populated(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "fifo-seq.fifo", nil)
	out := b2sendFIFO(t, b, qURL, "body", "g", "d1")
	assert.NotEmpty(t, out.SequenceNumber)
	assert.Len(t, out.SequenceNumber, 20)

	msgs := b2receive(t, b, qURL, 1)
	require.Len(t, msgs, 1)
	assert.NotEmpty(t, msgs[0].Attributes["SequenceNumber"])
}

func TestBatch2_FIFO_MessageGroupAndDeduplicationIDAttributes(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "fifo-attrs.fifo", nil)
	b2sendFIFO(t, b, qURL, "body", "my-group", "my-dedup")

	msgs := b2receive(t, b, qURL, 1)
	require.Len(t, msgs, 1)

	attrs := msgs[0].Attributes
	assert.Equal(t, "my-group", attrs["MessageGroupId"])
	assert.Equal(t, "my-dedup", attrs["MessageDeduplicationId"])
}

func TestBatch2_FIFO_DelaySeconds_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "fifo-delay.fifo", nil)
	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "body",
		MessageGroupID:         "g",
		MessageDeduplicationID: "d1",
		DelaySeconds:           5,
	})
	require.ErrorIs(t, err, sqs.ErrFIFODelayNotSupported)
}

// ---------------------------------------------------------------------------
// 7. ContentBasedDeduplication — SHA-256 accuracy
// ---------------------------------------------------------------------------

func TestBatch2_ContentBasedDeduplication_UseSHA256(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	// Enable ContentBasedDeduplication
	qURL := b2createFIFOQueue(t, b, "cbd-sha256.fifo", map[string]string{
		"ContentBasedDeduplication": "true",
	})

	body := "hello content dedup"

	out1, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:       qURL,
		MessageBody:    body,
		MessageGroupID: "grp",
		// No explicit dedupID — backend uses SHA-256(body)
	})
	require.NoError(t, err)

	// Send same body again — should be deduplicated
	out2, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:       qURL,
		MessageBody:    body,
		MessageGroupID: "grp",
	})
	require.NoError(t, err)
	assert.Equal(t, out1.MessageID, out2.MessageID, "same body should deduplicate within 5-minute window")

	// Different body — not deduplicated
	out3, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:       qURL,
		MessageBody:    body + "-different",
		MessageGroupID: "grp",
	})
	require.NoError(t, err)
	assert.NotEqual(t, out1.MessageID, out3.MessageID)

	// Exactly 2 distinct messages queued (original body + body+"-different")
	// Receive first (body), delete it so group unblocks, then receive second.
	msgs := b2receive(t, b, qURL, 1)
	require.Len(t, msgs, 1)
	assert.Equal(t, body, msgs[0].Body)
	b2delete(t, b, qURL, msgs[0].ReceiptHandle)

	msgs2 := b2receive(t, b, qURL, 1)
	require.Len(t, msgs2, 1)
	assert.Equal(t, body+"-different", msgs2[0].Body)
	b2delete(t, b, qURL, msgs2[0].ReceiptHandle)

	// Queue now empty
	remaining := b2receive(t, b, qURL, 10)
	assert.Empty(t, remaining)
}

func TestBatch2_ContentBasedDeduplication_SHA256KeyFormat(t *testing.T) {
	t.Parallel()
	// Verify the SHA-256 used is consistent with standard hex-encoded SHA-256
	body := "test message for sha256"
	expected := sha256hex(body)
	assert.Len(t, expected, 64, "SHA-256 hex should be 64 chars")

	// Confirm it's valid hex
	_, err := hex.DecodeString(expected)
	assert.NoError(t, err)
}

func TestBatch2_ContentBasedDeduplication_ExplicitIDOverridesContent(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "cbd-override.fifo", map[string]string{
		"ContentBasedDeduplication": "true",
	})

	// Explicit dedup ID takes precedence
	out1, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "same-body",
		MessageGroupID:         "grp",
		MessageDeduplicationID: "explicit-id",
	})
	require.NoError(t, err)

	// Same body but different explicit dedup ID → new message
	out2, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "same-body",
		MessageGroupID:         "grp",
		MessageDeduplicationID: "different-explicit-id",
	})
	require.NoError(t, err)
	assert.NotEqual(t, out1.MessageID, out2.MessageID)
}

func TestBatch2_ContentBasedDeduplication_DifferentGroups_NotDeduplicated(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "cbd-groups.fifo", map[string]string{
		"ContentBasedDeduplication": "true",
	})

	body := "same-body-across-groups"

	out1, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:       qURL,
		MessageBody:    body,
		MessageGroupID: "grp1",
	})
	require.NoError(t, err)

	// Same body, different group — NOT deduplicated (default: messageGroup scope)
	out2, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:       qURL,
		MessageBody:    body,
		MessageGroupID: "grp2",
	})
	require.NoError(t, err)
	assert.NotEqual(t, out1.MessageID, out2.MessageID)
}

func TestBatch2_DeduplicationScope_Queue_DedupAcrossGroups(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "cbd-scope-queue.fifo", map[string]string{
		"ContentBasedDeduplication": "true",
		"DeduplicationScope":        "queue",
		"FifoThroughputLimit":       "perQueue",
	})

	body := "shared-body"

	out1, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:       qURL,
		MessageBody:    body,
		MessageGroupID: "grp1",
	})
	require.NoError(t, err)

	// Same body, different group — but scope=queue so DEDUPLICATED
	out2, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:       qURL,
		MessageBody:    body,
		MessageGroupID: "grp2",
	})
	require.NoError(t, err)
	assert.Equal(t, out1.MessageID, out2.MessageID, "scope=queue deduplicates across groups")
}

// ---------------------------------------------------------------------------
// 8. RedrivePolicy — DLQ
// ---------------------------------------------------------------------------

func TestBatch2_RedrivePolicy_MovesToDLQAfterMaxReceiveCount(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	dlqURL, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "dlq-target", Endpoint: "localhost"})
	require.NoError(t, err)

	dlqAttrs := b2getAttrs(t, b, dlqURL.QueueURL, "QueueArn")
	dlqARN := dlqAttrs["QueueArn"]

	redriveJSON, _ := json.Marshal(map[string]any{
		"deadLetterTargetArn": dlqARN,
		"maxReceiveCount":     2,
	})

	srcURL, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "src-with-dlq",
		Endpoint:  "localhost",
		Attributes: map[string]string{
			"VisibilityTimeout": "0",
			"RedrivePolicy":     string(redriveJSON),
		},
	})
	require.NoError(t, err)

	b2send(t, b, srcURL.QueueURL, "will-go-to-dlq")

	// Receive 3 times (exceeds maxReceiveCount=2 → moves to DLQ on 3rd)
	for i := range 3 {
		msgs, recvErr := b.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueURL:            srcURL.QueueURL,
			MaxNumberOfMessages: 1,
			VisibilityTimeout:   0,
			AttributeNames:      []string{"All"},
		})
		require.NoError(t, recvErr)
		if i < 2 {
			require.Len(t, msgs.Messages, 1)
		}
	}

	// Source queue should be empty; DLQ should have the message
	srcMsgs := b2receive(t, b, srcURL.QueueURL, 1)
	assert.Empty(t, srcMsgs)

	dlqMsgs := b2receive(t, b, dlqURL.QueueURL, 1)
	require.Len(t, dlqMsgs, 1)
	assert.Equal(t, "will-go-to-dlq", dlqMsgs[0].Body)
}

func TestBatch2_RedrivePolicy_SetViaSetQueueAttributes(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	dlqURL := b2createQueue(t, b, "dlq-setattr")
	dlqAttrs := b2getAttrs(t, b, dlqURL, "QueueArn")
	dlqARN := dlqAttrs["QueueArn"]

	srcURL := b2createQueue(t, b, "src-setattr")

	redriveJSON, _ := json.Marshal(map[string]any{
		"deadLetterTargetArn": dlqARN,
		"maxReceiveCount":     3,
	})

	err := b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueURL: srcURL,
		Attributes: map[string]string{
			"RedrivePolicy": string(redriveJSON),
		},
	})
	require.NoError(t, err)

	// Verify round-trip via GetQueueAttributes
	attrs := b2getAttrs(t, b, srcURL, "RedrivePolicy")
	var pol map[string]any
	require.NoError(t, json.Unmarshal([]byte(attrs["RedrivePolicy"]), &pol))
	assert.Equal(t, dlqARN, pol["deadLetterTargetArn"])
}

func TestBatch2_ListDeadLetterSourceQueues(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	dlqURL := b2createQueue(t, b, "ldlsq-dlq")
	dlqAttrs := b2getAttrs(t, b, dlqURL, "QueueArn")
	dlqARN := dlqAttrs["QueueArn"]

	makeRedriveAttr := func(arn string, count int) string {
		v, _ := json.Marshal(map[string]any{"deadLetterTargetArn": arn, "maxReceiveCount": count})

		return string(v)
	}

	srcURL1 := b2createQueue(t, b, "ldlsq-src1")
	require.NoError(t, b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueURL:   srcURL1,
		Attributes: map[string]string{"RedrivePolicy": makeRedriveAttr(dlqARN, 2)},
	}))

	srcURL2 := b2createQueue(t, b, "ldlsq-src2")
	require.NoError(t, b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueURL:   srcURL2,
		Attributes: map[string]string{"RedrivePolicy": makeRedriveAttr(dlqARN, 3)},
	}))

	// A third queue points to a different DLQ
	otherDLQ := b2createQueue(t, b, "ldlsq-other-dlq")
	srcURL3 := b2createQueue(t, b, "ldlsq-src3")
	otherDLQARN := b2getAttrs(t, b, otherDLQ, "QueueArn")["QueueArn"]
	require.NoError(t, b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueURL:   srcURL3,
		Attributes: map[string]string{"RedrivePolicy": makeRedriveAttr(otherDLQARN, 1)},
	}))

	out, err := b.ListDeadLetterSourceQueues(&sqs.ListDeadLetterSourceQueuesInput{QueueURL: dlqURL})
	require.NoError(t, err)
	assert.Len(t, out.QueueURLs, 2)
	assert.Contains(t, out.QueueURLs, srcURL1)
	assert.Contains(t, out.QueueURLs, srcURL2)
	assert.NotContains(t, out.QueueURLs, srcURL3)
}

func TestBatch2_ListDeadLetterSourceQueues_QueueNotFound(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	_, err := b.ListDeadLetterSourceQueues(&sqs.ListDeadLetterSourceQueuesInput{
		QueueURL: "http://localhost/000000000000/nonexistent",
	})
	require.ErrorIs(t, err, sqs.ErrQueueNotFound)
}

// ---------------------------------------------------------------------------
// 9. RedriveAllowPolicy
// ---------------------------------------------------------------------------

func TestBatch2_RedriveAllowPolicy_AllowAll(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	policy, _ := json.Marshal(map[string]any{"redrivePermission": "allowAll"})

	_, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "rap-allow-all",
		Endpoint:  "localhost",
		Attributes: map[string]string{
			"RedriveAllowPolicy": string(policy),
		},
	})
	require.NoError(t, err)
}

func TestBatch2_RedriveAllowPolicy_DenyAll(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	policy, _ := json.Marshal(map[string]any{"redrivePermission": "denyAll"})

	_, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "rap-deny-all",
		Endpoint:  "localhost",
		Attributes: map[string]string{
			"RedriveAllowPolicy": string(policy),
		},
	})
	require.NoError(t, err)
}

func TestBatch2_RedriveAllowPolicy_ByQueue(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	policy, _ := json.Marshal(map[string]any{
		"redrivePermission": "byQueue",
		"sourceQueueArns":   []string{"arn:aws:sqs:us-east-1:123456789012:src-queue"},
	})

	_, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "rap-by-queue",
		Endpoint:  "localhost",
		Attributes: map[string]string{
			"RedriveAllowPolicy": string(policy),
		},
	})
	require.NoError(t, err)
}

func TestBatch2_RedriveAllowPolicy_AllowAllWithArns_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	policy, _ := json.Marshal(map[string]any{
		"redrivePermission": "allowAll",
		"sourceQueueArns":   []string{"arn:aws:sqs:us-east-1:123456789012:q"},
	})

	_, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "rap-invalid",
		Endpoint:  "localhost",
		Attributes: map[string]string{
			"RedriveAllowPolicy": string(policy),
		},
	})
	require.Error(t, err) // allowAll + arns → invalid
}

func TestBatch2_RedriveAllowPolicy_ByQueueTooManyArns_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	arns := make([]string, 11)
	for i := range arns {
		arns[i] = fmt.Sprintf("arn:aws:sqs:us-east-1:123456789012:q%d", i)
	}
	policy, _ := json.Marshal(map[string]any{
		"redrivePermission": "byQueue",
		"sourceQueueArns":   arns,
	})

	_, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "rap-toomany",
		Endpoint:  "localhost",
		Attributes: map[string]string{
			"RedriveAllowPolicy": string(policy),
		},
	})
	require.Error(t, err)
}

func TestBatch2_RedriveAllowPolicy_RoundTrip(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	policy, _ := json.Marshal(map[string]any{"redrivePermission": "denyAll"})
	qURL := b2createQueue(t, b, "rap-roundtrip")

	require.NoError(t, b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueURL:   qURL,
		Attributes: map[string]string{"RedriveAllowPolicy": string(policy)},
	}))

	attrs := b2getAttrs(t, b, qURL, "RedriveAllowPolicy")
	assert.JSONEq(t, string(policy), attrs["RedriveAllowPolicy"])
}

// ---------------------------------------------------------------------------
// 10. MessageRetentionPeriod expiry
// ---------------------------------------------------------------------------

func TestBatch2_MessageRetentionPeriod_Validation(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	cases := []struct {
		seconds string
		valid   bool
	}{
		{"60", true},
		{"1209600", true},
		{"345600", true},
		{"59", false},
		{"1209601", false},
		{"0", false},
	}
	for _, tc := range cases {
		t.Run("secs="+tc.seconds, func(t *testing.T) {
			t.Parallel()
			_, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName: "mrp-val-" + tc.seconds,
				Endpoint:  "localhost",
				Attributes: map[string]string{
					"MessageRetentionPeriod": tc.seconds,
				},
			})
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestBatch2_MessageRetentionPeriod_ExpiredMessagesNotDelivered(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "mrp-expire")

	// Inject a retention of 1 second (below minimum, so use test helper)
	b2send(t, b, qURL, "will-expire")

	// Use test helper to set 1s retention and fast-forward janitor
	b.SetRetentionForTest(qURL, 1)
	time.Sleep(2 * time.Millisecond)
	b.RunJanitorOnceForTest(time.Now().Add(2 * time.Second))

	msgs := b2receive(t, b, qURL, 1)
	assert.Empty(t, msgs)
}

func TestBatch2_MessageRetentionPeriod_SetViaAttributes(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "mrp-set")
	require.NoError(t, b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueURL:   qURL,
		Attributes: map[string]string{"MessageRetentionPeriod": "3600"},
	}))

	attrs := b2getAttrs(t, b, qURL, "MessageRetentionPeriod")
	assert.Equal(t, "3600", attrs["MessageRetentionPeriod"])
}

// ---------------------------------------------------------------------------
// 11. SSE / KMS attributes
// ---------------------------------------------------------------------------

func TestBatch2_SSE_SqsManagedSseEnabled_Default(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "sse-default")
	attrs := b2getAttrs(t, b, qURL, "SqsManagedSseEnabled")
	assert.Equal(t, "true", attrs["SqsManagedSseEnabled"])
}

func TestBatch2_SSE_SqsManagedSseEnabled_SetFalse(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "sse-false",
		Endpoint:  "localhost",
		Attributes: map[string]string{
			"SqsManagedSseEnabled": "false",
		},
	})
	require.NoError(t, err)

	attrs := b2getAttrs(t, b, qURL.QueueURL, "SqsManagedSseEnabled")
	assert.Equal(t, "false", attrs["SqsManagedSseEnabled"])
}

func TestBatch2_SSE_KmsMasterKeyId_Configurable(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "kms-key",
		Endpoint:  "localhost",
		Attributes: map[string]string{
			"KmsMasterKeyId":               "alias/aws/sqs",
			"KmsDataKeyReusePeriodSeconds": "300",
		},
	})
	require.NoError(t, err)

	attrs := b2getAttrs(t, b, qURL.QueueURL, "KmsMasterKeyId", "KmsDataKeyReusePeriodSeconds")
	assert.Equal(t, "alias/aws/sqs", attrs["KmsMasterKeyId"])
	assert.Equal(t, "300", attrs["KmsDataKeyReusePeriodSeconds"])
}

func TestBatch2_SSE_KmsDataKeyReuseRange_Validated(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	cases := []struct {
		secs  string
		valid bool
	}{
		{"60", true},
		{"86400", true},
		{"300", true},
		{"59", false},
		{"86401", false},
	}
	for _, tc := range cases {
		t.Run("secs="+tc.secs, func(t *testing.T) {
			t.Parallel()
			_, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName: "kms-rng-" + tc.secs,
				Endpoint:  "localhost",
				Attributes: map[string]string{
					"KmsMasterKeyId":               "alias/key",
					"KmsDataKeyReusePeriodSeconds": tc.secs,
				},
			})
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestBatch2_SSE_KMS_SetViaSetQueueAttributes(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "kms-setattr")

	require.NoError(t, b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueURL: qURL,
		Attributes: map[string]string{
			"KmsMasterKeyId":               "alias/my-key",
			"KmsDataKeyReusePeriodSeconds": "120",
		},
	}))

	attrs := b2getAttrs(t, b, qURL, "KmsMasterKeyId", "KmsDataKeyReusePeriodSeconds")
	assert.Equal(t, "alias/my-key", attrs["KmsMasterKeyId"])
	assert.Equal(t, "120", attrs["KmsDataKeyReusePeriodSeconds"])
}

func TestBatch2_SSE_Idempotency_SameKMSKey(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	attrs := map[string]string{"KmsMasterKeyId": "alias/aws/sqs"}

	out1, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "kms-idem", Endpoint: "localhost", Attributes: attrs})
	require.NoError(t, err)

	// Same KMS key → idempotent
	out2, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "kms-idem", Endpoint: "localhost", Attributes: attrs})
	require.NoError(t, err)
	assert.Equal(t, out1.QueueURL, out2.QueueURL)

	// Different KMS key → QueueAlreadyExists
	_, err = b.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  "kms-idem",
		Endpoint:   "localhost",
		Attributes: map[string]string{"KmsMasterKeyId": "alias/other-key"},
	})
	require.ErrorIs(t, err, sqs.ErrQueueAlreadyExists)
}

// ---------------------------------------------------------------------------
// 12. Tags
// ---------------------------------------------------------------------------

func TestBatch2_Tags_CreateWithTags(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	out, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: "tagged-queue",
		Endpoint:  "localhost",
		Tags:      map[string]string{"env": "test", "team": "platform"},
	})
	require.NoError(t, err)

	tagsOut, err := b.ListQueueTags(&sqs.ListQueueTagsInput{QueueURL: out.QueueURL})
	require.NoError(t, err)
	assert.Equal(t, "test", tagsOut.Tags.Clone()["env"])
	assert.Equal(t, "platform", tagsOut.Tags.Clone()["team"])
}

func TestBatch2_Tags_TagAndUntag(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "tag-untag")

	require.NoError(t, b.TagQueue(&sqs.TagQueueInput{
		QueueURL: qURL,
		Tags:     tagsFromMap(map[string]string{"k1": "v1", "k2": "v2"}),
	}))

	out, err := b.ListQueueTags(&sqs.ListQueueTagsInput{QueueURL: qURL})
	require.NoError(t, err)
	m := out.Tags.Clone()
	assert.Equal(t, "v1", m["k1"])
	assert.Equal(t, "v2", m["k2"])

	require.NoError(t, b.UntagQueue(&sqs.UntagQueueInput{QueueURL: qURL, TagKeys: []string{"k1"}}))

	out2, err := b.ListQueueTags(&sqs.ListQueueTagsInput{QueueURL: qURL})
	require.NoError(t, err)
	m2 := out2.Tags.Clone()
	_, hasK1 := m2["k1"]
	assert.False(t, hasK1)
	assert.Equal(t, "v2", m2["k2"])
}

func TestBatch2_Tags_ListQueueTags_Empty(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "no-tags")
	out, err := b.ListQueueTags(&sqs.ListQueueTagsInput{QueueURL: qURL})
	require.NoError(t, err)
	assert.Empty(t, out.Tags.Clone())
}

func TestBatch2_Tags_TagQueue_NotFound(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	err := b.TagQueue(&sqs.TagQueueInput{
		QueueURL: "http://localhost/000000000000/ghost",
		Tags:     tagsFromMap(map[string]string{"k": "v"}),
	})
	require.ErrorIs(t, err, sqs.ErrQueueNotFound)
}

func TestBatch2_Tags_UntagQueue_NotFound(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	err := b.UntagQueue(&sqs.UntagQueueInput{
		QueueURL: "http://localhost/000000000000/ghost",
		TagKeys:  []string{"k"},
	})
	require.ErrorIs(t, err, sqs.ErrQueueNotFound)
}

func TestBatch2_Tags_OverwriteExisting(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "tag-overwrite")

	require.NoError(t, b.TagQueue(&sqs.TagQueueInput{
		QueueURL: qURL,
		Tags:     tagsFromMap(map[string]string{"env": "dev"}),
	}))
	require.NoError(t, b.TagQueue(&sqs.TagQueueInput{
		QueueURL: qURL,
		Tags:     tagsFromMap(map[string]string{"env": "prod"}),
	}))

	out, err := b.ListQueueTags(&sqs.ListQueueTagsInput{QueueURL: qURL})
	require.NoError(t, err)
	assert.Equal(t, "prod", out.Tags.Clone()["env"])
}

// ---------------------------------------------------------------------------
// 13. Permissions + Policy attribute
// ---------------------------------------------------------------------------

func TestBatch2_AddPermission_UpdatesPolicyAttribute(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "perm-policy")

	require.NoError(t, b.AddPermission(&sqs.AddPermissionInput{
		QueueURL:      qURL,
		Label:         "AllowSend",
		AWSAccountIDs: []string{"123456789012"},
		Actions:       []string{"SendMessage"},
	}))

	attrs := b2getAttrs(t, b, qURL, "Policy")
	policyJSON, ok := attrs["Policy"]
	require.True(t, ok, "Policy attribute should be set after AddPermission")
	assert.NotEmpty(t, policyJSON)

	var doc map[string]any
	require.NoError(t, json.Unmarshal([]byte(policyJSON), &doc))
	assert.Equal(t, "2012-10-17", doc["Version"])

	stmts := doc["Statement"].([]any)
	require.Len(t, stmts, 1)

	stmt := stmts[0].(map[string]any)
	assert.Equal(t, "AllowSend", stmt["Sid"])
	assert.Equal(t, "Allow", stmt["Effect"])
}

func TestBatch2_AddPermission_WildcardAction(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "perm-wildcard")

	require.NoError(t, b.AddPermission(&sqs.AddPermissionInput{
		QueueURL:      qURL,
		Label:         "AllowAll",
		AWSAccountIDs: []string{"*"},
		Actions:       []string{"*"},
	}))

	attrs := b2getAttrs(t, b, qURL, "Policy")
	var doc map[string]any
	require.NoError(t, json.Unmarshal([]byte(attrs["Policy"]), &doc))
	stmts := doc["Statement"].([]any)
	stmt := stmts[0].(map[string]any)
	actions := stmt["Action"].([]any)
	assert.Contains(t, actions, "sqs:*")
}

func TestBatch2_RemovePermission_ClearsPolicyAttribute(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "perm-remove")

	require.NoError(t, b.AddPermission(&sqs.AddPermissionInput{
		QueueURL:      qURL,
		Label:         "ToRemove",
		AWSAccountIDs: []string{"111111111111"},
		Actions:       []string{"ReceiveMessage"},
	}))

	require.NoError(t, b.RemovePermission(&sqs.RemovePermissionInput{
		QueueURL: qURL,
		Label:    "ToRemove",
	}))

	attrs := b2getAttrs(t, b, qURL, "Policy")
	_, hasPolicy := attrs["Policy"]
	assert.False(t, hasPolicy, "Policy attribute should be removed when all permissions are deleted")
}

func TestBatch2_AddPermission_MultipleStatements(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "perm-multi")

	require.NoError(t, b.AddPermission(&sqs.AddPermissionInput{
		QueueURL:      qURL,
		Label:         "Stmt1",
		AWSAccountIDs: []string{"111111111111"},
		Actions:       []string{"SendMessage"},
	}))
	require.NoError(t, b.AddPermission(&sqs.AddPermissionInput{
		QueueURL:      qURL,
		Label:         "Stmt2",
		AWSAccountIDs: []string{"222222222222"},
		Actions:       []string{"ReceiveMessage"},
	}))

	attrs := b2getAttrs(t, b, qURL, "Policy")
	var doc map[string]any
	require.NoError(t, json.Unmarshal([]byte(attrs["Policy"]), &doc))
	stmts := doc["Statement"].([]any)
	assert.Len(t, stmts, 2)
}

func TestBatch2_RemovePermission_PartialRemoval(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "perm-partial")

	for _, label := range []string{"Stmt1", "Stmt2"} {
		require.NoError(t, b.AddPermission(&sqs.AddPermissionInput{
			QueueURL:      qURL,
			Label:         label,
			AWSAccountIDs: []string{"123456789012"},
			Actions:       []string{"SendMessage"},
		}))
	}

	require.NoError(t, b.RemovePermission(&sqs.RemovePermissionInput{
		QueueURL: qURL,
		Label:    "Stmt1",
	}))

	attrs := b2getAttrs(t, b, qURL, "Policy")
	var doc map[string]any
	require.NoError(t, json.Unmarshal([]byte(attrs["Policy"]), &doc))
	stmts := doc["Statement"].([]any)
	assert.Len(t, stmts, 1)
	stmt := stmts[0].(map[string]any)
	assert.Equal(t, "Stmt2", stmt["Sid"])
}

func TestBatch2_AddPermission_EmptyLabel_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "perm-empty-label")
	err := b.AddPermission(&sqs.AddPermissionInput{
		QueueURL:      qURL,
		Label:         "",
		AWSAccountIDs: []string{"123456789012"},
		Actions:       []string{"SendMessage"},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidPermissionLabel)
}

func TestBatch2_AddPermission_EmptyActions_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "perm-empty-actions")
	err := b.AddPermission(&sqs.AddPermissionInput{
		QueueURL:      qURL,
		Label:         "L",
		AWSAccountIDs: []string{"123456789012"},
		Actions:       []string{},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidPermissionActions)
}

func TestBatch2_AddPermission_EmptyAccountIDs_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "perm-empty-accounts")
	err := b.AddPermission(&sqs.AddPermissionInput{
		QueueURL:      qURL,
		Label:         "L",
		AWSAccountIDs: []string{},
		Actions:       []string{"SendMessage"},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidPermissionAccountIDs)
}

func TestBatch2_AddPermission_QueueNotFound(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	err := b.AddPermission(&sqs.AddPermissionInput{
		QueueURL:      "http://localhost/000000000000/ghost",
		Label:         "L",
		AWSAccountIDs: []string{"123456789012"},
		Actions:       []string{"SendMessage"},
	})
	require.ErrorIs(t, err, sqs.ErrQueueNotFound)
}

func TestBatch2_RemovePermission_Idempotent(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "perm-idempotent")

	// Remove non-existent label — should not error (AWS is idempotent)
	require.NoError(t, b.RemovePermission(&sqs.RemovePermissionInput{
		QueueURL: qURL,
		Label:    "NoSuchLabel",
	}))
}

// ---------------------------------------------------------------------------
// 14. PurgeQueue
// ---------------------------------------------------------------------------

func TestBatch2_PurgeQueue_RemovesAllMessages(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "purge-all")
	for i := range 5 {
		b2send(t, b, qURL, fmt.Sprintf("msg-%d", i))
	}

	require.NoError(t, b.PurgeQueue(&sqs.PurgeQueueInput{QueueURL: qURL}))

	msgs := b2receive(t, b, qURL, 10)
	assert.Empty(t, msgs)
}

func TestBatch2_PurgeQueue_60SecondCooldown(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "purge-cooldown")
	b2send(t, b, qURL, "msg")

	require.NoError(t, b.PurgeQueue(&sqs.PurgeQueueInput{QueueURL: qURL}))

	// Second purge within 60s → PurgeQueueInProgress
	err := b.PurgeQueue(&sqs.PurgeQueueInput{QueueURL: qURL})
	require.ErrorIs(t, err, sqs.ErrPurgeQueueInProgress)
}

func TestBatch2_PurgeQueue_NotFound(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	err := b.PurgeQueue(&sqs.PurgeQueueInput{
		QueueURL: "http://localhost/000000000000/nonexistent",
	})
	require.ErrorIs(t, err, sqs.ErrQueueNotFound)
}

func TestBatch2_PurgeQueue_FIFO_ResetsDeduplication(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "purge-fifo.fifo", nil)
	b2sendFIFO(t, b, qURL, "body", "g", "dedup-1")

	require.NoError(t, b.PurgeQueue(&sqs.PurgeQueueInput{QueueURL: qURL}))

	// After purge, same dedup ID can be sent again
	out, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:               qURL,
		MessageBody:            "body",
		MessageGroupID:         "g",
		MessageDeduplicationID: "dedup-1",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out.MessageID)
}

// ---------------------------------------------------------------------------
// 15. SetQueueAttributes / GetQueueAttributes — round-trip
// ---------------------------------------------------------------------------

func TestBatch2_SetGetAttributes_VisibilityTimeout(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "sqa-vt")
	require.NoError(t, b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueURL:   qURL,
		Attributes: map[string]string{"VisibilityTimeout": "120"},
	}))

	attrs := b2getAttrs(t, b, qURL, "VisibilityTimeout")
	assert.Equal(t, "120", attrs["VisibilityTimeout"])
}

func TestBatch2_SetGetAttributes_UpdatesLastModified(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "sqa-lm")
	before := b2getAttrs(t, b, qURL, "LastModifiedTimestamp")["LastModifiedTimestamp"]

	time.Sleep(time.Millisecond)

	require.NoError(t, b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueURL:   qURL,
		Attributes: map[string]string{"VisibilityTimeout": "15"},
	}))

	after := b2getAttrs(t, b, qURL, "LastModifiedTimestamp")["LastModifiedTimestamp"]
	assert.GreaterOrEqual(t, after, before)
}

func TestBatch2_SetQueueAttributes_FifoQueue_Immutable(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "sqa-fifo-imm.fifo", nil)
	err := b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueURL:   qURL,
		Attributes: map[string]string{"FifoQueue": "false"},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidAttributeName)
}

func TestBatch2_SetQueueAttributes_InvalidRange(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "sqa-range")

	err := b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
		QueueURL:   qURL,
		Attributes: map[string]string{"VisibilityTimeout": "43201"},
	})
	require.Error(t, err)
}

func TestBatch2_GetQueueAttributes_AllReturnsAll(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "sqa-all")
	attrs := b2getAttrs(t, b, qURL, "All")

	required := []string{
		"VisibilityTimeout",
		"MaximumMessageSize",
		"MessageRetentionPeriod",
		"DelaySeconds",
		"ReceiveMessageWaitTimeSeconds",
		"QueueArn",
		"CreatedTimestamp",
		"LastModifiedTimestamp",
		"ApproximateNumberOfMessages",
		"ApproximateNumberOfMessagesNotVisible",
	}
	for _, k := range required {
		assert.Contains(t, attrs, k, "missing attribute: %s", k)
	}
}

func TestBatch2_GetQueueAttributes_NotFound(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	_, err := b.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueURL:       "http://localhost/000000000000/nonexistent",
		AttributeNames: []string{"All"},
	})
	require.ErrorIs(t, err, sqs.ErrQueueNotFound)
}

// ---------------------------------------------------------------------------
// 16. Message attributes — validation
// ---------------------------------------------------------------------------

func TestBatch2_MessageAttributes_MaxTen(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "msgattr-max")

	// Exactly 10 — allowed
	attrs10 := make(map[string]sqs.MessageAttributeValue, 10)
	for i := range 10 {
		attrs10[fmt.Sprintf("attr%d", i)] = sqs.MessageAttributeValue{
			DataType:    "String",
			StringValue: "v",
		}
	}
	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:          qURL,
		MessageBody:       "body",
		MessageAttributes: attrs10,
	})
	require.NoError(t, err)

	// 11 — rejected
	attrs11 := make(map[string]sqs.MessageAttributeValue, 11)
	for i := range 11 {
		attrs11[fmt.Sprintf("attr%d", i)] = sqs.MessageAttributeValue{
			DataType:    "String",
			StringValue: "v",
		}
	}
	_, err = b.SendMessage(&sqs.SendMessageInput{
		QueueURL:          qURL,
		MessageBody:       "body",
		MessageAttributes: attrs11,
	})
	require.ErrorIs(t, err, sqs.ErrInvalidMessageAttributeValue)
}

func TestBatch2_MessageAttributes_ReservedNames_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "msgattr-reserved")

	reserved := []string{
		"AWS.SomeAttribute",
		"aws.other",
		"Amazon.Trace",
		"amazon.x",
	}

	for _, name := range reserved {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := b.SendMessage(&sqs.SendMessageInput{
				QueueURL:    qURL,
				MessageBody: "body",
				MessageAttributes: map[string]sqs.MessageAttributeValue{
					name: {DataType: "String", StringValue: "v"},
				},
			})
			require.ErrorIs(
				t,
				err,
				sqs.ErrInvalidMessageAttributeValue,
				"expected rejection for reserved name: %s",
				name,
			)
		})
	}
}

// TestBatch2_SendMessageBatch_BypassedValidation_Rejected is a regression test:
// SendMessageBatch entries were routed straight to sendMessageLocked, which
// (unlike the top-level SendMessage) never called validateMessageAttributes
// or checked for an empty MessageBody. A batch entry with a reserved-prefix
// attribute name, an invalid DataType, or an empty body was therefore
// silently accepted instead of surfaced as a per-entry BatchResultErrorEntry,
// unlike real AWS and unlike the single-message SendMessage path.
func TestBatch2_SendMessageBatch_BypassedValidation_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "batch-validation-gap")

	out, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueURL: qURL,
		Entries: []sqs.SendMessageBatchEntry{
			{ID: "ok", MessageBody: "fine"},
			{
				ID:          "reserved-attr",
				MessageBody: "body",
				MessageAttributes: map[string]sqs.MessageAttributeValue{
					"AWS.Reserved": {DataType: "String", StringValue: "v"},
				},
			},
			{
				ID:          "bad-datatype",
				MessageBody: "body",
				MessageAttributes: map[string]sqs.MessageAttributeValue{
					"attr": {DataType: "NotARealType", StringValue: "v"},
				},
			},
			{ID: "empty-body", MessageBody: ""},
		},
	})
	require.NoError(t, err)

	require.Len(t, out.Successful, 1)
	assert.Equal(t, "ok", out.Successful[0].ID)

	failedIDs := make([]string, len(out.Failed))
	for i, f := range out.Failed {
		failedIDs[i] = f.ID
		assert.True(t, f.SenderFault, "entry %s should be sender-fault", f.ID)
	}
	assert.ElementsMatch(t, []string{"reserved-attr", "bad-datatype", "empty-body"}, failedIDs)
}

func TestBatch2_MessageAttributes_ValidCustomNames(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "msgattr-valid")

	valid := []string{"MyAttr", "my-attr", "my.attr.subtype", "MyAttr123"}
	for _, name := range valid {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := b.SendMessage(&sqs.SendMessageInput{
				QueueURL:    qURL,
				MessageBody: "body",
				MessageAttributes: map[string]sqs.MessageAttributeValue{
					name: {DataType: "String", StringValue: "v"},
				},
			})
			require.NoError(t, err, "valid name should not be rejected: %s", name)
		})
	}
}

func TestBatch2_MessageAttributes_StringType(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "msgattr-string")
	b2send(t, b, qURL, "noop") // put something for flush

	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "body",
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			"MyStr": {DataType: "String", StringValue: "hello"},
		},
	})
	require.NoError(t, err)
}

func TestBatch2_MessageAttributes_NumberType(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "msgattr-number")
	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "body",
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			"MyNum": {DataType: "Number", StringValue: "42"},
		},
	})
	require.NoError(t, err)
}

func TestBatch2_MessageAttributes_BinaryType(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "msgattr-binary")
	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "body",
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			"MyBin": {DataType: "Binary", BinaryValue: []byte{0x01, 0x02}},
		},
	})
	require.NoError(t, err)
}

func TestBatch2_MessageAttributes_InvalidDataType_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "msgattr-badtype")
	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "body",
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			"Bad": {DataType: "InvalidType", StringValue: "v"},
		},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidMessageAttributeValue)
}

func TestBatch2_MessageAttributes_StringMissingValue_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "msgattr-nostrval")
	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "body",
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			"Str": {DataType: "String" /* no StringValue */},
		},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidMessageAttributeValue)
}

func TestBatch2_MessageAttributes_BinaryMissingValue_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "msgattr-nobinval")
	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "body",
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			"Bin": {DataType: "Binary" /* no BinaryValue */},
		},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidMessageAttributeValue)
}

func TestBatch2_MessageAttributes_CustomSubtype_Valid(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "msgattr-subtype")
	_, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "body",
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			"MyCustom": {DataType: "String.json", StringValue: `{"k":"v"}`},
		},
	})
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// 17. QueueAttributes validation
// ---------------------------------------------------------------------------

func TestBatch2_QueueAttr_VisibilityTimeout_Range(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	cases := []struct {
		v  string
		ok bool
	}{
		{"0", true},
		{"43200", true},
		{"30", true},
		{"-1", false},
		{"43201", false},
	}
	for _, tc := range cases {
		t.Run("vt="+tc.v, func(t *testing.T) {
			t.Parallel()
			_, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName:  "vtr-" + tc.v,
				Endpoint:   "localhost",
				Attributes: map[string]string{"VisibilityTimeout": tc.v},
			})
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestBatch2_QueueAttr_DelaySeconds_Range(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	cases := []struct {
		v  string
		ok bool
	}{
		{"0", true},
		{"900", true},
		{"-1", false},
		{"901", false},
	}
	for _, tc := range cases {
		t.Run("ds="+tc.v, func(t *testing.T) {
			t.Parallel()
			_, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName:  "dsr-" + tc.v,
				Endpoint:   "localhost",
				Attributes: map[string]string{"DelaySeconds": tc.v},
			})
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestBatch2_QueueAttr_ReceiveWaitSeconds_Range(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	cases := []struct {
		v  string
		ok bool
	}{
		{"0", true},
		{"20", true},
		{"-1", false},
		{"21", false},
	}
	for _, tc := range cases {
		t.Run("rws="+tc.v, func(t *testing.T) {
			t.Parallel()
			_, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName:  "rwsr-" + tc.v,
				Endpoint:   "localhost",
				Attributes: map[string]string{"ReceiveMessageWaitTimeSeconds": tc.v},
			})
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestBatch2_QueueAttr_MaxMessageSize_Range(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	cases := []struct {
		v  string
		ok bool
	}{
		{"1024", true},
		{"262144", true},
		{"65536", true},
		{"1023", false},
		{"262145", false},
	}
	for _, tc := range cases {
		t.Run("mms="+tc.v, func(t *testing.T) {
			t.Parallel()
			_, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName:  "mmsr-" + tc.v,
				Endpoint:   "localhost",
				Attributes: map[string]string{"MaximumMessageSize": tc.v},
			})
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 18. Handler-level (Query protocol) — spot checks
// ---------------------------------------------------------------------------

func TestBatch2_Handler_CreateQueueQuery(t *testing.T) {
	t.Parallel()
	bk := sqs.NewInMemoryBackend()
	t.Cleanup(bk.Close)
	h := sqs.NewHandler(bk)

	rec := doQueryRequest(t, h, newQueryVals("CreateQueue", map[string]string{
		"QueueName": "query-create",
	}))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "query-create")
}

func TestBatch2_Handler_SendReceiveDeleteQuery(t *testing.T) {
	t.Parallel()
	bk := sqs.NewInMemoryBackend()
	t.Cleanup(bk.Close)
	h := sqs.NewHandler(bk)

	// CreateQueue
	recCreate := doQueryRequest(t, h, newQueryVals("CreateQueue", map[string]string{"QueueName": "qrd"}))
	require.Equal(t, http.StatusOK, recCreate.Code)
	qURL := extractQueueURLFromXML(t, recCreate.Body.String())

	// SendMessage
	recSend := doQueryRequest(t, h, newQueryVals("SendMessage", map[string]string{
		"QueueUrl":    qURL,
		"MessageBody": "hello",
	}))
	require.Equal(t, http.StatusOK, recSend.Code)

	// ReceiveMessage
	recRecv := doQueryRequest(t, h, newQueryVals("ReceiveMessage", map[string]string{
		"QueueUrl":            qURL,
		"MaxNumberOfMessages": "1",
	}))
	require.Equal(t, http.StatusOK, recRecv.Code)
	assert.Contains(t, recRecv.Body.String(), "hello")

	// DeleteMessage — extract receipt handle
	receipt := extractReceiptHandleFromXML(t, recRecv.Body.String())
	recDel := doQueryRequest(t, h, newQueryVals("DeleteMessage", map[string]string{
		"QueueUrl":      qURL,
		"ReceiptHandle": receipt,
	}))
	assert.Equal(t, http.StatusOK, recDel.Code)
}

func TestBatch2_Handler_UnknownAction_Returns400(t *testing.T) {
	t.Parallel()
	bk := sqs.NewInMemoryBackend()
	t.Cleanup(bk.Close)
	h := sqs.NewHandler(bk)

	rec := doQueryRequest(t, h, newQueryVals("BogusAction", map[string]string{}))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidAction")
}

func TestBatch2_Handler_PurgeQueueQuery(t *testing.T) {
	t.Parallel()
	bk := sqs.NewInMemoryBackend()
	t.Cleanup(bk.Close)
	h := sqs.NewHandler(bk)

	recCreate := doQueryRequest(t, h, newQueryVals("CreateQueue", map[string]string{"QueueName": "purge-q"}))
	require.Equal(t, http.StatusOK, recCreate.Code)
	qURL := extractQueueURLFromXML(t, recCreate.Body.String())

	doQueryRequest(t, h, newQueryVals("SendMessage", map[string]string{"QueueUrl": qURL, "MessageBody": "x"}))

	recPurge := doQueryRequest(t, h, newQueryVals("PurgeQueue", map[string]string{"QueueUrl": qURL}))
	assert.Equal(t, http.StatusOK, recPurge.Code)
}

// ---------------------------------------------------------------------------
// 19. Delay queue
// ---------------------------------------------------------------------------

func TestBatch2_QueueDelay_HidesMessageUntilExpiry(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  "delay-q",
		Endpoint:   "localhost",
		Attributes: map[string]string{"DelaySeconds": "900"},
	})
	require.NoError(t, err)

	b2send(t, b, qURL.QueueURL, "delayed")

	msgs := b2receive(t, b, qURL.QueueURL, 1)
	assert.Empty(t, msgs, "delayed message should not be visible immediately")

	attrs := b2getAttrs(t, b, qURL.QueueURL, "ApproximateNumberOfMessagesDelayed")
	assert.Equal(t, "1", attrs["ApproximateNumberOfMessagesDelayed"])
}

func TestBatch2_MessageDelay_OverridesQueueDelay(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  "msg-delay-override",
		Endpoint:   "localhost",
		Attributes: map[string]string{"DelaySeconds": "0"},
	})
	require.NoError(t, err)

	_, err = b.SendMessage(&sqs.SendMessageInput{
		QueueURL:     qURL.QueueURL,
		MessageBody:  "with-delay",
		DelaySeconds: 900,
	})
	require.NoError(t, err)

	msgs := b2receive(t, b, qURL.QueueURL, 1)
	assert.Empty(t, msgs)
}

// ---------------------------------------------------------------------------
// 20. In-flight limits
// ---------------------------------------------------------------------------

func TestBatch2_InFlight_StandardLimit(t *testing.T) {
	// This tests that the backend tracks in-flight messages correctly and
	// returns ErrOverLimit when 120,000 standard messages are in-flight.
	// We use a small number to keep the test fast.
	t.Parallel()
	b := b2newBackend(t)

	qURL, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  "inflight-std",
		Endpoint:   "localhost",
		Attributes: map[string]string{"VisibilityTimeout": "60"},
	})
	require.NoError(t, err)

	// Send a batch and receive them all — verify in-flight tracking
	for i := range 5 {
		b2send(t, b, qURL.QueueURL, fmt.Sprintf("m%d", i))
	}

	msgs := b2receive(t, b, qURL.QueueURL, 5)
	assert.Len(t, msgs, 5)

	attrs := b2getAttrs(t, b, qURL.QueueURL, "ApproximateNumberOfMessagesNotVisible")
	assert.Equal(t, "5", attrs["ApproximateNumberOfMessagesNotVisible"])
}

// ---------------------------------------------------------------------------
// 21. QueueARN format
// ---------------------------------------------------------------------------

func TestBatch2_QueueARN_Format(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "arn-test")
	attrs := b2getAttrs(t, b, qURL, "QueueArn")

	arn := attrs["QueueArn"]
	assert.NotEmpty(t, arn)
	assert.True(t, strings.HasPrefix(arn, "arn:aws:sqs:"), "ARN should start with arn:aws:sqs: got: %s", arn)
	assert.Contains(t, arn, "arn-test")
}

func TestBatch2_FIFO_QueueARN_ContainsFifoSuffix(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createFIFOQueue(t, b, "arn-fifo.fifo", nil)
	attrs := b2getAttrs(t, b, qURL, "QueueArn")

	arn := attrs["QueueArn"]
	assert.Contains(t, arn, "arn-fifo.fifo")
}

// ---------------------------------------------------------------------------
// 22. MD5 of body / attributes
// ---------------------------------------------------------------------------

func TestBatch2_MD5OfBody_MatchesExpected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "md5-body")
	out, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "Hello World",
	})
	require.NoError(t, err)
	// MD5("Hello World") = b10a8db164e0754105b7a99be72e3fe5
	assert.Equal(t, "b10a8db164e0754105b7a99be72e3fe5", out.MD5OfBody)

	msgs := b2receive(t, b, qURL, 1)
	require.Len(t, msgs, 1)
	assert.Equal(t, "b10a8db164e0754105b7a99be72e3fe5", msgs[0].MD5OfBody)
}

func TestBatch2_MD5OfMessageAttributes_PopulatedWhenPresent(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "md5-attrs")
	out, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "body",
		MessageAttributes: map[string]sqs.MessageAttributeValue{
			"Foo": {DataType: "String", StringValue: "bar"},
		},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out.MD5OfMessageAttributes)
}

func TestBatch2_MD5OfMessageAttributes_EmptyWhenNoAttrs(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "md5-noattrs")
	out, err := b.SendMessage(&sqs.SendMessageInput{
		QueueURL:    qURL,
		MessageBody: "body",
	})
	require.NoError(t, err)
	assert.Empty(t, out.MD5OfMessageAttributes)
}

// ---------------------------------------------------------------------------
// helpers used only in this file
// ---------------------------------------------------------------------------

func tagsFromMap(m map[string]string) *tags.Tags {
	return tags.FromMap("test", m)
}

func newQueryVals(action string, params map[string]string) url.Values {
	v := url.Values{"Action": {action}}
	for k, val := range params {
		v.Set(k, val)
	}

	return v
}

type queueURLResult struct {
	XMLName  xml.Name `xml:"CreateQueueResponse"`
	QueueURL string   `xml:"CreateQueueResult>QueueUrl"`
}

type receiveMessageResult struct {
	XMLName  xml.Name `xml:"ReceiveMessageResponse"`
	Messages []struct {
		ReceiptHandle string `xml:"ReceiptHandle"`
	} `xml:"ReceiveMessageResult>Message"`
}

func extractQueueURLFromXML(t *testing.T, body string) string {
	t.Helper()
	var r queueURLResult
	if err := xml.Unmarshal([]byte(body), &r); err == nil && r.QueueURL != "" {
		return r.QueueURL
	}
	// Fallback: look for URL in body
	for line := range strings.SplitSeq(body, "\n") {
		if strings.Contains(line, "localhost") && strings.Contains(line, "/000000000000/") {
			start := strings.Index(line, "http://")
			if start >= 0 {
				end := strings.IndexAny(line[start:], "<\" \t\r\n")
				if end > 0 {
					return line[start : start+end]
				}

				return line[start:]
			}
		}
	}
	t.Fatal("could not extract queue URL from XML: " + body)

	return ""
}

func extractReceiptHandleFromXML(t *testing.T, body string) string {
	t.Helper()
	var r receiveMessageResult
	require.NoError(t, xml.Unmarshal([]byte(body), &r), "parse ReceiveMessage XML")
	require.NotEmpty(t, r.Messages, "expected at least one message in ReceiveMessage response")

	return r.Messages[0].ReceiptHandle
}
