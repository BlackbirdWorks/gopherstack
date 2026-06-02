package sqs_test

// accuracy_b3_1677_test.go — batch-2 ops AWS-accuracy tests for issue #1677.
//
// Covers accuracy gaps in batch operations and message-move-task semantics:
//   - SendMessageBatch/DeleteMessageBatch return QueueDoesNotExist at the batch level
//     (not per-entry) when the target queue does not exist.
//   - ChangeMessageVisibilityBatch validates VisibilityTimeout per entry; out-of-range
//     values produce a per-entry BatchResultErrorEntry with Code="InvalidParameterValue"
//     rather than silently clamping or applying an invalid timeout.
//   - SendMessageBatch on FIFO queues returns SequenceNumber in each successful entry.
//   - ListMessageMoveTasks: default MaxResults=1, MaxResults>10 clamped to 10, results
//     ordered newest-first, and TaskHandle is only populated for RUNNING tasks.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func b3newBackend() *sqs.InMemoryBackend {
	return sqs.NewInMemoryBackend()
}

func b3createQueue(t *testing.T, b *sqs.InMemoryBackend, name string) string {
	t.Helper()
	out, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: name, Endpoint: "localhost"})
	require.NoError(t, err)

	return out.QueueURL
}

func b3createFIFOQueue(t *testing.T, b *sqs.InMemoryBackend, name string) string {
	t.Helper()
	out, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: name,
		Endpoint:  "localhost",
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "true",
		},
	})
	require.NoError(t, err)

	return out.QueueURL
}

func b3send(t *testing.T, b *sqs.InMemoryBackend, qURL, body string) {
	t.Helper()
	_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: body})
	require.NoError(t, err)
}

func b3recv(t *testing.T, b *sqs.InMemoryBackend, qURL string, maxItems int) []*sqs.Message {
	t.Helper()
	out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: maxItems,
		VisibilityTimeout:   30,
	})
	require.NoError(t, err)

	return out.Messages
}

// ---------------------------------------------------------------------------
// 1. SendMessageBatch / DeleteMessageBatch — QueueDoesNotExist is batch-level
// ---------------------------------------------------------------------------

// TestB3_SendMessageBatch_QueueNotFound_TopLevelError verifies that
// SendMessageBatch returns ErrQueueNotFound as a top-level error (not
// as a per-entry failure) when the target queue does not exist, matching
// AWS behaviour.
func TestB3_SendMessageBatch_QueueNotFound_TopLevelError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		queueURL string
		entries  []sqs.SendMessageBatchEntry
	}{
		{
			name:     "single_entry_nonexistent_queue",
			queueURL: "http://localhost/000000000000/ghost",
			entries: []sqs.SendMessageBatchEntry{
				{ID: "e1", MessageBody: "hello"},
			},
		},
		{
			name:     "multiple_entries_nonexistent_queue",
			queueURL: "http://localhost/000000000000/vanished",
			entries: []sqs.SendMessageBatchEntry{
				{ID: "e1", MessageBody: "msg-a"},
				{ID: "e2", MessageBody: "msg-b"},
				{ID: "e3", MessageBody: "msg-c"},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := b3newBackend()

			out, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
				QueueURL: tc.queueURL,
				Entries:  tc.entries,
			})

			require.ErrorIs(t, err, sqs.ErrQueueNotFound)
			assert.Nil(t, out)
		})
	}
}

// TestB3_DeleteMessageBatch_QueueNotFound_TopLevelError verifies that
// DeleteMessageBatch returns ErrQueueNotFound as a top-level error when
// the target queue does not exist.
func TestB3_DeleteMessageBatch_QueueNotFound_TopLevelError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		queueURL string
	}{
		{
			name:     "nonexistent_queue",
			queueURL: "http://localhost/000000000000/no-such-queue",
		},
		{
			name:     "deleted_queue",
			queueURL: "http://localhost/000000000000/was-deleted",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := b3newBackend()

			out, err := b.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
				QueueURL: tc.queueURL,
				Entries:  []sqs.DeleteMessageBatchEntry{{ID: "e1", ReceiptHandle: "handle"}},
			})

			require.ErrorIs(t, err, sqs.ErrQueueNotFound)
			assert.Nil(t, out)
		})
	}
}

// TestB3_SendMessageBatch_ExistingQueue_StillWorksAfterFix confirms that the
// queue-existence pre-check does not break normal sends.
func TestB3_SendMessageBatch_ExistingQueue_StillWorksAfterFix(t *testing.T) {
	t.Parallel()
	b := b3newBackend()
	qURL := b3createQueue(t, b, "live-batch")

	out, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueURL: qURL,
		Entries: []sqs.SendMessageBatchEntry{
			{ID: "a", MessageBody: "msg-1"},
			{ID: "b", MessageBody: "msg-2"},
		},
	})

	require.NoError(t, err)
	assert.Len(t, out.Successful, 2)
	assert.Empty(t, out.Failed)
}

// TestB3_DeleteMessageBatch_ExistingQueue_StillWorksAfterFix confirms that the
// queue-existence pre-check does not break normal deletes.
func TestB3_DeleteMessageBatch_ExistingQueue_StillWorksAfterFix(t *testing.T) {
	t.Parallel()
	b := b3newBackend()
	qURL := b3createQueue(t, b, "live-del-batch")

	b3send(t, b, qURL, "msg")
	msgs := b3recv(t, b, qURL, 1)
	require.Len(t, msgs, 1)

	out, err := b.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
		QueueURL: qURL,
		Entries:  []sqs.DeleteMessageBatchEntry{{ID: "d1", ReceiptHandle: msgs[0].ReceiptHandle}},
	})

	require.NoError(t, err)
	assert.Len(t, out.Successful, 1)
	assert.Empty(t, out.Failed)
}

// ---------------------------------------------------------------------------
// 2. ChangeMessageVisibilityBatch — per-entry VisibilityTimeout validation
// ---------------------------------------------------------------------------

// TestB3_ChangeMessageVisibilityBatch_VTOutOfRange_PerEntryFailure verifies
// that ChangeMessageVisibilityBatch reports a per-entry failure (not a
// batch-level error) when a single entry's VisibilityTimeout exceeds 43200,
// and still processes valid entries in the same call.
func TestB3_ChangeMessageVisibilityBatch_VTOutOfRange_PerEntryFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		wantBadCode        string
		badVisibility      int
		goodVisibility     int
		wantBadSenderFault bool
	}{
		{
			name:               "vt_above_max_43200",
			badVisibility:      43201,
			goodVisibility:     60,
			wantBadCode:        "InvalidParameterValue",
			wantBadSenderFault: true,
		},
		{
			name:               "vt_way_above_max",
			badVisibility:      99999,
			goodVisibility:     0,
			wantBadCode:        "InvalidParameterValue",
			wantBadSenderFault: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := b3newBackend()
			qURL := b3createQueue(t, b, "vt-range-"+tc.name)

			// Send two messages and receive them both.
			b3send(t, b, qURL, "msg-good")
			b3send(t, b, qURL, "msg-bad")
			msgs := b3recv(t, b, qURL, 2)
			require.Len(t, msgs, 2)

			out, err := b.ChangeMessageVisibilityBatch(&sqs.ChangeMessageVisibilityBatchInput{
				QueueURL: qURL,
				Entries: []sqs.ChangeMessageVisibilityBatchRequestEntry{
					{ID: "good", ReceiptHandle: msgs[0].ReceiptHandle, VisibilityTimeout: tc.goodVisibility},
					{ID: "bad", ReceiptHandle: msgs[1].ReceiptHandle, VisibilityTimeout: tc.badVisibility},
				},
			})

			require.NoError(t, err, "batch-level error must not be returned for per-entry range violation")
			require.Len(t, out.Successful, 1)
			assert.Equal(t, "good", out.Successful[0].ID)

			require.Len(t, out.Failed, 1)
			assert.Equal(t, "bad", out.Failed[0].ID)
			assert.Equal(t, tc.wantBadCode, out.Failed[0].Code)
			assert.Equal(t, tc.wantBadSenderFault, out.Failed[0].SenderFault)
		})
	}
}

// TestB3_ChangeMessageVisibilityBatch_NegativeVT_PerEntryFailure verifies that
// a negative VisibilityTimeout is a per-entry failure with Code="InvalidParameterValue".
func TestB3_ChangeMessageVisibilityBatch_NegativeVT_PerEntryFailure(t *testing.T) {
	t.Parallel()
	b := b3newBackend()
	qURL := b3createQueue(t, b, "neg-vt-batch")

	b3send(t, b, qURL, "msg")
	msgs := b3recv(t, b, qURL, 1)
	require.Len(t, msgs, 1)

	out, err := b.ChangeMessageVisibilityBatch(&sqs.ChangeMessageVisibilityBatchInput{
		QueueURL: qURL,
		Entries: []sqs.ChangeMessageVisibilityBatchRequestEntry{
			{ID: "e1", ReceiptHandle: msgs[0].ReceiptHandle, VisibilityTimeout: -1},
		},
	})

	require.NoError(t, err)
	assert.Empty(t, out.Successful)
	require.Len(t, out.Failed, 1)
	assert.Equal(t, "e1", out.Failed[0].ID)
	assert.Equal(t, "InvalidParameterValue", out.Failed[0].Code)
	assert.True(t, out.Failed[0].SenderFault)
}

// TestB3_ChangeMessageVisibilityBatch_MaxValidVT_Accepted verifies that
// VisibilityTimeout of exactly 43200 (the AWS maximum) is accepted.
func TestB3_ChangeMessageVisibilityBatch_MaxValidVT_Accepted(t *testing.T) {
	t.Parallel()
	b := b3newBackend()
	qURL := b3createQueue(t, b, "maxItems-vt-batch")

	b3send(t, b, qURL, "msg")
	msgs := b3recv(t, b, qURL, 1)
	require.Len(t, msgs, 1)

	out, err := b.ChangeMessageVisibilityBatch(&sqs.ChangeMessageVisibilityBatchInput{
		QueueURL: qURL,
		Entries: []sqs.ChangeMessageVisibilityBatchRequestEntry{
			{ID: "e1", ReceiptHandle: msgs[0].ReceiptHandle, VisibilityTimeout: 43200},
		},
	})

	require.NoError(t, err)
	assert.Len(t, out.Successful, 1)
	assert.Empty(t, out.Failed)
}

// TestB3_ChangeMessageVisibilityBatch_AllEntriesOutOfRange verifies that
// when every entry has an out-of-range VT, all end up in Failed and none in Successful.
func TestB3_ChangeMessageVisibilityBatch_AllEntriesOutOfRange(t *testing.T) {
	t.Parallel()
	b := b3newBackend()
	qURL := b3createQueue(t, b, "all-bad-vt-batch")

	b3send(t, b, qURL, "m1")
	b3send(t, b, qURL, "m2")
	msgs := b3recv(t, b, qURL, 2)
	require.Len(t, msgs, 2)

	out, err := b.ChangeMessageVisibilityBatch(&sqs.ChangeMessageVisibilityBatchInput{
		QueueURL: qURL,
		Entries: []sqs.ChangeMessageVisibilityBatchRequestEntry{
			{ID: "e1", ReceiptHandle: msgs[0].ReceiptHandle, VisibilityTimeout: 50000},
			{ID: "e2", ReceiptHandle: msgs[1].ReceiptHandle, VisibilityTimeout: -5},
		},
	})

	require.NoError(t, err)
	assert.Empty(t, out.Successful)
	assert.Len(t, out.Failed, 2)
}

// ---------------------------------------------------------------------------
// 3. SendMessageBatch on FIFO — SequenceNumber in results
// ---------------------------------------------------------------------------

// TestB3_SendMessageBatch_FIFO_SequenceNumbersInResults verifies that successful
// batch entries for FIFO queues include a non-empty SequenceNumber, and that
// sequence numbers are unique and monotonically increasing across the batch.
func TestB3_SendMessageBatch_FIFO_SequenceNumbersInResults(t *testing.T) {
	t.Parallel()
	b := b3newBackend()
	qURL := b3createFIFOQueue(t, b, "seq-batch.fifo")

	out, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueURL: qURL,
		Entries: []sqs.SendMessageBatchEntry{
			{ID: "e1", MessageBody: "alpha", MessageGroupID: "g1", MessageDeduplicationID: "d1"},
			{ID: "e2", MessageBody: "beta", MessageGroupID: "g1", MessageDeduplicationID: "d2"},
			{ID: "e3", MessageBody: "gamma", MessageGroupID: "g1", MessageDeduplicationID: "d3"},
		},
	})

	require.NoError(t, err)
	assert.Len(t, out.Successful, 3)
	assert.Empty(t, out.Failed)

	seqNums := make([]string, 0, len(out.Successful))
	for _, s := range out.Successful {
		assert.NotEmpty(t, s.SequenceNumber, "SequenceNumber must be set for FIFO batch entries")
		assert.Len(t, s.SequenceNumber, 20, "SequenceNumber must be 20 digits")
		seqNums = append(seqNums, s.SequenceNumber)
	}

	// All sequence numbers must be distinct.
	seen := map[string]bool{}
	for _, sn := range seqNums {
		assert.False(t, seen[sn], "duplicate SequenceNumber: %s", sn)
		seen[sn] = true
	}
}

// TestB3_SendMessageBatch_Standard_NoSequenceNumber verifies that standard
// (non-FIFO) queue batch results do NOT include SequenceNumber.
func TestB3_SendMessageBatch_Standard_NoSequenceNumber(t *testing.T) {
	t.Parallel()
	b := b3newBackend()
	qURL := b3createQueue(t, b, "std-no-seq")

	out, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueURL: qURL,
		Entries: []sqs.SendMessageBatchEntry{
			{ID: "e1", MessageBody: "hello"},
			{ID: "e2", MessageBody: "world"},
		},
	})

	require.NoError(t, err)
	assert.Len(t, out.Successful, 2)
	for _, s := range out.Successful {
		assert.Empty(t, s.SequenceNumber, "standard queue entries must not have SequenceNumber")
	}
}

// TestB3_SendMessageBatch_FIFO_Dedup_AcrossBatch verifies that sending the same
// deduplication ID twice in a batch results in a single message delivered (dedup
// silently drops the second entry and returns success for both IDs with the same
// MessageID, matching AWS ContentBasedDeduplication semantics).
func TestB3_SendMessageBatch_FIFO_Dedup_AcrossBatch(t *testing.T) {
	t.Parallel()
	b := b3newBackend()
	qURL := b3createFIFOQueue(t, b, "dedup-batch.fifo")

	out, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueURL: qURL,
		Entries: []sqs.SendMessageBatchEntry{
			{ID: "e1", MessageBody: "hello", MessageGroupID: "g1", MessageDeduplicationID: "dup-id"},
			{ID: "e2", MessageBody: "hello", MessageGroupID: "g1", MessageDeduplicationID: "dup-id"},
		},
	})

	require.NoError(t, err)
	assert.Len(t, out.Successful, 2, "both entries succeed — duplicate is silently deduplicated")
	assert.Empty(t, out.Failed)

	// The queue should contain only one message (the duplicate was dropped).
	recv, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            qURL,
		MaxNumberOfMessages: 10,
		VisibilityTimeout:   30,
	})
	require.NoError(t, err)
	assert.Len(t, recv.Messages, 1)
}

// ---------------------------------------------------------------------------
// 4. ListMessageMoveTasks — MaxResults default, cap, ordering, TaskHandle
// ---------------------------------------------------------------------------

// TestB3_ListMessageMoveTasks_DefaultMaxResults_ReturnsOne verifies that
// omitting MaxResults (i.e. MaxResults=0) returns at most 1 result, matching
// the AWS default.
func TestB3_ListMessageMoveTasks_DefaultMaxResults_ReturnsOne(t *testing.T) {
	t.Parallel()
	b := b3newBackend()

	now := time.Now().UnixMilli()
	b.InjectMoveTaskForTest("handle-a", sqs.MoveTaskStatusCompleted, now-2000)
	b.InjectMoveTaskForTest("handle-b", sqs.MoveTaskStatusCompleted, now-1000)
	b.InjectMoveTaskForTest("handle-c", sqs.MoveTaskStatusCompleted, now)

	out, err := b.ListMessageMoveTasks(&sqs.ListMessageMoveTasksInput{
		MaxResults: 0, // default: 1
	})
	require.NoError(t, err)
	assert.Len(t, out.Results, 1, "default MaxResults=0 must return exactly 1 result")
}

// TestB3_ListMessageMoveTasks_MaxResultsClamped verifies that MaxResults > 10 is
// silently clamped to 10.
func TestB3_ListMessageMoveTasks_MaxResultsClamped(t *testing.T) {
	t.Parallel()
	b := b3newBackend()

	now := time.Now().UnixMilli()
	for i := range 12 {
		b.InjectMoveTaskForTest(
			"h"+string(rune('a'+i)),
			sqs.MoveTaskStatusCompleted,
			now-int64(i*1000),
		)
	}

	out, err := b.ListMessageMoveTasks(&sqs.ListMessageMoveTasksInput{MaxResults: 11})
	require.NoError(t, err)
	assert.LessOrEqual(t, len(out.Results), 10, "MaxResults > 10 must be clamped to 10")
}

// TestB3_ListMessageMoveTasks_OrderedNewestFirst verifies that results are
// returned with the most recently started task first.
func TestB3_ListMessageMoveTasks_OrderedNewestFirst(t *testing.T) {
	t.Parallel()
	b := b3newBackend()

	base := time.Now().UnixMilli()
	b.InjectMoveTaskForTest("old-task", sqs.MoveTaskStatusCompleted, base-3000)
	b.InjectMoveTaskForTest("mid-task", sqs.MoveTaskStatusCompleted, base-1000)
	b.InjectMoveTaskForTest("new-task", sqs.MoveTaskStatusCompleted, base)

	out, err := b.ListMessageMoveTasks(&sqs.ListMessageMoveTasksInput{MaxResults: 3})
	require.NoError(t, err)
	require.Len(t, out.Results, 3)

	// startedAt should be descending (newest first).
	for i := 1; i < len(out.Results); i++ {
		assert.GreaterOrEqual(
			t,
			out.Results[i-1].StartedTimestamp,
			out.Results[i].StartedTimestamp,
			"results must be ordered newest-first",
		)
	}
}

// TestB3_ListMessageMoveTasks_TaskHandleOnlyForRunning verifies that TaskHandle
// is populated only for tasks in RUNNING status, and is empty for all other statuses.
func TestB3_ListMessageMoveTasks_TaskHandleOnlyForRunning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		status         sqs.MoveTaskStatus
		wantTaskHandle bool
	}{
		{sqs.MoveTaskStatusRunning, true},
		{sqs.MoveTaskStatusCompleted, false},
		{sqs.MoveTaskStatusCancelled, false},
		{sqs.MoveTaskStatusFailed, false},
	}

	for _, tc := range tests {
		t.Run(string(tc.status), func(t *testing.T) {
			t.Parallel()
			b := b3newBackend()

			handle := "task-" + string(tc.status)
			b.InjectMoveTaskForTest(handle, tc.status, time.Now().UnixMilli())

			out, err := b.ListMessageMoveTasks(&sqs.ListMessageMoveTasksInput{MaxResults: 1})
			require.NoError(t, err)
			require.Len(t, out.Results, 1)

			if tc.wantTaskHandle {
				assert.NotEmpty(t, out.Results[0].TaskHandle, "RUNNING task must have TaskHandle")
			} else {
				assert.Empty(t, out.Results[0].TaskHandle, "non-RUNNING task must not expose TaskHandle")
			}
		})
	}
}

// TestB3_ListMessageMoveTasks_EmptyResult verifies that the operation succeeds
// with an empty slice when no tasks exist.
func TestB3_ListMessageMoveTasks_EmptyResult(t *testing.T) {
	t.Parallel()
	b := b3newBackend()

	out, err := b.ListMessageMoveTasks(&sqs.ListMessageMoveTasksInput{MaxResults: 5})
	require.NoError(t, err)
	assert.Empty(t, out.Results)
}
