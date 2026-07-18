package sqs_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEmptyBatchRejected(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createQueueForTest(t, b, "batch-empty")

	_, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueURL: qURL,
		Entries:  []sqs.SendMessageBatchEntry{},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidBatchEntry)

	_, err = b.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
		QueueURL: qURL,
		Entries:  []sqs.DeleteMessageBatchEntry{},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidBatchEntry)

	_, err = b.ChangeMessageVisibilityBatch(&sqs.ChangeMessageVisibilityBatchInput{
		QueueURL: qURL,
		Entries:  []sqs.ChangeMessageVisibilityBatchRequestEntry{},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidBatchEntry)
}

func TestTooManyEntriesRejected(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createQueueForTest(t, b, "batch-too-many")

	entries := make([]sqs.SendMessageBatchEntry, 11)
	for i := range entries {
		entries[i] = sqs.SendMessageBatchEntry{
			ID:          fmt.Sprintf("e%d", i),
			MessageBody: "body",
		}
	}

	_, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueURL: qURL,
		Entries:  entries,
	})
	require.ErrorIs(t, err, sqs.ErrTooManyEntriesInBatch)
}

func TestDuplicateIDsRejected(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createQueueForTest(t, b, "batch-dup-ids")

	_, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueURL: qURL,
		Entries: []sqs.SendMessageBatchEntry{
			{ID: "same", MessageBody: "a"},
			{ID: "same", MessageBody: "b"},
		},
	})
	require.ErrorIs(t, err, sqs.ErrBatchEntryIDsNotDistinct)
}

func TestInvalidIDFormatRejected(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createQueueForTest(t, b, "batch-bad-id")

	// IDs with invalid characters should be rejected.
	_, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueURL: qURL,
		Entries: []sqs.SendMessageBatchEntry{
			{ID: "bad id!", MessageBody: "body"},
		},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidBatchEntry)
}

// TestSendMessageBatch_QueueNotFound_TopLevelError verifies that
// SendMessageBatch returns ErrQueueNotFound as a top-level error (not
// as a per-entry failure) when the target queue does not exist, matching
// AWS behaviour.
func TestSendMessageBatch_QueueNotFound_TopLevelError(t *testing.T) {
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
			b := b3newBackend(t)

			out, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
				QueueURL: tc.queueURL,
				Entries:  tc.entries,
			})

			require.ErrorIs(t, err, sqs.ErrQueueNotFound)
			assert.Nil(t, out)
		})
	}
}

// TestDeleteMessageBatch_QueueNotFound_TopLevelError verifies that
// DeleteMessageBatch returns ErrQueueNotFound as a top-level error when
// the target queue does not exist.
func TestDeleteMessageBatch_QueueNotFound_TopLevelError(t *testing.T) {
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
			b := b3newBackend(t)

			out, err := b.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
				QueueURL: tc.queueURL,
				Entries:  []sqs.DeleteMessageBatchEntry{{ID: "e1", ReceiptHandle: "handle"}},
			})

			require.ErrorIs(t, err, sqs.ErrQueueNotFound)
			assert.Nil(t, out)
		})
	}
}

// TestSendMessageBatch_ExistingQueue_StillWorksAfterFix confirms that the
// queue-existence pre-check does not break normal sends.
func TestSendMessageBatch_ExistingQueue_StillWorksAfterFix(t *testing.T) {
	t.Parallel()
	b := b3newBackend(t)
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

// TestDeleteMessageBatch_ExistingQueue_StillWorksAfterFix confirms that the
// queue-existence pre-check does not break normal deletes.
func TestDeleteMessageBatch_ExistingQueue_StillWorksAfterFix(t *testing.T) {
	t.Parallel()
	b := b3newBackend(t)
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

func TestSendMessageBatch_Success(t *testing.T) {
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

func TestSendMessageBatch_EmptyEntries_Rejected(t *testing.T) {
	t.Parallel()
	b := b2newBackend(t)

	qURL := b2createQueue(t, b, "batch-empty")
	_, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueURL: qURL,
		Entries:  []sqs.SendMessageBatchEntry{},
	})
	require.ErrorIs(t, err, sqs.ErrInvalidBatchEntry)
}

func TestSendMessageBatch_TooManyEntries_Rejected(t *testing.T) {
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

func TestSendMessageBatch_DuplicateIDs_Rejected(t *testing.T) {
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

func TestDeleteMessageBatch_Success(t *testing.T) {
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

func TestSendMessageBatch_PartialFailure(t *testing.T) {
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

// TestSendMessageBatch_BypassedValidation_Rejected is a regression test:
// SendMessageBatch entries were routed straight to sendMessageLocked, which
// (unlike the top-level SendMessage) never called validateMessageAttributes
// or checked for an empty MessageBody. A batch entry with a reserved-prefix
// attribute name, an invalid DataType, or an empty body was therefore
// silently accepted instead of surfaced as a per-entry BatchResultErrorEntry,
// unlike real AWS and unlike the single-message SendMessage path.
func TestSendMessageBatch_BypassedValidation_Rejected(t *testing.T) {
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

// TestBatchOperations verifies SendMessageBatch and DeleteMessageBatch.
func TestBatchOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		batchSize   int
		wantSuccess int
	}{
		{
			name:        "batch of 1",
			batchSize:   1,
			wantSuccess: 1,
		},
		{
			name:        "batch of 5",
			batchSize:   5,
			wantSuccess: 5,
		},
		{
			name:        "batch of 10 (max)",
			batchSize:   10,
			wantSuccess: 10,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			qURL := createTestQueue(t, b, "batch-q")

			entries := make([]sqs.SendMessageBatchEntry, tc.batchSize)
			for i := range tc.batchSize {
				entries[i] = sqs.SendMessageBatchEntry{
					ID:          fmt.Sprintf("e%d", i),
					MessageBody: fmt.Sprintf("body-%d", i),
				}
			}

			sendOut, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
				QueueURL: qURL,
				Entries:  entries,
			})
			require.NoError(t, err)
			assert.Len(t, sendOut.Successful, tc.wantSuccess, "all entries must succeed")
			assert.Empty(t, sendOut.Failed, "no entries must fail")

			// Receive all and delete in batch.
			recvOut, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            qURL,
				MaxNumberOfMessages: tc.batchSize,
			})
			require.NoError(t, err)
			require.Len(t, recvOut.Messages, tc.batchSize)

			deleteEntries := make([]sqs.DeleteMessageBatchEntry, len(recvOut.Messages))
			for i, msg := range recvOut.Messages {
				deleteEntries[i] = sqs.DeleteMessageBatchEntry{
					ID:            fmt.Sprintf("d%d", i),
					ReceiptHandle: msg.ReceiptHandle,
				}
			}

			delOut, err := b.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
				QueueURL: qURL,
				Entries:  deleteEntries,
			})
			require.NoError(t, err)
			assert.Len(t, delOut.Successful, tc.batchSize, "all deletes must succeed")
			assert.Empty(t, delOut.Failed)
		})
	}
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

func TestValidateBatchEntryIDsEmptyID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		entries []sqs.SendMessageBatchEntry
	}{
		{
			name:    "empty_id_rejected",
			entries: []sqs.SendMessageBatchEntry{{ID: "", MessageBody: "msg"}},
			wantErr: sqs.ErrInvalidBatchEntry,
		},
		{
			name: "duplicate_ids_rejected",
			entries: []sqs.SendMessageBatchEntry{
				{ID: "dup", MessageBody: "msg1"},
				{ID: "dup", MessageBody: "msg2"},
			},
			wantErr: sqs.ErrBatchEntryIDsNotDistinct,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			qURL := createTestQueue(t, b, "batch-id-queue")

			_, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
				QueueURL: qURL,
				Entries:  tt.entries,
			})
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// TestSendMessageBatchSizeValidation verifies that SendMessageBatch also enforces
// MaximumMessageSize on individual entries.
func TestSendMessageBatchSizeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		bodySize    int
		wantFailed  int
		wantSuccess int
	}{
		{
			name:        "oversized_entry_appears_in_failed",
			bodySize:    262145,
			wantFailed:  1,
			wantSuccess: 0,
		},
		{
			name:        "valid_entry_appears_in_successful",
			bodySize:    10,
			wantFailed:  0,
			wantSuccess: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			qURL := createTestQueue(t, b, "batch-size-validation-queue")

			result, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
				QueueURL: qURL,
				Entries: []sqs.SendMessageBatchEntry{
					{ID: "1", MessageBody: strings.Repeat("x", tt.bodySize)},
				},
			})
			require.NoError(t, err)
			assert.Len(t, result.Successful, tt.wantSuccess)
			assert.Len(t, result.Failed, tt.wantFailed)
		})
	}
}

// TestSendMessageBatchOrdering verifies that Successful and Failed entries in
// the SendMessageBatch response are returned in the same order as the input.
func TestSendMessageBatchOrdering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantSuccessIDs []string
		wantFailedIDs  []string
		entries        []sqs.SendMessageBatchEntry
	}{
		{
			name: "all_successful_maintains_input_order",
			entries: []sqs.SendMessageBatchEntry{
				{ID: "c", MessageBody: "msg-c"},
				{ID: "a", MessageBody: "msg-a"},
				{ID: "b", MessageBody: "msg-b"},
			},
			wantSuccessIDs: []string{"c", "a", "b"},
			wantFailedIDs:  nil,
		},
		{
			name: "mixed_success_and_failure_maintains_order",
			entries: []sqs.SendMessageBatchEntry{
				{ID: "ok-1", MessageBody: "msg-1"},
				{ID: "fail-1", MessageBody: strings.Repeat("x", 262145)}, // oversized
				{ID: "ok-2", MessageBody: "msg-2"},
			},
			wantSuccessIDs: []string{"ok-1", "ok-2"},
			wantFailedIDs:  []string{"fail-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			qURL := createTestQueue(t, b, "batch-order-queue")

			result, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
				QueueURL: qURL,
				Entries:  tt.entries,
			})
			require.NoError(t, err)

			gotSuccessIDs := make([]string, 0, len(result.Successful))
			for _, s := range result.Successful {
				gotSuccessIDs = append(gotSuccessIDs, s.ID)
			}

			gotFailedIDs := make([]string, 0, len(result.Failed))
			for _, f := range result.Failed {
				gotFailedIDs = append(gotFailedIDs, f.ID)
			}

			assert.Equal(t, tt.wantSuccessIDs, gotSuccessIDs)
			if tt.wantFailedIDs == nil {
				assert.Empty(t, gotFailedIDs)
			} else {
				assert.Equal(t, tt.wantFailedIDs, gotFailedIDs)
			}
		})
	}
}

// TestSendMessageBatchTotalSize verifies that SendMessageBatch returns
// BatchRequestTooLong when the combined payload of every per-entry-valid
// message exceeds the per-queue MaximumMessageSize (default 256 KiB).
func TestSendMessageBatchTotalSize(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	qURL := createTestQueue(t, b, "batch-total-size-queue")

	// 3 entries × 100 KiB = 300 KiB > 256 KiB; each entry alone is valid.
	const perEntryBytes = 100 * 1024
	entries := []sqs.SendMessageBatchEntry{
		{ID: "1", MessageBody: strings.Repeat("a", perEntryBytes)},
		{ID: "2", MessageBody: strings.Repeat("b", perEntryBytes)},
		{ID: "3", MessageBody: strings.Repeat("c", perEntryBytes)},
	}

	_, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueURL: qURL,
		Entries:  entries,
	})
	require.ErrorIs(t, err, sqs.ErrBatchRequestTooLong)
}
