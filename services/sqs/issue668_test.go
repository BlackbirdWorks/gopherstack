package sqs_test

import (
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

// ─── Memory Leaks ───────────────────────────────────────────────────────────

// TestFIFODedupPrunedOnSendMessage verifies that expired deduplication IDs are
// removed during SendMessage so that send-only FIFO queues don't leak memory.
func TestFIFODedupPrunedOnSendMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		expiredIDs   []string // IDs to inject with an expired timestamp
		wantDedupLen int      // expected dedup map size after the pruning SendMessage
	}{
		{
			name:         "expired_ids_removed_on_send",
			expiredIDs:   []string{"id-1", "id-2"},
			wantDedupLen: 1, // only the freshly added entry remains
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			const qName = "prune-send.fifo"
			qURL := createFIFOQueueWithDedup(t, b, qName)

			// Inject expired dedup IDs directly so we don't have to wait 300 s for
			// the real window to expire.
			for _, id := range tt.expiredIDs {
				b.InjectExpiredDedupID(qName, id)
			}

			// Sanity-check: expired entries are now present in the map.
			require.Equal(t, len(tt.expiredIDs), b.DedupMapLen(qName))

			// SendMessage on a FIFO queue calls pruneDedup before storing the new
			// entry; the expired entries should be swept out.
			_, err := b.SendMessage(&sqs.SendMessageInput{
				QueueURL:               qURL,
				MessageBody:            "fresh-body",
				MessageGroupID:         "g1",
				MessageDeduplicationID: "fresh-id",
			})
			require.NoError(t, err)

			// After SendMessage, only the freshly added entry should remain.
			assert.Equal(t, tt.wantDedupLen, b.DedupMapLen(qName))
		})
	}
}

// TestDeleteQueueClosesNotifyChannel verifies that deleting a queue wakes any
// goroutine blocked on long-polling and that the goroutine receives an error.
func TestDeleteQueueClosesNotifyChannel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "long_poll_receiver_wakes_when_queue_deleted"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			qURL := createTestQueue(t, b, "close-notify-queue")

			errCh := make(chan error, 1)
			go func() {
				_, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
					QueueURL:            qURL,
					MaxNumberOfMessages: 1,
					WaitTimeSeconds:     10,
				})
				errCh <- err
			}()

			// Give the goroutine time to enter the long-poll select.
			time.Sleep(50 * time.Millisecond)

			require.NoError(t, b.DeleteQueue(&sqs.DeleteQueueInput{QueueURL: qURL}))

			select {
			case err := <-errCh:
				// The closed notify channel should cause the goroutine to wake up and
				// return ErrQueueNotFound from the next receiveOnce call.
				require.ErrorIs(t, err, sqs.ErrQueueNotFound, tt.name)
			case <-time.After(2 * time.Second):
				require.FailNow(t, "goroutine did not wake up after queue deletion")
			}
		})
	}
}

// ─── Bugs ────────────────────────────────────────────────────────────────────

// TestReceiveMessageWaitTimeSecondsValidation verifies that WaitTimeSeconds
// outside the valid AWS range (0–20) is rejected.
func TestReceiveMessageWaitTimeSecondsValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr         error
		name            string
		waitTimeSeconds int
	}{
		{
			name:            "negative_wait_time",
			waitTimeSeconds: -1,
			wantErr:         sqs.ErrInvalidWaitTime,
		},
		{
			name:            "over_max_wait_time",
			waitTimeSeconds: 21,
			wantErr:         sqs.ErrInvalidWaitTime,
		},
		{
			name:            "max_valid_wait_time",
			waitTimeSeconds: 20,
			wantErr:         nil,
		},
		{
			name:            "zero_wait_time",
			waitTimeSeconds: 0,
			wantErr:         nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			qURL := createTestQueue(t, b, "wait-validation-queue")

			// Send a message so a non-zero wait doesn't block forever.
			_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "msg"})
			require.NoError(t, err)

			_, err = b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            qURL,
				MaxNumberOfMessages: 1,
				WaitTimeSeconds:     tt.waitTimeSeconds,
			})

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestSendMessageSizeValidation verifies that messages exceeding the queue's
// MaximumMessageSize attribute are rejected.
func TestSendMessageSizeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr    error
		name       string
		maxMsgSize string
		bodySize   int
	}{
		{
			name:     "body_at_default_limit_is_accepted",
			bodySize: 262144,
			wantErr:  nil,
		},
		{
			name:     "body_over_default_limit_rejected",
			bodySize: 262145,
			wantErr:  sqs.ErrMessageTooLarge,
		},
		{
			name:       "body_over_custom_limit_rejected",
			bodySize:   1025,
			maxMsgSize: "1024",
			wantErr:    sqs.ErrMessageTooLarge,
		},
		{
			name:       "body_at_custom_limit_accepted",
			bodySize:   1024,
			maxMsgSize: "1024",
			wantErr:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			attrs := map[string]string{}
			if tt.maxMsgSize != "" {
				attrs["MaximumMessageSize"] = tt.maxMsgSize
			}

			out, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName:  "size-validation-queue",
				Endpoint:   testEndpoint,
				Attributes: attrs,
			})
			require.NoError(t, err)

			_, err = b.SendMessage(&sqs.SendMessageInput{
				QueueURL:    out.QueueURL,
				MessageBody: strings.Repeat("x", tt.bodySize),
			})

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
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

			b := newBackend()
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

			b := newBackend()
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

// ─── Broadcast wake-up ───────────────────────────────────────────────────────

// TestLongPollBroadcastWakeup verifies that all concurrent long-poll receivers
// are woken by a single SendMessage, not just one of them.
func TestLongPollBroadcastWakeup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		numReceivers int
		numMessages  int
	}{
		{
			name:         "two_receivers_both_woken_by_messages",
			numReceivers: 2,
			numMessages:  2,
		},
		{
			name:         "three_receivers_all_woken_by_messages",
			numReceivers: 3,
			numMessages:  3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			qURL := createTestQueue(t, b, "broadcast-wake-queue")

			ready := make(chan struct{})
			results := make(chan int, tt.numReceivers)

			var wg sync.WaitGroup

			wg.Add(tt.numReceivers)

			for range tt.numReceivers {
				go func() {
					wg.Done() // signal that this goroutine has started
					<-ready   // wait until released by the test
					out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
						QueueURL:            qURL,
						MaxNumberOfMessages: 1,
						WaitTimeSeconds:     5,
					})
					if err == nil {
						results <- len(out.Messages)
					} else {
						results <- -1
					}
				}()
			}

			// Ensure all goroutines have been scheduled before releasing them.
			wg.Wait()
			close(ready)

			// Sleep briefly so all goroutines enter the long-poll select before
			// any message is sent.
			time.Sleep(50 * time.Millisecond)

			// Send one message per receiver so each one should wake and return a msg.
			for i := range tt.numMessages {
				_, err := b.SendMessage(&sqs.SendMessageInput{
					QueueURL:    qURL,
					MessageBody: strings.Repeat("m", i+1),
				})
				require.NoError(t, err)
			}

			deadline := time.After(3 * time.Second)
			for range tt.numReceivers {
				select {
				case n := <-results:
					assert.Equal(t, 1, n)
				case <-deadline:
					require.FailNow(t, "at least one long-poll receiver did not wake in time")
				}
			}
		})
	}
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// createFIFOQueueWithDedup creates a FIFO queue and returns its URL.
func createFIFOQueueWithDedup(t *testing.T, b *sqs.InMemoryBackend, name string) string {
	t.Helper()

	out, err := b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: name,
		Endpoint:  testEndpoint,
	})
	require.NoError(t, err)

	return out.QueueURL
}

// ─── AWS Realism: extra validations ──────────────────────────────────────────

// TestChangeMessageVisibilityBoundsValidation checks that ChangeMessageVisibility
// rejects visibility timeouts outside the AWS-allowed range (0–43200 s).
func TestChangeMessageVisibilityBoundsValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr           error
		name              string
		visibilityTimeout int
	}{
		{name: "negative_rejected", visibilityTimeout: -1, wantErr: sqs.ErrInvalidVisibilityTimeout},
		{name: "over_max_rejected", visibilityTimeout: 43201, wantErr: sqs.ErrInvalidVisibilityTimeout},
		{name: "zero_accepted", visibilityTimeout: 0, wantErr: nil},
		{name: "max_accepted", visibilityTimeout: 43200, wantErr: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			qURL := createTestQueue(t, b, "vis-bounds-queue")

			_, sendErr := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "msg"})
			require.NoError(t, sendErr)

			out, recvErr := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL: qURL, MaxNumberOfMessages: 1, VisibilityTimeout: 30,
			})
			require.NoError(t, recvErr)
			require.Len(t, out.Messages, 1)

			err := b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
				QueueURL:          qURL,
				ReceiptHandle:     out.Messages[0].ReceiptHandle,
				VisibilityTimeout: tt.visibilityTimeout,
			})

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestFIFOMessageGroupIDRequired verifies that SendMessage on a FIFO queue
// returns ErrMissingMessageGroupID when MessageGroupID is absent.
func TestFIFOMessageGroupIDRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr        error
		name           string
		messageGroupID string
		dedupID        string
	}{
		{
			name:    "missing_group_id_rejected",
			wantErr: sqs.ErrMissingMessageGroupID,
		},
		{
			name:           "group_id_present_accepted",
			messageGroupID: "g1",
			dedupID:        "d1",
			wantErr:        nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			qURL := createTestQueue(t, b, "fifo-groupid-queue.fifo")

			_, err := b.SendMessage(&sqs.SendMessageInput{
				QueueURL:               qURL,
				MessageBody:            "msg",
				MessageGroupID:         tt.messageGroupID,
				MessageDeduplicationID: tt.dedupID,
			})

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestFIFODeduplicationIDRequired verifies that SendMessage on a FIFO queue
// with ContentBasedDeduplication disabled requires an explicit MessageDeduplicationID.
func TestFIFODeduplicationIDRequired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr                   error
		name                      string
		contentBasedDeduplication string
		dedupID                   string
	}{
		{
			name:    "dedup_id_missing_content_based_off_rejected",
			wantErr: sqs.ErrMissingDeduplicationID,
		},
		{
			name:    "dedup_id_present_accepted",
			dedupID: "d1",
			wantErr: nil,
		},
		{
			name:                      "content_based_dedup_on_no_id_accepted",
			contentBasedDeduplication: "true",
			wantErr:                   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			attrs := map[string]string{}
			if tt.contentBasedDeduplication != "" {
				attrs["ContentBasedDeduplication"] = tt.contentBasedDeduplication
			}

			out, createErr := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName:  "fifo-dedupid-queue.fifo",
				Endpoint:   testEndpoint,
				Attributes: attrs,
			})
			require.NoError(t, createErr)

			_, err := b.SendMessage(&sqs.SendMessageInput{
				QueueURL:               out.QueueURL,
				MessageBody:            "msg",
				MessageGroupID:         "g1",
				MessageDeduplicationID: tt.dedupID,
			})

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestMessageRetentionPeriodExpiry verifies that messages older than the queue's
// MessageRetentionPeriod are silently discarded and never delivered to consumers.
func TestMessageRetentionPeriodExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		retentionSecs int
		wantMsgCount  int
	}{
		{
			name:          "expired_message_not_delivered",
			retentionSecs: 1, // 1 second retention
			wantMsgCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			out, err := b.CreateQueue(&sqs.CreateQueueInput{
				QueueName: "retention-queue",
				Endpoint:  testEndpoint,
				Attributes: map[string]string{
					"MessageRetentionPeriod": strconv.Itoa(tt.retentionSecs),
				},
			})
			require.NoError(t, err)

			_, err = b.SendMessage(&sqs.SendMessageInput{
				QueueURL:    out.QueueURL,
				MessageBody: "old-msg",
			})
			require.NoError(t, err)

			// Wait for the retention period to pass.
			time.Sleep(time.Duration(tt.retentionSecs+1) * time.Second)

			recv, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            out.QueueURL,
				MaxNumberOfMessages: 10,
			})
			require.NoError(t, err)
			assert.Len(t, recv.Messages, tt.wantMsgCount)
		})
	}
}
