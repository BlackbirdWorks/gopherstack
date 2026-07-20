package sqs_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

func runSnapshotTest(
	t *testing.T,
	setup func(b *sqs.InMemoryBackend) string,
	verify func(t *testing.T, b *sqs.InMemoryBackend, id string),
) {
	t.Helper()
	t.Parallel()

	original := sqs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	t.Cleanup(original.Close)
	id := setup(original)

	snap := original.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := sqs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	t.Cleanup(fresh.Close)
	require.NoError(t, fresh.Restore(t.Context(), snap))

	verify(t, fresh, id)
}

func TestInMemoryBackend_SnapshotRestore_RoundTripPreservesState(t *testing.T) {
	t.Parallel()
	runSnapshotTest(t, func(b *sqs.InMemoryBackend) string {
		out, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "test-queue"})
		if err != nil {
			return ""
		}

		return out.QueueURL
	},
		func(t *testing.T, b *sqs.InMemoryBackend, id string) {
			t.Helper()

			out, err := b.ListQueues(&sqs.ListQueuesInput{})
			require.NoError(t, err)
			require.Len(t, out.QueueURLs, 1)
			assert.Equal(t, id, out.QueueURLs[0])
		},
	)
}

func TestInMemoryBackend_SnapshotRestore_EmptyBackendRoundTrip(t *testing.T) {
	t.Parallel()
	runSnapshotTest(t, func(_ *sqs.InMemoryBackend) string { return "" },
		func(t *testing.T, b *sqs.InMemoryBackend, _ string) {
			t.Helper()

			out, err := b.ListQueues(&sqs.ListQueuesInput{})
			require.NoError(t, err)
			assert.Empty(t, out.QueueURLs)
		},
	)
}

func TestInMemoryBackend_SnapshotRestore_PermissionsRoundTrip(t *testing.T) {
	t.Parallel()
	runSnapshotTest(t, func(b *sqs.InMemoryBackend) string {
		_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "perm-queue", Endpoint: "localhost"})
		if err != nil {
			return ""
		}

		if addErr := b.AddPermission(&sqs.AddPermissionInput{
			QueueURL:      "http://localhost/000000000000/perm-queue",
			Label:         "AllowSend",
			AWSAccountIDs: []string{"123456789012"},
			Actions:       []string{"SendMessage"},
		}); addErr != nil {
			return ""
		}

		return "AllowSend"
	},
		func(t *testing.T, b *sqs.InMemoryBackend, label string) {
			t.Helper()

			// Queue and its permissions should survive the round-trip.
			out, err := b.ListQueues(&sqs.ListQueuesInput{})
			require.NoError(t, err)
			require.Len(t, out.QueueURLs, 1)

			// The permission should still be there — removing it should succeed (no error).
			removeErr := b.RemovePermission(&sqs.RemovePermissionInput{
				QueueURL: out.QueueURLs[0],
				Label:    label,
			})
			require.NoError(t, removeErr)
		},
	)
}

func TestInMemoryBackend_SnapshotRestore_InFlightMessageRoundTrip(t *testing.T) {
	t.Parallel()
	// Regression guard for the pkgs/store conversion: an in-flight
	// (received but not yet deleted) message must round-trip through
	// Snapshot/Restore still invisible, with its original receipt
	// handle still valid — proving the store.Table-backed queues and
	// their inline messages/inFlightMessages survive the swap intact.
	runSnapshotTest(t, func(b *sqs.InMemoryBackend) string {
		out, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "inflight-queue", Endpoint: "localhost"})
		if err != nil {
			return ""
		}

		if _, sendErr := b.SendMessage(&sqs.SendMessageInput{
			QueueURL:    out.QueueURL,
			MessageBody: "in-flight-body",
		}); sendErr != nil {
			return ""
		}

		recvOut, recvErr := b.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueURL:            out.QueueURL,
			MaxNumberOfMessages: 1,
			VisibilityTimeout:   300,
		})
		if recvErr != nil || len(recvOut.Messages) != 1 {
			return ""
		}

		return out.QueueURL + "|" + recvOut.Messages[0].ReceiptHandle
	},
		func(t *testing.T, b *sqs.InMemoryBackend, id string) {
			t.Helper()
			require.NotEmpty(t, id)

			parts := strings.SplitN(id, "|", 2)
			require.Len(t, parts, 2)
			queueURL, receiptHandle := parts[0], parts[1]

			// The message must still be in flight (invisible) after the round trip.
			recvOut, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            queueURL,
				MaxNumberOfMessages: 10,
			})
			require.NoError(t, err)
			assert.Empty(t, recvOut.Messages,
				"in-flight message must remain invisible across a snapshot/restore round trip")

			// Its original receipt handle must still be valid for deletion.
			err = b.DeleteMessage(&sqs.DeleteMessageInput{
				QueueURL:      queueURL,
				ReceiptHandle: receiptHandle,
			})
			require.NoError(t, err, "receipt handle issued before the snapshot must remain valid after restore")
		},
	)
}

func TestInMemoryBackend_SnapshotRestore_FifoSequenceNumberPersists(t *testing.T) {
	t.Parallel()
	runSnapshotTest(t, func(b *sqs.InMemoryBackend) string {
		out, err := b.CreateQueue(&sqs.CreateQueueInput{
			QueueName: "seq-fifo.fifo",
			Endpoint:  "localhost",
			Attributes: map[string]string{
				"FifoQueue":                 "true",
				"ContentBasedDeduplication": "true",
			},
		})
		if err != nil {
			return ""
		}

		// Send several messages, each in its own MessageGroupID, so the FIFO
		// sequence counter advances well past its zero value before the
		// snapshot is taken. Distinct groups let a single ReceiveMessage
		// call in verify() drain all of them at once (this backend only
		// allows one in-flight message per group at a time).
		const sendCount = 5
		for i := range sendCount {
			if _, sendErr := b.SendMessage(&sqs.SendMessageInput{
				QueueURL:       out.QueueURL,
				MessageBody:    fmt.Sprintf("body-%d", i),
				MessageGroupID: fmt.Sprintf("group-%d", i),
			}); sendErr != nil {
				return ""
			}
		}

		return out.QueueURL
	},
		func(t *testing.T, b *sqs.InMemoryBackend, queueURL string) {
			t.Helper()
			require.NotEmpty(t, queueURL)

			// Drain the 5 pre-restore messages so only the post-restore send's
			// SequenceNumber remains to inspect.
			drainOut, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            queueURL,
				MaxNumberOfMessages: 10,
			})
			require.NoError(t, err)
			require.Len(t, drainOut.Messages, 5, "all 5 pre-restore messages should survive the round trip")

			var lastSeq string
			for _, m := range drainOut.Messages {
				if m.SequenceNumber > lastSeq {
					lastSeq = m.SequenceNumber
				}
			}

			// If the FIFO sequence counter were reset to zero by Restore (the
			// bug), this new message's SequenceNumber would regress behind (or
			// duplicate) the ones already handed out above, breaking the AWS
			// guarantee that SequenceNumber strictly increases per queue.
			sendOut, err := b.SendMessage(&sqs.SendMessageInput{
				QueueURL:       queueURL,
				MessageBody:    "post-restore",
				MessageGroupID: "group-a",
			})
			require.NoError(t, err)
			assert.Greater(t, sendOut.SequenceNumber, lastSeq,
				"SequenceNumber must keep increasing across a snapshot/restore round trip")
		},
	)
}

func TestInMemoryBackend_SnapshotRestore_RestoredQueueJanitor(t *testing.T) {
	t.Parallel()
	runSnapshotTest(t, func(b *sqs.InMemoryBackend) string {
		out, err := b.CreateQueue(&sqs.CreateQueueInput{
			QueueName: "restore-janitor-queue",
			Endpoint:  "localhost",
		})
		if err != nil {
			return ""
		}

		if _, sendErr := b.SendMessage(&sqs.SendMessageInput{
			QueueURL:    out.QueueURL,
			MessageBody: "expire-me",
		}); sendErr != nil {
			return ""
		}

		// Shorten retention so the message is already expired by the time
		// the janitor next runs (simulated below via a future `now`).
		b.SetRetentionForTest(out.QueueURL, 1)

		return out.QueueURL
	},
		func(t *testing.T, b *sqs.InMemoryBackend, queueURL string) {
			t.Helper()
			require.NotEmpty(t, queueURL)

			// Regression guard: a freshly restored non-FIFO queue must have its
			// hasActivity flag seeded from its message/in-flight counts.
			// Previously Restore left it false, so the background janitor's
			// pruneState (which skips non-FIFO queues with hasActivity==false)
			// silently ignored this queue forever — the retention-expired
			// message would never be pruned until an unrelated SendMessage
			// happened to flip the flag. Simulate a janitor tick 2 seconds in
			// the future (past the 1-second retention window) without a real sleep.
			b.RunJanitorOnceForTest(time.Now().Add(2 * time.Second))

			out, err := b.GetQueueAttributes(&sqs.GetQueueAttributesInput{
				QueueURL:       queueURL,
				AttributeNames: []string{"ApproximateNumberOfMessages"},
			})
			require.NoError(t, err)
			assert.Equal(t, "0", out.Attributes["ApproximateNumberOfMessages"],
				"janitor should prune the retention-expired message on a restored queue")
		},
	)
}

func TestInMemoryBackend_SnapshotRestore_CompletedMoveTaskHistoryRoundTrip(t *testing.T) {
	t.Parallel()
	runSnapshotTest(t, func(b *sqs.InMemoryBackend) string {
		_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "hist-dlq", Endpoint: "localhost"})
		if err != nil {
			return ""
		}

		_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: "hist-dest", Endpoint: "localhost"})
		if err != nil {
			return ""
		}

		dlqARN := "arn:aws:sqs:us-east-1:000000000000:hist-dlq"

		out, err := b.StartMessageMoveTask(&sqs.StartMessageMoveTaskInput{
			SourceArn:      dlqARN,
			DestinationArn: "arn:aws:sqs:us-east-1:000000000000:hist-dest",
		})
		if err != nil {
			return ""
		}

		// Wait for the task to complete (queue was empty so it completes immediately).
		for range 50 {
			listOut, listErr := b.ListMessageMoveTasks(&sqs.ListMessageMoveTasksInput{
				SourceArn:  dlqARN,
				MaxResults: 1,
			})
			if listErr == nil && len(listOut.Results) > 0 &&
				listOut.Results[0].Status == sqs.MoveTaskStatusCompleted {
				break
			}

			time.Sleep(20 * time.Millisecond)
		}

		return out.TaskHandle
	},
		func(t *testing.T, b *sqs.InMemoryBackend, _ string) {
			t.Helper()

			// After restore, the completed task should still be visible.
			dlqARN := "arn:aws:sqs:us-east-1:000000000000:hist-dlq"

			out, err := b.ListMessageMoveTasks(&sqs.ListMessageMoveTasksInput{
				SourceArn:  dlqARN,
				MaxResults: 1,
			})
			require.NoError(t, err)
			require.Len(t, out.Results, 1, "completed task should survive snapshot/restore")
			assert.Equal(t, sqs.MoveTaskStatusCompleted, out.Results[0].Status)
			// TaskHandle is NOT populated for non-RUNNING tasks per AWS semantics.
			assert.Empty(t, out.Results[0].TaskHandle)
		},
	)
}

func TestInMemoryBackend_RestoreDiscardsIncompatibleSnapshotVersion(t *testing.T) {
	t.Parallel()

	b := sqs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	t.Cleanup(b.Close)

	_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "pre-existing-queue", Endpoint: "localhost"})
	require.NoError(t, err)

	// Well-formed JSON, but tagged with a version this build does not know how
	// to decode (e.g. the pre-pkgs/store on-disk shape, or a future one).
	incompatibleSnapshot := []byte(`{"version":1,"queues":{},"accountID":"000000000000","region":"us-east-1"}`)

	err = b.Restore(t.Context(), incompatibleSnapshot)
	require.NoError(t, err,
		"an incompatible snapshot version must be discarded cleanly, not surfaced as a restore error")

	out, err := b.ListQueues(&sqs.ListQueuesInput{})
	require.NoError(t, err)
	assert.Empty(t, out.QueueURLs, "backend must start empty after discarding an incompatible snapshot")
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := sqs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	t.Cleanup(b.Close)
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}
