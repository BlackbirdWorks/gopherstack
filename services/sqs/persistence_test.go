package sqs_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *sqs.InMemoryBackend) string
		verify func(t *testing.T, b *sqs.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *sqs.InMemoryBackend) string {
				out, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "test-queue"})
				if err != nil {
					return ""
				}

				return out.QueueURL
			},
			verify: func(t *testing.T, b *sqs.InMemoryBackend, id string) {
				t.Helper()

				out, err := b.ListQueues(&sqs.ListQueuesInput{})
				require.NoError(t, err)
				require.Len(t, out.QueueURLs, 1)
				assert.Equal(t, id, out.QueueURLs[0])
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *sqs.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *sqs.InMemoryBackend, _ string) {
				t.Helper()

				out, err := b.ListQueues(&sqs.ListQueuesInput{})
				require.NoError(t, err)
				assert.Empty(t, out.QueueURLs)
			},
		},
		{
			name: "permissions_round_trip",
			setup: func(b *sqs.InMemoryBackend) string {
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
			verify: func(t *testing.T, b *sqs.InMemoryBackend, label string) {
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
		},
		{
			name: "completed_move_task_history_round_trip",
			setup: func(b *sqs.InMemoryBackend) string {
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
			verify: func(t *testing.T, b *sqs.InMemoryBackend, _ string) {
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
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := sqs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			t.Cleanup(original.Close)
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := sqs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			t.Cleanup(fresh.Close)
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := sqs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	t.Cleanup(b.Close)
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}
