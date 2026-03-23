package sqs_test

import (
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

// buildQueueARN constructs an SQS ARN in the default test account/region.
func buildQueueARN(queueName string) string {
	return "arn:aws:sqs:us-east-1:000000000000:" + queueName
}

// TestSQS_DLQRedrive validates that StartMessageMoveTask, CancelMessageMoveTask,
// and ListMessageMoveTasks work correctly for the async DLQ redrive use case.
func TestSQS_DLQRedrive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(t *testing.T, b *sqs.InMemoryBackend) (srcARN, destARN, srcURL string)
		name           string
		msgCount       int
		wantTaskHandle bool
		wantErr        bool
	}{
		{
			name:           "move_messages_from_dlq_to_source",
			msgCount:       3,
			wantTaskHandle: true,
			setup: func(t *testing.T, b *sqs.InMemoryBackend) (string, string, string) {
				t.Helper()

				dlqName := "dlq-redrive"
				srcName := "src-redrive"

				_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: dlqName, Endpoint: "localhost"})
				require.NoError(t, err)

				_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: srcName, Endpoint: "localhost"})
				require.NoError(t, err)

				dlqARN := buildQueueARN(dlqName)
				srcARN := buildQueueARN(srcName)
				dlqURL := "http://localhost/000000000000/" + dlqName

				return dlqARN, srcARN, dlqURL
			},
		},
		{
			name:     "start_task_with_nonexistent_source_returns_error",
			msgCount: 0,
			wantErr:  true,
			setup: func(_ *testing.T, _ *sqs.InMemoryBackend) (string, string, string) {
				return buildQueueARN("nonexistent-dlq"), buildQueueARN("some-dest"), ""
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()
			srcARN, destARN, srcURL := tt.setup(t, b)

			// Seed messages into the source queue if needed.
			for i := range tt.msgCount {
				_, err := b.SendMessage(&sqs.SendMessageInput{
					QueueURL:    srcURL,
					MessageBody: "msg-body-" + strconv.Itoa(i),
				})
				require.NoError(t, err)
			}

			out, err := b.StartMessageMoveTask(&sqs.StartMessageMoveTaskInput{
				SourceArn:      srcARN,
				DestinationArn: destARN,
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotEmpty(t, out.TaskHandle)
		})
	}
}

func TestSQS_DLQRedrive_MovesMessages(t *testing.T) {
	t.Parallel()

	b := sqs.NewInMemoryBackend()

	dlqName := "dlq-moves"
	destName := "dest-moves"

	_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: dlqName, Endpoint: "localhost"})
	require.NoError(t, err)

	_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: destName, Endpoint: "localhost"})
	require.NoError(t, err)

	dlqURL := "http://localhost/000000000000/" + dlqName
	destURL := "http://localhost/000000000000/" + destName
	dlqARN := buildQueueARN(dlqName)
	destARN := buildQueueARN(destName)

	const msgCount = 5

	for i := range msgCount {
		_, sendErr := b.SendMessage(&sqs.SendMessageInput{
			QueueURL:    dlqURL,
			MessageBody: "message-" + strconv.Itoa(i),
		})
		require.NoError(t, sendErr)
	}

	out, err := b.StartMessageMoveTask(&sqs.StartMessageMoveTaskInput{
		SourceArn:      dlqARN,
		DestinationArn: destARN,
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.TaskHandle)

	// Wait for the goroutine to drain the DLQ.
	require.Eventually(t, func() bool {
		listOut, listErr := b.ListMessageMoveTasks(&sqs.ListMessageMoveTasksInput{
			SourceArn: dlqARN,
		})
		if listErr != nil {
			return false
		}

		for _, task := range listOut.Results {
			if task.TaskHandle == out.TaskHandle {
				return task.Status == sqs.MoveTaskStatusCompleted
			}
		}

		return false
	}, 5*time.Second, 50*time.Millisecond, "task should complete")

	// DLQ should now be empty.
	dlqCheck, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            dlqURL,
		MaxNumberOfMessages: 10,
	})
	require.NoError(t, err)
	assert.Empty(t, dlqCheck.Messages, "DLQ should be empty after redrive")

	// Destination queue should contain all moved messages.
	destCheck, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            destURL,
		MaxNumberOfMessages: 10,
	})
	require.NoError(t, err)
	assert.Len(t, destCheck.Messages, msgCount, "destination queue should have all messages")
}

func TestSQS_DLQRedrive_DefaultDestination(t *testing.T) {
	t.Parallel()

	b := sqs.NewInMemoryBackend()

	dlqName := "dlq-default-dest"
	srcName := "src-default-dest"

	_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: dlqName, Endpoint: "localhost"})
	require.NoError(t, err)

	dlqARN := buildQueueARN(dlqName)
	dlqURL := "http://localhost/000000000000/" + dlqName
	srcURL := "http://localhost/000000000000/" + srcName

	// Create source queue with a RedrivePolicy pointing to the DLQ.
	redrivePolicy, _ := json.Marshal(map[string]any{
		"deadLetterTargetArn": dlqARN,
		"maxReceiveCount":     3,
	})

	_, err = b.CreateQueue(&sqs.CreateQueueInput{
		QueueName: srcName,
		Endpoint:  "localhost",
		Attributes: map[string]string{
			"RedrivePolicy": string(redrivePolicy),
		},
	})
	require.NoError(t, err)

	// Put a message in the DLQ.
	_, err = b.SendMessage(&sqs.SendMessageInput{QueueURL: dlqURL, MessageBody: "redrive-me"})
	require.NoError(t, err)

	// Start task without specifying DestinationArn — it should default to srcName.
	out, err := b.StartMessageMoveTask(&sqs.StartMessageMoveTaskInput{
		SourceArn: dlqARN,
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.TaskHandle)

	// Wait for completion.
	require.Eventually(t, func() bool {
		listOut, listErr := b.ListMessageMoveTasks(&sqs.ListMessageMoveTasksInput{
			SourceArn: dlqARN,
		})
		if listErr != nil {
			return false
		}

		for _, task := range listOut.Results {
			if task.TaskHandle == out.TaskHandle {
				return task.Status == sqs.MoveTaskStatusCompleted
			}
		}

		return false
	}, 5*time.Second, 50*time.Millisecond, "task should complete")

	// Source queue should have the re-driven message.
	srcCheck, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueURL:            srcURL,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	assert.Len(t, srcCheck.Messages, 1)

	if len(srcCheck.Messages) > 0 {
		assert.Equal(t, "redrive-me", srcCheck.Messages[0].Body)
	}
}

func TestSQS_CancelMessageMoveTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, b *sqs.InMemoryBackend) string
		name    string
		wantErr bool
	}{
		{
			name: "cancel_nonexistent_task_returns_error",
			setup: func(_ *testing.T, _ *sqs.InMemoryBackend) string {
				return "nonexistent-handle"
			},
			wantErr: true,
		},
		{
			name: "cancel_running_task_succeeds",
			setup: func(t *testing.T, b *sqs.InMemoryBackend) string {
				t.Helper()

				dlqName := "dlq-cancel"
				destName := "dest-cancel"

				_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: dlqName, Endpoint: "localhost"})
				require.NoError(t, err)

				_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: destName, Endpoint: "localhost"})
				require.NoError(t, err)

				out, err := b.StartMessageMoveTask(&sqs.StartMessageMoveTaskInput{
					SourceArn:      buildQueueARN(dlqName),
					DestinationArn: buildQueueARN(destName),
				})
				require.NoError(t, err)

				return out.TaskHandle
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()
			handle := tt.setup(t, b)

			cancelOut, err := b.CancelMessageMoveTask(&sqs.CancelMessageMoveTaskInput{
				TaskHandle: handle,
			})

			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, sqs.ErrTaskHandleInvalid)

				return
			}

			require.NoError(t, err)
			assert.GreaterOrEqual(t, cancelOut.ApproximateNumberOfMessagesMoved, int64(0))
		})
	}
}

func TestSQS_ListMessageMoveTasks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, b *sqs.InMemoryBackend) (srcARN string, taskCount int)
		name       string
		maxResults int32
		wantMin    int
		wantMax    int
	}{
		{
			name:       "empty_list_when_no_tasks",
			maxResults: 0,
			wantMin:    0,
			wantMax:    0,
			setup: func(_ *testing.T, _ *sqs.InMemoryBackend) (string, int) {
				return buildQueueARN("no-tasks-queue"), 0
			},
		},
		{
			name:       "lists_started_tasks",
			maxResults: 0,
			wantMin:    1,
			wantMax:    2,
			setup: func(t *testing.T, b *sqs.InMemoryBackend) (string, int) {
				t.Helper()

				dlqName := "dlq-list"
				destName := "dest-list"

				_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: dlqName, Endpoint: "localhost"})
				require.NoError(t, err)

				_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: destName, Endpoint: "localhost"})
				require.NoError(t, err)

				_, err = b.StartMessageMoveTask(&sqs.StartMessageMoveTaskInput{
					SourceArn:      buildQueueARN(dlqName),
					DestinationArn: buildQueueARN(destName),
				})
				require.NoError(t, err)

				return buildQueueARN(dlqName), 1
			},
		},
		{
			name:       "max_results_limits_output",
			maxResults: 1,
			wantMin:    1,
			wantMax:    1,
			setup: func(t *testing.T, b *sqs.InMemoryBackend) (string, int) {
				t.Helper()

				dlqName := "dlq-maxresults"
				destName := "dest-maxresults"

				_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: dlqName, Endpoint: "localhost"})
				require.NoError(t, err)

				_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: destName, Endpoint: "localhost"})
				require.NoError(t, err)

				// Start two tasks on the same DLQ (second will complete immediately since DLQ is empty).
				_, err = b.StartMessageMoveTask(&sqs.StartMessageMoveTaskInput{
					SourceArn:      buildQueueARN(dlqName),
					DestinationArn: buildQueueARN(destName),
				})
				require.NoError(t, err)

				_, err = b.StartMessageMoveTask(&sqs.StartMessageMoveTaskInput{
					SourceArn:      buildQueueARN(dlqName),
					DestinationArn: buildQueueARN(destName),
				})
				require.NoError(t, err)

				return buildQueueARN(dlqName), 2
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()
			srcARN, _ := tt.setup(t, b)

			// Give goroutines a moment to start.
			time.Sleep(10 * time.Millisecond)

			out, err := b.ListMessageMoveTasks(&sqs.ListMessageMoveTasksInput{
				SourceArn:  srcARN,
				MaxResults: tt.maxResults,
			})
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(out.Results), tt.wantMin)
			assert.LessOrEqual(t, len(out.Results), tt.wantMax)
		})
	}
}

func TestSQS_MessageMoveTasks_Handler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup           func(t *testing.T, h *sqs.Handler) map[string]any
		name            string
		action          string
		wantBodyContain string
		wantCode        int
	}{
		{
			name:   "StartMessageMoveTask_success",
			action: "StartMessageMoveTask",
			setup: func(t *testing.T, h *sqs.Handler) map[string]any {
				t.Helper()
				doCreateQueue(t, h, "smt-dlq")
				doCreateQueue(t, h, "smt-dest")

				return map[string]any{
					"SourceArn":      buildQueueARN("smt-dlq"),
					"DestinationArn": buildQueueARN("smt-dest"),
				}
			},
			wantCode:        http.StatusOK,
			wantBodyContain: "TaskHandle",
		},
		{
			name:   "StartMessageMoveTask_source_not_found",
			action: "StartMessageMoveTask",
			setup: func(_ *testing.T, _ *sqs.Handler) map[string]any {
				return map[string]any{
					"SourceArn":      buildQueueARN("nonexistent"),
					"DestinationArn": buildQueueARN("also-nonexistent"),
				}
			},
			wantCode:        http.StatusBadRequest,
			wantBodyContain: "QueueDoesNotExist",
		},
		{
			name:   "CancelMessageMoveTask_invalid_handle",
			action: "CancelMessageMoveTask",
			setup: func(_ *testing.T, _ *sqs.Handler) map[string]any {
				return map[string]any{
					"TaskHandle": "nonexistent-handle",
				}
			},
			wantCode:        http.StatusBadRequest,
			wantBodyContain: "InvalidParameterValue",
		},
		{
			name:   "ListMessageMoveTasks_empty",
			action: "ListMessageMoveTasks",
			setup: func(_ *testing.T, _ *sqs.Handler) map[string]any {
				return map[string]any{
					"SourceArn": buildQueueARN("empty-source"),
				}
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := tt.setup(t, h)

			rec := doRequest(t, h, tt.action, body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantBodyContain != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBodyContain)
			}
		})
	}
}
