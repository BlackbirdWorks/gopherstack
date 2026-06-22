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
		wantErrIs      error
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
			name:      "start_task_with_nonexistent_source_returns_error",
			msgCount:  0,
			wantErr:   true,
			wantErrIs: sqs.ErrQueueNotFound,
			setup: func(_ *testing.T, _ *sqs.InMemoryBackend) (string, string, string) {
				return buildQueueARN("nonexistent-dlq"), buildQueueARN("some-dest"), ""
			},
		},
		{
			name:      "start_second_task_for_same_source_returns_conflict",
			msgCount:  0, // setup already seeds messages
			wantErr:   true,
			wantErrIs: sqs.ErrMoveTaskAlreadyRunning,
			setup: func(t *testing.T, b *sqs.InMemoryBackend) (string, string, string) {
				t.Helper()

				dlqName := "dlq-conflict"
				destName := "dest-conflict"

				_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: dlqName, Endpoint: "localhost"})
				require.NoError(t, err)

				_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: destName, Endpoint: "localhost"})
				require.NoError(t, err)

				dlqURL := "http://localhost/000000000000/" + dlqName

				dlqARN := buildQueueARN(dlqName)
				destARN := buildQueueARN(destName)

				// Pre-seed the source so the task stays RUNNING.
				for i := range 10 {
					_, sendErr := b.SendMessage(&sqs.SendMessageInput{
						QueueURL:    dlqURL,
						MessageBody: "conflict-" + strconv.Itoa(i),
					})
					require.NoError(t, sendErr)
				}

				// Start first task with rate limiting so it stays alive.
				_, err = b.StartMessageMoveTask(&sqs.StartMessageMoveTaskInput{
					SourceArn:                    dlqARN,
					DestinationArn:               destARN,
					MaxNumberOfMessagesPerSecond: 1,
				})
				require.NoError(t, err)

				// Return the same ARNs; the test will attempt to start a second task.
				return dlqARN, destARN, ""
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()
			t.Cleanup(b.Close)
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

				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}

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
	t.Cleanup(b.Close)

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
	// Per AWS semantics, TaskHandle is only populated for RUNNING tasks in the
	// ListMessageMoveTasks response, so we check Status directly.
	require.Eventually(t, func() bool {
		listOut, listErr := b.ListMessageMoveTasks(&sqs.ListMessageMoveTasksInput{
			SourceArn:  dlqARN,
			MaxResults: 1,
		})
		if listErr != nil {
			return false
		}

		for _, task := range listOut.Results {
			if task.Status == sqs.MoveTaskStatusCompleted {
				return true
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
	t.Cleanup(b.Close)

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
	// Per AWS semantics, TaskHandle is only populated for RUNNING tasks.
	require.Eventually(t, func() bool {
		listOut, listErr := b.ListMessageMoveTasks(&sqs.ListMessageMoveTasksInput{
			SourceArn:  dlqARN,
			MaxResults: 1,
		})
		if listErr != nil {
			return false
		}

		for _, task := range listOut.Results {
			if task.Status == sqs.MoveTaskStatusCompleted {
				return true
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

				// Seed many messages so the task stays RUNNING long enough to be cancelled.
				// Without messages the task drains instantly (goroutine completes before Cancel).
				dlqURL := "http://localhost/000000000000/" + dlqName
				for i := range 50 {
					_, sendErr := b.SendMessage(&sqs.SendMessageInput{
						QueueURL:    dlqURL,
						MessageBody: "msg-" + strconv.Itoa(i),
					})
					require.NoError(t, sendErr)
				}

				// Rate-limit to 1 msg/sec so the task sleeps between messages and is
				// still RUNNING when CancelMessageMoveTask is called immediately below.
				out, err := b.StartMessageMoveTask(&sqs.StartMessageMoveTaskInput{
					SourceArn:                    buildQueueARN(dlqName),
					DestinationArn:               buildQueueARN(destName),
					MaxNumberOfMessagesPerSecond: 1,
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
			t.Cleanup(b.Close)
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

				dlqName1 := "dlq-maxresults-1"
				dlqName2 := "dlq-maxresults-2"
				destName := "dest-maxresults"

				_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: dlqName1, Endpoint: "localhost"})
				require.NoError(t, err)

				_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: dlqName2, Endpoint: "localhost"})
				require.NoError(t, err)

				_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: destName, Endpoint: "localhost"})
				require.NoError(t, err)

				// Start two tasks on different source queues (each completes immediately as queues are empty).
				_, err = b.StartMessageMoveTask(&sqs.StartMessageMoveTaskInput{
					SourceArn:      buildQueueARN(dlqName1),
					DestinationArn: buildQueueARN(destName),
				})
				require.NoError(t, err)

				// Wait for first task to complete so the second doesn't conflict.
				require.Eventually(t, func() bool {
					listOut, listErr := b.ListMessageMoveTasks(&sqs.ListMessageMoveTasksInput{
						SourceArn: buildQueueARN(dlqName1),
					})
					if listErr != nil || len(listOut.Results) == 0 {
						return false
					}

					return listOut.Results[0].Status == sqs.MoveTaskStatusCompleted
				}, 2*time.Second, 10*time.Millisecond)

				_, err = b.StartMessageMoveTask(&sqs.StartMessageMoveTaskInput{
					SourceArn:      buildQueueARN(dlqName2),
					DestinationArn: buildQueueARN(destName),
				})
				require.NoError(t, err)

				// Return dlqName1's ARN as source for filtering; both tasks in the backend but
				// MaxResults=1 should limit the output to 1.
				// We list with empty SourceArn to get all tasks, but test pagination with MaxResults.
				return "", 2
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()
			t.Cleanup(b.Close)
			srcARN, _ := tt.setup(t, b)

			var out *sqs.ListMessageMoveTasksOutput

			require.Eventually(t, func() bool {
				var err error

				out, err = b.ListMessageMoveTasks(&sqs.ListMessageMoveTasksInput{
					SourceArn:  srcARN,
					MaxResults: tt.maxResults,
				})
				require.NoError(t, err)

				return len(out.Results) >= tt.wantMin
			}, 2*time.Second, 10*time.Millisecond, "expected at least %d task(s) to be listed", tt.wantMin)

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
		{
			name:   "StartMessageMoveTask_already_running_returns_conflict",
			action: "StartMessageMoveTask",
			setup: func(t *testing.T, h *sqs.Handler) map[string]any {
				t.Helper()
				doCreateQueue(t, h, "smt-conflict-dlq")
				doCreateQueue(t, h, "smt-conflict-dest")

				// Seed messages so the task stays RUNNING with rate limiting.
				for i := range 10 {
					rec := doRequest(t, h, "SendMessage", map[string]any{
						"QueueUrl":    "http://localhost/000000000000/smt-conflict-dlq",
						"MessageBody": "msg-" + strconv.Itoa(i),
					})
					require.Equal(t, http.StatusOK, rec.Code)
				}

				// Start first task with rate limiting.
				rec := doRequest(t, h, "StartMessageMoveTask", map[string]any{
					"SourceArn":                    buildQueueARN("smt-conflict-dlq"),
					"DestinationArn":               buildQueueARN("smt-conflict-dest"),
					"MaxNumberOfMessagesPerSecond": 1,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				// The second request body (returned for the test handler loop to use).
				return map[string]any{
					"SourceArn":      buildQueueARN("smt-conflict-dlq"),
					"DestinationArn": buildQueueARN("smt-conflict-dest"),
				}
			},
			wantCode:        http.StatusBadRequest,
			wantBodyContain: "ResourceInConflict",
		},
		{
			name:   "StartMessageMoveTask_empty_source_arn_returns_error",
			action: "StartMessageMoveTask",
			setup: func(_ *testing.T, _ *sqs.Handler) map[string]any {
				return map[string]any{
					"SourceArn":      "",
					"DestinationArn": buildQueueARN("some-dest"),
				}
			},
			wantCode:        http.StatusBadRequest,
			wantBodyContain: "InvalidParameterValue",
		},
		{
			name:   "CancelMessageMoveTask_on_completed_task_returns_conflict",
			action: "CancelMessageMoveTask",
			setup: func(t *testing.T, h *sqs.Handler) map[string]any {
				t.Helper()
				doCreateQueue(t, h, "smt-cancel-done-dlq")
				doCreateQueue(t, h, "smt-cancel-done-dest")

				// Start a task on an empty queue; it completes immediately.
				rec := doRequest(t, h, "StartMessageMoveTask", map[string]any{
					"SourceArn":      buildQueueARN("smt-cancel-done-dlq"),
					"DestinationArn": buildQueueARN("smt-cancel-done-dest"),
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var startResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startResp))
				handle, _ := startResp["TaskHandle"].(string)
				require.NotEmpty(t, handle)

				// Wait for the task to complete via polling ListMessageMoveTasks.
				dlqARN := buildQueueARN("smt-cancel-done-dlq")
				b := h.Backend.(*sqs.InMemoryBackend)

				require.Eventually(t, func() bool {
					listOut, listErr := b.ListMessageMoveTasks(&sqs.ListMessageMoveTasksInput{
						SourceArn:  dlqARN,
						MaxResults: 1,
					})

					return listErr == nil && len(listOut.Results) > 0 &&
						listOut.Results[0].Status == sqs.MoveTaskStatusCompleted
				}, 2*time.Second, 10*time.Millisecond)

				return map[string]any{"TaskHandle": handle}
			},
			wantCode:        http.StatusBadRequest,
			wantBodyContain: "ResourceInConflict",
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

// TestSQS_ListMessageMoveTasks_TaskHandleOnlyForRunning verifies that the TaskHandle
// field is only populated in ListMessageMoveTasks for tasks in RUNNING status,
// matching AWS ListMessageMoveTasksResultEntry semantics.
func TestSQS_ListMessageMoveTasks_TaskHandleOnlyForRunning(t *testing.T) {
	t.Parallel()

	b := sqs.NewInMemoryBackend()
	t.Cleanup(b.Close)

	dlqName := "dlq-taskhandle-check"
	destName := "dest-taskhandle-check"

	_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: dlqName, Endpoint: "localhost"})
	require.NoError(t, err)

	_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: destName, Endpoint: "localhost"})
	require.NoError(t, err)

	dlqARN := buildQueueARN(dlqName)
	destARN := buildQueueARN(destName)

	// Start a task on an empty queue — it will complete immediately.
	startOut, err := b.StartMessageMoveTask(&sqs.StartMessageMoveTaskInput{
		SourceArn:      dlqARN,
		DestinationArn: destARN,
	})
	require.NoError(t, err)
	require.NotEmpty(t, startOut.TaskHandle)

	// Wait for the task to complete.
	require.Eventually(t, func() bool {
		listOut, listErr := b.ListMessageMoveTasks(&sqs.ListMessageMoveTasksInput{
			SourceArn:  dlqARN,
			MaxResults: 1,
		})
		if listErr != nil || len(listOut.Results) == 0 {
			return false
		}

		return listOut.Results[0].Status == sqs.MoveTaskStatusCompleted
	}, 2*time.Second, 10*time.Millisecond, "task should complete")

	// After completion, TaskHandle must be empty in the list response.
	listOut, err := b.ListMessageMoveTasks(&sqs.ListMessageMoveTasksInput{
		SourceArn:  dlqARN,
		MaxResults: 1,
	})
	require.NoError(t, err)
	require.Len(t, listOut.Results, 1)
	assert.Equal(t, sqs.MoveTaskStatusCompleted, listOut.Results[0].Status)
	assert.Empty(t, listOut.Results[0].TaskHandle,
		"TaskHandle must not be populated for completed tasks (AWS semantics)")

	// StartedTimestamp and ApproximateNumberOfMessagesMoved should always be present.
	assert.NotZero(t, listOut.Results[0].StartedTimestamp, "StartedTimestamp should always be present")
}

// TestSQS_ListMessageMoveTasks_MaxResults validates default (1) and max (10) semantics.
func TestSQS_ListMessageMoveTasks_MaxResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults int32
		// taskCount is how many tasks to create (on different source queues).
		taskCount int
		wantCount int
	}{
		{
			name:       "default_max_results_is_1",
			maxResults: 0, // unset → defaults to 1
			taskCount:  3,
			wantCount:  1,
		},
		{
			name:       "explicit_max_results_respected",
			maxResults: 2,
			taskCount:  3,
			wantCount:  2,
		},
		{
			name:       "max_results_capped_at_10",
			maxResults: 20, // over limit → capped to 10
			taskCount:  11,
			wantCount:  10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()
			t.Cleanup(b.Close)

			// Create taskCount source queues and start a task on each.
			for i := range tt.taskCount {
				dlqName := "dlq-maxr-" + tt.name + "-" + strconv.Itoa(i)
				destName := "dest-maxr-" + tt.name + "-" + strconv.Itoa(i)

				_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: dlqName, Endpoint: "localhost"})
				require.NoError(t, err)

				_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: destName, Endpoint: "localhost"})
				require.NoError(t, err)

				_, err = b.StartMessageMoveTask(&sqs.StartMessageMoveTaskInput{
					SourceArn:      buildQueueARN(dlqName),
					DestinationArn: buildQueueARN(destName),
				})
				require.NoError(t, err)
			}

			// Wait for all tasks to complete (each queue is empty so they finish fast).
			require.Eventually(t, func() bool {
				listOut, listErr := b.ListMessageMoveTasks(&sqs.ListMessageMoveTasksInput{
					MaxResults: 10,
				})
				if listErr != nil {
					return false
				}

				completedOrDone := 0
				for _, task := range listOut.Results {
					if task.Status == sqs.MoveTaskStatusCompleted {
						completedOrDone++
					}
				}

				// We want at least min(taskCount, 10) completed in the capped window.
				expected := min(tt.taskCount, 10)

				return completedOrDone >= expected
			}, 5*time.Second, 20*time.Millisecond, "all tasks should complete")

			// Now query with the test's MaxResults.
			out, err := b.ListMessageMoveTasks(&sqs.ListMessageMoveTasksInput{
				MaxResults: tt.maxResults,
			})
			require.NoError(t, err)
			assert.Len(t, out.Results, tt.wantCount)
		})
	}
}

// TestSQS_ListMessageMoveTasks_NewestFirst verifies that results are returned newest
// first (descending by startedAt) matching AWS semantics.
func TestSQS_ListMessageMoveTasks_NewestFirst(t *testing.T) {
	t.Parallel()

	b := sqs.NewInMemoryBackend()
	t.Cleanup(b.Close)

	const numQueues = 3

	// Create and start tasks in sequence, ensuring distinct timestamps by letting
	// each complete before starting the next (to force different startedAt values).
	for i := range numQueues {
		name := "dlq-order-" + strconv.Itoa(i)
		dest := "dest-order-" + strconv.Itoa(i)

		_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: name, Endpoint: "localhost"})
		require.NoError(t, err)

		_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: dest, Endpoint: "localhost"})
		require.NoError(t, err)

		_, err = b.StartMessageMoveTask(&sqs.StartMessageMoveTaskInput{
			SourceArn:      buildQueueARN(name),
			DestinationArn: buildQueueARN(dest),
		})
		require.NoError(t, err)

		// Let the task run to completion before starting the next one.
		require.Eventually(t, func() bool {
			listOut, listErr := b.ListMessageMoveTasks(&sqs.ListMessageMoveTasksInput{
				SourceArn:  buildQueueARN(name),
				MaxResults: 1,
			})

			return listErr == nil && len(listOut.Results) > 0 &&
				listOut.Results[0].Status == sqs.MoveTaskStatusCompleted
		}, 2*time.Second, 10*time.Millisecond)
	}

	// Retrieve all tasks (up to 10).
	out, err := b.ListMessageMoveTasks(&sqs.ListMessageMoveTasksInput{
		MaxResults: 10,
	})
	require.NoError(t, err)
	require.Len(t, out.Results, numQueues)

	// Verify descending order: each result should have a StartedTimestamp ≥ the next.
	for i := 1; i < len(out.Results); i++ {
		assert.GreaterOrEqual(t, out.Results[i-1].StartedTimestamp, out.Results[i].StartedTimestamp,
			"results should be sorted newest first")
	}
}

// TestSQS_StartMessageMoveTask_Validation tests input validation for StartMessageMoveTask.
func TestSQS_StartMessageMoveTask_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		name      string
		input     sqs.StartMessageMoveTaskInput
	}{
		{
			name: "empty_source_arn_returns_error",
			input: sqs.StartMessageMoveTaskInput{
				SourceArn:      "",
				DestinationArn: "arn:aws:sqs:us-east-1:000000000000:dest",
			},
			wantErrIs: sqs.ErrInvalidSourceArn,
		},
		{
			name: "negative_rate_returns_error",
			input: sqs.StartMessageMoveTaskInput{
				SourceArn:                    "arn:aws:sqs:us-east-1:000000000000:src",
				MaxNumberOfMessagesPerSecond: -1,
			},
			wantErrIs: sqs.ErrInvalidMaxMessagesPerSecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()
			t.Cleanup(b.Close)

			_, err := b.StartMessageMoveTask(&tt.input)
			require.Error(t, err)
			require.ErrorIs(t, err, tt.wantErrIs)
		})
	}
}

// TestSQS_CancelMessageMoveTask_NotRunning verifies that cancelling a task that has
// already completed returns ErrMoveTaskNotRunning, matching AWS ResourceInConflict semantics.
func TestSQS_CancelMessageMoveTask_NotRunning(t *testing.T) {
	t.Parallel()

	b := sqs.NewInMemoryBackend()
	t.Cleanup(b.Close)

	dlqName := "dlq-cancel-complete"
	destName := "dest-cancel-complete"

	_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: dlqName, Endpoint: "localhost"})
	require.NoError(t, err)

	_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: destName, Endpoint: "localhost"})
	require.NoError(t, err)

	dlqARN := buildQueueARN(dlqName)

	// Start a task on an empty queue — it will complete immediately.
	startOut, err := b.StartMessageMoveTask(&sqs.StartMessageMoveTaskInput{
		SourceArn:      dlqARN,
		DestinationArn: buildQueueARN(destName),
	})
	require.NoError(t, err)
	require.NotEmpty(t, startOut.TaskHandle)

	// Wait for task to reach COMPLETED.
	require.Eventually(t, func() bool {
		listOut, listErr := b.ListMessageMoveTasks(&sqs.ListMessageMoveTasksInput{
			SourceArn:  dlqARN,
			MaxResults: 1,
		})

		return listErr == nil && len(listOut.Results) > 0 &&
			listOut.Results[0].Status == sqs.MoveTaskStatusCompleted
	}, 2*time.Second, 10*time.Millisecond, "task should complete")

	// Cancelling a completed task must return ErrMoveTaskNotRunning.
	_, err = b.CancelMessageMoveTask(&sqs.CancelMessageMoveTaskInput{
		TaskHandle: startOut.TaskHandle,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, sqs.ErrMoveTaskNotRunning)
}

func TestSQS_MoveTaskJanitorPruning(t *testing.T) {
	t.Parallel()

	type taskFixture struct {
		handle    string
		status    sqs.MoveTaskStatus
		startedAt int64
	}

	now := time.Now()
	tests := []struct {
		name      string
		tasks     []taskFixture
		wantCount int
	}{
		{
			name: "old_terminal_tasks_are_removed",
			tasks: []taskFixture{
				{
					handle:    "completed-old",
					status:    sqs.MoveTaskStatusCompleted,
					startedAt: now.Add(-16 * time.Minute).UnixMilli(),
				},
				{
					handle:    "cancelled-old",
					status:    sqs.MoveTaskStatusCancelled,
					startedAt: now.Add(-16 * time.Minute).UnixMilli(),
				},
				{
					handle:    "failed-old",
					status:    sqs.MoveTaskStatusFailed,
					startedAt: now.Add(-16 * time.Minute).UnixMilli(),
				},
			},
			wantCount: 0,
		},
		{
			name: "running_and_recent_terminal_tasks_are_kept",
			tasks: []taskFixture{
				{
					handle:    "running-old",
					status:    sqs.MoveTaskStatusRunning,
					startedAt: now.Add(-30 * time.Minute).UnixMilli(),
				},
				{
					handle:    "completed-recent",
					status:    sqs.MoveTaskStatusCompleted,
					startedAt: now.Add(-5 * time.Minute).UnixMilli(),
				},
			},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()
			t.Cleanup(b.Close)
			for _, task := range tt.tasks {
				b.InjectMoveTaskForTest(task.handle, task.status, task.startedAt)
			}

			b.RunJanitorOnceForTest(now)
			assert.Equal(t, tt.wantCount, b.MoveTaskCountForTest())
		})
	}
}

// TestSQS_AddPermission_Validation tests input validation for AddPermission.
func TestSQS_AddPermission_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		name      string
		input     sqs.AddPermissionInput
	}{
		{
			name: "empty_label_returns_error",
			input: sqs.AddPermissionInput{
				QueueURL:      "http://localhost/000000000000/q",
				Label:         "",
				AWSAccountIDs: []string{"123456789012"},
				Actions:       []string{"SendMessage"},
			},
			wantErrIs: sqs.ErrInvalidPermissionLabel,
		},
		{
			name: "empty_actions_returns_error",
			input: sqs.AddPermissionInput{
				QueueURL:      "http://localhost/000000000000/q",
				Label:         "MyLabel",
				AWSAccountIDs: []string{"123456789012"},
				Actions:       []string{},
			},
			wantErrIs: sqs.ErrInvalidPermissionActions,
		},
		{
			name: "empty_aws_account_ids_returns_error",
			input: sqs.AddPermissionInput{
				QueueURL:      "http://localhost/000000000000/q",
				Label:         "MyLabel",
				AWSAccountIDs: []string{},
				Actions:       []string{"SendMessage"},
			},
			wantErrIs: sqs.ErrInvalidPermissionAccountIDs,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()
			t.Cleanup(b.Close)

			_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "q", Endpoint: "localhost"})
			require.NoError(t, err)

			err = b.AddPermission(&tt.input)
			require.Error(t, err)
			require.ErrorIs(t, err, tt.wantErrIs)
		})
	}
}

// TestSQS_DeleteQueue_CancelsMoveTasks verifies that deleting a queue that is the
// source of an active move task cancels the task, preventing a goroutine leak.
func TestSQS_DeleteQueue_CancelsMoveTasks(t *testing.T) {
	t.Parallel()

	b := sqs.NewInMemoryBackend()
	t.Cleanup(b.Close)

	dlqName := "dlq-delete-cancel"
	destName := "dest-delete-cancel"

	_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: dlqName, Endpoint: "localhost"})
	require.NoError(t, err)

	_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: destName, Endpoint: "localhost"})
	require.NoError(t, err)

	dlqURL := "http://localhost/000000000000/" + dlqName
	dlqARN := buildQueueARN(dlqName)

	// Seed messages so the task doesn't complete before we delete the queue.
	for i := range 10 {
		_, sendErr := b.SendMessage(&sqs.SendMessageInput{
			QueueURL:    dlqURL,
			MessageBody: "msg-" + strconv.Itoa(i),
		})
		require.NoError(t, sendErr)
	}

	// Start a rate-limited task so it stays RUNNING.
	startOut, err := b.StartMessageMoveTask(&sqs.StartMessageMoveTaskInput{
		SourceArn:                    dlqARN,
		DestinationArn:               buildQueueARN(destName),
		MaxNumberOfMessagesPerSecond: 1,
	})
	require.NoError(t, err)
	require.NotEmpty(t, startOut.TaskHandle)

	// Delete the DLQ — the associated move task must reach a terminal state.
	err = b.DeleteQueue(&sqs.DeleteQueueInput{QueueURL: dlqURL})
	require.NoError(t, err)

	// The task should eventually transition from RUNNING/CANCELLING to a terminal state.
	require.Eventually(t, func() bool {
		listOut, listErr := b.ListMessageMoveTasks(&sqs.ListMessageMoveTasksInput{
			MaxResults: 1,
		})
		if listErr != nil || len(listOut.Results) == 0 {
			return false
		}

		s := listOut.Results[0].Status

		return s == sqs.MoveTaskStatusCancelled || s == sqs.MoveTaskStatusCompleted || s == sqs.MoveTaskStatusFailed
	}, 3*time.Second, 10*time.Millisecond, "move task should reach a terminal state after DeleteQueue")
}
