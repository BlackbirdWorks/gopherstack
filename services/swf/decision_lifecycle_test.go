package swf_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

// pollDecisionTask polls for a decision task and returns the task token.
func pollDecisionTask(t *testing.T, b *swf.InMemoryBackend, domain, taskList string) string {
	t.Helper()

	task := b.PollForDecisionTask(domain, taskList, 0, "")
	require.NotNil(t, task, "expected a decision task")

	return task.TaskToken
}

func TestRespondDecisionTaskCompleted_CompleteWorkflow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		result          string
		wantCloseStatus string
		wantEventType   string
	}{
		{
			name:            "complete_with_result",
			result:          `{"output":"done"}`,
			wantCloseStatus: "COMPLETED",
			wantEventType:   "WorkflowExecutionCompleted",
		},
		{
			name:            "complete_empty_result",
			result:          "",
			wantCloseStatus: "COMPLETED",
			wantEventType:   "WorkflowExecutionCompleted",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := swf.NewInMemoryBackend()
			require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
			_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
				Domain:     "dom",
				WorkflowID: "wf-1",
				TaskList:   "default",
			})
			require.NoError(t, err)

			b.EnqueueDecisionTaskInternal("dom", "default", "wf-1", "run-1")
			token := pollDecisionTask(t, b, "dom", "default")

			decisions := []swf.Decision{{
				DecisionType: "CompleteWorkflowExecution",
				CompleteWorkflowExecutionAttrs: &swf.CompleteWorkflowExecutionDecisionAttrs{
					Result: tt.result,
				},
			}}
			require.NoError(t, b.RespondDecisionTaskCompleted(token, "", decisions))

			exec, err := b.DescribeWorkflowExecution("dom", "wf-1", "")
			require.NoError(t, err)
			assert.Equal(t, tt.wantCloseStatus, exec.Status)
			assert.Equal(t, tt.wantCloseStatus, exec.CloseStatus)
			assert.NotZero(t, exec.CloseTimestamp)

			events, _ := b.GetWorkflowExecutionHistory("dom", "wf-1", "", 0, "", false)
			var found bool
			for _, ev := range events {
				if ev.EventType == tt.wantEventType {
					found = true
				}
			}
			assert.True(t, found, "expected %s event in history", tt.wantEventType)
		})
	}
}

func TestRespondDecisionTaskCompleted_FailWorkflow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		reason  string
		details string
	}{
		{
			name:    "fail_with_reason",
			reason:  "timed out",
			details: "after 300s",
		},
		{
			name: "fail_no_reason",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := swf.NewInMemoryBackend()
			require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
			_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
				Domain:     "dom",
				WorkflowID: "wf-1",
				TaskList:   "default",
			})
			require.NoError(t, err)

			b.EnqueueDecisionTaskInternal("dom", "default", "wf-1", "run-1")
			token := pollDecisionTask(t, b, "dom", "default")

			decisions := []swf.Decision{{
				DecisionType: "FailWorkflowExecution",
				FailWorkflowExecutionAttrs: &swf.FailWorkflowExecutionDecisionAttrs{
					Reason:  tt.reason,
					Details: tt.details,
				},
			}}
			require.NoError(t, b.RespondDecisionTaskCompleted(token, "", decisions))

			exec, err := b.DescribeWorkflowExecution("dom", "wf-1", "")
			require.NoError(t, err)
			assert.Equal(t, "FAILED", exec.Status)
			assert.Equal(t, "FAILED", exec.CloseStatus)
			assert.NotZero(t, exec.CloseTimestamp)

			events, _ := b.GetWorkflowExecutionHistory("dom", "wf-1", "", 0, "", false)
			var failEvent *swf.HistoryEvent
			for i := range events {
				if events[i].EventType == "WorkflowExecutionFailed" {
					failEvent = &events[i]
				}
			}
			require.NotNil(t, failEvent)
			if tt.reason != "" {
				attrs := failEvent.Attributes["workflowExecutionFailedEventAttributes"].(map[string]any)
				assert.Equal(t, tt.reason, attrs["reason"])
			}
		})
	}
}

func TestRespondDecisionTaskCompleted_CancelWorkflow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		details string
	}{
		{name: "cancel_with_details", details: "user cancelled"},
		{name: "cancel_no_details"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := swf.NewInMemoryBackend()
			require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
			_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
				Domain:     "dom",
				WorkflowID: "wf-1",
				TaskList:   "default",
			})
			require.NoError(t, err)

			b.EnqueueDecisionTaskInternal("dom", "default", "wf-1", "run-1")
			token := pollDecisionTask(t, b, "dom", "default")

			decisions := []swf.Decision{{
				DecisionType: "CancelWorkflowExecution",
				CancelWorkflowExecutionAttrs: &swf.CancelWorkflowExecutionDecisionAttrs{
					Details: tt.details,
				},
			}}
			require.NoError(t, b.RespondDecisionTaskCompleted(token, "", decisions))

			exec, err := b.DescribeWorkflowExecution("dom", "wf-1", "")
			require.NoError(t, err)
			assert.Equal(t, "CANCELED", exec.Status)
			assert.Equal(t, "CANCELED", exec.CloseStatus)
		})
	}
}

// TestRespondDecisionTaskCompleted_ContinueAsNew verifies ContinueAsNewWorkflowExecution
// performs a real state transition (new RunID, RUNNING again, fresh decision
// task enqueued) instead of leaving the execution permanently dead-ended in
// CONTINUED_AS_NEW with no decision task ever following -- see the historical
// PARITY.md gap this closes.
func TestRespondDecisionTaskCompleted_ContinueAsNew(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	started, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain:     "dom",
		WorkflowID: "wf-1",
		TaskList:   "default",
	})
	require.NoError(t, err)
	oldRunID := started.RunID

	token := pollDecisionTask(t, b, "dom", "default")

	decisions := []swf.Decision{{
		DecisionType: "ContinueAsNewWorkflowExecution",
		ContinueAsNewWorkflowExecutionAttrs: &swf.ContinueAsNewWorkflowExecutionDecisionAttrs{
			Input: `{"round":2}`,
			// No registered WorkflowType exists here to source a default
			// task list from, so the decision must carry one explicitly --
			// matches real AWS, which faults with DEFAULT_TASK_LIST_UNDEFINED
			// otherwise.
			TaskList: "default",
		},
	}}
	require.NoError(t, b.RespondDecisionTaskCompleted(token, "", decisions))

	exec, err := b.DescribeWorkflowExecution("dom", "wf-1", "")
	require.NoError(t, err)
	assert.Equal(t, "RUNNING", exec.Status)
	assert.Empty(t, exec.CloseStatus)
	assert.Zero(t, exec.CloseTimestamp)
	assert.NotEqual(t, oldRunID, exec.RunID, "continue-as-new must assign a fresh RunID")
	assert.Equal(t, `{"round":2}`, exec.Input)

	// The old run's own history (independently queryable by RunId now that
	// executions/history are keyed by domain+workflowID+runID -- gopherstack-jsi8)
	// carries the closing WorkflowExecutionContinuedAsNew event; it is NOT
	// commingled with the new run's history.
	oldEvents, _ := b.GetWorkflowExecutionHistory("dom", "wf-1", oldRunID, 0, "", false)
	var continuedEvent *swf.HistoryEvent
	for i := range oldEvents {
		if oldEvents[i].EventType == "WorkflowExecutionContinuedAsNew" {
			continuedEvent = &oldEvents[i]
		}
	}
	require.NotNil(t, continuedEvent, "expected WorkflowExecutionContinuedAsNew in the OLD run's history")
	continuedAttrs, ok := continuedEvent.Attributes["workflowExecutionContinuedAsNewEventAttributes"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, exec.RunID, continuedAttrs["newExecutionRunId"])

	newEvents, _ := b.GetWorkflowExecutionHistory("dom", "wf-1", "", 0, "", false)
	var startedAgainEvent *swf.HistoryEvent
	for i := range newEvents {
		if newEvents[i].EventType == "WorkflowExecutionStarted" {
			startedAgainEvent = &newEvents[i]
		}
	}
	require.NotNil(t, startedAgainEvent, "expected a fresh WorkflowExecutionStarted for the continuation")
	startedAttrs, ok := startedAgainEvent.Attributes["workflowExecutionStartedEventAttributes"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, oldRunID, startedAttrs["continuedExecutionRunId"])

	// The old run is independently queryable after continuation -- the core
	// fix: it is no longer overwritten by the new run.
	oldExec, err := b.DescribeWorkflowExecution("dom", "wf-1", oldRunID)
	require.NoError(t, err)
	assert.Equal(t, "CONTINUED_AS_NEW", oldExec.Status)
	assert.Equal(t, "CONTINUED_AS_NEW", oldExec.CloseStatus)

	// A fresh decision task must have been enqueued so the decider can make
	// progress on the new run -- this is the core bug being fixed (the old
	// behavior left the workflow stuck OPEN forever with no way to resume).
	task := b.PollForDecisionTask("dom", "default", 0, "")
	require.NotNil(t, task, "expected a decision task for the continued run")
	assert.Equal(t, exec.RunID, task.RunID)
}

// TestRespondDecisionTaskCompleted_ContinueAsNew_UnknownWorkflowType verifies
// that when the (possibly re-versioned) workflow type can't be resolved, the
// execution stays open/RUNNING (real AWS never closes the run on a rejected
// decision) and records ContinueAsNewWorkflowExecutionFailed instead.
func TestRespondDecisionTaskCompleted_ContinueAsNew_UnknownWorkflowType(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	require.NoError(t, b.RegisterWorkflowType("dom", "greeter", "1.0", "", swf.WorkflowTypeDefaults{}))
	started, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain:              "dom",
		WorkflowID:          "wf-1",
		TaskList:            "default",
		WorkflowTypeName:    "greeter",
		WorkflowTypeVersion: "1.0",
	})
	require.NoError(t, err)

	token := pollDecisionTask(t, b, "dom", "default")
	decisions := []swf.Decision{{
		DecisionType: "ContinueAsNewWorkflowExecution",
		ContinueAsNewWorkflowExecutionAttrs: &swf.ContinueAsNewWorkflowExecutionDecisionAttrs{
			WorkflowTypeVersion: "does-not-exist",
		},
	}}
	require.NoError(t, b.RespondDecisionTaskCompleted(token, "", decisions))

	exec, err := b.DescribeWorkflowExecution("dom", "wf-1", "")
	require.NoError(t, err)
	assert.Equal(t, "RUNNING", exec.Status, "a rejected continue-as-new must leave the execution open")
	assert.Equal(t, started.RunID, exec.RunID, "the run must not change on a rejected continuation")

	events, _ := b.GetWorkflowExecutionHistory("dom", "wf-1", "", 0, "", false)
	var failedEvent *swf.HistoryEvent
	for i := range events {
		if events[i].EventType == "ContinueAsNewWorkflowExecutionFailed" {
			failedEvent = &events[i]
		}
	}
	require.NotNil(t, failedEvent, "expected ContinueAsNewWorkflowExecutionFailed in history")
	attrs, ok := failedEvent.Attributes["continueAsNewWorkflowExecutionFailedEventAttributes"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "WORKFLOW_TYPE_DOES_NOT_EXIST", attrs["cause"])
}

func TestRespondDecisionTaskCompleted_ScheduleActivityTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		taskList   string
		activityID string
		input      string
	}{
		{
			name:       "schedule_with_explicit_tasklist",
			taskList:   "activity-list",
			activityID: "act-001",
			input:      `{"key":"value"}`,
		},
		{
			name:       "schedule_inherits_execution_tasklist",
			taskList:   "",
			activityID: "act-002",
			input:      "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := swf.NewInMemoryBackend()
			require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
			_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
				Domain:     "dom",
				WorkflowID: "wf-1",
				TaskList:   "default",
			})
			require.NoError(t, err)

			b.EnqueueDecisionTaskInternal("dom", "default", "wf-1", "run-1")
			token := pollDecisionTask(t, b, "dom", "default")

			expectedTaskList := tt.taskList
			if expectedTaskList == "" {
				expectedTaskList = "default"
			}

			decisions := []swf.Decision{{
				DecisionType: "ScheduleActivityTask",
				ScheduleActivityTaskAttrs: &swf.ScheduleActivityTaskDecisionAttrs{
					ActivityType: swf.ActivityTaskActivityType{Name: "my-act", Version: "1.0"},
					ActivityID:   tt.activityID,
					Input:        tt.input,
					TaskList:     tt.taskList,
				},
			}}
			require.NoError(t, b.RespondDecisionTaskCompleted(token, "", decisions))

			assert.Equal(t, 1, b.CountPendingActivityTasks("dom", expectedTaskList))

			task := b.PollForActivityTask("dom", expectedTaskList)
			require.NotNil(t, task)
			assert.Equal(t, tt.activityID, task.ActivityID)
			assert.Equal(t, "my-act", task.ActivityType.Name)
			assert.Equal(t, tt.input, task.Input)
		})
	}
}

func TestRespondDecisionTaskCompleted_ExecutionContext(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain:     "dom",
		WorkflowID: "wf-1",
		TaskList:   "default",
	})
	require.NoError(t, err)

	b.EnqueueDecisionTaskInternal("dom", "default", "wf-1", "run-1")
	token := pollDecisionTask(t, b, "dom", "default")

	require.NoError(t, b.RespondDecisionTaskCompleted(token, `{"state":"step2"}`, nil))

	exec, err := b.DescribeWorkflowExecution("dom", "wf-1", "")
	require.NoError(t, err)
	assert.JSONEq(t, `{"state":"step2"}`, exec.LatestExecutionContext)
}

func TestRespondDecisionTaskCompleted_UnknownToken(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	err := b.RespondDecisionTaskCompleted("nonexistent-token", "", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, swf.ErrNotFound)
}

func TestRespondDecisionTaskCompleted_ClosedCountUpdates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		decision        swf.Decision
		name            string
		wantCloseStatus string
	}{
		{
			name: "complete",
			decision: swf.Decision{
				DecisionType:                   "CompleteWorkflowExecution",
				CompleteWorkflowExecutionAttrs: &swf.CompleteWorkflowExecutionDecisionAttrs{},
			},
			wantCloseStatus: "COMPLETED",
		},
		{
			name: "fail",
			decision: swf.Decision{
				DecisionType:               "FailWorkflowExecution",
				FailWorkflowExecutionAttrs: &swf.FailWorkflowExecutionDecisionAttrs{},
			},
			wantCloseStatus: "FAILED",
		},
		{
			name: "cancel",
			decision: swf.Decision{
				DecisionType:                 "CancelWorkflowExecution",
				CancelWorkflowExecutionAttrs: &swf.CancelWorkflowExecutionDecisionAttrs{},
			},
			wantCloseStatus: "CANCELED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := swf.NewInMemoryBackend()
			require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
			_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
				Domain:     "dom",
				WorkflowID: "wf-1",
				TaskList:   "default",
			})
			require.NoError(t, err)

			assert.Equal(t, 1, b.CountOpenWorkflowExecutions("dom", swf.ExecutionFilter{}))
			assert.Equal(t, 0, b.CountClosedWorkflowExecutions("dom", swf.ExecutionFilter{}))

			b.EnqueueDecisionTaskInternal("dom", "default", "wf-1", "run-1")
			token := pollDecisionTask(t, b, "dom", "default")
			require.NoError(t, b.RespondDecisionTaskCompleted(token, "", []swf.Decision{tt.decision}))

			assert.Equal(t, 0, b.CountOpenWorkflowExecutions("dom", swf.ExecutionFilter{}))
			assert.Equal(t, 1, b.CountClosedWorkflowExecutions("dom", swf.ExecutionFilter{}))

			filter := swf.ExecutionFilter{CloseStatus: tt.wantCloseStatus}
			assert.Equal(t, 1, b.CountClosedWorkflowExecutions("dom", filter))
		})
	}
}

func TestDecisionTask_HistoryIncluded(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain:     "dom",
		WorkflowID: "wf-1",
		TaskList:   "default",
	})
	require.NoError(t, err)

	b.EnqueueDecisionTaskInternal("dom", "default", "wf-1", "run-1")
	task := b.PollForDecisionTask("dom", "default", 0, "")
	require.NotNil(t, task)
	assert.NotEmpty(t, task.Events, "decision task should include history events")
	assert.NotEmpty(t, task.TaskToken)
}

// TestRespondDecisionTask_ViaHandler exercises RespondDecisionTaskCompleted through the HTTP handler.
func TestRespondDecisionTask_ViaHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantExecStatus string
		name           string
		setupFn        func(*swf.InMemoryBackend) string
		decisions      []map[string]any
		wantCode       int
	}{
		{
			name: "complete_via_handler",
			setupFn: func(b *swf.InMemoryBackend) string {
				require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
				_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
					Domain:     "dom",
					WorkflowID: "wf-1",
					TaskList:   "default",
				})
				require.NoError(t, err)
				b.EnqueueDecisionTaskInternal("dom", "default", "wf-1", "run-1")
				task := b.PollForDecisionTask("dom", "default", 0, "")
				require.NotNil(t, task)

				return task.TaskToken
			},
			decisions: []map[string]any{
				{
					"decisionType": "CompleteWorkflowExecution",
					"completeWorkflowExecutionDecisionAttributes": map[string]any{
						"result": "all-done",
					},
				},
			},
			wantCode:       http.StatusOK,
			wantExecStatus: "COMPLETED",
		},
		{
			name: "fail_via_handler",
			setupFn: func(b *swf.InMemoryBackend) string {
				require.NoError(t, b.RegisterDomain("dom2", "", "NONE"))
				_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
					Domain:     "dom2",
					WorkflowID: "wf-2",
					TaskList:   "default",
				})
				require.NoError(t, err)
				b.EnqueueDecisionTaskInternal("dom2", "default", "wf-2", "run-1")
				task := b.PollForDecisionTask("dom2", "default", 0, "")
				require.NotNil(t, task)

				return task.TaskToken
			},
			decisions: []map[string]any{
				{
					"decisionType": "FailWorkflowExecution",
					"failWorkflowExecutionDecisionAttributes": map[string]any{
						"reason":  "business error",
						"details": "step 3 failed",
					},
				},
			},
			wantCode:       http.StatusOK,
			wantExecStatus: "FAILED",
		},
		{
			name: "unknown_token_returns_404",
			setupFn: func(_ *swf.InMemoryBackend) string {
				return "bad-token"
			},
			decisions: nil,
			wantCode:  http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := swf.NewInMemoryBackend()
			token := tt.setupFn(b)
			h := swf.NewHandler(b)

			body := map[string]any{
				"taskToken": token,
				"decisions": tt.decisions,
			}
			rec := doSWFRequest(t, h, "RespondDecisionTaskCompleted", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantExecStatus != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				// Verify via describe - pick the right domain/workflowID
				var domainName, wfID string
				if tt.wantExecStatus == "COMPLETED" {
					domainName, wfID = "dom", "wf-1"
				} else {
					domainName, wfID = "dom2", "wf-2"
				}
				exec, err := b.DescribeWorkflowExecution(domainName, wfID, "")
				require.NoError(t, err)
				assert.Equal(t, tt.wantExecStatus, exec.Status)
			}
		})
	}
}

func TestRespondDecisionTaskCompleted_MultipleDecisions(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain:     "dom",
		WorkflowID: "wf-1",
		TaskList:   "default",
	})
	require.NoError(t, err)

	b.EnqueueDecisionTaskInternal("dom", "default", "wf-1", "run-1")
	token := pollDecisionTask(t, b, "dom", "default")

	// Schedule two activity tasks, then complete the workflow.
	decisions := []swf.Decision{
		{
			DecisionType: "ScheduleActivityTask",
			ScheduleActivityTaskAttrs: &swf.ScheduleActivityTaskDecisionAttrs{
				ActivityType: swf.ActivityTaskActivityType{Name: "act", Version: "1.0"},
				ActivityID:   "a1",
				TaskList:     "act-list",
			},
		},
		{
			DecisionType: "ScheduleActivityTask",
			ScheduleActivityTaskAttrs: &swf.ScheduleActivityTaskDecisionAttrs{
				ActivityType: swf.ActivityTaskActivityType{Name: "act", Version: "1.0"},
				ActivityID:   "a2",
				TaskList:     "act-list",
			},
		},
		{
			DecisionType:                   "CompleteWorkflowExecution",
			CompleteWorkflowExecutionAttrs: &swf.CompleteWorkflowExecutionDecisionAttrs{Result: "done"},
		},
	}
	require.NoError(t, b.RespondDecisionTaskCompleted(token, "", decisions))

	assert.Equal(t, 2, b.CountPendingActivityTasks("dom", "act-list"))
	exec, err := b.DescribeWorkflowExecution("dom", "wf-1", "")
	require.NoError(t, err)
	assert.Equal(t, "COMPLETED", exec.Status)
}

func TestDecisionTask_DecisionTaskCompletedEvent(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain:     "dom",
		WorkflowID: "wf-1",
		TaskList:   "default",
	})
	require.NoError(t, err)

	b.EnqueueDecisionTaskInternal("dom", "default", "wf-1", "run-1")
	token := pollDecisionTask(t, b, "dom", "default")
	require.NoError(t, b.RespondDecisionTaskCompleted(token, "ctx-value", nil))

	events, _ := b.GetWorkflowExecutionHistory("dom", "wf-1", "", 0, "", false)
	var found bool
	for _, ev := range events {
		if ev.EventType == "DecisionTaskCompleted" {
			found = true
		}
	}
	assert.True(t, found, "DecisionTaskCompleted event must appear in history")
}

func TestListClosedWorkflowExecutions_CloseStatusFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		decisionType    string
		closeStatusFilt string
		wantCount       int
	}{
		{
			name:            "filter_completed",
			decisionType:    "CompleteWorkflowExecution",
			closeStatusFilt: "COMPLETED",
			wantCount:       1,
		},
		{
			name:            "filter_failed",
			decisionType:    "FailWorkflowExecution",
			closeStatusFilt: "FAILED",
			wantCount:       1,
		},
		{
			name:            "filter_completed_no_match",
			decisionType:    "FailWorkflowExecution",
			closeStatusFilt: "COMPLETED",
			wantCount:       0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := swf.NewInMemoryBackend()
			require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
			_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
				Domain:     "dom",
				WorkflowID: "wf-1",
				TaskList:   "default",
			})
			require.NoError(t, err)

			b.EnqueueDecisionTaskInternal("dom", "default", "wf-1", "run-1")
			token := pollDecisionTask(t, b, "dom", "default")

			var decision swf.Decision
			switch tt.decisionType {
			case "CompleteWorkflowExecution":
				decision = swf.Decision{
					DecisionType:                   "CompleteWorkflowExecution",
					CompleteWorkflowExecutionAttrs: &swf.CompleteWorkflowExecutionDecisionAttrs{},
				}
			default:
				decision = swf.Decision{
					DecisionType:               "FailWorkflowExecution",
					FailWorkflowExecutionAttrs: &swf.FailWorkflowExecutionDecisionAttrs{},
				}
			}
			require.NoError(t, b.RespondDecisionTaskCompleted(token, "", []swf.Decision{decision}))

			filter := swf.ExecutionFilter{CloseStatus: tt.closeStatusFilt}
			execs := b.ListClosedWorkflowExecutions("dom", filter)
			assert.Len(t, execs, tt.wantCount)
		})
	}
}

// TestRespondDecisionTaskCompleted_TaskTimerMarkerAttrsPropagate verifies that
// RequestCancelActivityTask/StartTimer/CancelTimer/RecordMarker decision
// attributes sent over the wire actually reach the resulting history event's
// attributes, instead of being silently dropped during JSON->Decision
// conversion (the decisions were previously applied -- the correct event type
// was recorded -- but every attribute the decider sent was discarded).
func TestRespondDecisionTaskCompleted_TaskTimerMarkerAttrsPropagate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantAttrs     map[string]any
		name          string
		wantEventType string
		attrKey       string
		decisions     []map[string]any
	}{
		{
			name: "RequestCancelActivityTask",
			decisions: []map[string]any{{
				"decisionType": "RequestCancelActivityTask",
				"requestCancelActivityTaskDecisionAttributes": map[string]any{
					"activityId": "act-42",
				},
			}},
			wantEventType: "ActivityTaskCancelRequested",
			attrKey:       "activityTaskCancelRequestedEventAttributes",
			wantAttrs:     map[string]any{"activityId": "act-42"},
		},
		{
			name: "StartTimer",
			decisions: []map[string]any{{
				"decisionType": "StartTimer",
				"startTimerDecisionAttributes": map[string]any{
					"timerId":            "timer-1",
					"startToFireTimeout": "60",
				},
			}},
			wantEventType: "TimerStarted",
			attrKey:       "timerStartedEventAttributes",
			wantAttrs:     map[string]any{"timerId": "timer-1", "startToFireTimeout": "60"},
		},
		{
			// CancelTimer only succeeds against a timer that's actually
			// open (real AWS rejects an unknown timerId with
			// CancelTimerFailed/TIMER_ID_UNKNOWN -- see
			// TestRespondDecisionTaskCompleted_CancelTimer_UnknownID),
			// so this batch starts timer-1 first in the same decision
			// task before canceling it.
			name: "CancelTimer",
			decisions: []map[string]any{
				{
					"decisionType": "StartTimer",
					"startTimerDecisionAttributes": map[string]any{
						"timerId":            "timer-1",
						"startToFireTimeout": "60",
					},
				},
				{
					"decisionType": "CancelTimer",
					"cancelTimerDecisionAttributes": map[string]any{
						"timerId": "timer-1",
					},
				},
			},
			wantEventType: "TimerCanceled",
			attrKey:       "timerCanceledEventAttributes",
			wantAttrs:     map[string]any{"timerId": "timer-1"},
		},
		{
			name: "RecordMarker",
			decisions: []map[string]any{{
				"decisionType": "RecordMarker",
				"recordMarkerDecisionAttributes": map[string]any{
					"markerName": "checkpoint",
					"details":    "step-3",
				},
			}},
			wantEventType: "MarkerRecorded",
			attrKey:       "markerRecordedEventAttributes",
			wantAttrs:     map[string]any{"markerName": "checkpoint", "details": "step-3"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := swf.NewInMemoryBackend()
			require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
			_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
				Domain:     "dom",
				WorkflowID: "wf-1",
				TaskList:   "default",
			})
			require.NoError(t, err)

			token := pollDecisionTask(t, b, "dom", "default")
			h := swf.NewHandler(b)
			rec := doSWFRequest(t, h, "RespondDecisionTaskCompleted", map[string]any{
				"taskToken": token,
				"decisions": tt.decisions,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			events, _ := b.GetWorkflowExecutionHistory("dom", "wf-1", "", 0, "", false)
			var found *swf.HistoryEvent
			for i := range events {
				if events[i].EventType == tt.wantEventType {
					found = &events[i]
				}
			}
			require.NotNil(t, found, "expected %s event in history", tt.wantEventType)

			attrs, ok := found.Attributes[tt.attrKey].(map[string]any)
			require.True(t, ok, "expected attributes under %s", tt.attrKey)
			for k, want := range tt.wantAttrs {
				assert.Equal(t, want, attrs[k], "attribute %s", k)
			}
		})
	}
}

// TestRespondDecisionTaskCompleted_NewDecisionTypes verifies 7 additional
// decision types are processed without error.
func TestRespondDecisionTaskCompleted_NewDecisionTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		decisionType string
	}{
		{name: "RequestCancelActivityTask", decisionType: "RequestCancelActivityTask"},
		{name: "StartTimer", decisionType: "StartTimer"},
		{name: "CancelTimer", decisionType: "CancelTimer"},
		{name: "RecordMarker", decisionType: "RecordMarker"},
		{name: "StartChildWorkflowExecution", decisionType: "StartChildWorkflowExecution"},
		{name: "SignalExternalWorkflowExecution", decisionType: "SignalExternalWorkflowExecution"},
		{name: "RequestCancelExternalWorkflowExecution", decisionType: "RequestCancelExternalWorkflowExecution"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := swf.NewInMemoryBackend()
			require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
			_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
				Domain: "dom", WorkflowID: "wf-1", TaskList: "tasks",
			})
			require.NoError(t, err)

			b.EnqueueDecisionTaskInternal("dom", "tasks", "wf-1", "run-1")
			token := pollDecisionTask(t, b, "dom", "tasks")

			err = b.RespondDecisionTaskCompleted(token, "", []swf.Decision{
				{DecisionType: tt.decisionType},
			})
			require.NoError(t, err)

			events, _ := b.GetWorkflowExecutionHistory("dom", "wf-1", "", 0, "", false)
			assert.NotEmpty(t, events)
		})
	}
}
