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
//
//nolint:unparam // domain is always "dom" in current tests but kept for clarity
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

			exec, err := b.DescribeWorkflowExecution("dom", "wf-1")
			require.NoError(t, err)
			assert.Equal(t, tt.wantCloseStatus, exec.Status)
			assert.Equal(t, tt.wantCloseStatus, exec.CloseStatus)
			assert.NotZero(t, exec.CloseTimestamp)

			events, _ := b.GetWorkflowExecutionHistory("dom", "wf-1", 0, "", false)
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

			exec, err := b.DescribeWorkflowExecution("dom", "wf-1")
			require.NoError(t, err)
			assert.Equal(t, "FAILED", exec.Status)
			assert.Equal(t, "FAILED", exec.CloseStatus)
			assert.NotZero(t, exec.CloseTimestamp)

			events, _ := b.GetWorkflowExecutionHistory("dom", "wf-1", 0, "", false)
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

			exec, err := b.DescribeWorkflowExecution("dom", "wf-1")
			require.NoError(t, err)
			assert.Equal(t, "CANCELED", exec.Status)
			assert.Equal(t, "CANCELED", exec.CloseStatus)
		})
	}
}

func TestRespondDecisionTaskCompleted_ContinueAsNew(t *testing.T) {
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
		DecisionType: "ContinueAsNewWorkflowExecution",
	}}
	require.NoError(t, b.RespondDecisionTaskCompleted(token, "", decisions))

	exec, err := b.DescribeWorkflowExecution("dom", "wf-1")
	require.NoError(t, err)
	assert.Equal(t, "CONTINUED_AS_NEW", exec.Status)
	assert.Equal(t, "CONTINUED_AS_NEW", exec.CloseStatus)
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

	exec, err := b.DescribeWorkflowExecution("dom", "wf-1")
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
				exec, err := b.DescribeWorkflowExecution(domainName, wfID)
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
	exec, err := b.DescribeWorkflowExecution("dom", "wf-1")
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

	events, _ := b.GetWorkflowExecutionHistory("dom", "wf-1", 0, "", false)
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
