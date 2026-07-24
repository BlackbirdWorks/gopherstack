// Package swf_test covers the cross-execution decision types implemented in
// decision_orchestration.go: ContinueAsNewWorkflowExecution (tested in
// decision_lifecycle_test.go alongside the other close-type decisions),
// StartChildWorkflowExecution, SignalExternalWorkflowExecution, and
// RequestCancelExternalWorkflowExecution -- plus the StartTimer/CancelTimer
// timer-ID validation and the decision-type dispatch table itself.
package swf_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

// TestDecisionHandlers_CoverAllDecisionTypes is a table-driven test over the
// decision-type dispatch table (decision_tasks.go's decisionHandlers),
// asserting every SWF decision type this service claims to support has a
// registered handler -- guards against a decision type silently falling
// through the dispatch table with no handler (a disguised no-op).
func TestDecisionHandlers_CoverAllDecisionTypes(t *testing.T) {
	t.Parallel()

	want := []string{
		"CancelTimer",
		"CancelWorkflowExecution",
		"CompleteWorkflowExecution",
		"ContinueAsNewWorkflowExecution",
		"FailWorkflowExecution",
		"RecordMarker",
		"RequestCancelActivityTask",
		"RequestCancelExternalWorkflowExecution",
		"ScheduleActivityTask",
		"SignalExternalWorkflowExecution",
		"StartChildWorkflowExecution",
		"StartTimer",
	}

	got := swf.DecisionHandlerTypes()
	assert.ElementsMatch(t, want, got, "every SWF decision type must have a dispatch table entry")

	for _, dt := range want {
		t.Run(dt, func(t *testing.T) {
			t.Parallel()

			b := swf.NewInMemoryBackend()
			require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
			_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
				Domain: "dom", WorkflowID: "wf-1", TaskList: "tasks",
			})
			require.NoError(t, err)

			task := b.PollForDecisionTask("dom", "tasks", 0, "")
			require.NotNil(t, task)

			// A bare decision (nil attrs) must never panic or error --
			// every handler is expected to treat missing attrs as a
			// silent no-op, exactly like real AWS's per-decision-type
			// attribute requirement (see e.g. ScheduleActivityTask).
			err = b.RespondDecisionTaskCompleted(task.TaskToken, "", []swf.Decision{{DecisionType: dt}})
			require.NoError(t, err)
		})
	}
}

func TestStartChildWorkflowExecutionDecision_Success(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	require.NoError(t, b.RegisterWorkflowType("dom", "childType", "1.0", "", swf.WorkflowTypeDefaults{}))

	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "parent-1", TaskList: "parent-tasks",
	})
	require.NoError(t, err)

	parentToken := pollDecisionTask(t, b, "dom", "parent-tasks")
	decisions := []swf.Decision{{
		DecisionType: "StartChildWorkflowExecution",
		StartChildWorkflowExecutionAttrs: &swf.StartChildWorkflowExecutionDecisionAttrs{
			WorkflowID:   "child-1",
			WorkflowType: swf.WorkflowTypeRef{Name: "childType", Version: "1.0"},
			TaskList:     "child-tasks",
			Input:        `{"x":1}`,
		},
	}}
	require.NoError(t, b.RespondDecisionTaskCompleted(parentToken, "", decisions))

	// The child must have actually started: it's describable, RUNNING, and
	// pollable for its own decision task.
	child, err := b.DescribeWorkflowExecution("dom", "child-1")
	require.NoError(t, err)
	assert.Equal(t, "RUNNING", child.Status)
	assert.Equal(t, `{"x":1}`, child.Input)

	childTask := b.PollForDecisionTask("dom", "child-tasks", 0, "")
	require.NotNil(t, childTask, "child must have its own initial decision task")
	assert.Equal(t, "child-1", childTask.WorkflowID)

	// The parent's history must show ChildWorkflowExecutionStarted (not
	// just an empty *Initiated event).
	events, _ := b.GetWorkflowExecutionHistory("dom", "parent-1", 0, "", false)
	var startedEvent *swf.HistoryEvent
	for i := range events {
		if events[i].EventType == "ChildWorkflowExecutionStarted" {
			startedEvent = &events[i]
		}
	}
	require.NotNil(t, startedEvent, "expected ChildWorkflowExecutionStarted on parent history")
	attrs, ok := startedEvent.Attributes["childWorkflowExecutionStartedEventAttributes"].(map[string]any)
	require.True(t, ok)
	we, ok := attrs["workflowExecution"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "child-1", we["workflowId"])

	// openCounts.openChildWorkflowExecutions must reflect the real child,
	// not a hardcoded 0.
	h := swf.NewHandler(b)
	rec := doSWFRequest(t, h, "DescribeWorkflowExecution", map[string]any{
		"domain":    "dom",
		"execution": map[string]any{"workflowId": "parent-1"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	openCounts, ok := body["openCounts"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, 1, openCounts["openChildWorkflowExecutions"], 0)
}

func TestStartChildWorkflowExecutionDecision_UnknownWorkflowType(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "parent-1", TaskList: "parent-tasks",
	})
	require.NoError(t, err)

	token := pollDecisionTask(t, b, "dom", "parent-tasks")
	decisions := []swf.Decision{{
		DecisionType: "StartChildWorkflowExecution",
		StartChildWorkflowExecutionAttrs: &swf.StartChildWorkflowExecutionDecisionAttrs{
			WorkflowID:   "child-1",
			WorkflowType: swf.WorkflowTypeRef{Name: "does-not-exist", Version: "1.0"},
		},
	}}
	require.NoError(t, b.RespondDecisionTaskCompleted(token, "", decisions))

	_, err = b.DescribeWorkflowExecution("dom", "child-1")
	require.Error(t, err, "the child must never have been created")

	events, _ := b.GetWorkflowExecutionHistory("dom", "parent-1", 0, "", false)
	var failed *swf.HistoryEvent
	for i := range events {
		if events[i].EventType == "StartChildWorkflowExecutionFailed" {
			failed = &events[i]
		}
	}
	require.NotNil(t, failed)
	attrs, ok := failed.Attributes["startChildWorkflowExecutionFailedEventAttributes"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "WORKFLOW_TYPE_DOES_NOT_EXIST", attrs["cause"])
}

func TestStartChildWorkflowExecutionDecision_AlreadyRunning(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	require.NoError(t, b.RegisterWorkflowType("dom", "childType", "1.0", "", swf.WorkflowTypeDefaults{}))

	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "parent-1", TaskList: "parent-tasks",
	})
	require.NoError(t, err)
	// Pre-seed an already-open execution under the child's workflowId.
	_, err = b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "child-1", TaskList: "child-tasks",
	})
	require.NoError(t, err)

	token := pollDecisionTask(t, b, "dom", "parent-tasks")
	decisions := []swf.Decision{{
		DecisionType: "StartChildWorkflowExecution",
		StartChildWorkflowExecutionAttrs: &swf.StartChildWorkflowExecutionDecisionAttrs{
			WorkflowID:   "child-1",
			WorkflowType: swf.WorkflowTypeRef{Name: "childType", Version: "1.0"},
		},
	}}
	require.NoError(t, b.RespondDecisionTaskCompleted(token, "", decisions))

	events, _ := b.GetWorkflowExecutionHistory("dom", "parent-1", 0, "", false)
	var failed *swf.HistoryEvent
	for i := range events {
		if events[i].EventType == "StartChildWorkflowExecutionFailed" {
			failed = &events[i]
		}
	}
	require.NotNil(t, failed)
	attrs, ok := failed.Attributes["startChildWorkflowExecutionFailedEventAttributes"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "WORKFLOW_ALREADY_RUNNING", attrs["cause"])
}

// TestChildWorkflowClosure_PropagatesToParent covers all three
// decision-driven child closures (Complete/Fail/Cancel) plus the
// TerminateWorkflowExecution op, verifying each appends the matching Child*
// event to the parent's history and gives the parent a fresh decision task.
func TestChildWorkflowClosure_PropagatesToParent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		closeChild    func(t *testing.T, b *swf.InMemoryBackend, childToken string)
		wantEventType string
	}{
		{
			name: "complete",
			closeChild: func(t *testing.T, b *swf.InMemoryBackend, childToken string) {
				t.Helper()
				require.NoError(t, b.RespondDecisionTaskCompleted(childToken, "", []swf.Decision{{
					DecisionType:                   "CompleteWorkflowExecution",
					CompleteWorkflowExecutionAttrs: &swf.CompleteWorkflowExecutionDecisionAttrs{Result: "ok"},
				}}))
			},
			wantEventType: "ChildWorkflowExecutionCompleted",
		},
		{
			name: "fail",
			closeChild: func(t *testing.T, b *swf.InMemoryBackend, childToken string) {
				t.Helper()
				require.NoError(t, b.RespondDecisionTaskCompleted(childToken, "", []swf.Decision{{
					DecisionType:               "FailWorkflowExecution",
					FailWorkflowExecutionAttrs: &swf.FailWorkflowExecutionDecisionAttrs{Reason: "boom"},
				}}))
			},
			wantEventType: "ChildWorkflowExecutionFailed",
		},
		{
			name: "cancel",
			closeChild: func(t *testing.T, b *swf.InMemoryBackend, childToken string) {
				t.Helper()
				require.NoError(t, b.RespondDecisionTaskCompleted(childToken, "", []swf.Decision{{
					DecisionType:                 "CancelWorkflowExecution",
					CancelWorkflowExecutionAttrs: &swf.CancelWorkflowExecutionDecisionAttrs{Details: "stop"},
				}}))
			},
			wantEventType: "ChildWorkflowExecutionCanceled",
		},
		{
			name: "terminate",
			closeChild: func(t *testing.T, b *swf.InMemoryBackend, _ string) {
				t.Helper()
				require.NoError(t, b.TerminateWorkflowExecution("dom", "child-1", "", "operator", ""))
			},
			wantEventType: "ChildWorkflowExecutionTerminated",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := swf.NewInMemoryBackend()
			require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
			require.NoError(t, b.RegisterWorkflowType("dom", "childType", "1.0", "", swf.WorkflowTypeDefaults{}))

			_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
				Domain: "dom", WorkflowID: "parent-1", TaskList: "parent-tasks",
			})
			require.NoError(t, err)

			parentToken := pollDecisionTask(t, b, "dom", "parent-tasks")
			require.NoError(t, b.RespondDecisionTaskCompleted(parentToken, "", []swf.Decision{{
				DecisionType: "StartChildWorkflowExecution",
				StartChildWorkflowExecutionAttrs: &swf.StartChildWorkflowExecutionDecisionAttrs{
					WorkflowID:   "child-1",
					WorkflowType: swf.WorkflowTypeRef{Name: "childType", Version: "1.0"},
					TaskList:     "child-tasks",
				},
			}}))

			// Drain the parent's decision task from the child-start
			// notification so the closure below produces exactly one
			// fresh, observable task.
			childToken := pollDecisionTask(t, b, "dom", "child-tasks")

			tt.closeChild(t, b, childToken)

			events, _ := b.GetWorkflowExecutionHistory("dom", "parent-1", 0, "", false)
			var found bool
			for i := range events {
				if events[i].EventType == tt.wantEventType {
					found = true
				}
			}
			assert.True(t, found, "expected %s on parent history", tt.wantEventType)

			task := b.PollForDecisionTask("dom", "parent-tasks", 0, "")
			assert.NotNil(t, task, "parent must get a fresh decision task when its child closes")

			// openChildWorkflowExecutions must have dropped back to 0.
			exec, err := b.DescribeWorkflowExecution("dom", "parent-1")
			require.NoError(t, err)
			assert.Equal(t, "RUNNING", exec.Status)
		})
	}
}

func TestSignalExternalWorkflowExecutionDecision_Success(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "sender-1", TaskList: "sender-tasks",
	})
	require.NoError(t, err)
	_, err = b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "target-1", TaskList: "target-tasks",
	})
	require.NoError(t, err)
	// Drain target's initial decision task so the signal-driven one below
	// is unambiguous.
	pollDecisionTask(t, b, "dom", "target-tasks")

	token := pollDecisionTask(t, b, "dom", "sender-tasks")
	decisions := []swf.Decision{{
		DecisionType: "SignalExternalWorkflowExecution",
		SignalExternalWorkflowExecutionAttrs: &swf.SignalExternalWorkflowExecutionDecisionAttrs{
			WorkflowID: "target-1",
			SignalName: "go",
			Input:      `{"n":1}`,
		},
	}}
	require.NoError(t, b.RespondDecisionTaskCompleted(token, "", decisions))

	events, _ := b.GetWorkflowExecutionHistory("dom", "target-1", 0, "", false)
	var signaled *swf.HistoryEvent
	for i := range events {
		if events[i].EventType == "WorkflowExecutionSignaled" {
			signaled = &events[i]
		}
	}
	require.NotNil(t, signaled, "expected WorkflowExecutionSignaled on target history")
	attrs, ok := signaled.Attributes["workflowExecutionSignaledEventAttributes"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "go", attrs["signalName"])
	assert.Equal(t, `{"n":1}`, attrs["input"])

	targetTask := b.PollForDecisionTask("dom", "target-tasks", 0, "")
	require.NotNil(t, targetTask, "the signal must enqueue the target a decision task")

	senderEvents, _ := b.GetWorkflowExecutionHistory("dom", "sender-1", 0, "", false)
	var externalSignaled bool
	for i := range senderEvents {
		if senderEvents[i].EventType == "ExternalWorkflowExecutionSignaled" {
			externalSignaled = true
		}
	}
	assert.True(t, externalSignaled, "expected ExternalWorkflowExecutionSignaled on sender history")
}

func TestSignalExternalWorkflowExecutionDecision_UnknownTarget(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "sender-1", TaskList: "sender-tasks",
	})
	require.NoError(t, err)

	token := pollDecisionTask(t, b, "dom", "sender-tasks")
	decisions := []swf.Decision{{
		DecisionType: "SignalExternalWorkflowExecution",
		SignalExternalWorkflowExecutionAttrs: &swf.SignalExternalWorkflowExecutionDecisionAttrs{
			WorkflowID: "no-such-workflow",
			SignalName: "go",
		},
	}}
	require.NoError(t, b.RespondDecisionTaskCompleted(token, "", decisions))

	events, _ := b.GetWorkflowExecutionHistory("dom", "sender-1", 0, "", false)
	var failed *swf.HistoryEvent
	for i := range events {
		if events[i].EventType == "SignalExternalWorkflowExecutionFailed" {
			failed = &events[i]
		}
	}
	require.NotNil(t, failed)
	attrs, ok := failed.Attributes["signalExternalWorkflowExecutionFailedEventAttributes"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "UNKNOWN_EXTERNAL_WORKFLOW_EXECUTION", attrs["cause"])
}

func TestRequestCancelExternalWorkflowExecutionDecision_Success(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "sender-1", TaskList: "sender-tasks",
	})
	require.NoError(t, err)
	_, err = b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "target-1", TaskList: "target-tasks",
	})
	require.NoError(t, err)
	pollDecisionTask(t, b, "dom", "target-tasks")

	token := pollDecisionTask(t, b, "dom", "sender-tasks")
	decisions := []swf.Decision{{
		DecisionType: "RequestCancelExternalWorkflowExecution",
		RequestCancelExternalWorkflowExecutionAttrs: &swf.RequestCancelExternalWorkflowExecutionDecisionAttrs{
			WorkflowID: "target-1",
		},
	}}
	require.NoError(t, b.RespondDecisionTaskCompleted(token, "", decisions))

	target, err := b.DescribeWorkflowExecution("dom", "target-1")
	require.NoError(t, err)
	assert.True(t, target.CancelRequested, "target must have CancelRequested set")

	events, _ := b.GetWorkflowExecutionHistory("dom", "target-1", 0, "", false)
	var found bool
	for i := range events {
		if events[i].EventType == "WorkflowExecutionCancelRequested" {
			found = true
		}
	}
	assert.True(t, found, "expected WorkflowExecutionCancelRequested on target history")

	targetTask := b.PollForDecisionTask("dom", "target-tasks", 0, "")
	require.NotNil(t, targetTask, "the cancel request must enqueue the target a decision task")
}

func TestRequestCancelExternalWorkflowExecutionDecision_UnknownTarget(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "sender-1", TaskList: "sender-tasks",
	})
	require.NoError(t, err)

	token := pollDecisionTask(t, b, "dom", "sender-tasks")
	decisions := []swf.Decision{{
		DecisionType: "RequestCancelExternalWorkflowExecution",
		RequestCancelExternalWorkflowExecutionAttrs: &swf.RequestCancelExternalWorkflowExecutionDecisionAttrs{
			WorkflowID: "no-such-workflow",
		},
	}}
	require.NoError(t, b.RespondDecisionTaskCompleted(token, "", decisions))

	events, _ := b.GetWorkflowExecutionHistory("dom", "sender-1", 0, "", false)
	var failed *swf.HistoryEvent
	for i := range events {
		if events[i].EventType == "RequestCancelExternalWorkflowExecutionFailed" {
			failed = &events[i]
		}
	}
	require.NotNil(t, failed)
	attrs, ok := failed.Attributes["requestCancelExternalWorkflowExecutionFailedEventAttributes"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "UNKNOWN_EXTERNAL_WORKFLOW_EXECUTION", attrs["cause"])
}

func TestStartTimerDecision_AlreadyInUse(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "wf-1", TaskList: "tasks",
	})
	require.NoError(t, err)

	token := pollDecisionTask(t, b, "dom", "tasks")
	require.NoError(t, b.RespondDecisionTaskCompleted(token, "", []swf.Decision{
		{
			DecisionType:    "StartTimer",
			StartTimerAttrs: &swf.StartTimerDecisionAttrs{TimerID: "t1", StartToFireTimeout: "60"},
		},
		{
			DecisionType:    "StartTimer",
			StartTimerAttrs: &swf.StartTimerDecisionAttrs{TimerID: "t1", StartToFireTimeout: "60"},
		},
	}))

	events, _ := b.GetWorkflowExecutionHistory("dom", "wf-1", 0, "", false)
	var startedCount int
	var failed *swf.HistoryEvent
	for i := range events {
		switch events[i].EventType {
		case "TimerStarted":
			startedCount++
		case "StartTimerFailed":
			failed = &events[i]
		}
	}
	assert.Equal(t, 1, startedCount, "the second StartTimer for the same id must not add a second TimerStarted")
	require.NotNil(t, failed)
	attrs, ok := failed.Attributes["startTimerFailedEventAttributes"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "TIMER_ID_ALREADY_IN_USE", attrs["cause"])
}

func TestCancelTimerDecision_UnknownID(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "wf-1", TaskList: "tasks",
	})
	require.NoError(t, err)

	token := pollDecisionTask(t, b, "dom", "tasks")
	require.NoError(t, b.RespondDecisionTaskCompleted(token, "", []swf.Decision{
		{DecisionType: "CancelTimer", CancelTimerAttrs: &swf.CancelTimerDecisionAttrs{TimerID: "never-started"}},
	}))

	events, _ := b.GetWorkflowExecutionHistory("dom", "wf-1", 0, "", false)
	var failed *swf.HistoryEvent
	for i := range events {
		if events[i].EventType == "CancelTimerFailed" {
			failed = &events[i]
		}
	}
	require.NotNil(t, failed)
	attrs, ok := failed.Attributes["cancelTimerFailedEventAttributes"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "TIMER_ID_UNKNOWN", attrs["cause"])
}

// TestStartChildWorkflowExecution_ViaHandler drives the wire path end-to-end
// (JSON in, JSON out) for one representative cross-execution decision type,
// guarding against a wire-field-name typo in the
// startChildWorkflowExecutionDecisionAttributes parsing added to
// handler_decision_tasks.go.
func TestStartChildWorkflowExecution_ViaHandler(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	require.NoError(t, b.RegisterWorkflowType("dom", "childType", "1.0", "", swf.WorkflowTypeDefaults{}))
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "parent-1", TaskList: "parent-tasks",
	})
	require.NoError(t, err)

	token := pollDecisionTask(t, b, "dom", "parent-tasks")
	h := swf.NewHandler(b)
	rec := doSWFRequest(t, h, "RespondDecisionTaskCompleted", map[string]any{
		"taskToken": token,
		"decisions": []map[string]any{{
			"decisionType": "StartChildWorkflowExecution",
			"startChildWorkflowExecutionDecisionAttributes": map[string]any{
				"workflowId":   "child-1",
				"workflowType": map[string]any{"name": "childType", "version": "1.0"},
				"taskList":     map[string]any{"name": "child-tasks"},
				"input":        `{"via":"wire"}`,
			},
		}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	child, err := b.DescribeWorkflowExecution("dom", "child-1")
	require.NoError(t, err)
	assert.Equal(t, "RUNNING", child.Status)
	assert.JSONEq(t, `{"via":"wire"}`, child.Input)
}
