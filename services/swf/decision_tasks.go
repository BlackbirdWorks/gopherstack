package swf

import (
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CountPendingDecisionTasks returns the number of pending decision tasks for a task list.
func (b *InMemoryBackend) CountPendingDecisionTasks(domain, taskList string) int {
	b.mu.RLock("CountPendingDecisionTasks")
	defer b.mu.RUnlock()

	return len(b.decisionQueues[domain+":"+taskList])
}

// PollForDecisionTask returns the next available decision task for a task list, or nil if none.
func (b *InMemoryBackend) PollForDecisionTask(
	domain, taskList string,
	maxPageSize int,
	nextPageToken string,
) *DecisionTask {
	b.mu.Lock("PollForDecisionTask")
	defer b.mu.Unlock()

	key := domain + ":" + taskList
	queue := b.decisionQueues[key]
	if len(queue) == 0 {
		return nil
	}

	task := *queue[0]
	b.decisionQueues[key] = queue[1:]
	task.TaskToken = uuid.New().String()

	b.activeDecisionTasks.Put(&activeDecisionTaskRecord{
		Domain:     domain,
		WorkflowID: task.WorkflowID,
		RunID:      task.RunID,
		TaskToken:  task.TaskToken,
	})

	histEvents := b.history[domain+":"+task.WorkflowID]
	if len(histEvents) > 0 {
		cp := make([]HistoryEvent, len(histEvents))
		copy(cp, histEvents)
		const maxDefault = 1000
		p := page.New(cp, nextPageToken, maxPageSize, maxDefault)
		task.Events = p.Data
		task.NextPageToken = p.Next
	}

	// Populate workflow type from execution if known.
	if exec, ok := b.executions.Get(domain + ":" + task.WorkflowID); ok {
		task.WorkflowTypeName = exec.WorkflowTypeName
		task.WorkflowTypeVersion = exec.WorkflowTypeVersion
	}

	return &task
}

// RespondDecisionTaskCompleted processes a completed decision task and applies decisions.
func (b *InMemoryBackend) RespondDecisionTaskCompleted(
	taskToken, executionContext string,
	decisions []Decision,
) error {
	b.mu.Lock("RespondDecisionTaskCompleted")
	defer b.mu.Unlock()

	rec, ok := b.activeDecisionTasks.Get(taskToken)
	if !ok {
		return fmt.Errorf("%w: decision task token %s not found", ErrNotFound, taskToken)
	}
	b.activeDecisionTasks.Delete(taskToken)

	key := rec.Domain + ":" + rec.WorkflowID
	exec, ok := b.executions.Get(key)
	if !ok {
		return nil
	}

	if executionContext != "" {
		exec.LatestExecutionContext = executionContext
	}

	dtcEventID := b.appendHistoryEventLocked(rec.Domain, rec.WorkflowID, "DecisionTaskCompleted", map[string]any{
		eventAttrKey("DecisionTaskCompleted"): map[string]any{
			"executionContext": executionContext,
		},
	})

	for _, d := range decisions {
		dc := decisionCtx{
			domain:                       rec.Domain,
			workflowID:                   rec.WorkflowID,
			exec:                         exec,
			decision:                     d,
			decisionTaskCompletedEventID: dtcEventID,
		}
		if handler, known := decisionHandlers()[d.DecisionType]; known {
			handler(b, dc)
		}
	}

	return nil
}

// decisionCtx bundles the parameters every per-decision-type handler needs,
// so decisionHandlers's dispatch table entries can share one function
// signature regardless of which of the 12+ SWF decision types they handle.
type decisionCtx struct {
	exec                         *WorkflowExecution
	domain                       string
	workflowID                   string
	decision                     Decision
	decisionTaskCompletedEventID int64
}

// decisionHandlerFunc processes one decision. Caller (RespondDecisionTaskCompleted)
// must hold the write lock.
type decisionHandlerFunc func(b *InMemoryBackend, dc decisionCtx)

// decisionHandlers lazily builds the DecisionType -> handler dispatch table
// exactly once. This replaces a single large switch (previously suppressed
// with a cyclop/funlen nolint) with one small function per decision type,
// decomposing per-decision-type complexity instead of suppressing it --
// mirrors services/apigatewayv2's onceOpTable routing-table pattern.
//
//nolint:gochecknoglobals // read-only package-level lookup table (apigatewayv2 style)
var decisionHandlersOnce = sync.OnceValue(func() map[string]decisionHandlerFunc {
	return map[string]decisionHandlerFunc{
		"CompleteWorkflowExecution":              (*InMemoryBackend).handleCompleteWorkflowExecutionDecision,
		"FailWorkflowExecution":                  (*InMemoryBackend).handleFailWorkflowExecutionDecision,
		"CancelWorkflowExecution":                (*InMemoryBackend).handleCancelWorkflowExecutionDecision,
		"ContinueAsNewWorkflowExecution":         (*InMemoryBackend).handleContinueAsNewWorkflowExecutionDecision,
		"ScheduleActivityTask":                   (*InMemoryBackend).handleScheduleActivityTaskDecision,
		"RequestCancelActivityTask":              (*InMemoryBackend).handleRequestCancelActivityTaskDecision,
		"StartTimer":                             (*InMemoryBackend).handleStartTimerDecision,
		"CancelTimer":                            (*InMemoryBackend).handleCancelTimerDecision,
		"RecordMarker":                           (*InMemoryBackend).handleRecordMarkerDecision,
		"StartChildWorkflowExecution":            (*InMemoryBackend).handleStartChildWorkflowExecutionDecision,
		"SignalExternalWorkflowExecution":        (*InMemoryBackend).handleSignalExternalWorkflowExecutionDecision,
		"RequestCancelExternalWorkflowExecution": (*InMemoryBackend).handleRequestCancelExternalWorkflowExecutionDecision,
	}
})

// decisionHandlers returns the DecisionType -> handler dispatch table.
func decisionHandlers() map[string]decisionHandlerFunc { return decisionHandlersOnce() }

func (b *InMemoryBackend) handleCompleteWorkflowExecutionDecision(dc decisionCtx) {
	result := ""
	if dc.decision.CompleteWorkflowExecutionAttrs != nil {
		result = dc.decision.CompleteWorkflowExecutionAttrs.Result
	}
	dc.exec.Status = statusCompleted
	dc.exec.CloseStatus = statusCompleted
	dc.exec.CloseTimestamp = float64(time.Now().UnixMilli()) / milliDivisor
	b.appendHistoryEventLocked(dc.domain, dc.workflowID, "WorkflowExecutionCompleted", map[string]any{
		eventAttrKey("WorkflowExecutionCompleted"): map[string]any{
			attrDTCEventID: dc.decisionTaskCompletedEventID,
			attrResult:     result,
		},
	})
	b.propagateChildClosureLocked(
		dc.domain,
		dc.exec,
		"ChildWorkflowExecutionCompleted",
		map[string]any{attrResult: result},
	)
}

func (b *InMemoryBackend) handleFailWorkflowExecutionDecision(dc decisionCtx) {
	reason, details := "", ""
	if dc.decision.FailWorkflowExecutionAttrs != nil {
		reason = dc.decision.FailWorkflowExecutionAttrs.Reason
		details = dc.decision.FailWorkflowExecutionAttrs.Details
	}
	dc.exec.Status = statusFailed
	dc.exec.CloseStatus = statusFailed
	dc.exec.CloseTimestamp = float64(time.Now().UnixMilli()) / milliDivisor
	b.appendHistoryEventLocked(dc.domain, dc.workflowID, "WorkflowExecutionFailed", map[string]any{
		eventAttrKey("WorkflowExecutionFailed"): map[string]any{
			attrDTCEventID: dc.decisionTaskCompletedEventID,
			attrReason:     reason,
			attrDetails:    details,
		},
	})
	b.propagateChildClosureLocked(dc.domain, dc.exec, "ChildWorkflowExecutionFailed", map[string]any{
		attrReason: reason, attrDetails: details,
	})
}

func (b *InMemoryBackend) handleCancelWorkflowExecutionDecision(dc decisionCtx) {
	details := ""
	if dc.decision.CancelWorkflowExecutionAttrs != nil {
		details = dc.decision.CancelWorkflowExecutionAttrs.Details
	}
	dc.exec.Status = statusCanceled
	dc.exec.CloseStatus = statusCanceled
	dc.exec.CloseTimestamp = float64(time.Now().UnixMilli()) / milliDivisor
	b.appendHistoryEventLocked(dc.domain, dc.workflowID, "WorkflowExecutionCanceled", map[string]any{
		eventAttrKey("WorkflowExecutionCanceled"): map[string]any{
			attrDTCEventID: dc.decisionTaskCompletedEventID,
			attrDetails:    details,
		},
	})
	b.propagateChildClosureLocked(
		dc.domain,
		dc.exec,
		"ChildWorkflowExecutionCanceled",
		map[string]any{attrDetails: details},
	)
}

func (b *InMemoryBackend) handleScheduleActivityTaskDecision(dc decisionCtx) {
	attrs := dc.decision.ScheduleActivityTaskAttrs
	if attrs == nil {
		return
	}
	taskList := attrs.TaskList
	if taskList == "" {
		taskList = dc.exec.TaskList
	}
	scheduledEventID := b.appendHistoryEventLocked(dc.domain, dc.workflowID, "ActivityTaskScheduled", map[string]any{
		eventAttrKey("ActivityTaskScheduled"): map[string]any{
			attrDTCEventID: dc.decisionTaskCompletedEventID,
			"activityType": map[string]any{
				attrName:    attrs.ActivityType.Name,
				attrVersion: attrs.ActivityType.Version,
			},
			"activityId": attrs.ActivityID,
			attrInput:    attrs.Input,
			attrTaskList: map[string]any{attrName: taskList},
		},
	})
	qkey := dc.domain + ":" + taskList
	b.activityQueues[qkey] = append(b.activityQueues[qkey], &ActivityTask{
		ActivityID:       attrs.ActivityID,
		ActivityType:     attrs.ActivityType,
		Input:            attrs.Input,
		WorkflowID:       dc.workflowID,
		RunID:            dc.exec.RunID,
		ScheduledEventID: scheduledEventID,
	})
}

func (b *InMemoryBackend) handleRequestCancelActivityTaskDecision(dc decisionCtx) {
	activityID := ""
	if dc.decision.RequestCancelActivityTaskAttrs != nil {
		activityID = dc.decision.RequestCancelActivityTaskAttrs.ActivityID
	}
	b.appendHistoryEventLocked(dc.domain, dc.workflowID, "ActivityTaskCancelRequested", map[string]any{
		eventAttrKey("ActivityTaskCancelRequested"): map[string]any{
			attrDTCEventID: dc.decisionTaskCompletedEventID,
			"activityId":   activityID,
		},
	})
}

// handleStartTimerDecision records a StartTimer decision. If timerID is
// already open on this execution, real AWS rejects the decision with a
// StartTimerFailed(TIMER_ID_ALREADY_IN_USE) event instead of starting a
// second timer under the same ID -- confirmed against the real SDK's
// StartTimerFailedCause enum.
func (b *InMemoryBackend) handleStartTimerDecision(dc decisionCtx) {
	attrs := dc.decision.StartTimerAttrs
	if attrs == nil {
		return
	}
	if slices.Contains(dc.exec.OpenTimerIDs, attrs.TimerID) {
		b.appendHistoryEventLocked(dc.domain, dc.workflowID, "StartTimerFailed", map[string]any{
			eventAttrKey("StartTimerFailed"): map[string]any{
				attrDTCEventID: dc.decisionTaskCompletedEventID,
				attrTimerID:    attrs.TimerID,
				attrCause:      "TIMER_ID_ALREADY_IN_USE",
			},
		})

		return
	}
	dc.exec.OpenTimerIDs = append(dc.exec.OpenTimerIDs, attrs.TimerID)
	b.appendHistoryEventLocked(dc.domain, dc.workflowID, "TimerStarted", map[string]any{
		eventAttrKey("TimerStarted"): map[string]any{
			attrDTCEventID:       dc.decisionTaskCompletedEventID,
			attrTimerID:          attrs.TimerID,
			"startToFireTimeout": attrs.StartToFireTimeout,
		},
	})
}

// handleCancelTimerDecision records a CancelTimer decision. If timerID isn't
// currently open on this execution, real AWS rejects the decision with a
// CancelTimerFailed(TIMER_ID_UNKNOWN) event -- confirmed against the real
// SDK's CancelTimerFailedCause enum.
func (b *InMemoryBackend) handleCancelTimerDecision(dc decisionCtx) {
	attrs := dc.decision.CancelTimerAttrs
	if attrs == nil {
		return
	}
	idx := slices.Index(dc.exec.OpenTimerIDs, attrs.TimerID)
	if idx == -1 {
		b.appendHistoryEventLocked(dc.domain, dc.workflowID, "CancelTimerFailed", map[string]any{
			eventAttrKey("CancelTimerFailed"): map[string]any{
				attrDTCEventID: dc.decisionTaskCompletedEventID,
				attrTimerID:    attrs.TimerID,
				attrCause:      "TIMER_ID_UNKNOWN",
			},
		})

		return
	}
	dc.exec.OpenTimerIDs = slices.Delete(dc.exec.OpenTimerIDs, idx, idx+1)
	b.appendHistoryEventLocked(dc.domain, dc.workflowID, "TimerCanceled", map[string]any{
		eventAttrKey("TimerCanceled"): map[string]any{
			attrDTCEventID: dc.decisionTaskCompletedEventID,
			attrTimerID:    attrs.TimerID,
		},
	})
}

func (b *InMemoryBackend) handleRecordMarkerDecision(dc decisionCtx) {
	markerName, details := "", ""
	if dc.decision.RecordMarkerAttrs != nil {
		markerName = dc.decision.RecordMarkerAttrs.MarkerName
		details = dc.decision.RecordMarkerAttrs.Details
	}
	b.appendHistoryEventLocked(dc.domain, dc.workflowID, "MarkerRecorded", map[string]any{
		eventAttrKey("MarkerRecorded"): map[string]any{
			attrDTCEventID: dc.decisionTaskCompletedEventID,
			"markerName":   markerName,
			"details":      details,
		},
	})
}
