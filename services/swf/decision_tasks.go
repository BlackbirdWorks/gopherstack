package swf

import (
	"fmt"
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

	b.appendHistoryEventLocked(rec.Domain, rec.WorkflowID, "DecisionTaskCompleted", map[string]any{
		eventAttrKey("DecisionTaskCompleted"): map[string]any{
			"executionContext": executionContext,
		},
	})

	for _, d := range decisions {
		b.processDecisionLocked(rec.Domain, rec.WorkflowID, exec, d)
	}

	return nil
}

// processDecisionLocked applies a single decision to an execution.
// Caller must hold the write lock.
//
//nolint:cyclop,funlen // 12 SWF decision types; cannot reduce without artificial splitting
func (b *InMemoryBackend) processDecisionLocked(domain, workflowID string, exec *WorkflowExecution, d Decision) {
	now := float64(time.Now().UnixMilli()) / milliDivisor

	switch d.DecisionType {
	case "CompleteWorkflowExecution":
		result := ""
		if d.CompleteWorkflowExecutionAttrs != nil {
			result = d.CompleteWorkflowExecutionAttrs.Result
		}
		exec.Status = statusCompleted
		exec.CloseStatus = statusCompleted
		exec.CloseTimestamp = now
		b.appendHistoryEventLocked(domain, workflowID, "WorkflowExecutionCompleted", map[string]any{
			eventAttrKey("WorkflowExecutionCompleted"): map[string]any{"result": result},
		})

	case "FailWorkflowExecution":
		reason, details := "", ""
		if d.FailWorkflowExecutionAttrs != nil {
			reason = d.FailWorkflowExecutionAttrs.Reason
			details = d.FailWorkflowExecutionAttrs.Details
		}
		exec.Status = statusFailed
		exec.CloseStatus = statusFailed
		exec.CloseTimestamp = now
		b.appendHistoryEventLocked(domain, workflowID, "WorkflowExecutionFailed", map[string]any{
			eventAttrKey("WorkflowExecutionFailed"): map[string]any{
				attrReason: reason, attrDetails: details,
			},
		})

	case "CancelWorkflowExecution":
		details := ""
		if d.CancelWorkflowExecutionAttrs != nil {
			details = d.CancelWorkflowExecutionAttrs.Details
		}
		exec.Status = statusCanceled
		exec.CloseStatus = statusCanceled
		exec.CloseTimestamp = now
		b.appendHistoryEventLocked(domain, workflowID, "WorkflowExecutionCanceled", map[string]any{
			eventAttrKey("WorkflowExecutionCanceled"): map[string]any{attrDetails: details},
		})

	case "ContinueAsNewWorkflowExecution":
		exec.Status = statusContinuedAsNew
		exec.CloseStatus = statusContinuedAsNew
		exec.CloseTimestamp = now
		b.appendHistoryEventLocked(domain, workflowID, "WorkflowExecutionContinuedAsNew", map[string]any{
			eventAttrKey("WorkflowExecutionContinuedAsNew"): map[string]any{},
		})

	case "ScheduleActivityTask":
		if d.ScheduleActivityTaskAttrs == nil {
			return
		}
		attrs := d.ScheduleActivityTaskAttrs
		taskList := attrs.TaskList
		if taskList == "" {
			taskList = exec.TaskList
		}
		scheduledEventID := b.appendHistoryEventLocked(domain, workflowID, "ActivityTaskScheduled", map[string]any{
			eventAttrKey("ActivityTaskScheduled"): map[string]any{
				"activityType": map[string]any{
					attrName:  attrs.ActivityType.Name,
					"version": attrs.ActivityType.Version,
				},
				"activityId": attrs.ActivityID,
				attrInput:    attrs.Input,
				"taskList":   map[string]any{attrName: taskList},
			},
		})
		qkey := domain + ":" + taskList
		b.activityQueues[qkey] = append(b.activityQueues[qkey], &ActivityTask{
			ActivityID:       attrs.ActivityID,
			ActivityType:     attrs.ActivityType,
			Input:            attrs.Input,
			WorkflowID:       workflowID,
			RunID:            exec.RunID,
			ScheduledEventID: scheduledEventID,
		})

	case "RequestCancelActivityTask":
		activityID := ""
		if d.RequestCancelActivityTaskAttrs != nil {
			activityID = d.RequestCancelActivityTaskAttrs.ActivityID
		}
		b.appendHistoryEventLocked(domain, workflowID, "ActivityTaskCancelRequested", map[string]any{
			eventAttrKey("ActivityTaskCancelRequested"): map[string]any{
				"activityId": activityID,
			},
		})

	case "StartTimer":
		timerID, startToFireTimeout := "", ""
		if d.StartTimerAttrs != nil {
			timerID = d.StartTimerAttrs.TimerID
			startToFireTimeout = d.StartTimerAttrs.StartToFireTimeout
		}
		b.appendHistoryEventLocked(domain, workflowID, "TimerStarted", map[string]any{
			eventAttrKey("TimerStarted"): map[string]any{
				"timerId":            timerID,
				"startToFireTimeout": startToFireTimeout,
			},
		})

	case "CancelTimer":
		timerID := ""
		if d.CancelTimerAttrs != nil {
			timerID = d.CancelTimerAttrs.TimerID
		}
		b.appendHistoryEventLocked(domain, workflowID, "TimerCanceled", map[string]any{
			eventAttrKey("TimerCanceled"): map[string]any{
				"timerId": timerID,
			},
		})

	case "RecordMarker":
		markerName, details := "", ""
		if d.RecordMarkerAttrs != nil {
			markerName = d.RecordMarkerAttrs.MarkerName
			details = d.RecordMarkerAttrs.Details
		}
		b.appendHistoryEventLocked(domain, workflowID, "MarkerRecorded", map[string]any{
			eventAttrKey("MarkerRecorded"): map[string]any{
				"markerName": markerName,
				"details":    details,
			},
		})

	case "StartChildWorkflowExecution":
		b.appendHistoryEventLocked(domain, workflowID, "StartChildWorkflowExecutionInitiated", map[string]any{
			eventAttrKey("StartChildWorkflowExecutionInitiated"): map[string]any{},
		})

	case "SignalExternalWorkflowExecution":
		b.appendHistoryEventLocked(domain, workflowID, "SignalExternalWorkflowExecutionInitiated", map[string]any{
			eventAttrKey("SignalExternalWorkflowExecutionInitiated"): map[string]any{},
		})

	case "RequestCancelExternalWorkflowExecution":
		evType := "RequestCancelExternalWorkflowExecutionInitiated"
		b.appendHistoryEventLocked(domain, workflowID, evType, map[string]any{
			eventAttrKey(evType): map[string]any{},
		})
	}
}
