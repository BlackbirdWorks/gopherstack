package swf

import (
	"fmt"

	"github.com/google/uuid"
)

// CountPendingActivityTasks returns the number of pending activity tasks for a task list.
func (b *InMemoryBackend) CountPendingActivityTasks(domain, taskList string) int {
	b.mu.RLock("CountPendingActivityTasks")
	defer b.mu.RUnlock()

	return len(b.activityQueues[domain+":"+taskList])
}

// PollForActivityTask returns the next available activity task for a task list, or nil if none.
func (b *InMemoryBackend) PollForActivityTask(domain, taskList string) *ActivityTask {
	b.mu.Lock("PollForActivityTask")
	defer b.mu.Unlock()

	key := domain + ":" + taskList
	queue := b.activityQueues[key]
	if len(queue) == 0 {
		return nil
	}

	task := *queue[0]
	b.activityQueues[key] = queue[1:]
	token := uuid.New().String()
	task.TaskToken = token

	// Emit ActivityTaskStarted event and record active task.
	startedEventID := b.appendHistoryEventLocked(
		domain, task.WorkflowID, task.RunID, "ActivityTaskStarted",
		map[string]any{
			eventAttrKey("ActivityTaskStarted"): map[string]any{
				attrScheduledEvID: task.ScheduledEventID,
			},
		},
	)
	task.StartedEventID = startedEventID

	b.activeActivityTasks.Put(&activeActivityTaskRecord{
		Domain:           domain,
		WorkflowID:       task.WorkflowID,
		RunID:            task.RunID,
		ActivityID:       task.ActivityID,
		ActivityType:     task.ActivityType,
		ScheduledEventID: task.ScheduledEventID,
		StartedEventID:   startedEventID,
		TaskList:         taskList,
		TaskToken:        token,
	})

	return &task
}

// RecordActivityTaskHeartbeat acknowledges a heartbeat for an activity task token.
// Returns true if cancel has been requested for the workflow; always false in this emulator.
func (b *InMemoryBackend) RecordActivityTaskHeartbeat(taskToken string) (bool, error) {
	b.mu.RLock("RecordActivityTaskHeartbeat")
	defer b.mu.RUnlock()

	rec, ok := b.activeActivityTasks.Get(taskToken)
	if !ok {
		return false, fmt.Errorf("%w: task token %s not found", ErrNotFound, taskToken)
	}

	exec, ok := b.executions.Get(executionKey(rec.Domain, rec.WorkflowID, rec.RunID))
	if !ok {
		return false, nil
	}

	return exec.CancelRequested, nil
}

// RespondActivityTaskCanceled marks an activity task as canceled.
func (b *InMemoryBackend) RespondActivityTaskCanceled(taskToken, details string) error {
	b.mu.Lock("RespondActivityTaskCanceled")
	defer b.mu.Unlock()

	rec, ok := b.activeActivityTasks.Get(taskToken)
	if !ok {
		return fmt.Errorf("%w: task token %s not found", ErrNotFound, taskToken)
	}
	b.activeActivityTasks.Delete(taskToken)

	attrKey := eventAttrKey("ActivityTaskCanceled")
	attrs := map[string]any{
		attrKey: map[string]any{
			"details":         details,
			attrScheduledEvID: rec.ScheduledEventID,
			attrStartedEvID:   rec.StartedEventID,
		},
	}
	b.appendHistoryEventLocked(rec.Domain, rec.WorkflowID, rec.RunID, "ActivityTaskCanceled", attrs)
	b.enqueueDecisionTaskLocked(rec.Domain, rec.WorkflowID, rec.RunID)

	return nil
}

// RespondActivityTaskCompleted marks an activity task as completed.
func (b *InMemoryBackend) RespondActivityTaskCompleted(taskToken, result string) error {
	b.mu.Lock("RespondActivityTaskCompleted")
	defer b.mu.Unlock()

	rec, ok := b.activeActivityTasks.Get(taskToken)
	if !ok {
		return fmt.Errorf("%w: task token %s not found", ErrNotFound, taskToken)
	}
	b.activeActivityTasks.Delete(taskToken)

	attrKey := eventAttrKey("ActivityTaskCompleted")
	attrs := map[string]any{
		attrKey: map[string]any{
			attrResult:        result,
			attrScheduledEvID: rec.ScheduledEventID,
			attrStartedEvID:   rec.StartedEventID,
		},
	}
	b.appendHistoryEventLocked(rec.Domain, rec.WorkflowID, rec.RunID, "ActivityTaskCompleted", attrs)
	b.enqueueDecisionTaskLocked(rec.Domain, rec.WorkflowID, rec.RunID)

	return nil
}

// RespondActivityTaskFailed marks an activity task as failed.
func (b *InMemoryBackend) RespondActivityTaskFailed(taskToken, reason, details string) error {
	b.mu.Lock("RespondActivityTaskFailed")
	defer b.mu.Unlock()

	rec, ok := b.activeActivityTasks.Get(taskToken)
	if !ok {
		return fmt.Errorf("%w: task token %s not found", ErrNotFound, taskToken)
	}
	b.activeActivityTasks.Delete(taskToken)

	attrKey := eventAttrKey("ActivityTaskFailed")
	attrs := map[string]any{
		attrKey: map[string]any{
			attrReason:        reason,
			attrDetails:       details,
			attrScheduledEvID: rec.ScheduledEventID,
			attrStartedEvID:   rec.StartedEventID,
		},
	}
	b.appendHistoryEventLocked(rec.Domain, rec.WorkflowID, rec.RunID, "ActivityTaskFailed", attrs)
	b.enqueueDecisionTaskLocked(rec.Domain, rec.WorkflowID, rec.RunID)

	return nil
}
