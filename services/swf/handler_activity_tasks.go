package swf

import "context"

// --- CountPendingActivityTasks ---

type pendingTaskCountOutput struct {
	Count     int  `json:"count"`
	Truncated bool `json:"truncated"`
}

type handleCountPendingActivityTasksInput struct {
	Domain   string      `json:"domain"`
	TaskList taskListRef `json:"taskList"`
}

func (h *Handler) handleCountPendingActivityTasks(
	_ context.Context,
	in *handleCountPendingActivityTasksInput,
) (*pendingTaskCountOutput, error) {
	count := h.Backend.CountPendingActivityTasks(in.Domain, in.TaskList.Name)

	return &pendingTaskCountOutput{Count: count}, nil
}

// --- PollForActivityTask ---

type handlePollForActivityTaskInput struct {
	Domain   string      `json:"domain"`
	TaskList taskListRef `json:"taskList"`
	Identity string      `json:"identity,omitempty"`
}

type pollForActivityTaskOutput struct {
	WorkflowExecution *workflowExecutionRef     `json:"workflowExecution,omitempty"`
	ActivityType      *ActivityTaskActivityType `json:"activityType,omitempty"`
	TaskToken         string                    `json:"taskToken,omitempty"`
	ActivityID        string                    `json:"activityId,omitempty"`
	Input             string                    `json:"input,omitempty"`
	StartedEventID    int64                     `json:"startedEventId,omitempty"`
	ScheduledEventID  int64                     `json:"scheduledEventId,omitempty"`
}

func (h *Handler) handlePollForActivityTask(
	_ context.Context,
	in *handlePollForActivityTaskInput,
) (*pollForActivityTaskOutput, error) {
	task := h.Backend.PollForActivityTask(in.Domain, in.TaskList.Name)
	if task == nil {
		return &pollForActivityTaskOutput{}, nil
	}
	out := &pollForActivityTaskOutput{
		TaskToken:        task.TaskToken,
		ActivityID:       task.ActivityID,
		Input:            task.Input,
		StartedEventID:   task.StartedEventID,
		ScheduledEventID: task.ScheduledEventID,
	}
	if task.ActivityType.Name != "" {
		out.ActivityType = &task.ActivityType
	}
	if task.WorkflowID != "" {
		out.WorkflowExecution = &workflowExecutionRef{WorkflowID: task.WorkflowID, RunID: task.RunID}
	}

	return out, nil
}

// --- RecordActivityTaskHeartbeat ---

type handleRecordActivityTaskHeartbeatInput struct {
	TaskToken string `json:"taskToken"`
	Details   string `json:"details,omitempty"`
}

type recordActivityTaskHeartbeatOutput struct {
	CancelRequested bool `json:"cancelRequested"`
}

func (h *Handler) handleRecordActivityTaskHeartbeat(
	_ context.Context,
	in *handleRecordActivityTaskHeartbeatInput,
) (*recordActivityTaskHeartbeatOutput, error) {
	cancelRequested, err := h.Backend.RecordActivityTaskHeartbeat(in.TaskToken)
	if err != nil {
		// Unknown token: per AWS, return cancelRequested:false rather than error
		// for heartbeats on tokens that may have expired.
		//nolint:nilerr // AWS returns false for unknown tokens, not an error
		return &recordActivityTaskHeartbeatOutput{CancelRequested: false}, nil
	}

	return &recordActivityTaskHeartbeatOutput{CancelRequested: cancelRequested}, nil
}

// --- RespondActivityTaskCanceled ---

type handleRespondActivityTaskCanceledInput struct {
	TaskToken string `json:"taskToken"`
	Details   string `json:"details,omitempty"`
}

type respondActivityTaskCanceledOutput struct{}

func (h *Handler) handleRespondActivityTaskCanceled(
	_ context.Context,
	in *handleRespondActivityTaskCanceledInput,
) (*respondActivityTaskCanceledOutput, error) {
	if err := h.Backend.RespondActivityTaskCanceled(in.TaskToken, in.Details); err != nil {
		return nil, err
	}

	return &respondActivityTaskCanceledOutput{}, nil
}

// --- RespondActivityTaskCompleted ---

type handleRespondActivityTaskCompletedInput struct {
	TaskToken string `json:"taskToken"`
	Result    string `json:"result,omitempty"`
}

type respondActivityTaskCompletedOutput struct{}

func (h *Handler) handleRespondActivityTaskCompleted(
	_ context.Context,
	in *handleRespondActivityTaskCompletedInput,
) (*respondActivityTaskCompletedOutput, error) {
	if err := h.Backend.RespondActivityTaskCompleted(in.TaskToken, in.Result); err != nil {
		return nil, err
	}

	return &respondActivityTaskCompletedOutput{}, nil
}

// --- RespondActivityTaskFailed ---

type handleRespondActivityTaskFailedInput struct {
	TaskToken string `json:"taskToken"`
	Reason    string `json:"reason,omitempty"`
	Details   string `json:"details,omitempty"`
}

type respondActivityTaskFailedOutput struct{}

func (h *Handler) handleRespondActivityTaskFailed(
	_ context.Context,
	in *handleRespondActivityTaskFailedInput,
) (*respondActivityTaskFailedOutput, error) {
	if err := h.Backend.RespondActivityTaskFailed(in.TaskToken, in.Reason, in.Details); err != nil {
		return nil, err
	}

	return &respondActivityTaskFailedOutput{}, nil
}
