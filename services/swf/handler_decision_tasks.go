package swf

import "context"

// --- CountPendingDecisionTasks ---

type handleCountPendingDecisionTasksInput struct {
	Domain   string      `json:"domain"`
	TaskList taskListRef `json:"taskList"`
}

func (h *Handler) handleCountPendingDecisionTasks(
	_ context.Context,
	in *handleCountPendingDecisionTasksInput,
) (*pendingTaskCountOutput, error) {
	count := h.Backend.CountPendingDecisionTasks(in.Domain, in.TaskList.Name)

	return &pendingTaskCountOutput{Count: count}, nil
}

// --- PollForDecisionTask ---

type decisionTaskWorkflowTypeOutput struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type decisionTaskOutput struct {
	WorkflowType           *decisionTaskWorkflowTypeOutput `json:"workflowType,omitempty"`
	WorkflowExecution      *workflowExecutionRef           `json:"workflowExecution,omitempty"`
	TaskToken              string                          `json:"taskToken"`
	NextPageToken          string                          `json:"nextPageToken,omitempty"`
	Events                 []HistoryEvent                  `json:"events"`
	StartedEventID         int64                           `json:"startedEventId"`
	PreviousStartedEventID int64                           `json:"previousStartedEventId"`
}

type handlePollForDecisionTaskInput struct {
	Domain          string      `json:"domain"`
	TaskList        taskListRef `json:"taskList"`
	NextPageToken   string      `json:"nextPageToken,omitempty"`
	Identity        string      `json:"identity,omitempty"`
	MaximumPageSize int         `json:"maximumPageSize,omitempty"`
	ReverseOrder    bool        `json:"reverseOrder,omitempty"`
}

func (h *Handler) handlePollForDecisionTask(
	_ context.Context,
	in *handlePollForDecisionTaskInput,
) (*decisionTaskOutput, error) {
	task := h.Backend.PollForDecisionTask(in.Domain, in.TaskList.Name, in.MaximumPageSize, in.NextPageToken)
	if task == nil {
		return &decisionTaskOutput{}, nil
	}
	out := &decisionTaskOutput{
		TaskToken:              task.TaskToken,
		Events:                 task.Events,
		StartedEventID:         task.StartedEventID,
		PreviousStartedEventID: task.PreviousStartedEventID,
		NextPageToken:          task.NextPageToken,
	}
	if task.WorkflowTypeName != "" {
		out.WorkflowType = &decisionTaskWorkflowTypeOutput{
			Name:    task.WorkflowTypeName,
			Version: task.WorkflowTypeVersion,
		}
	}
	if task.WorkflowID != "" {
		out.WorkflowExecution = &workflowExecutionRef{WorkflowID: task.WorkflowID, RunID: task.RunID}
	}

	return out, nil
}

// --- RespondDecisionTaskCompleted ---

type completeWorkflowDecisionAttrs struct {
	Result string `json:"result,omitempty"`
}

type failWorkflowDecisionAttrs struct {
	Reason  string `json:"reason,omitempty"`
	Details string `json:"details,omitempty"`
}

type cancelWorkflowDecisionAttrs struct {
	Details string `json:"details,omitempty"`
}

type scheduleActivityDecisionAttrs struct {
	ActivityType           activityTypeRef `json:"activityType"`
	ActivityID             string          `json:"activityId"`
	Input                  string          `json:"input,omitempty"`
	TaskList               *taskListRef    `json:"taskList,omitempty"`
	ScheduleToCloseTimeout string          `json:"scheduleToCloseTimeout,omitempty"`
	ScheduleToStartTimeout string          `json:"scheduleToStartTimeout,omitempty"`
	StartToCloseTimeout    string          `json:"startToCloseTimeout,omitempty"`
	HeartbeatTimeout       string          `json:"heartbeatTimeout,omitempty"`
}

type requestCancelActivityDecisionAttrs struct {
	ActivityID string `json:"activityId"`
}

type startTimerDecisionAttrs struct {
	TimerID            string `json:"timerId"`
	StartToFireTimeout string `json:"startToFireTimeout,omitempty"`
}

type cancelTimerDecisionAttrs struct {
	TimerID string `json:"timerId"`
}

type recordMarkerDecisionAttrs struct {
	MarkerName string `json:"markerName"`
	Details    string `json:"details,omitempty"`
}

//nolint:lll // AWS API field names exceed 120 chars; cannot shorten JSON tags
type decisionInput struct {
	CompleteWorkflowExecutionDecisionAttributes *completeWorkflowDecisionAttrs      `json:"completeWorkflowExecutionDecisionAttributes,omitempty"`
	FailWorkflowExecutionDecisionAttributes     *failWorkflowDecisionAttrs          `json:"failWorkflowExecutionDecisionAttributes,omitempty"`
	CancelWorkflowExecutionDecisionAttributes   *cancelWorkflowDecisionAttrs        `json:"cancelWorkflowExecutionDecisionAttributes,omitempty"`
	ScheduleActivityTaskDecisionAttributes      *scheduleActivityDecisionAttrs      `json:"scheduleActivityTaskDecisionAttributes,omitempty"`
	RequestCancelActivityTaskDecisionAttributes *requestCancelActivityDecisionAttrs `json:"requestCancelActivityTaskDecisionAttributes,omitempty"`
	StartTimerDecisionAttributes                *startTimerDecisionAttrs            `json:"startTimerDecisionAttributes,omitempty"`
	CancelTimerDecisionAttributes               *cancelTimerDecisionAttrs           `json:"cancelTimerDecisionAttributes,omitempty"`
	RecordMarkerDecisionAttributes              *recordMarkerDecisionAttrs          `json:"recordMarkerDecisionAttributes,omitempty"`
	DecisionType                                string                              `json:"decisionType"`
}

type handleRespondDecisionTaskCompletedInput struct {
	TaskToken        string          `json:"taskToken"`
	ExecutionContext string          `json:"executionContext,omitempty"`
	Decisions        []decisionInput `json:"decisions,omitempty"`
}

type respondDecisionTaskCompletedOutput struct{}

// convertDecisionCloseAttrs copies the workflow-closing decision attributes
// (Complete/Fail/Cancel/ScheduleActivityTask) from the wire input onto dec.
func convertDecisionCloseAttrs(d decisionInput, dec *Decision) {
	if d.CompleteWorkflowExecutionDecisionAttributes != nil {
		dec.CompleteWorkflowExecutionAttrs = &CompleteWorkflowExecutionDecisionAttrs{
			Result: d.CompleteWorkflowExecutionDecisionAttributes.Result,
		}
	}
	if d.FailWorkflowExecutionDecisionAttributes != nil {
		dec.FailWorkflowExecutionAttrs = &FailWorkflowExecutionDecisionAttrs{
			Reason:  d.FailWorkflowExecutionDecisionAttributes.Reason,
			Details: d.FailWorkflowExecutionDecisionAttributes.Details,
		}
	}
	if d.CancelWorkflowExecutionDecisionAttributes != nil {
		dec.CancelWorkflowExecutionAttrs = &CancelWorkflowExecutionDecisionAttrs{
			Details: d.CancelWorkflowExecutionDecisionAttributes.Details,
		}
	}
	if d.ScheduleActivityTaskDecisionAttributes != nil {
		sa := d.ScheduleActivityTaskDecisionAttributes
		taskList := ""
		if sa.TaskList != nil {
			taskList = sa.TaskList.Name
		}
		dec.ScheduleActivityTaskAttrs = &ScheduleActivityTaskDecisionAttrs{
			ActivityType: ActivityTaskActivityType{
				Name:    sa.ActivityType.Name,
				Version: sa.ActivityType.Version,
			},
			ActivityID:             sa.ActivityID,
			Input:                  sa.Input,
			TaskList:               taskList,
			ScheduleToCloseTimeout: sa.ScheduleToCloseTimeout,
			ScheduleToStartTimeout: sa.ScheduleToStartTimeout,
			StartToCloseTimeout:    sa.StartToCloseTimeout,
			HeartbeatTimeout:       sa.HeartbeatTimeout,
		}
	}
}

// convertDecisionTaskAttrs copies the task/timer/marker decision attributes
// (RequestCancelActivityTask/StartTimer/CancelTimer/RecordMarker) from the
// wire input onto dec.
func convertDecisionTaskAttrs(d decisionInput, dec *Decision) {
	if d.RequestCancelActivityTaskDecisionAttributes != nil {
		dec.RequestCancelActivityTaskAttrs = &RequestCancelActivityTaskDecisionAttrs{
			ActivityID: d.RequestCancelActivityTaskDecisionAttributes.ActivityID,
		}
	}
	if d.StartTimerDecisionAttributes != nil {
		dec.StartTimerAttrs = &StartTimerDecisionAttrs{
			TimerID:            d.StartTimerDecisionAttributes.TimerID,
			StartToFireTimeout: d.StartTimerDecisionAttributes.StartToFireTimeout,
		}
	}
	if d.CancelTimerDecisionAttributes != nil {
		dec.CancelTimerAttrs = &CancelTimerDecisionAttrs{
			TimerID: d.CancelTimerDecisionAttributes.TimerID,
		}
	}
	if d.RecordMarkerDecisionAttributes != nil {
		dec.RecordMarkerAttrs = &RecordMarkerDecisionAttrs{
			MarkerName: d.RecordMarkerDecisionAttributes.MarkerName,
			Details:    d.RecordMarkerDecisionAttributes.Details,
		}
	}
}

func (h *Handler) handleRespondDecisionTaskCompleted(
	_ context.Context,
	in *handleRespondDecisionTaskCompletedInput,
) (*respondDecisionTaskCompletedOutput, error) {
	decisions := make([]Decision, 0, len(in.Decisions))
	for _, d := range in.Decisions {
		dec := Decision{DecisionType: d.DecisionType}
		convertDecisionCloseAttrs(d, &dec)
		convertDecisionTaskAttrs(d, &dec)
		decisions = append(decisions, dec)
	}
	if err := h.Backend.RespondDecisionTaskCompleted(in.TaskToken, in.ExecutionContext, decisions); err != nil {
		return nil, err
	}

	return &respondDecisionTaskCompletedOutput{}, nil
}
