package swf

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// --- Execution counts ---

type workflowExecutionCountOutput struct {
	Count     int  `json:"count"`
	Truncated bool `json:"truncated"`
}

type timeFilter struct {
	OldestDate *float64 `json:"oldestDate,omitempty"`
	LatestDate *float64 `json:"latestDate,omitempty"`
}

type executionFilterInput struct {
	WorkflowID string `json:"workflowId,omitempty"`
}

type typeFilterInput struct {
	Name    string `json:"name,omitempty"`
	Version string `json:"version,omitempty"`
}

type closeStatusFilterInput struct {
	Status string `json:"status,omitempty"`
}

type tagFilterInput struct {
	Tag string `json:"tag,omitempty"`
}

func buildExecutionFilter(
	execFilter *executionFilterInput,
	typeFilter *typeFilterInput,
	tagFilter *tagFilterInput,
	closeStatusFilter *closeStatusFilterInput,
	startTimeFilter *timeFilter,
	closeTimeFilter *timeFilter,
) ExecutionFilter {
	f := ExecutionFilter{}
	if execFilter != nil {
		f.WorkflowID = execFilter.WorkflowID
	}
	if typeFilter != nil {
		f.WorkflowTypeName = typeFilter.Name
		f.WorkflowTypeVersion = typeFilter.Version
	}
	if tagFilter != nil {
		f.Tag = tagFilter.Tag
	}
	if closeStatusFilter != nil {
		f.CloseStatus = closeStatusFilter.Status
	}
	if startTimeFilter != nil {
		if startTimeFilter.OldestDate != nil {
			t := time.Unix(int64(*startTimeFilter.OldestDate), 0)
			f.OldestDate = &t
		}
		if startTimeFilter.LatestDate != nil {
			t := time.Unix(int64(*startTimeFilter.LatestDate), 0)
			f.LatestDate = &t
		}
	}
	if closeTimeFilter != nil {
		if closeTimeFilter.OldestDate != nil {
			t := time.Unix(int64(*closeTimeFilter.OldestDate), 0)
			f.CloseOldestDate = &t
		}
		if closeTimeFilter.LatestDate != nil {
			t := time.Unix(int64(*closeTimeFilter.LatestDate), 0)
			f.CloseLatestDate = &t
		}
	}

	return f
}

type handleCountOpenWorkflowExecutionsInput struct {
	StartTimeFilter *timeFilter           `json:"startTimeFilter,omitempty"`
	ExecutionFilter *executionFilterInput `json:"executionFilter,omitempty"`
	TypeFilter      *typeFilterInput      `json:"typeFilter,omitempty"`
	TagFilter       *tagFilterInput       `json:"tagFilter,omitempty"`
	Domain          string                `json:"domain"`
}

func (h *Handler) handleCountOpenWorkflowExecutions(
	_ context.Context,
	in *handleCountOpenWorkflowExecutionsInput,
) (*workflowExecutionCountOutput, error) {
	f := buildExecutionFilter(in.ExecutionFilter, in.TypeFilter, in.TagFilter, nil, in.StartTimeFilter, nil)
	count := h.Backend.CountOpenWorkflowExecutions(in.Domain, f)

	return &workflowExecutionCountOutput{Count: count}, nil
}

type handleCountClosedWorkflowExecutionsInput struct {
	StartTimeFilter   *timeFilter             `json:"startTimeFilter,omitempty"`
	CloseTimeFilter   *timeFilter             `json:"closeTimeFilter,omitempty"`
	ExecutionFilter   *executionFilterInput   `json:"executionFilter,omitempty"`
	TypeFilter        *typeFilterInput        `json:"typeFilter,omitempty"`
	TagFilter         *tagFilterInput         `json:"tagFilter,omitempty"`
	CloseStatusFilter *closeStatusFilterInput `json:"closeStatusFilter,omitempty"`
	Domain            string                  `json:"domain"`
}

func (h *Handler) handleCountClosedWorkflowExecutions(
	_ context.Context,
	in *handleCountClosedWorkflowExecutionsInput,
) (*workflowExecutionCountOutput, error) {
	f := buildExecutionFilter(
		in.ExecutionFilter,
		in.TypeFilter,
		in.TagFilter,
		in.CloseStatusFilter,
		in.StartTimeFilter,
		in.CloseTimeFilter,
	)
	count := h.Backend.CountClosedWorkflowExecutions(in.Domain, f)

	return &workflowExecutionCountOutput{Count: count}, nil
}

// --- StartWorkflowExecution ---

type startWorkflowExecutionOutput struct {
	RunID string `json:"runId"`
}

type workflowTypeRefInput struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type handleStartWorkflowExecutionInput struct {
	TaskList                     *taskListInput       `json:"taskList,omitempty"`
	WorkflowType                 workflowTypeRefInput `json:"workflowType"`
	Domain                       string               `json:"domain"`
	WorkflowID                   string               `json:"workflowId"`
	Input                        string               `json:"input,omitempty"`
	ChildPolicy                  string               `json:"childPolicy,omitempty"`
	LambdaRole                   string               `json:"lambdaRole,omitempty"`
	ExecutionStartToCloseTimeout string               `json:"executionStartToCloseTimeout,omitempty"`
	TaskStartToCloseTimeout      string               `json:"taskStartToCloseTimeout,omitempty"`
	TaskPriority                 string               `json:"taskPriority,omitempty"`
	TagList                      []string             `json:"tagList,omitempty"`
}

func (h *Handler) handleStartWorkflowExecution(
	_ context.Context,
	in *handleStartWorkflowExecutionInput,
) (*startWorkflowExecutionOutput, error) {
	taskList := ""
	if in.TaskList != nil {
		taskList = in.TaskList.Name
	}
	input := StartWorkflowExecutionInput{
		Domain:                       in.Domain,
		WorkflowID:                   in.WorkflowID,
		RunID:                        uuid.New().String(),
		WorkflowTypeName:             in.WorkflowType.Name,
		WorkflowTypeVersion:          in.WorkflowType.Version,
		TaskList:                     taskList,
		Input:                        in.Input,
		TagList:                      in.TagList,
		ChildPolicy:                  in.ChildPolicy,
		LambdaRole:                   in.LambdaRole,
		ExecutionStartToCloseTimeout: in.ExecutionStartToCloseTimeout,
		TaskStartToCloseTimeout:      in.TaskStartToCloseTimeout,
		TaskPriority:                 in.TaskPriority,
	}
	exec, err := h.Backend.StartWorkflowExecution(input)
	if err != nil {
		return nil, err
	}

	return &startWorkflowExecutionOutput{RunID: exec.RunID}, nil
}

// --- DescribeWorkflowExecution ---

type workflowExecutionRef struct {
	WorkflowID string `json:"workflowId"`
	RunID      string `json:"runId"`
}

type executionInfoOutput struct {
	WorkflowType    *workflowTypeRef      `json:"workflowType,omitempty"`
	Parent          *workflowExecutionRef `json:"parent,omitempty"`
	Execution       workflowExecutionRef  `json:"execution"`
	ExecutionStatus string                `json:"executionStatus"`
	CloseStatus     string                `json:"closeStatus,omitempty"`
	TagList         []string              `json:"tagList,omitempty"`
	StartTimestamp  float64               `json:"startTimestamp"`
	CloseTimestamp  float64               `json:"closeTimestamp,omitempty"`
	CancelRequested bool                  `json:"cancelRequested,omitempty"`
}

type executionConfigOutput struct {
	TaskList                     *taskListRef `json:"taskList,omitempty"`
	ExecutionStartToCloseTimeout string       `json:"executionStartToCloseTimeout,omitempty"`
	TaskStartToCloseTimeout      string       `json:"taskStartToCloseTimeout,omitempty"`
	ChildPolicy                  string       `json:"childPolicy,omitempty"`
	LambdaRole                   string       `json:"lambdaRole,omitempty"`
	TaskPriority                 string       `json:"taskPriority,omitempty"`
}

type openCountsOutput struct {
	OpenActivityTasks           int `json:"openActivityTasks"`
	OpenDecisionTasks           int `json:"openDecisionTasks"`
	OpenTimers                  int `json:"openTimers"`
	OpenChildWorkflowExecutions int `json:"openChildWorkflowExecutions"`
	OpenLambdaFunctions         int `json:"openLambdaFunctions"`
}

type describeWorkflowExecutionOutput struct {
	ExecutionConfiguration      executionConfigOutput `json:"executionConfiguration"`
	LatestExecutionContext      string                `json:"latestExecutionContext,omitempty"`
	ExecutionInfo               executionInfoOutput   `json:"executionInfo"`
	OpenCounts                  openCountsOutput      `json:"openCounts"`
	LatestActivityTaskTimestamp float64               `json:"latestActivityTaskTimestamp,omitempty"`
}

type handleDescribeWorkflowExecutionInput struct {
	Domain    string               `json:"domain"`
	Execution workflowExecutionRef `json:"execution"`
}

func (h *Handler) handleDescribeWorkflowExecution(
	_ context.Context,
	in *handleDescribeWorkflowExecutionInput,
) (*describeWorkflowExecutionOutput, error) {
	exec, err := h.Backend.DescribeWorkflowExecution(in.Domain, in.Execution.WorkflowID)
	if err != nil {
		return nil, err
	}

	info := executionInfoOutput{
		Execution:       workflowExecutionRef{WorkflowID: exec.WorkflowID, RunID: exec.RunID},
		StartTimestamp:  exec.StartTimestamp,
		CloseTimestamp:  exec.CloseTimestamp,
		ExecutionStatus: execStatusToAPIStatus(exec.Status),
		CloseStatus:     exec.CloseStatus,
		TagList:         exec.TagList,
		CancelRequested: exec.CancelRequested,
	}
	if exec.WorkflowTypeName != "" {
		info.WorkflowType = &workflowTypeRef{Name: exec.WorkflowTypeName, Version: exec.WorkflowTypeVersion}
	}
	if exec.ParentWorkflowID != "" {
		info.Parent = &workflowExecutionRef{WorkflowID: exec.ParentWorkflowID, RunID: exec.ParentRunID}
	}

	cfg := executionConfigOutput{
		ExecutionStartToCloseTimeout: exec.ExecutionStartToCloseTimeout,
		TaskStartToCloseTimeout:      exec.TaskStartToCloseTimeout,
		ChildPolicy:                  exec.ChildPolicy,
		LambdaRole:                   exec.LambdaRole,
		TaskPriority:                 exec.TaskPriority,
	}
	if exec.TaskList != "" {
		cfg.TaskList = &taskListRef{Name: exec.TaskList}
	}

	// Retrieve open counts from the backend.
	b, ok := h.Backend.(*InMemoryBackend)
	var counts openCountsOutput
	if ok {
		b.mu.RLock("openCounts")
		c := b.openCountsLocked(in.Domain, in.Execution.WorkflowID)
		b.mu.RUnlock()
		counts = openCountsOutput{
			OpenActivityTasks:           c["openActivityTasks"],
			OpenDecisionTasks:           c["openDecisionTasks"],
			OpenTimers:                  c["openTimers"],
			OpenChildWorkflowExecutions: c["openChildWorkflowExecutions"],
			OpenLambdaFunctions:         0,
		}
	}

	return &describeWorkflowExecutionOutput{
		ExecutionInfo:          info,
		ExecutionConfiguration: cfg,
		OpenCounts:             counts,
		LatestExecutionContext: exec.LatestExecutionContext,
	}, nil
}

// --- TerminateWorkflowExecution ---

type terminateWorkflowExecutionOutput struct{}

type handleTerminateWorkflowExecutionInput struct {
	Domain      string `json:"domain"`
	WorkflowID  string `json:"workflowId"`
	RunID       string `json:"runId,omitempty"`
	Reason      string `json:"reason,omitempty"`
	Details     string `json:"details,omitempty"`
	ChildPolicy string `json:"childPolicy,omitempty"`
}

func (h *Handler) handleTerminateWorkflowExecution(
	_ context.Context,
	in *handleTerminateWorkflowExecutionInput,
) (*terminateWorkflowExecutionOutput, error) {
	if err := h.Backend.TerminateWorkflowExecution(
		in.Domain,
		in.WorkflowID,
		in.RunID,
		in.Reason,
		in.Details,
		in.ChildPolicy,
	); err != nil {
		return nil, err
	}

	return &terminateWorkflowExecutionOutput{}, nil
}

// --- ListOpenWorkflowExecutions ---

type listWorkflowExecutionsOutput struct {
	NextPageToken  string                `json:"nextPageToken,omitempty"`
	ExecutionInfos []executionInfoOutput `json:"executionInfos"`
}

type handleListOpenWorkflowExecutionsInput struct {
	Domain          string                `json:"domain"`
	StartTimeFilter *timeFilter           `json:"startTimeFilter,omitempty"`
	ExecutionFilter *executionFilterInput `json:"executionFilter,omitempty"`
	TypeFilter      *typeFilterInput      `json:"typeFilter,omitempty"`
	TagFilter       *tagFilterInput       `json:"tagFilter,omitempty"`
	NextPageToken   string                `json:"nextPageToken,omitempty"`
	MaximumPageSize int                   `json:"maximumPageSize,omitempty"`
}

// execStatusToAPIStatus converts an internal execution status to the AWS API status.
// AWS only uses "OPEN" or "CLOSED" for executionStatus.
func execStatusToAPIStatus(internalStatus string) string {
	if internalStatus == statusRunning {
		return "OPEN"
	}

	return "CLOSED"
}

func executionToInfo(e WorkflowExecution) executionInfoOutput {
	info := executionInfoOutput{
		Execution:       workflowExecutionRef{WorkflowID: e.WorkflowID, RunID: e.RunID},
		StartTimestamp:  e.StartTimestamp,
		CloseTimestamp:  e.CloseTimestamp,
		ExecutionStatus: execStatusToAPIStatus(e.Status),
		CloseStatus:     e.CloseStatus,
		TagList:         e.TagList,
		CancelRequested: e.CancelRequested,
	}
	if e.WorkflowTypeName != "" {
		info.WorkflowType = &workflowTypeRef{Name: e.WorkflowTypeName, Version: e.WorkflowTypeVersion}
	}
	if e.ParentWorkflowID != "" {
		info.Parent = &workflowExecutionRef{WorkflowID: e.ParentWorkflowID, RunID: e.ParentRunID}
	}

	return info
}

func (h *Handler) handleListOpenWorkflowExecutions(
	_ context.Context,
	in *handleListOpenWorkflowExecutionsInput,
) (*listWorkflowExecutionsOutput, error) {
	f := buildExecutionFilter(in.ExecutionFilter, in.TypeFilter, in.TagFilter, nil, in.StartTimeFilter, nil)
	execs := h.Backend.ListOpenWorkflowExecutions(in.Domain, f)
	infos := make([]executionInfoOutput, len(execs))
	for i, e := range execs {
		infos[i] = executionToInfo(e)
	}
	infos, nextPageToken := applyPageTokenSlice(infos, in.NextPageToken, in.MaximumPageSize)

	return &listWorkflowExecutionsOutput{ExecutionInfos: infos, NextPageToken: nextPageToken}, nil
}

// --- ListClosedWorkflowExecutions ---

type handleListClosedWorkflowExecutionsInput struct {
	Domain            string                  `json:"domain"`
	StartTimeFilter   *timeFilter             `json:"startTimeFilter,omitempty"`
	CloseTimeFilter   *timeFilter             `json:"closeTimeFilter,omitempty"`
	ExecutionFilter   *executionFilterInput   `json:"executionFilter,omitempty"`
	TypeFilter        *typeFilterInput        `json:"typeFilter,omitempty"`
	TagFilter         *tagFilterInput         `json:"tagFilter,omitempty"`
	CloseStatusFilter *closeStatusFilterInput `json:"closeStatusFilter,omitempty"`
	NextPageToken     string                  `json:"nextPageToken,omitempty"`
	MaximumPageSize   int                     `json:"maximumPageSize,omitempty"`
}

func (h *Handler) handleListClosedWorkflowExecutions(
	_ context.Context,
	in *handleListClosedWorkflowExecutionsInput,
) (*listWorkflowExecutionsOutput, error) {
	f := buildExecutionFilter(
		in.ExecutionFilter,
		in.TypeFilter,
		in.TagFilter,
		in.CloseStatusFilter,
		in.StartTimeFilter,
		in.CloseTimeFilter,
	)
	execs := h.Backend.ListClosedWorkflowExecutions(in.Domain, f)
	infos := make([]executionInfoOutput, len(execs))
	for i, e := range execs {
		infos[i] = executionToInfo(e)
	}
	infos, nextPageToken := applyPageTokenSlice(infos, in.NextPageToken, in.MaximumPageSize)

	return &listWorkflowExecutionsOutput{ExecutionInfos: infos, NextPageToken: nextPageToken}, nil
}

// --- RequestCancelWorkflowExecution ---

type handleRequestCancelWorkflowExecutionInput struct {
	Domain     string `json:"domain"`
	WorkflowID string `json:"workflowId"`
	RunID      string `json:"runId,omitempty"`
}

type requestCancelWorkflowExecutionOutput struct{}

func (h *Handler) handleRequestCancelWorkflowExecution(
	_ context.Context,
	in *handleRequestCancelWorkflowExecutionInput,
) (*requestCancelWorkflowExecutionOutput, error) {
	if err := h.Backend.RequestCancelWorkflowExecution(in.Domain, in.WorkflowID, in.RunID); err != nil {
		return nil, err
	}

	return &requestCancelWorkflowExecutionOutput{}, nil
}
