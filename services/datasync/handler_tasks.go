package datasync

import (
	"context"
	"fmt"
)

// --- Task operations ---

type createTaskInput struct {
	SourceLocationArn      string     `json:"SourceLocationArn"`
	DestinationLocationArn string     `json:"DestinationLocationArn"`
	Name                   string     `json:"Name"`
	CloudWatchLogGroupArn  string     `json:"CloudWatchLogGroupArn,omitempty"`
	Tags                   []tagInput `json:"Tags"`
}

type createTaskOutput struct {
	TaskArn string `json:"TaskArn"`
}

func (h *Handler) handleCreateTask(_ context.Context, in *createTaskInput) (*createTaskOutput, error) {
	if in.SourceLocationArn == "" {
		return nil, fmt.Errorf("%w: SourceLocationArn is required", errInvalidRequest)
	}

	if in.DestinationLocationArn == "" {
		return nil, fmt.Errorf("%w: DestinationLocationArn is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)

	t, err := h.Backend.CreateTask(
		in.SourceLocationArn,
		in.DestinationLocationArn,
		in.Name,
		in.CloudWatchLogGroupArn,
		tags,
	)
	if err != nil {
		return nil, err
	}

	return &createTaskOutput{TaskArn: t.TaskArn}, nil
}

type describeTaskInput struct {
	TaskArn string `json:"TaskArn"`
}

type describeTaskOutput struct {
	TaskArn                 string `json:"TaskArn"`
	Name                    string `json:"Name"`
	Status                  string `json:"Status"`
	SourceLocationArn       string `json:"SourceLocationArn"`
	DestinationLocationArn  string `json:"DestinationLocationArn"`
	CloudWatchLogGroupArn   string `json:"CloudWatchLogGroupArn,omitempty"`
	CurrentTaskExecutionArn string `json:"CurrentTaskExecutionArn,omitempty"`
	CreationTime            int64  `json:"CreationTime"`
}

func (h *Handler) handleDescribeTask(_ context.Context, in *describeTaskInput) (*describeTaskOutput, error) {
	if in.TaskArn == "" {
		return nil, fmt.Errorf("%w: TaskArn is required", errInvalidRequest)
	}

	t, err := h.Backend.DescribeTask(in.TaskArn)
	if err != nil {
		return nil, err
	}

	return &describeTaskOutput{
		TaskArn:                 t.TaskArn,
		Name:                    t.Name,
		Status:                  t.Status,
		SourceLocationArn:       t.SourceLocationArn,
		DestinationLocationArn:  t.DestinationLocationArn,
		CloudWatchLogGroupArn:   t.CloudWatchLogGroupArn,
		CurrentTaskExecutionArn: t.CurrentTaskExecutionArn,
		CreationTime:            t.CreationTime.Unix(),
	}, nil
}

type updateTaskInput struct {
	TaskArn               string `json:"TaskArn"`
	Name                  string `json:"Name,omitempty"`
	CloudWatchLogGroupArn string `json:"CloudWatchLogGroupArn,omitempty"`
}

type updateTaskOutput struct{}

func (h *Handler) handleUpdateTask(_ context.Context, in *updateTaskInput) (*updateTaskOutput, error) {
	if in.TaskArn == "" {
		return nil, fmt.Errorf("%w: TaskArn is required", errInvalidRequest)
	}

	if err := h.Backend.UpdateTask(in.TaskArn, in.Name, in.CloudWatchLogGroupArn); err != nil {
		return nil, err
	}

	return &updateTaskOutput{}, nil
}

type deleteTaskInput struct {
	TaskArn string `json:"TaskArn"`
}

type deleteTaskOutput struct{}

func (h *Handler) handleDeleteTask(_ context.Context, in *deleteTaskInput) (*deleteTaskOutput, error) {
	if in.TaskArn == "" {
		return nil, fmt.Errorf("%w: TaskArn is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteTask(in.TaskArn); err != nil {
		return nil, err
	}

	return &deleteTaskOutput{}, nil
}

type listTasksInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type taskListEntryOutput struct {
	TaskArn string `json:"TaskArn"`
	Name    string `json:"Name"`
	Status  string `json:"Status"`
}

type listTasksOutput struct {
	NextToken string                `json:"NextToken,omitempty"`
	Tasks     []taskListEntryOutput `json:"Tasks"`
}

func (h *Handler) handleListTasks(_ context.Context, in *listTasksInput) (*listTasksOutput, error) {
	tasks, nextToken, err := h.Backend.ListTasks(in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]taskListEntryOutput, 0, len(tasks))
	for _, t := range tasks {
		out = append(out, taskListEntryOutput{
			TaskArn: t.TaskArn,
			Name:    t.Name,
			Status:  t.Status,
		})
	}

	return &listTasksOutput{Tasks: out, NextToken: nextToken}, nil
}

// --- Task execution operations ---

type startTaskExecutionInput struct {
	TaskArn string `json:"TaskArn"`
}

type startTaskExecutionOutput struct {
	TaskExecutionArn string `json:"TaskExecutionArn"`
}

func (h *Handler) handleStartTaskExecution(
	_ context.Context,
	in *startTaskExecutionInput,
) (*startTaskExecutionOutput, error) {
	if in.TaskArn == "" {
		return nil, fmt.Errorf("%w: TaskArn is required", errInvalidRequest)
	}

	e, err := h.Backend.StartTaskExecution(in.TaskArn)
	if err != nil {
		return nil, err
	}

	return &startTaskExecutionOutput{TaskExecutionArn: e.TaskExecutionArn}, nil
}

type cancelTaskExecutionInput struct {
	TaskExecutionArn string `json:"TaskExecutionArn"`
}

type cancelTaskExecutionOutput struct{}

func (h *Handler) handleCancelTaskExecution(
	_ context.Context,
	in *cancelTaskExecutionInput,
) (*cancelTaskExecutionOutput, error) {
	if in.TaskExecutionArn == "" {
		return nil, fmt.Errorf("%w: TaskExecutionArn is required", errInvalidRequest)
	}

	if err := h.Backend.CancelTaskExecution(in.TaskExecutionArn); err != nil {
		return nil, err
	}

	return &cancelTaskExecutionOutput{}, nil
}

type describeTaskExecutionInput struct {
	TaskExecutionArn string `json:"TaskExecutionArn"`
}

type describeTaskExecutionOutput struct {
	Options                  map[string]any `json:"Options,omitempty"`
	TaskExecutionArn         string         `json:"TaskExecutionArn"`
	Status                   string         `json:"Status"`
	StartTime                int64          `json:"StartTime"`
	EstimatedFilesToTransfer int64          `json:"EstimatedFilesToTransfer"`
	EstimatedBytesToTransfer int64          `json:"EstimatedBytesToTransfer"`
	FilesTransferred         int64          `json:"FilesTransferred"`
	BytesTransferred         int64          `json:"BytesTransferred"`
}

func (h *Handler) handleDescribeTaskExecution(
	_ context.Context,
	in *describeTaskExecutionInput,
) (*describeTaskExecutionOutput, error) {
	if in.TaskExecutionArn == "" {
		return nil, fmt.Errorf("%w: TaskExecutionArn is required", errInvalidRequest)
	}

	e, err := h.Backend.DescribeTaskExecution(in.TaskExecutionArn)
	if err != nil {
		return nil, err
	}

	return &describeTaskExecutionOutput{
		TaskExecutionArn:         e.TaskExecutionArn,
		Status:                   e.Status,
		StartTime:                e.StartTime.Unix(),
		Options:                  e.Options,
		EstimatedFilesToTransfer: e.EstimatedFilesToTransfer,
		EstimatedBytesToTransfer: e.EstimatedBytesToTransfer,
		FilesTransferred:         e.FilesTransferred,
		BytesTransferred:         e.BytesTransferred,
	}, nil
}

type listTaskExecutionsInput struct {
	TaskArn    string `json:"TaskArn"`
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type taskExecutionListEntryOutput struct {
	TaskExecutionArn string `json:"TaskExecutionArn"`
	Status           string `json:"Status"`
}

type listTaskExecutionsOutput struct {
	NextToken      string                         `json:"NextToken,omitempty"`
	TaskExecutions []taskExecutionListEntryOutput `json:"TaskExecutions"`
}

func (h *Handler) handleListTaskExecutions(
	_ context.Context,
	in *listTaskExecutionsInput,
) (*listTaskExecutionsOutput, error) {
	executions, nextToken, err := h.Backend.ListTaskExecutions(in.TaskArn, in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]taskExecutionListEntryOutput, 0, len(executions))
	for _, e := range executions {
		out = append(out, taskExecutionListEntryOutput{
			TaskExecutionArn: e.TaskExecutionArn,
			Status:           e.Status,
		})
	}

	return &listTaskExecutionsOutput{TaskExecutions: out, NextToken: nextToken}, nil
}

// --- UpdateTaskExecution ---

type updateTaskExecutionInput struct {
	Options          map[string]any `json:"Options"`
	TaskExecutionArn string         `json:"TaskExecutionArn"`
}

type updateTaskExecutionOutput struct{}

func (h *Handler) handleUpdateTaskExecution(
	_ context.Context,
	in *updateTaskExecutionInput,
) (*updateTaskExecutionOutput, error) {
	if in.TaskExecutionArn == "" {
		return nil, fmt.Errorf("%w: TaskExecutionArn is required", errInvalidRequest)
	}

	// AWS requires the Options member on UpdateTaskExecution.
	if len(in.Options) == 0 {
		return nil, fmt.Errorf("%w: Options is required", errInvalidRequest)
	}

	if err := h.Backend.UpdateTaskExecution(in.TaskExecutionArn, in.Options); err != nil {
		return nil, err
	}

	return &updateTaskExecutionOutput{}, nil
}
