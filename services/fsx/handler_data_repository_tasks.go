package fsx

import "context"

// --- CancelDataRepositoryTask ---

type cancelDataRepositoryTaskInput struct {
	TaskID string `json:"TaskId"`
}

type cancelDataRepositoryTaskOutput struct {
	TaskID    string `json:"TaskId"`
	Lifecycle string `json:"Lifecycle"`
}

func (h *Handler) handleCancelDataRepositoryTask(
	_ context.Context,
	in *cancelDataRepositoryTaskInput,
) (*cancelDataRepositoryTaskOutput, error) {
	if err := h.Backend.CancelDataRepositoryTask(in.TaskID); err != nil {
		return nil, err
	}

	return &cancelDataRepositoryTaskOutput{TaskID: in.TaskID, Lifecycle: "CANCELING"}, nil
}

// --- CreateDataRepositoryTask ---

type createDataRepositoryTaskOutput struct {
	DataRepositoryTask *DataRepositoryTask `json:"DataRepositoryTask"`
}

func (h *Handler) handleCreateDataRepositoryTask(
	_ context.Context,
	in *createDataRepositoryTaskInput,
) (*createDataRepositoryTaskOutput, error) {
	t, err := h.Backend.CreateDataRepositoryTask(in)
	if err != nil {
		return nil, err
	}

	return &createDataRepositoryTaskOutput{DataRepositoryTask: t}, nil
}

// --- DescribeDataRepositoryTasks ---

type describeDataRepositoryTasksInput struct {
	NextToken  string   `json:"NextToken,omitempty"`
	TaskIDs    []string `json:"TaskIds,omitempty"`
	MaxResults int32    `json:"MaxResults,omitempty"`
}

type describeDataRepositoryTasksOutput struct {
	NextToken           string                `json:"NextToken,omitempty"`
	DataRepositoryTasks []*DataRepositoryTask `json:"DataRepositoryTasks"`
}

func (h *Handler) handleDescribeDataRepositoryTasks(
	_ context.Context,
	in *describeDataRepositoryTasksInput,
) (*describeDataRepositoryTasksOutput, error) {
	tasks, next, err := h.Backend.DescribeDataRepositoryTasks(in.TaskIDs, in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	return &describeDataRepositoryTasksOutput{DataRepositoryTasks: tasks, NextToken: next}, nil
}
