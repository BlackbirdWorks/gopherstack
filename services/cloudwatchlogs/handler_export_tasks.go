package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"fmt"
)

type cancelExportTaskInput struct {
	TaskID string `json:"taskId"`
}

type cancelExportTaskOutput struct{}

type cancelImportTaskInput struct {
	ImportID string `json:"importId"`
}

type cancelImportTaskOutput struct {
	CreationTime    *int64 `json:"creationTime,omitempty"`
	LastUpdatedTime *int64 `json:"lastUpdatedTime,omitempty"`
	ImportID        string `json:"importId,omitempty"`
	ImportStatus    string `json:"importStatus,omitempty"`
}

type createExportTaskInput struct {
	TaskName            string `json:"taskName"`
	LogGroupName        string `json:"logGroupName"`
	LogStreamNamePrefix string `json:"logStreamNamePrefix"`
	Destination         string `json:"destination"`
	DestinationPrefix   string `json:"destinationPrefix"`
	From                int64  `json:"from"`
	To                  int64  `json:"to"`
}

type createExportTaskOutput struct {
	TaskID string `json:"taskId,omitempty"`
}

type createImportTaskInput struct {
	ImportRoleArn   string `json:"importRoleArn"`
	ImportSourceArn string `json:"importSourceArn"`
}

type createImportTaskOutput struct {
	CreationTime         *int64 `json:"creationTime,omitempty"`
	ImportID             string `json:"importId,omitempty"`
	ImportDestinationArn string `json:"importDestinationArn,omitempty"`
}

// --- DescribeExportTasks ---.
type describeExportTasksInput struct {
	TaskID     string `json:"taskId"`
	StatusCode string `json:"statusCode"`
	NextToken  string `json:"nextToken"`
	Limit      int    `json:"limit"`
}

type describeExportTasksOutput struct {
	NextToken   string       `json:"nextToken,omitempty"`
	ExportTasks []ExportTask `json:"exportTasks"`
}

// --- DescribeImportTasks ---.
type describeImportTasksInput struct {
	TaskID    string `json:"taskId"`
	NextToken string `json:"nextToken"`
	Limit     int    `json:"limit"`
}

type describeImportTasksOutput struct {
	NextToken   string       `json:"nextToken,omitempty"`
	ImportTasks []ImportTask `json:"importTasks"`
}

func (h *Handler) handleCancelExportTask(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input cancelExportTaskInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	if err := h.Backend.CancelExportTask(input.TaskID); err != nil {
		return nil, err
	}

	return &cancelExportTaskOutput{}, nil
}

func (h *Handler) handleCancelImportTask(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input cancelImportTaskInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	task, err := h.Backend.CancelImportTask(input.ImportID)
	if err != nil {
		return nil, err
	}

	return &cancelImportTaskOutput{
		ImportID:        task.ImportID,
		ImportStatus:    task.Status,
		CreationTime:    &task.CreationTime,
		LastUpdatedTime: &task.LastUpdatedTime,
	}, nil
}

func (h *Handler) handleCreateExportTask(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input createExportTaskInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	taskID, err := h.Backend.CreateExportTask(
		input.TaskName, input.LogGroupName, input.LogStreamNamePrefix,
		input.Destination, input.DestinationPrefix, input.From, input.To,
	)
	if err != nil {
		return nil, err
	}

	return &createExportTaskOutput{TaskID: taskID}, nil
}

func (h *Handler) handleCreateImportTask(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input createImportTaskInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	task, err := h.Backend.CreateImportTask(input.ImportRoleArn, input.ImportSourceArn)
	if err != nil {
		return nil, err
	}

	return &createImportTaskOutput{
		ImportID:             task.ImportID,
		ImportDestinationArn: task.ImportDestinationArn,
		CreationTime:         &task.CreationTime,
	}, nil
}

func (h *Handler) handleDescribeExportTasks(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input describeExportTasksInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	tasks, next, err := h.Backend.DescribeExportTasks(input.TaskID, input.StatusCode, input.Limit, input.NextToken)
	if err != nil {
		return nil, err
	}

	return &describeExportTasksOutput{ExportTasks: tasks, NextToken: next}, nil
}

func (h *Handler) handleDescribeImportTasks(
	ctx context.Context, //nolint:revive // existing issue.
	b []byte,
) (any, error) {
	var input describeImportTasksInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	tasks, next, err := h.Backend.DescribeImportTasks(input.TaskID, input.Limit, input.NextToken)
	if err != nil {
		return nil, err
	}

	return &describeImportTasksOutput{ImportTasks: tasks, NextToken: next}, nil
}

// handleDescribeImportTaskBatches validates the request and returns an
// empty-but-valid response. The backend tracks import tasks (DescribeImportTasks)
// but does not model per-task import batches, so this is validation-only: the
// task identifier is required and, when supplied, must reference a known import
// task; otherwise an empty importTaskBatches list is returned.
func (h *Handler) handleDescribeImportTaskBatches(
	ctx context.Context, //nolint:revive // existing issue.
	body []byte,
) (any, error) {
	var input struct {
		TaskID string `json:"taskId"`
	}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &input); err != nil {
			return nil, err
		}
	}

	if input.TaskID == "" {
		return nil, fmt.Errorf("%w: taskId is required", ErrValidation)
	}

	if b := cwlBackend(h); b != nil {
		tasks, _, err := b.DescribeImportTasks(input.TaskID, 1, "")
		if err != nil {
			return nil, err
		}
		if len(tasks) == 0 {
			return nil, fmt.Errorf("%w: import task %s not found", ErrImportTaskNotFound, input.TaskID)
		}
	}

	return map[string]any{"importTaskBatches": []any{}}, nil
}
