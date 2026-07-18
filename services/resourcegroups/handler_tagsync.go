package resourcegroups

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// handleStartTagSyncTask creates a new tag-sync task.
type startTagSyncTaskInput struct {
	ResourceQuery *ResourceQuery `json:"ResourceQuery,omitempty"`
	Group         string         `json:"Group"`
	RoleArn       string         `json:"RoleArn"`
	TagKey        string         `json:"TagKey,omitempty"`
	TagValue      string         `json:"TagValue,omitempty"`
}

type startTagSyncTaskOutput struct {
	ResourceQuery *ResourceQuery `json:"ResourceQuery,omitempty"`
	GroupArn      string         `json:"GroupArn"`
	GroupName     string         `json:"GroupName"`
	RoleArn       string         `json:"RoleArn"`
	TagKey        string         `json:"TagKey,omitempty"`
	TagValue      string         `json:"TagValue,omitempty"`
	TaskArn       string         `json:"TaskArn"`
}

func (h *Handler) handleStartTagSyncTask(
	ctx context.Context,
	in *startTagSyncTaskInput,
) (*startTagSyncTaskOutput, error) {
	if in.Group == "" {
		return nil, fmt.Errorf("%w: Group is required", ErrValidation)
	}

	if in.RoleArn == "" {
		return nil, fmt.Errorf("%w: RoleArn is required", ErrValidation)
	}

	task, err := h.Backend.StartTagSyncTask(ctx, in.Group, in.RoleArn, in.TagKey, in.TagValue, in.ResourceQuery)
	if err != nil {
		return nil, err
	}

	return &startTagSyncTaskOutput{
		GroupArn:      task.GroupArn,
		GroupName:     task.GroupName,
		RoleArn:       task.RoleArn,
		TagKey:        task.TagKey,
		TagValue:      task.TagValue,
		TaskArn:       task.TaskArn,
		ResourceQuery: task.ResourceQuery,
	}, nil
}

// handleCancelTagSyncTask cancels a tag-sync task.
type cancelTagSyncTaskInput struct {
	TaskArn string `json:"TaskArn"`
}

type cancelTagSyncTaskOutput struct{}

func (h *Handler) handleCancelTagSyncTask(
	ctx context.Context,
	in *cancelTagSyncTaskInput,
) (*cancelTagSyncTaskOutput, error) {
	if in.TaskArn == "" {
		return nil, fmt.Errorf("%w: TaskArn is required", ErrValidation)
	}

	if err := h.Backend.CancelTagSyncTask(ctx, in.TaskArn); err != nil {
		return nil, err
	}

	return &cancelTagSyncTaskOutput{}, nil
}

// tagSyncTaskItem is the AWS wire shape shared by GetTagSyncTask's response
// body and each element of ListTagSyncTasks' TagSyncTasks array. CreatedAt is
// serialized as a JSON number of seconds since the Unix epoch (the
// unixTimestamp format used by the rest-json protocol), not an RFC3339/ISO8601
// string -- see pkgs/awstime.
type tagSyncTaskItem struct {
	ResourceQuery *ResourceQuery `json:"ResourceQuery,omitempty"`
	GroupArn      string         `json:"GroupArn"`
	GroupName     string         `json:"GroupName"`
	RoleArn       string         `json:"RoleArn"`
	TagKey        string         `json:"TagKey,omitempty"`
	TagValue      string         `json:"TagValue,omitempty"`
	TaskArn       string         `json:"TaskArn"`
	Status        string         `json:"Status"`
	ErrorMessage  string         `json:"ErrorMessage,omitempty"`
	CreatedAt     float64        `json:"CreatedAt,omitempty"`
}

// tagSyncTaskItemFromTask builds the wire-shaped item from the backend's
// internal representation.
func tagSyncTaskItemFromTask(t *TagSyncTask) tagSyncTaskItem {
	return tagSyncTaskItem{
		TaskArn:       t.TaskArn,
		GroupArn:      t.GroupArn,
		GroupName:     t.GroupName,
		RoleArn:       t.RoleArn,
		TagKey:        t.TagKey,
		TagValue:      t.TagValue,
		ResourceQuery: t.ResourceQuery,
		Status:        t.Status,
		ErrorMessage:  t.ErrorMessage,
		CreatedAt:     awstime.Epoch(t.CreatedAt),
	}
}

// handleGetTagSyncTask returns the details of a tag-sync task.
type getTagSyncTaskInput struct {
	TaskArn string `json:"TaskArn"`
}

func (h *Handler) handleGetTagSyncTask(ctx context.Context, in *getTagSyncTaskInput) (*tagSyncTaskItem, error) {
	if in.TaskArn == "" {
		return nil, fmt.Errorf("%w: TaskArn is required", ErrValidation)
	}

	task, err := h.Backend.GetTagSyncTask(ctx, in.TaskArn)
	if err != nil {
		return nil, err
	}

	out := tagSyncTaskItemFromTask(task)

	return &out, nil
}

// handleListTagSyncTasks lists tag-sync tasks.
type listTagSyncTasksInput struct { //nolint:govet // fieldalignment: readability over micro-optimization
	Filters    []ListTagSyncTasksFilter `json:"Filters,omitempty"`
	NextToken  string                   `json:"NextToken"`
	MaxResults int                      `json:"MaxResults"`
}

type listTagSyncTasksOutput struct { //nolint:govet // fieldalignment: readability over micro-optimization
	TagSyncTasks []tagSyncTaskItem `json:"TagSyncTasks"`
	NextToken    string            `json:"NextToken,omitempty"`
}

func (h *Handler) handleListTagSyncTasks(
	ctx context.Context,
	in *listTagSyncTasksInput,
) (*listTagSyncTasksOutput, error) {
	tasks, nextToken, err := h.Backend.ListTagSyncTasks(ctx, in.Filters, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	items := make([]tagSyncTaskItem, 0, len(tasks))
	for i := range tasks {
		items = append(items, tagSyncTaskItemFromTask(&tasks[i]))
	}

	return &listTagSyncTasksOutput{TagSyncTasks: items, NextToken: nextToken}, nil
}
