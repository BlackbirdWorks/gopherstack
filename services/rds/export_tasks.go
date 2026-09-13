package rds

import (
	"fmt"
	"net/url"
	"slices"
)

// StartExportTask creates a new export task for the given source ARN.
func (b *InMemoryBackend) StartExportTask(
	taskID, sourceARN, s3Bucket, iamRoleARN, kmsKeyID string,
) (*ExportTask, error) {
	if taskID == "" {
		return nil, fmt.Errorf("%w: ExportTaskIdentifier must not be empty", ErrInvalidParameter)
	}
	if iamRoleARN == "" {
		return nil, fmt.Errorf("%w: IamRoleArn must not be empty", ErrInvalidParameter)
	}
	if kmsKeyID == "" {
		return nil, fmt.Errorf("%w: KmsKeyId must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("StartExportTask")
	defer b.mu.Unlock()
	if _, exists := b.exportTasks.Get(taskID); exists {
		return nil, fmt.Errorf("%w: export task %s already exists", ErrExportTaskAlreadyExists, taskID)
	}
	task := &ExportTask{
		ExportTaskIdentifier: taskID,
		SourceArn:            sourceARN,
		Status:               "complete",
		S3Bucket:             s3Bucket,
		IamRoleArn:           iamRoleARN,
		KmsKeyID:             kmsKeyID,
	}
	b.exportTasks.Put(task)
	cp := *task

	return &cp, nil
}

// DescribeExportTasks returns export tasks, optionally filtered by task ID.
func (b *InMemoryBackend) DescribeExportTasks(taskID string) ([]ExportTask, error) {
	b.mu.RLock("DescribeExportTasks")
	defer b.mu.RUnlock()
	if taskID != "" {
		task, exists := b.exportTasks.Get(taskID)
		if !exists {
			return nil, fmt.Errorf("%w: export task %s not found", ErrExportTaskNotFound, taskID)
		}
		cp := *task

		return []ExportTask{cp}, nil
	}
	result := make([]ExportTask, 0, b.exportTasks.Len())
	for _, task := range b.exportTasks.All() {
		result = append(result, *task)
	}
	slices.SortFunc(result, func(a, b ExportTask) int {
		if a.ExportTaskIdentifier < b.ExportTaskIdentifier {
			return -1
		}
		if a.ExportTaskIdentifier > b.ExportTaskIdentifier {
			return 1
		}

		return 0
	})

	return result, nil
}

// isKnownExportTaskFilterName reports whether name is a Filters.Filter.N.Name
// value AWS recognizes for DescribeExportTasks (rds@v1.124.1
// api_op_DescribeExportTasks.go:35-63).
func isKnownExportTaskFilterName(name string) bool {
	switch name {
	case filterNameExportTaskIdentifier, filterNameS3Bucket, filterNameSourceArn, filterNameStatus:
		return true
	default:
		return false
	}
}

// applyExportTaskFilters narrows tasks per the AWS DescribeExportTasks
// Filters contract: each filter ANDs together, and a filter's Values list is
// OR-matched against the corresponding task field. An unrecognized filter
// name returns InvalidParameterValue, matching real AWS.
func applyExportTaskFilters(vals url.Values, tasks []ExportTask) ([]ExportTask, error) {
	filters := parseDescribeFilters(vals)
	if len(filters) == 0 {
		return tasks, nil
	}

	for name := range filters {
		if !isKnownExportTaskFilterName(name) {
			return nil, fmt.Errorf("%w: Unrecognized filter name: %s", ErrInvalidParameter, name)
		}
	}

	filtered := make([]ExportTask, 0, len(tasks))
	for _, t := range tasks {
		if matchesAllExportTaskFilters(t, filters) {
			filtered = append(filtered, t)
		}
	}

	return filtered, nil
}

func matchesAllExportTaskFilters(t ExportTask, filters map[string][]string) bool {
	for name, values := range filters {
		switch name {
		case filterNameExportTaskIdentifier:
			if !slices.Contains(values, t.ExportTaskIdentifier) {
				return false
			}
		case filterNameS3Bucket:
			if !slices.Contains(values, t.S3Bucket) {
				return false
			}
		case filterNameSourceArn:
			if !slices.Contains(values, t.SourceArn) {
				return false
			}
		case filterNameStatus:
			if !slices.Contains(values, t.Status) {
				return false
			}
		}
	}

	return true
}

// CancelExportTask cancels and removes the export task with the given identifier.
func (b *InMemoryBackend) CancelExportTask(taskID string) (*ExportTask, error) {
	if taskID == "" {
		return nil, fmt.Errorf("%w: ExportTaskIdentifier must not be empty", ErrInvalidParameter)
	}
	b.mu.Lock("CancelExportTask")
	defer b.mu.Unlock()
	task, exists := b.exportTasks.Get(taskID)
	if !exists {
		return nil, fmt.Errorf("%w: export task %s not found", ErrExportTaskNotFound, taskID)
	}
	task.Status = "canceled"
	cp := *task
	b.exportTasks.Delete(taskID)

	return &cp, nil
}
