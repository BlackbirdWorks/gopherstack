package rds

import "fmt"

// StartExportTask creates a new export task for the given source ARN.
func (b *InMemoryBackend) StartExportTask(taskID, sourceARN, s3Bucket string) (*ExportTask, error) {
	if taskID == "" {
		return nil, fmt.Errorf("%w: ExportTaskIdentifier must not be empty", ErrInvalidParameter)
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

	return result, nil
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
