package fsx

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

type storedDataRepositoryTask struct {
	CreationTime time.Time         `json:"creationTime"`
	DeadlineAt   time.Time         `json:"deadlineAt"`
	EndTime      time.Time         `json:"endTime"`
	Report       *CompletionReport `json:"report,omitempty"`
	Tags         map[string]string `json:"tags"`
	TaskID       string            `json:"taskId"`
	FileSystemID string            `json:"fileSystemId"`
	Type         string            `json:"type"`
	Lifecycle    string            `json:"lifecycle"`
	ResourceARN  string            `json:"resourceArn"`
	Paths        []string          `json:"paths,omitempty"`
}

func (t *storedDataRepositoryTask) toPublic() *DataRepositoryTask {
	pub := &DataRepositoryTask{
		CreationTime: epochTime(t.CreationTime),
		Report:       t.Report,
		TaskID:       t.TaskID,
		FileSystemID: t.FileSystemID,
		Type:         t.Type,
		Lifecycle:    t.Lifecycle,
		ResourceARN:  t.ResourceARN,
		Paths:        t.Paths,
		Tags:         tagsMapToSlice(t.Tags),
	}

	if !t.EndTime.IsZero() {
		end := epochTime(t.EndTime)
		pub.EndTime = &end
		pub.Status = t.status()
	}

	return pub
}

// status computes DataRepositoryTaskStatus from t's Paths and terminal
// Lifecycle. TotalCount/SucceededCount/FailedCount are fully derived rather
// than separately stored: a SUCCEEDED task processed everything it was
// asked to (SucceededCount = TotalCount, FailedCount 0 -- this backend
// models no per-file failures); a CANCELED task processed nothing (both 0).
func (t *storedDataRepositoryTask) status() *DataRepositoryTaskStatus {
	total := int64(len(t.Paths))
	if total == 0 {
		total = 1
	}

	var succeeded int64
	if t.Lifecycle == drtLifecycleSucceeded {
		succeeded = total
	}

	return &DataRepositoryTaskStatus{
		TotalCount:      total,
		SucceededCount:  succeeded,
		LastUpdatedTime: epochTime(t.EndTime),
	}
}

type createDataRepositoryTaskInput struct {
	Report       *CompletionReport `json:"Report"`
	FileSystemID string            `json:"FileSystemId"`
	Type         string            `json:"Type"`
	Paths        []string          `json:"Paths,omitempty"`
	Tags         []Tag             `json:"Tags,omitempty"`
}

// CreateDataRepositoryTask creates a data repository task. Report is a
// required CreateDataRepositoryTaskInput member (verified against
// validateOpCreateDataRepositoryTaskInput, validators.go), and its own
// Enabled member is required whenever Report is present (validateCompletionReport)
// -- the pre-fix request never read Report at all.
func (b *InMemoryBackend) CreateDataRepositoryTask(input *createDataRepositoryTaskInput) (*DataRepositoryTask, error) {
	if err := validateCreateTags(input.Tags); err != nil {
		return nil, err
	}

	if input.Report == nil {
		return nil, fmt.Errorf("%w: Report is required", ErrValidation)
	}

	if input.Report.Enabled == nil {
		return nil, fmt.Errorf("%w: Report.Enabled is required", ErrValidation)
	}

	b.mu.Lock("CreateDataRepositoryTask")
	defer b.mu.Unlock()

	b.sweepDataRepositoryTasksLocked(time.Now())

	if !b.fileSystems.Has(input.FileSystemID) {
		return nil, ErrFileSystemNotFound
	}

	if b.hasExecutingTaskLocked(input.FileSystemID) {
		return nil, ErrDataRepositoryTaskExecuting
	}

	id := newDataRepositoryTaskID()
	arn := b.drtARN(id)
	now := time.Now().UTC()
	tags := tagsSliceToMap(input.Tags)

	t := &storedDataRepositoryTask{
		CreationTime: now,
		DeadlineAt:   now.Add(dataRepositoryTaskCompletionDelay),
		Report:       input.Report,
		Tags:         tags,
		Paths:        input.Paths,
		TaskID:       id,
		FileSystemID: input.FileSystemID,
		Type:         input.Type,
		Lifecycle:    drtLifecycleExecuting,
		ResourceARN:  arn,
	}

	b.dataRepositoryTasks.Put(t)
	b.tags[arn] = tags

	return t.toPublic(), nil
}

// hasExecutingTaskLocked reports whether fileSystemID already has a task
// with Lifecycle EXECUTING. Caller must already hold b.mu.
func (b *InMemoryBackend) hasExecutingTaskLocked(fileSystemID string) bool {
	found := false

	b.dataRepositoryTasks.Range(func(t *storedDataRepositoryTask) bool {
		if t.FileSystemID == fileSystemID && t.Lifecycle == drtLifecycleExecuting {
			found = true

			return false
		}

		return true
	})

	return found
}

// sweepDataRepositoryTasksLocked advances tasks past their modeled
// deadline. This backend has no background timer or goroutine (see
// PARITY.md's leaks note), so completion is lazily evaluated here at the
// top of every op that reads or mutates task state -- the same pattern
// services/swf/timeout_sweep.go and services/glue/reconciler.go use. An
// EXECUTING task whose DeadlineAt has passed settles at SUCCEEDED; any task
// left CANCELING settles at CANCELED on this, the next sweep after Cancel
// was issued. Caller must hold the write lock.
func (b *InMemoryBackend) sweepDataRepositoryTasksLocked(now time.Time) {
	for _, t := range b.dataRepositoryTasks.All() {
		switch t.Lifecycle {
		case drtLifecycleExecuting:
			if !now.Before(t.DeadlineAt) {
				t.Lifecycle = drtLifecycleSucceeded
				t.EndTime = now
			}
		case drtLifecycleCanceling:
			t.Lifecycle = drtLifecycleCanceled
			t.EndTime = now
		}
	}
}

// CancelDataRepositoryTask marks a task as cancelled. It settles at the
// terminal CANCELED on the next sweep (the next Cancel/Create/Describe
// call), matching the transient CANCELING window a real client observes
// before AWS finishes tearing the task down.
func (b *InMemoryBackend) CancelDataRepositoryTask(taskID string) error {
	b.mu.Lock("CancelDataRepositoryTask")
	defer b.mu.Unlock()

	b.sweepDataRepositoryTasksLocked(time.Now())

	t, ok := b.dataRepositoryTasks.Get(taskID)
	if !ok {
		return ErrDataRepositoryTaskNotFound
	}

	t.Lifecycle = drtLifecycleCanceling

	return nil
}

// DescribeDataRepositoryTasks returns tasks, optionally filtered by ID or
// Filters. Real DataRepositoryTaskFilterName (aws-sdk-go-v2/service/fsx@v1.68.4
// types/enums.go) has 4 values: file-system-id, task-lifecycle,
// data-repository-association-id, file-cache-id. Only the first two are
// recognized here -- CreateDataRepositoryTask never accepts an association or
// file-cache reference to track, so those two have no honest value; matches
// everything for them, same as an unset filter.
func (b *InMemoryBackend) DescribeDataRepositoryTasks(
	ids []string,
	filters []wireFilter,
	maxResults int32,
	nextToken string,
) ([]*DataRepositoryTask, string, error) {
	b.mu.Lock("DescribeDataRepositoryTasks")
	defer b.mu.Unlock()

	b.sweepDataRepositoryTasksLocked(time.Now())

	if maxResults <= 0 {
		maxResults = maxResultsDefault
	}

	var all []*storedDataRepositoryTask

	if len(ids) > 0 {
		for _, id := range ids {
			t, ok := b.dataRepositoryTasks.Get(id)
			if !ok {
				return nil, "", ErrDataRepositoryTaskNotFound
			}

			all = append(all, t)
		}
	} else {
		for _, t := range b.dataRepositoryTasks.All() {
			if matchesFilters(filters, func(name string) (string, bool) {
				switch name {
				case filterNameFileSystemID:
					return t.FileSystemID, true
				case "task-lifecycle":
					return t.Lifecycle, true
				default:
					return "", false
				}
			}) {
				all = append(all, t)
			}
		}

		sort.Slice(all, func(i, j int) bool { return all[i].TaskID < all[j].TaskID })
	}

	start, end, next := paginate(len(all), int(maxResults), nextToken, func(i int) string {
		return all[i].TaskID
	})

	result := make([]*DataRepositoryTask, end-start)
	for i, t := range all[start:end] {
		result[i] = t.toPublic()
	}

	return result, next, nil
}

func (b *InMemoryBackend) drtARN(id string) string {
	return arn.Build("fsx", b.region, b.accountID, fmt.Sprintf("task/%s", id))
}
