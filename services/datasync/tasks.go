package datasync

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CreateTask creates a new DataSync task.
func (b *InMemoryBackend) CreateTask(
	sourceLocationArn, destinationLocationArn, name, cloudWatchLogGroupArn string,
	tags map[string]string,
) (*Task, error) {
	b.mu.Lock("CreateTask")
	defer b.mu.Unlock()

	if !b.locations.Has(sourceLocationArn) {
		return nil, fmt.Errorf("source location %s not found: %w", sourceLocationArn, ErrInvalidParameter)
	}

	if !b.locations.Has(destinationLocationArn) {
		return nil, fmt.Errorf("destination location %s not found: %w", destinationLocationArn, ErrInvalidParameter)
	}

	id := newID()
	taskArn := b.taskARN(id)
	now := time.Now().UTC()

	taskTags := make(map[string]string)
	maps.Copy(taskTags, tags)

	t := &storedTask{
		TaskArn:                taskArn,
		Name:                   name,
		Status:                 taskStatusAvailable,
		SourceLocationArn:      sourceLocationArn,
		DestinationLocationArn: destinationLocationArn,
		CloudWatchLogGroupArn:  cloudWatchLogGroupArn,
		CreationTime:           now,
		Tags:                   taskTags,
	}
	b.tasks.Put(t)

	if len(taskTags) > 0 {
		b.tags[taskArn] = make(map[string]string)
		maps.Copy(b.tags[taskArn], taskTags)
	}

	cp := t.toTask()

	return &cp, nil
}

// DescribeTask returns task details.
func (b *InMemoryBackend) DescribeTask(taskArn string) (*Task, error) {
	b.mu.RLock("DescribeTask")
	defer b.mu.RUnlock()

	t, ok := b.tasks.Get(taskArn)
	if !ok {
		return nil, ErrNotFound
	}

	cp := t.toTask()

	return &cp, nil
}

// UpdateTask updates the task's name and CloudWatch log group.
func (b *InMemoryBackend) UpdateTask(taskArn, name, cloudWatchLogGroupArn string) error {
	b.mu.Lock("UpdateTask")
	defer b.mu.Unlock()

	t, ok := b.tasks.Get(taskArn)
	if !ok {
		return ErrNotFound
	}

	if name != "" {
		t.Name = name
	}

	t.CloudWatchLogGroupArn = cloudWatchLogGroupArn

	return nil
}

// DeleteTask deletes a task.
func (b *InMemoryBackend) DeleteTask(taskArn string) error {
	b.mu.Lock("DeleteTask")
	defer b.mu.Unlock()

	if !b.tasks.Has(taskArn) {
		return ErrNotFound
	}

	b.tasks.Delete(taskArn)

	execs := slices.Clone(b.executionsByTask.Get(taskArn))
	for _, e := range execs {
		b.executions.Delete(e.TaskExecutionArn)
	}

	delete(b.tags, taskArn)

	return nil
}

// ListTasks returns tasks, sorted by ARN.
func (b *InMemoryBackend) ListTasks(maxResults int32, nextToken string) ([]*TaskListEntry, string, error) {
	b.mu.RLock("ListTasks")
	defer b.mu.RUnlock()

	sorted := b.tasks.Snapshot()

	all := make([]*TaskListEntry, 0, len(sorted))
	for _, t := range sorted {
		all = append(all, &TaskListEntry{
			TaskArn: t.TaskArn,
			Name:    t.Name,
			Status:  t.Status,
		})
	}

	limit := int(maxResults)
	pg := page.New(all, nextToken, limit, defaultMaxResults)

	return pg.Data, pg.Next, nil
}

// isTerminalExecutionStatus reports whether a task execution status is a
// terminal (finished) state. AWS only allows one task execution in progress
// per task at a time, so StartTaskExecution consults this to decide whether
// the task's current execution has actually finished.
func isTerminalExecutionStatus(status string) bool {
	return status == executionStatusSuccess || status == executionStatusError
}

// StartTaskExecution starts a new task execution.
//
// AWS allows only one task execution in progress per task at a time
// ("For each task, you can only run one task execution at a time.", per the
// StartTaskExecution API docs); attempting to start another while one is
// still running is rejected as an invalid request.
func (b *InMemoryBackend) StartTaskExecution(taskArn string) (*TaskExecution, error) {
	b.mu.Lock("StartTaskExecution")
	defer b.mu.Unlock()

	t, ok := b.tasks.Get(taskArn)
	if !ok {
		return nil, ErrNotFound
	}

	if t.CurrentTaskExecutionArn != "" {
		if cur, found := b.executions.Get(t.CurrentTaskExecutionArn); found && !isTerminalExecutionStatus(cur.Status) {
			return nil, fmt.Errorf(
				"%w: task %s already has an execution in progress (%s)",
				ErrInvalidParameter, taskArn, t.CurrentTaskExecutionArn,
			)
		}
	}

	id := newID()
	execArn := b.executionARN(taskArn, id)
	now := time.Now().UTC()

	e := &storedTaskExecution{
		TaskExecutionArn: execArn,
		Status:           executionStatusLaunching,
		StartTime:        now,
	}

	b.executions.Put(e)
	t.CurrentTaskExecutionArn = execArn
	t.Status = taskStatusRunning

	cp := e.toTaskExecution()

	return &cp, nil
}

// CancelTaskExecution cancels a running task execution.
//
// AWS has no CANCELLED task-execution status (see types.TaskExecutionStatus);
// a cancelled execution settles into ERROR, matching the documented
// "some transient states... interrupt a task execution" behavior. Unlike a
// truly "current" (actively running) marker, CurrentTaskExecutionArn on the
// parent task is documented as "the ARN of the most recent task execution",
// so it is left pointing at the cancelled execution rather than cleared.
func (b *InMemoryBackend) CancelTaskExecution(taskExecutionArn string) error {
	b.mu.Lock("CancelTaskExecution")
	defer b.mu.Unlock()

	taskArn := extractTaskArnFromExecution(taskExecutionArn)
	if taskArn == "" {
		return ErrNotFound
	}

	e, ok := b.executions.Get(taskExecutionArn)
	if !ok {
		return ErrNotFound
	}

	e.Status = executionStatusError

	if t, found := b.tasks.Get(taskArn); found && t.CurrentTaskExecutionArn == taskExecutionArn {
		t.Status = taskStatusAvailable
	}

	return nil
}

// DescribeTaskExecution returns task execution details.
// Executions in LAUNCHING state are lazily advanced to SUCCESS on first
// describe. When the execution that finishes is still the task's current
// one, the parent task's Status reverts from RUNNING to AVAILABLE, matching
// AWS (task Status is RUNNING only while a task execution is in progress).
func (b *InMemoryBackend) DescribeTaskExecution(taskExecutionArn string) (*TaskExecution, error) {
	b.mu.Lock("DescribeTaskExecution")
	defer b.mu.Unlock()

	taskArn := extractTaskArnFromExecution(taskExecutionArn)
	if taskArn == "" {
		return nil, ErrNotFound
	}

	e, ok := b.executions.Get(taskExecutionArn)
	if !ok {
		return nil, ErrNotFound
	}

	if e.Status == executionStatusLaunching {
		e.Status = executionStatusSuccess

		if t, found := b.tasks.Get(taskArn); found && t.CurrentTaskExecutionArn == taskExecutionArn {
			t.Status = taskStatusAvailable
		}
	}

	cp := e.toTaskExecution()

	return &cp, nil
}

// ListTaskExecutions returns executions for a task (or, when taskArn is
// empty, every execution across all tasks -- TaskArn is an optional filter
// per the real ListTaskExecutions API), sorted by ARN.
func (b *InMemoryBackend) ListTaskExecutions(
	taskArn string,
	maxResults int32,
	nextToken string,
) ([]*TaskExecutionListEntry, string, error) {
	b.mu.RLock("ListTaskExecutions")
	defer b.mu.RUnlock()

	var execs []*storedTaskExecution
	if taskArn != "" {
		if !b.tasks.Has(taskArn) {
			return nil, "", ErrNotFound
		}

		execs = slices.Clone(b.executionsByTask.Get(taskArn))
	} else {
		execs = b.executions.Snapshot()
	}

	slices.SortFunc(execs, func(a, b *storedTaskExecution) int {
		return strings.Compare(a.TaskExecutionArn, b.TaskExecutionArn)
	})

	all := make([]*TaskExecutionListEntry, 0, len(execs))
	for _, e := range execs {
		all = append(all, &TaskExecutionListEntry{
			TaskExecutionArn: e.TaskExecutionArn,
			Status:           e.Status,
		})
	}

	limit := int(maxResults)
	pg := page.New(all, nextToken, limit, defaultMaxResults)

	return pg.Data, pg.Next, nil
}

// UpdateTaskExecution updates a task execution (no-op: options are advisory only).
func (b *InMemoryBackend) UpdateTaskExecution(taskExecutionArn string, options map[string]any) error {
	b.mu.Lock("UpdateTaskExecution")
	defer b.mu.Unlock()

	taskArn := extractTaskArnFromExecution(taskExecutionArn)
	if taskArn == "" {
		return ErrNotFound
	}

	exec, ok := b.executions.Get(taskExecutionArn)
	if !ok {
		return ErrNotFound
	}

	// AWS only allows UpdateTaskExecution while the execution is still in a
	// pre-transfer/transfer phase; terminal (SUCCESS/ERROR) executions cannot
	// be updated.
	if exec.Status == executionStatusSuccess || exec.Status == executionStatusError {
		return fmt.Errorf(
			"%w: task execution %s is in terminal state %s and cannot be updated",
			ErrInvalidParameter, taskExecutionArn, exec.Status,
		)
	}

	// Merge the supplied Options onto the execution (AWS updates only the
	// fields present in the request). BytesPerSecond is the most common knob.
	if len(options) > 0 {
		if exec.Options == nil {
			exec.Options = make(map[string]any, len(options))
		}

		maps.Copy(exec.Options, options)
	}

	return nil
}

// extractTaskArnFromExecution extracts the task ARN from a task execution ARN.
// Execution ARN format: arn:aws:datasync:region:account:task/<task-id>/execution/<exec-id>.
func extractTaskArnFromExecution(execArn string) string {
	// Find /execution/ suffix and strip it.
	idx := strings.LastIndex(execArn, "/execution/")
	if idx < 0 {
		return ""
	}

	return execArn[:idx]
}
