package sqs

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/google/uuid"
)

// moveTaskVisibilityTimeoutSecs is the visibility timeout used when receiving
// messages during a message move task. It provides enough time to send the
// message to the destination and delete it from the source.
const moveTaskVisibilityTimeoutSecs = 30

// moveTaskState tracks the live state of a StartMessageMoveTask goroutine.
type moveTaskState struct {
	cancel        context.CancelFunc
	taskHandle    string
	sourceArn     string
	destArn       string
	failureReason string
	status        MoveTaskStatus
	startedAt     int64
	movedCount    int64
	totalCount    int64
	mu            sync.Mutex
	maxPerSec     int32
}

// moveTaskTableKey is the [store.Table] key function for b.moveTasks: a move
// task's own taskHandle is already its unique identity.
func moveTaskTableKey(t *moveTaskState) string {
	return t.taskHandle
}

// cancelAllMoveTasks cancels every in-flight StartMessageMoveTask goroutine so
// none outlives the backend. Safe to call multiple times; cancelling an
// already-finished task is a no-op.
func (b *InMemoryBackend) cancelAllMoveTasks() {
	var tasks []*moveTaskState

	func() {
		b.mu.Lock("cancelAllMoveTasks")
		defer b.mu.Unlock()

		tasks = b.moveTasks.All()
	}()

	for _, task := range tasks {
		if task.cancel != nil {
			task.cancel()
		}
	}
}

// cancelMoveTaskIfInvolved cancels task if it is active and references queueARN.
func (b *InMemoryBackend) cancelMoveTaskIfInvolved(task *moveTaskState, queueARN string) {
	var isActive, involves bool

	func() {
		task.mu.Lock()
		defer task.mu.Unlock()

		isActive = task.status == MoveTaskStatusRunning || task.status == MoveTaskStatusCancelling
		involves = task.sourceArn == queueARN || task.destArn == queueARN
	}()

	if isActive && involves {
		task.cancel()
	}
}

// findDefaultMoveDestinationLocked finds the URL of the queue that has a RedrivePolicy
// pointing to the DLQ identified by dlqARN.
// Must be called with b.mu held (either read or write).
func (b *InMemoryBackend) findDefaultMoveDestinationLocked(dlqARN string) (string, bool) {
	for _, q := range b.queues.All() {
		raw, ok := q.Attributes[attrRedrivePolicy]
		if !ok || raw == "" {
			continue
		}

		var pol redrivePolicy

		if err := json.Unmarshal([]byte(raw), &pol); err != nil {
			continue
		}

		if pol.DeadLetterTargetArn == dlqARN {
			return q.URL, true
		}
	}

	return "", false
}

// approximateQueueDepthLocked returns the approximate number of visible messages in the queue with the given name.
// Must be called with b.mu held (either read or write).
func approximateQueueDepthLocked(q *Queue) int64 {
	now := time.Now()
	visible := 0

	for _, msg := range q.messages {
		if !now.Before(msg.VisibleAt) {
			visible++
		}
	}

	return int64(visible)
}

// StartMessageMoveTask starts an asynchronous task that moves messages from the
// source queue (typically a DLQ) to the destination queue.
// If DestinationArn is empty, the backend looks for a queue whose RedrivePolicy
// points to the source ARN and uses that as the destination.
// Returns ErrMoveTaskAlreadyRunning if there is already a RUNNING task for the source ARN.
func (b *InMemoryBackend) StartMessageMoveTask(
	input *StartMessageMoveTaskInput,
) (*StartMessageMoveTaskOutput, error) {
	if input.SourceArn == "" {
		return nil, ErrInvalidSourceArn
	}

	if input.MaxNumberOfMessagesPerSecond < 0 {
		return nil, ErrInvalidMaxMessagesPerSecond
	}

	taskHandle, srcURL, destURL, state, ctx, err := b.startMessageMoveTaskLocked(input)
	if err != nil {
		return nil, err
	}

	go b.runMoveTask(ctx, state, srcURL, destURL)

	return &StartMessageMoveTaskOutput{TaskHandle: taskHandle}, nil
}

// startMessageMoveTaskLocked checks for an already-running task on the same
// source ARN, resolves the source/destination queues, and registers the new
// move task, all under b.mu. Extracted from StartMessageMoveTask so the locked
// region is a plain method body rather than a function literal.
func (b *InMemoryBackend) startMessageMoveTaskLocked(
	input *StartMessageMoveTaskInput,
) (string, string, string, *moveTaskState, context.Context, error) {
	b.mu.Lock("StartMessageMoveTask")
	defer b.mu.Unlock()

	// Check for existing running task on the same source ARN (AWS realism).
	// We check task status while holding both b.mu and t.mu to ensure the
	// status snapshot is consistent with the subsequent task insertion.
	if b.hasActiveMoveTaskForSourceLocked(input.SourceArn) {
		return "", "", "", nil, nil, ErrMoveTaskAlreadyRunning
	}

	// Resolve source queue under the lock to avoid TOCTOU races.
	srcURL, ok := b.queueURLFromARNLocked(input.SourceArn)
	if !ok {
		return "", "", "", nil, nil, ErrQueueNotFound
	}

	// Resolve destination queue under the same lock.
	destArn := input.DestinationArn

	var destURL string

	if destArn == "" {
		destURL, ok = b.findDefaultMoveDestinationLocked(input.SourceArn)
		if !ok {
			return "", "", "", nil, nil, ErrQueueNotFound
		}

		// Derive destination ARN from its URL for task metadata.
		destName := queueNameFromInput(destURL)
		destArn = arn.Build("sqs", b.region, b.accountID, destName)
	} else {
		destURL, ok = b.queueURLFromARNLocked(destArn)
		if !ok {
			return "", "", "", nil, nil, ErrQueueNotFound
		}
	}

	// Snapshot queue depth under the lock so the estimate is consistent.
	srcQueue, _ := b.lookupQueueByURL("", srcURL)
	totalCount := approximateQueueDepthLocked(srcQueue)

	taskHandle := uuid.New().String()

	ctx, cancel := context.WithCancel(b.svcCtx)

	state := &moveTaskState{
		cancel:     cancel,
		taskHandle: taskHandle,
		sourceArn:  input.SourceArn,
		destArn:    destArn,
		status:     MoveTaskStatusRunning,
		maxPerSec:  input.MaxNumberOfMessagesPerSecond,
		startedAt:  time.Now().UnixMilli(),
		totalCount: totalCount,
	}

	b.moveTasks.Put(state)

	return taskHandle, srcURL, destURL, state, ctx, nil
}

// hasActiveMoveTaskForSourceLocked reports whether there is already a RUNNING or
// CANCELLING move task for sourceArn. Must be called with b.mu held.
func (b *InMemoryBackend) hasActiveMoveTaskForSourceLocked(sourceArn string) bool {
	for _, t := range b.moveTasks.All() {
		if t.sourceArn != sourceArn {
			continue
		}

		t.mu.Lock()
		isActive := t.status == MoveTaskStatusRunning || t.status == MoveTaskStatusCancelling
		t.mu.Unlock()

		if isActive {
			return true
		}
	}

	return false
}

// from the source queue one at a time and writes them to the destination queue
// until the source is empty, the context is cancelled, or a fatal error occurs.
func (b *InMemoryBackend) runMoveTask(
	ctx context.Context,
	state *moveTaskState,
	srcURL, destURL string,
) {
	defer func() {
		state.mu.Lock()
		defer state.mu.Unlock()

		switch state.status {
		case MoveTaskStatusRunning:
			state.status = MoveTaskStatusCompleted
		case MoveTaskStatusCancelling:
			// CancelMessageMoveTask set status to CANCELLING and called cancel().
			// Whether the context was done (cancelled) or the queue was drained,
			// the task is now stopped — report CANCELLED either way.
			state.status = MoveTaskStatusCancelled
		case MoveTaskStatusCompleted, MoveTaskStatusCancelled, MoveTaskStatusFailed:
			// Terminal state already set (e.g. by an error path within the loop).
		}
	}()

	// Use a Ticker for MaxNumberOfMessagesPerSecond so the interval accounts for
	// the receive+send work time, maintaining the configured rate accurately.
	// A per-iteration Timer would add receive+send latency to each period, reducing
	// effective throughput below the requested rate.
	var rateTicker *time.Ticker
	var rateC <-chan time.Time

	if state.maxPerSec > 0 {
		interval := time.Second / time.Duration(state.maxPerSec)
		rateTicker = time.NewTicker(interval)
		rateC = rateTicker.C
		defer rateTicker.Stop()
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if rateC != nil {
			select {
			case <-ctx.Done():
				return
			case <-rateC:
			}
		}

		// Receive one message from the source queue.
		out, err := b.ReceiveMessage(&ReceiveMessageInput{
			QueueURL:            srcURL,
			MaxNumberOfMessages: 1,
			VisibilityTimeout:   moveTaskVisibilityTimeoutSecs,
		})
		if err != nil {
			state.mu.Lock()
			defer state.mu.Unlock()

			state.status = MoveTaskStatusFailed
			state.failureReason = "failed to receive message from source queue: " + err.Error()

			return
		}

		if len(out.Messages) == 0 {
			// Source queue is empty; task is complete.
			return
		}

		msg := out.Messages[0]

		// Send the message to the destination queue.
		// Preserve MessageGroupID and MessageDeduplicationID for FIFO queues so that
		// moved messages retain their original message group and deduplication identities.
		_, sendErr := b.SendMessage(&SendMessageInput{
			QueueURL:               destURL,
			MessageBody:            msg.Body,
			MessageAttributes:      msg.MessageAttributes,
			MessageGroupID:         msg.MessageGroupID,
			MessageDeduplicationID: msg.MessageDeduplicationID,
		})
		if sendErr != nil {
			// Re-make message visible so it is not lost.
			_ = b.ChangeMessageVisibility(&ChangeMessageVisibilityInput{
				QueueURL:          srcURL,
				ReceiptHandle:     msg.ReceiptHandle,
				VisibilityTimeout: 0,
			})
			state.mu.Lock()
			defer state.mu.Unlock()

			state.status = MoveTaskStatusFailed
			state.failureReason = "failed to send message to destination queue: " + sendErr.Error()

			return
		}

		// Delete the original from the source queue.
		_ = b.DeleteMessage(&DeleteMessageInput{
			QueueURL:      srcURL,
			ReceiptHandle: msg.ReceiptHandle,
		})

		func() {
			state.mu.Lock()
			defer state.mu.Unlock()

			state.movedCount++
		}()
	}
}

// CancelMessageMoveTask cancels an active message move task.
// Returns ErrMoveTaskNotRunning if the task is not in RUNNING or CANCELLING state,
// matching AWS behaviour ("A message move task with the specified task handle is not running.").
func (b *InMemoryBackend) CancelMessageMoveTask(
	input *CancelMessageMoveTaskInput,
) (*CancelMessageMoveTaskOutput, error) {
	var (
		state *moveTaskState
		ok    bool
	)

	func() {
		b.mu.RLock("CancelMessageMoveTask")
		defer b.mu.RUnlock()

		state, ok = b.moveTasks.Get(input.TaskHandle)
	}()

	if !ok {
		return nil, ErrTaskHandleInvalid
	}

	var (
		out      *CancelMessageMoveTaskOutput
		err      error
		doCancel bool
	)

	func() {
		state.mu.Lock()
		defer state.mu.Unlock()

		switch state.status {
		case MoveTaskStatusRunning, MoveTaskStatusCancelling:
			state.status = MoveTaskStatusCancelling
			out = &CancelMessageMoveTaskOutput{
				ApproximateNumberOfMessagesMoved: state.movedCount,
			}
			doCancel = true
		default:
			// Terminal states (Completed, Cancelled, Failed) cannot be cancelled;
			// AWS rejects the request rather than treating it as idempotent.
			err = ErrMoveTaskNotRunning
		}
	}()

	if doCancel {
		state.cancel()
	}

	return out, err
}

// listMessageMoveTasksDefaultMaxResults is the default number of results returned by
// ListMessageMoveTasks when MaxResults is not specified, matching AWS behaviour.
const listMessageMoveTasksDefaultMaxResults = 1

// listMessageMoveTasksMaxAllowed is the maximum number of results AWS allows.
const listMessageMoveTasksMaxAllowed = 10

// ListMessageMoveTasks returns message move tasks for the given source ARN.
//
// Per AWS semantics:
//   - If MaxResults is 0 (not set), it defaults to 1 (the most recent task).
//   - The maximum allowed value for MaxResults is 10.
//   - Results are sorted newest-first (descending by startedAt timestamp).
//   - TaskHandle is only populated for tasks in RUNNING status.
func (b *InMemoryBackend) ListMessageMoveTasks(
	input *ListMessageMoveTasksInput,
) (*ListMessageMoveTasksOutput, error) {
	var tasks []*moveTaskState

	func() {
		b.mu.RLock("ListMessageMoveTasks")
		defer b.mu.RUnlock()

		for _, t := range b.moveTasks.All() {
			if input.SourceArn == "" || t.sourceArn == input.SourceArn {
				tasks = append(tasks, t)
			}
		}
	}()

	// Sort descending by startedAt: higher timestamps are newer, so index[0] is the
	// most recent task. This matches the AWS default of returning the most recent task first.
	sort.Slice(tasks, func(i, j int) bool {
		return tasks[i].startedAt > tasks[j].startedAt // descending (larger = newer)
	})

	// Apply MaxResults: default to 1 when unset; cap at the AWS maximum of 10.
	limit := int(input.MaxResults)
	if limit <= 0 {
		limit = listMessageMoveTasksDefaultMaxResults
	}

	if limit > listMessageMoveTasksMaxAllowed {
		limit = listMessageMoveTasksMaxAllowed
	}

	if len(tasks) > limit {
		tasks = tasks[:limit]
	}

	results := make([]MessageMoveTask, 0, len(tasks))

	for _, t := range tasks {
		t.mu.Lock()
		movedCount := t.movedCount
		totalCount := t.totalCount
		status := t.status
		startedAt := t.startedAt
		maxPerSec := t.maxPerSec
		taskHandle := t.taskHandle
		sourceArn := t.sourceArn
		destArn := t.destArn
		failureReason := t.failureReason
		t.mu.Unlock()

		task := MessageMoveTask{
			SourceArn:                        sourceArn,
			DestinationArn:                   destArn,
			Status:                           status,
			ApproximateNumberOfMessagesMoved: movedCount,
			StartedTimestamp:                 startedAt,
		}

		// Per AWS: TaskHandle is only populated for RUNNING tasks.
		if status == MoveTaskStatusRunning {
			task.TaskHandle = taskHandle
		}

		if totalCount > 0 {
			task.ApproximateNumberOfMessagesToMove = &totalCount
		}

		if maxPerSec > 0 {
			task.MaxNumberOfMessagesPerSecond = &maxPerSec
		}

		if failureReason != "" {
			task.FailureReason = &failureReason
		}

		results = append(results, task)
	}

	return &ListMessageMoveTasksOutput{Results: results}, nil
}
