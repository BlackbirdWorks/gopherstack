package sqs

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	defaultSQSJanitorInterval = time.Minute
	sqsJanitorService         = "sqs"
	sqsJanitorComponent       = "MessageRetentionSweeper"
)

// Janitor is the SQS background worker that deletes messages that have exceeded
// their MessageRetentionPeriod.
type Janitor struct {
	Backend  *InMemoryBackend
	Interval time.Duration
}

// NewJanitor creates a new SQS Janitor for the given backend.
func NewJanitor(backend *InMemoryBackend, interval time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultSQSJanitorInterval
	}

	return &Janitor{
		Backend:  backend,
		Interval: interval,
	}
}

// Run runs the janitor loop until ctx is cancelled.
func (j *Janitor) Run(ctx context.Context) {
	g := worker.NewGroup(ctx, sqsJanitorService)
	g.Ticker(sqsJanitorComponent, j.Interval, 0, j.sweepExpiredMessages)

	<-ctx.Done()
	g.Stop()
}

// sweepExpiredMessages removes messages that have exceeded the queue's MessageRetentionPeriod
// and runs all standard backend maintenance (dedup pruning, visibility-timeout requeue, DLQ drain).
// It delegates to InMemoryBackend.pruneState so the handler and internal janitor share one code path.
func (j *Janitor) sweepExpiredMessages(ctx context.Context) {
	before := j.Backend.totalMessages()
	j.Backend.pruneState(time.Now())
	after := j.Backend.totalMessages()

	if purged := before - after; purged > 0 {
		telemetry.RecordWorkerItems(sqsJanitorService, sqsJanitorComponent, purged)
		logger.Load(ctx).InfoContext(ctx, "SQS janitor: expired messages purged", "count", purged)
	}
	telemetry.RecordWorkerTask(sqsJanitorService, sqsJanitorComponent, "success")
}

const (
	janitorInterval      = 30 * time.Second
	moveTaskRetentionTTL = 15 * time.Minute
)

func (b *InMemoryBackend) startJanitor() {
	b.janitorStop = make(chan struct{})

	go b.runJanitor()
}

// stopInternalJanitor stops the internal janitor goroutine started by
// NewInMemoryBackendWithConfig. Safe to call multiple times.
func (b *InMemoryBackend) stopInternalJanitor() {
	var ch chan struct{}

	func() {
		b.mu.Lock("stopInternalJanitor")
		defer b.mu.Unlock()

		ch = b.janitorStop
	}()

	if ch == nil {
		return
	}

	select {
	case <-ch:
		// already closed
	default:
		close(ch)
	}
}

func (b *InMemoryBackend) runJanitor() {
	ticker := time.NewTicker(janitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.janitorStop:
			return
		case <-ticker.C:
			b.pruneState(time.Now())
		}
	}
}

func (b *InMemoryBackend) pruneState(now time.Time) {
	// Collect queue snapshot under RLock so hot-path senders/receivers for
	// other queues are not blocked during per-queue cleanup (#55).
	// Only include queues with pending activity (hasActivity flag set) or FIFO
	// queues (which may have dedup IDs to expire regardless of message count).
	// This avoids allocating a full-width snapshot when most queues are idle.
	queues := make([]*Queue, 0)

	func() {
		b.mu.RLock("pruneState.collect")
		defer b.mu.RUnlock()

		b.queues.Range(func(q *Queue) bool {
			if q.hasActivity.Load() || q.IsFIFO {
				queues = append(queues, q)
			}

			return true
		})
	}()

	dedupPruned := 0
	msgExpired := 0

	for _, q := range queues {
		func() {
			q.mu.Lock()
			defer q.mu.Unlock()

			if q.IsFIFO {
				before := len(q.DeduplicationIDs)
				pruneDedup(q, now)
				dedupPruned += before - len(q.DeduplicationIDs)
			}

			before := len(q.messages)
			// prepareAndPickMessages with maxMessages=0 performs all cleanup (re-queue
			// expired in-flight, expire retained, drain to DLQ) without picking (#54).
			prepareAndPickMessages(q, "", 0, 0, now)
			msgExpired += max(0, before-len(q.messages))

			// When the queue is fully idle, clear hasActivity so subsequent janitor
			// ticks skip it until new messages arrive.
			if len(q.messages) == 0 && len(q.inFlightMessages) == 0 &&
				len(q.DeduplicationIDs) == 0 {
				q.hasActivity.Store(false)
			}
		}()
	}

	tasksPruned := 0
	pruneBefore := now.Add(-moveTaskRetentionTTL).UnixMilli()

	func() {
		b.mu.Lock("pruneState.tasks")
		defer b.mu.Unlock()

		// Deleting a Table entry mid-Range is safe for the same reason it is safe
		// for a bare map: Go guarantees a concurrently deleted key is not produced
		// later in the same range.
		b.moveTasks.Range(func(task *moveTaskState) bool {
			task.mu.Lock()
			isTerminal := task.status == MoveTaskStatusCompleted ||
				task.status == MoveTaskStatusCancelled ||
				task.status == MoveTaskStatusFailed
			isExpired := task.startedAt <= pruneBefore
			task.mu.Unlock()

			if isTerminal && isExpired {
				b.moveTasks.Delete(task.taskHandle)
				tasksPruned++
			}

			return true
		})
	}()

	recentlyDeletedPruned := b.pruneRecentlyDeleted(now)

	totalItems := dedupPruned + msgExpired + tasksPruned + recentlyDeletedPruned
	telemetry.RecordWorkerItems("sqs", "JanitorSweeper", totalItems)
	telemetry.RecordWorkerTask("sqs", "JanitorSweeper", "success")
}

// pruneRecentlyDeleted removes entries from b.recentlyDeleted (the
// ErrQueueDeletedRecently cooldown map) whose 60-second window has already
// elapsed as of now, so the map stays bounded across a long-lived backend
// that deletes many queues over time. Returns the number of entries removed.
// Extracted from pruneState to keep that function under the gocognit limit.
func (b *InMemoryBackend) pruneRecentlyDeleted(now time.Time) int {
	pruned := 0

	b.mu.Lock("pruneState.recentlyDeleted")
	defer b.mu.Unlock()

	for key, deletedAt := range b.recentlyDeleted {
		if now.Sub(deletedAt) >= queueDeletedRecentlyWindowSecs*time.Second {
			delete(b.recentlyDeleted, key)
			pruned++
		}
	}

	return pruned
}
