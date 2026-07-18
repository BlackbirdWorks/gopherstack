package glue

import (
	"context"
	"time"
)

// advanceJobRunState applies STARTING→RUNNING and RUNNING→SUCCEEDED transitions for a
// single run, consulting the readyMap and doneMap timing tables. Must be called with b.mu held.
func advanceJobRunState(run *JobRun, readyMap, doneMap map[string]time.Time, now time.Time) {
	if readyMap != nil {
		if t, ok := readyMap[run.ID]; ok && now.After(t) {
			if run.JobRunState == stateStarting {
				run.JobRunState = stateRunning
			}
			delete(readyMap, run.ID)
		}
	}

	if doneMap != nil {
		if t, ok := doneMap[run.ID]; ok && now.After(t) {
			if run.JobRunState == stateRunning {
				run.JobRunState = stateSucceeded
				run.CompletedOn = float64(now.Unix())
				run.ExecutionTime = int(jobSucceededDelay.Seconds())
			}
			delete(doneMap, run.ID)
		}
	}
}

// reconcileLocked applies pending lifecycle transitions as of now. Must be called
// with b.mu held. Taking now as a parameter (rather than reading the clock again)
// keeps the due-scan and the application consistent and makes advancement
// deterministically testable.
func (b *InMemoryBackend) reconcileLocked(now time.Time) {
	// Job run transitions: STARTING→RUNNING, RUNNING→SUCCEEDED.
	for jobName, runs := range b.jobRuns {
		readyMap := b.jobRunReadyAt[jobName]
		doneMap := b.jobRunDoneAt[jobName]

		for _, run := range runs {
			advanceJobRunState(run, readyMap, doneMap, now)
		}
	}

	// Crawler transitions:
	//   RUNNING→READY  — crawl completes; create catalog tables from S3 targets.
	//   STOPPING→READY — StopCrawler was issued; the crawler winds down to READY
	//                    without creating tables (the crawl was interrupted).
	for name, readyAt := range b.crawlerReadyAt {
		if now.After(readyAt) {
			c, ok := b.crawlers.Get(name)
			if ok && c.State == stateRunning {
				c.State = stateReady
				c.LastUpdated = float64(now.Unix())
				created := b.createCrawlerTablesLocked(c)
				b.finishCrawlHistoryLocked(name, "COMPLETED", created, now)
			} else if ok && c.State == stateStopping {
				c.State = stateReady
				c.LastUpdated = float64(now.Unix())
				b.finishCrawlHistoryLocked(name, "STOPPED", 0, now)
			}

			delete(b.crawlerReadyAt, name)
		}
	}

	b.pruneOrphanJobRunTimersLocked()
}

// pruneOrphanJobRunTimersLocked removes job-run timing entries whose job or run no
// longer exists. Without this, a stale due timer would make pendingDueLocked report
// work forever, so the reconciler would take the global write lock every tick — the
// exact hot-loop the performance fix is meant to avoid. Must be called with b.mu held.
func (b *InMemoryBackend) pruneOrphanJobRunTimersLocked() {
	for jobName, timers := range b.jobRunReadyAt {
		b.pruneJobTimerMapLocked(jobName, timers, b.jobRunReadyAt)
	}

	for jobName, timers := range b.jobRunDoneAt {
		b.pruneJobTimerMapLocked(jobName, timers, b.jobRunDoneAt)
	}
}

// pruneJobTimerMapLocked drops entries in a single job's timer sub-map for runs that
// no longer exist, and drops the whole sub-map when the job or all its timers are
// gone. Must be called with b.mu held.
func (b *InMemoryBackend) pruneJobTimerMapLocked(
	jobName string,
	timers map[string]time.Time,
	parent map[string]map[string]time.Time,
) {
	live := make(map[string]struct{}, len(b.jobRuns[jobName]))
	for _, run := range b.jobRuns[jobName] {
		live[run.ID] = struct{}{}
	}

	for runID := range timers {
		if _, ok := live[runID]; !ok {
			delete(timers, runID)
		}
	}

	if len(timers) == 0 {
		delete(parent, jobName)
	}
}

// defaultReconcileInterval is how often the managed reconciler wakes to advance
// due lifecycle transitions (job-run STARTING→RUNNING→SUCCEEDED and crawler
// RUNNING/STOPPING→READY). It is derived from the shortest transition delay so
// transitions surface promptly without busy-spinning.
const defaultReconcileInterval = jobTransitionDelay / reconcilerTickDivisor

// pendingDueLocked reports whether any scheduled lifecycle transition is due at
// now. Callers MUST hold at least b.mu.RLock. It is the cheap guard that lets the
// reconciler (and lazy read-path advancement) skip taking the global write lock
// on the overwhelmingly common tick where nothing has come due yet.
func (b *InMemoryBackend) pendingDueLocked(now time.Time) bool {
	for _, timers := range b.jobRunReadyAt {
		for _, t := range timers {
			if now.After(t) {
				return true
			}
		}
	}

	for _, timers := range b.jobRunDoneAt {
		for _, t := range timers {
			if now.After(t) {
				return true
			}
		}
	}

	for _, readyAt := range b.crawlerReadyAt {
		if now.After(readyAt) {
			return true
		}
	}

	return false
}

// advanceStates applies every lifecycle transition whose scheduled time has passed.
// It first scans under a read lock and only upgrades to the global write lock when
// there is work to do, keeping the common (nothing-due) case cheap — this is the
// performance fix for the previous reconciler, which took the write lock every tick
// regardless of pending work.
//
// It is safe for concurrent use and is called both lazily on reads (so SDK waiters
// polling GetJobRun/GetCrawler always observe the true state, even when the
// background reconciler is not running) and periodically by the reconciler loop.
func (b *InMemoryBackend) advanceStates(now time.Time) {
	b.mu.RLock("advanceStates.scan")
	due := b.pendingDueLocked(now)
	b.mu.RUnlock()

	if !due {
		return
	}

	b.mu.Lock("advanceStates.apply")
	defer b.mu.Unlock()

	// reconcileLocked re-checks each timer against now, so it is idempotent when a
	// lazy read and the reconciler race to advance the same resource.
	b.reconcileLocked(now)
}

// StartReconciler starts the managed background reconciler that advances Glue job-run
// and crawler lifecycle transitions. It replaces the previous unmanaged
// `go b.runReconciler()` (which leaked because nothing called Close) with a single
// goroutine owning a stop channel and WaitGroup, so it can be cancelled
// deterministically via ctx or StopReconciler. Calling it while already running is a
// no-op.
//
// ctx originates from the service framework's background-worker lifecycle, so no
// context.Background() is introduced here.
func (b *InMemoryBackend) StartReconciler(ctx context.Context) {
	b.reconcileMu.Lock()
	defer b.reconcileMu.Unlock()

	if b.reconcileOn {
		return
	}

	if b.reconcileInterval <= 0 {
		b.reconcileInterval = defaultReconcileInterval
	}

	stop := make(chan struct{})
	b.reconcileStop = stop
	b.reconcileOn = true

	b.reconcileWG.Add(1)

	go b.reconcileLoop(ctx, stop, b.reconcileInterval)
}

// reconcileLoop is the reconciler goroutine body. It exits cleanly when either the
// context is cancelled or the stop channel is closed, then signals the WaitGroup.
func (b *InMemoryBackend) reconcileLoop(ctx context.Context, stop <-chan struct{}, interval time.Duration) {
	defer b.reconcileWG.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-ticker.C:
			b.advanceStates(time.Now())
		}
	}
}

// StopReconciler signals the reconciler to stop and blocks until the goroutine has
// exited, guaranteeing no leaked goroutine survives shutdown. It is idempotent and
// safe to call even when the reconciler was never started.
func (b *InMemoryBackend) StopReconciler() {
	wasOn := func() bool {
		b.reconcileMu.Lock()
		defer b.reconcileMu.Unlock()

		if !b.reconcileOn {
			return false
		}

		b.reconcileOn = false
		close(b.reconcileStop)

		return true
	}()

	if !wasOn {
		return
	}

	b.reconcileWG.Wait()
}

// Close stops the background reconciler and waits for its goroutine to exit. It is
// retained for callers (and tests) that manage a backend's lifecycle directly; it is
// an idempotent alias for StopReconciler.
func (b *InMemoryBackend) Close() { b.StopReconciler() }
