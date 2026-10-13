package omics

import "time"

const (
	statusSubmitted  = "SUBMITTED"
	statusInProgress = "IN_PROGRESS"
	statusStarting   = "STARTING"
	statusActivating = "ACTIVATING"

	jobHopProgress = 1
	jobHopDone     = 2
	runHopStarting = 1
	runHopRunning  = 2
	runHopDone     = 3
)

// SetLifecycleDelay makes async resources (workflows, stores, shares, runs, import/export/activation
// jobs) advance on a timer, one hop per d. Zero keeps the poll-driven default.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) { b.lifecycleNanos.Store(int64(d)) }

func (b *InMemoryBackend) timedLifecycle() bool { return b.lifecycleNanos.Load() > 0 }

// afterHops runs fn under the write lock once hops*delay has elapsed; callers hold b.mu.
func (b *InMemoryBackend) afterHops(hops int, fn func()) {
	d := time.Duration(b.lifecycleNanos.Load()) * time.Duration(hops)

	time.AfterFunc(d, func() {
		b.mu.Lock("lifecycle")
		defer b.mu.Unlock()

		fn()
	})
}

// jobInitialStatus is the status a new import/export/activation job starts in.
func (b *InMemoryBackend) jobInitialStatus() string {
	if b.timedLifecycle() {
		return statusSubmitted
	}

	return statusCompleted
}

// scheduleJob walks a job SUBMITTED -> IN_PROGRESS -> COMPLETED when timed lifecycle is on.
// progress and complete mutate the stored job; they are no-ops if it was deleted.
func (b *InMemoryBackend) scheduleJob(progress, complete func(now time.Time)) {
	if !b.timedLifecycle() {
		return
	}

	b.afterHops(jobHopProgress, func() { progress(time.Now().UTC()) })
	b.afterHops(jobHopDone, func() { complete(time.Now().UTC()) })
}
