package athena

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	defaultAthenaJanitorInterval = time.Minute
	defaultAthenaExecutionTTL    = 24 * time.Hour

	athenaWorkerServiceName   = "athena"
	executionSweeperComponent = "QueryExecutionSweeper"
)

// isTerminalExecution reports whether the given query execution state is terminal.
func isTerminalExecution(state string) bool {
	return state == stateSucceeded || state == stateFailed || state == stateCancelled
}

// isTerminalSession reports whether a session state is terminal (eligible for
// eviction once stale).
func isTerminalSession(state string) bool {
	return state == sessionStateTerminated || state == sessionStateFailed
}

// isTerminalCalculation reports whether a calculation state is terminal.
func isTerminalCalculation(state string) bool {
	return state == calcStateCompleted || state == calcStateFailed || state == calcStateCanceled
}

// Janitor is the Athena background worker that evicts completed query executions,
// their cached result sets, and stale sessions/calculations after a configurable
// TTL to prevent unbounded growth of in-memory state.
type Janitor struct {
	Backend      *InMemoryBackend
	Interval     time.Duration
	ExecutionTTL time.Duration
	// TaskTimeout bounds each individual janitor task. When non-zero, each task
	// runs with a child context that expires after this duration, preventing a
	// stalled operation from blocking the janitor loop indefinitely.
	TaskTimeout time.Duration
}

// NewJanitor creates a new Athena Janitor for the given backend.
// Zero values for interval or executionTTL fall back to defaults.
func NewJanitor(backend *InMemoryBackend, interval, executionTTL time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultAthenaJanitorInterval
	}

	if executionTTL == 0 {
		executionTTL = defaultAthenaExecutionTTL
	}

	return &Janitor{
		Backend:      backend,
		Interval:     interval,
		ExecutionTTL: executionTTL,
	}
}

// Run runs the janitor loop until ctx is cancelled.
func (j *Janitor) Run(ctx context.Context) {
	g := worker.NewGroup(ctx, athenaWorkerServiceName)
	g.Ticker(
		executionSweeperComponent,
		j.Interval,
		j.TaskTimeout,
		j.sweep,
	)

	<-ctx.Done()
	g.Stop()
}

// SweepOnce runs a single sweep pass. Exposed for testing.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.sweep(ctx)
}

// sweep evicts stale terminal executions (and their result sets), sessions, and
// calculations.
func (j *Janitor) sweep(ctx context.Context) {
	cutoff := float64(time.Now().Add(-j.ExecutionTTL).UnixMilli()) / millisToSeconds

	count := j.sweepExecutions(cutoff)
	count += j.sweepSessions(cutoff)
	count += j.sweepCalculations(cutoff)

	telemetry.RecordWorkerTask(athenaWorkerServiceName, executionSweeperComponent, "success")

	if count == 0 {
		return
	}

	telemetry.RecordWorkerItems(athenaWorkerServiceName, executionSweeperComponent, count)

	logger.Load(ctx).InfoContext(ctx, "Athena janitor: stale resources evicted", "count", count)
}

// sweepExecutions removes query executions in terminal states whose completion
// time predates cutoff, along with their cached result sets. It collects
// candidates under a read lock, then deletes under the write lock, re-verifying
// each candidate to avoid racing a concurrent revival — this narrows the write
// lock to the deletion phase rather than holding it across the whole scan.
func (j *Janitor) sweepExecutions(cutoff float64) int {
	b := j.Backend

	b.mu.RLock("AthenaJanitor-collectExecutions")

	candidates := make([]string, 0)

	for id, qe := range b.queryExecutions {
		if isEvictable(qe.Status.State, qe.Status.CompletionDateTime, cutoff, isTerminalExecution) {
			candidates = append(candidates, id)
		}
	}

	b.mu.RUnlock()

	if len(candidates) == 0 {
		return 0
	}

	swept := 0

	b.mu.Lock("AthenaJanitor-deleteExecutions")

	for _, id := range candidates {
		qe, ok := b.queryExecutions[id]
		if !ok || !isEvictable(qe.Status.State, qe.Status.CompletionDateTime, cutoff, isTerminalExecution) {
			continue
		}

		delete(b.queryExecutions, id)
		delete(b.queryResults, id)

		swept++
	}

	b.mu.Unlock()

	return swept
}

// sweepSessions removes terminal sessions whose end time predates cutoff.
func (j *Janitor) sweepSessions(cutoff float64) int {
	b := j.Backend

	b.mu.RLock("AthenaJanitor-collectSessions")

	candidates := make([]string, 0)

	for id, s := range b.sessions {
		if isEvictable(s.Status.State, sessionEndTime(s), cutoff, isTerminalSession) {
			candidates = append(candidates, id)
		}
	}

	b.mu.RUnlock()

	if len(candidates) == 0 {
		return 0
	}

	swept := 0

	b.mu.Lock("AthenaJanitor-deleteSessions")

	for _, id := range candidates {
		s, ok := b.sessions[id]
		if !ok || !isEvictable(s.Status.State, sessionEndTime(s), cutoff, isTerminalSession) {
			continue
		}

		delete(b.sessions, id)

		swept++
	}

	b.mu.Unlock()

	return swept
}

// sweepCalculations removes terminal calculations whose completion time predates
// cutoff.
func (j *Janitor) sweepCalculations(cutoff float64) int {
	b := j.Backend

	b.mu.RLock("AthenaJanitor-collectCalculations")

	candidates := make([]string, 0)

	for id, c := range b.calculations {
		if isEvictable(c.Status.State, c.Status.CompletionDateTime, cutoff, isTerminalCalculation) {
			candidates = append(candidates, id)
		}
	}

	b.mu.RUnlock()

	if len(candidates) == 0 {
		return 0
	}

	swept := 0

	b.mu.Lock("AthenaJanitor-deleteCalculations")

	for _, id := range candidates {
		c, ok := b.calculations[id]
		if !ok || !isEvictable(c.Status.State, c.Status.CompletionDateTime, cutoff, isTerminalCalculation) {
			continue
		}

		delete(b.calculations, id)

		swept++
	}

	b.mu.Unlock()

	return swept
}

// isEvictable reports whether a resource in the given state, with the given
// completion timestamp, is terminal and older than cutoff.
func isEvictable(state string, completion, cutoff float64, terminal func(string) bool) bool {
	return terminal(state) && completion > 0 && completion < cutoff
}

// sessionEndTime returns the timestamp used to age a session for eviction,
// preferring EndDateTime and falling back to LastModifiedDateTime.
func sessionEndTime(s *Session) float64 {
	if s.Status.EndDateTime > 0 {
		return s.Status.EndDateTime
	}

	return s.Status.LastModifiedDateTime
}
