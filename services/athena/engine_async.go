package athena

import (
	"context"
	"errors"
	"time"
)

var errQueryTimeout = errors.New("query timeout")

const stateQueued = "QUEUED"
const stateRunning = "RUNNING"

// startEngineQuery registers the cancel hook and runs plan in the background.
// The execution record must already exist in the QUEUED state.
func (b *InMemoryBackend) startEngineQuery(id string, plan *enginePlan) {
	b.mu.Lock("startEngineQuery")
	limits := b.limits

	ctx, cancel := context.WithTimeoutCause(context.Background(), limits.QueryTimeout, errQueryTimeout)
	b.cancels[id] = cancel
	b.mu.Unlock()

	go func() {
		defer cancel()

		if !b.markRunning(id) {
			return
		}

		start := time.Now()
		result, scanned, outcome := plan.run(ctx, limits)

		b.completeEngineQuery(ctx, id, result, scanned, time.Since(start), outcome)
	}()
}

func (b *InMemoryBackend) markRunning(id string) bool {
	b.mu.Lock("markRunning")
	defer b.mu.Unlock()

	qe, ok := b.queryExecutions.Get(id)
	if !ok || qe.Status.State != stateQueued {
		delete(b.cancels, id)

		return false
	}

	qe.Status.State = stateRunning

	return true
}

func (b *InMemoryBackend) completeEngineQuery(
	ctx context.Context, id string,
	result *sqlResult, scanned int64, elapsed time.Duration, outcome statementOutcome,
) {
	b.mu.Lock("completeEngineQuery")
	defer b.mu.Unlock()

	delete(b.cancels, id)

	qe, ok := b.queryExecutions.Get(id)
	if !ok || qe.Status.State == stateCancelled {
		return
	}

	if ctx.Err() != nil {
		outcome = stmtFail(athenaErrTypeSyntax,
			"Query timeout: the query did not finish within %s", b.limits.QueryTimeout)
		if !errors.Is(context.Cause(ctx), errQueryTimeout) {
			outcome = stmtFail(athenaErrTypeSyntax, "Query was interrupted: %v", context.Cause(ctx))
		}
	}

	ms := max(elapsed.Milliseconds(), 1)
	qe.Statistics.EngineExecutionTimeInMillis = ms
	qe.Statistics.TotalExecutionTimeInMillis = ms + qe.Statistics.ServiceProcessingTimeInMillis
	qe.Statistics.DataScannedInBytes = scanned

	if outcome.failed {
		b.finalizeExecution(id, nil, outcome)

		return
	}

	qe.Status.State = stateSucceeded
	qe.Status.CompletionDateTime = nowSeconds()
	b.finalizeExecution(id, result, outcome)
}

func (b *InMemoryBackend) cancelEngineQuery(id string) {
	if cancel, ok := b.cancels[id]; ok {
		cancel()
		delete(b.cancels, id)
	}
}
