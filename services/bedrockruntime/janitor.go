package bedrockruntime

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	// defaultAsyncInvokeRetention is how long completed/failed async invocations are kept.
	defaultAsyncInvokeRetention = 24 * time.Hour

	// defaultAsyncInvokeCompletionDelay is how long an async invocation stays InProgress
	// before the janitor advances it to Completed.
	defaultAsyncInvokeCompletionDelay = 5 * time.Second
)

// RunJanitor periodically cleans up old async invocations and advances InProgress ones.
func (b *InMemoryBackend) RunJanitor(ctx context.Context, interval time.Duration) {
	g := worker.NewGroup(ctx, "bedrockruntime")
	g.Ticker("AsyncInvokeAdvancer", interval, 0, b.sweepAsyncInvokes)

	<-ctx.Done()
	g.Stop()
}

// sweepAsyncInvokes runs one janitor pass: advance InProgress invocations and
// clean up old completed/failed ones.
func (b *InMemoryBackend) sweepAsyncInvokes(ctx context.Context) {
	b.advanceAsyncInvokes(ctx)
	b.sweepOldInvocations(ctx)
}

// advanceAsyncInvokes moves InProgress invocations to Completed once the completion delay elapses.
func (b *InMemoryBackend) advanceAsyncInvokes(ctx context.Context) {
	b.mu.Lock("advanceAsyncInvokes")
	defer b.mu.Unlock()

	now := time.Now()
	advancedCount := 0

	for _, inv := range b.asyncInvokes {
		if inv.Status != AsyncInvokeStatusInProgress {
			continue
		}

		if now.Sub(inv.SubmitTime) >= defaultAsyncInvokeCompletionDelay {
			endTime := now.UTC()
			inv.Status = AsyncInvokeStatusCompleted
			inv.EndTime = &endTime
			inv.LastModifiedTime = now.UTC()
			advancedCount++
		}
	}

	if advancedCount > 0 {
		telemetry.RecordWorkerItems("bedrockruntime", "AsyncInvokeAdvancer", advancedCount)
		logger.Load(ctx).DebugContext(ctx, "BedrockRuntime janitor: advanced async invocations",
			"count", advancedCount)
	}

	telemetry.RecordWorkerTask("bedrockruntime", "AsyncInvokeAdvancer", "success")
}

func (b *InMemoryBackend) sweepOldInvocations(ctx context.Context) {
	b.mu.Lock("sweepOldInvocations")
	defer b.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-defaultAsyncInvokeRetention)
	removedCount := 0

	for arn, inv := range b.asyncInvokes {
		// Only remove if it's finished (Completed/Failed) and old,
		// or if it's been in progress for a suspiciously long time (e.g. 48h).
		shouldRemove := false
		if inv.Status != AsyncInvokeStatusInProgress {
			if inv.LastModifiedTime.Before(cutoff) {
				shouldRemove = true
			}
		} else if inv.SubmitTime.Before(now.Add(-48 * time.Hour)) {
			shouldRemove = true
		}

		if shouldRemove {
			if inv.ClientRequestToken != nil {
				delete(b.tokenIndex, *inv.ClientRequestToken)
			}
			delete(b.asyncInvokes, arn)
			removedCount++
		}
	}

	if removedCount > 0 {
		telemetry.RecordWorkerItems("bedrockruntime", "AsyncInvokeCleaner", removedCount)
		logger.Load(ctx).DebugContext(ctx, "BedrockRuntime janitor: cleaned up old async invocations",
			"count", removedCount)
	}
	telemetry.RecordWorkerTask("bedrockruntime", "AsyncInvokeCleaner", "success")
}
