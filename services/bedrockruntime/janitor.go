package bedrockruntime

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	// defaultAsyncInvokeRetention is how long completed/failed async invocations are kept.
	defaultAsyncInvokeRetention = 24 * time.Hour
)

// RunJanitor periodically cleans up old async invocations and their idempotency tokens.
func (b *InMemoryBackend) RunJanitor(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.sweepOldInvocations(ctx)
		}
	}
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
