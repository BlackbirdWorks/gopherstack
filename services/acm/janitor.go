package acm

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	// defaultIdempotencyRetention is how long idempotency tokens are retained.
	defaultIdempotencyRetention = 24 * time.Hour
)

// RunJanitor periodically cleans up expired idempotency tokens.
func (b *InMemoryBackend) RunJanitor(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.sweepIdempotencyMaps(ctx)
		}
	}
}

func (b *InMemoryBackend) sweepIdempotencyMaps(ctx context.Context) {
	b.mu.Lock("sweepIdempotencyMaps")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	cutoffIdempotency := now.Add(-defaultIdempotencyRetention)
	removedCount := 0

	for token, entry := range b.idempotencyMap {
		if entry.CreatedAt.Before(cutoffIdempotency) {
			delete(b.idempotencyMap, token)
			removedCount++
		}
	}

	for token, entry := range b.accountIdempotency {
		if entry.CreatedAt.Before(cutoffIdempotency) {
			delete(b.accountIdempotency, token)
			removedCount++
		}
	}

	// Abandoned pending validations (72h limit in AWS)
	cutoffPending := now.Add(-72 * time.Hour)

	for _, cert := range b.certs {
		if cert.Status == statusPendingValidation && cert.CreatedAt.Before(cutoffPending) {
			cert.Status = statusValidationTimedOut
			cert.FailureReason = "VALIDATION_TIMED_OUT"
		}

		if cert.Status == statusIssued && !cert.NotAfter.IsZero() && cert.NotAfter.Before(now) {
			cert.Status = statusExpired
		}
	}

	removedCount += b.sweepTimers()

	if removedCount > 0 {
		telemetry.RecordWorkerItems("acm", "AcmJanitor", removedCount)
		logger.Load(ctx).DebugContext(ctx, "ACM janitor: resources purged",
			"count", removedCount)
	}
	telemetry.RecordWorkerTask("acm", "AcmJanitor", "success")
}

func (b *InMemoryBackend) sweepTimers() int {
	removedCount := 0
	for arn, timer := range b.timers {
		cert, ok := b.certs[arn]
		if !ok {
			timer.Stop()
			delete(b.timers, arn)
			removedCount++

			continue
		}

		isPending := cert.Status == statusPendingValidation
		hasRenewal := cert.RenewalSummary != nil && cert.RenewalSummary.RenewalStatus == renewalStatusPendingValidation

		if !isPending && !hasRenewal {
			timer.Stop()
			delete(b.timers, arn)
			removedCount++
		}
	}

	return removedCount
}
