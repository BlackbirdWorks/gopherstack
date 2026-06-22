package acm

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	// defaultIdempotencyRetention is how long idempotency tokens are retained.
	defaultIdempotencyRetention = 24 * time.Hour
)

// RunJanitor periodically cleans up expired idempotency tokens.
func (b *InMemoryBackend) RunJanitor(ctx context.Context, interval time.Duration) {
	worker.RunTicker(ctx, "acm", "AcmJanitor", interval, 0, b.sweepIdempotencyMaps)
}

func (b *InMemoryBackend) sweepIdempotencyMaps(ctx context.Context) {
	b.mu.Lock("sweepIdempotencyMaps")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	cutoffIdempotency := now.Add(-defaultIdempotencyRetention)
	removedCount := 0

	removedCount += sweepCertTokens(b.idempotencyMap, cutoffIdempotency)
	removedCount += sweepAccountTokens(b.accountIdempotency, cutoffIdempotency)

	b.sweepStaleCerts(now)

	removedCount += b.sweepTimers()

	if removedCount > 0 {
		telemetry.RecordWorkerItems("acm", "AcmJanitor", removedCount)
		logger.Load(ctx).DebugContext(ctx, "ACM janitor: resources purged",
			"count", removedCount)
	}
	telemetry.RecordWorkerTask("acm", "AcmJanitor", "success")
}

// sweepCertTokens removes expired RequestCertificate idempotency tokens across all
// regions and returns the number removed.
func sweepCertTokens(m map[string]map[string]certIdempotencyEntry, cutoff time.Time) int {
	removed := 0
	for _, regionTokens := range m {
		for token, entry := range regionTokens {
			if entry.CreatedAt.Before(cutoff) {
				delete(regionTokens, token)
				removed++
			}
		}
	}

	return removed
}

// sweepAccountTokens removes expired PutAccountConfiguration idempotency tokens across
// all regions and returns the number removed.
func sweepAccountTokens(m map[string]map[string]accountIdempotencyEntry, cutoff time.Time) int {
	removed := 0
	for _, regionTokens := range m {
		for token, entry := range regionTokens {
			if entry.CreatedAt.Before(cutoff) {
				delete(regionTokens, token)
				removed++
			}
		}
	}

	return removed
}

// sweepStaleCerts times out abandoned pending validations and expires certificates whose
// NotAfter has passed, across all regions. Callers must hold b.mu.
func (b *InMemoryBackend) sweepStaleCerts(now time.Time) {
	// Abandoned pending validations (72h limit in AWS).
	cutoffPending := now.Add(-72 * time.Hour)

	for _, regionCerts := range b.certs {
		for _, cert := range regionCerts {
			if cert.Status == statusPendingValidation && cert.CreatedAt.Before(cutoffPending) {
				cert.Status = statusValidationTimedOut
				cert.FailureReason = "VALIDATION_TIMED_OUT"
			}

			if cert.Status == statusIssued && !cert.NotAfter.IsZero() && cert.NotAfter.Before(now) {
				cert.Status = statusExpired
			}
		}
	}
}

func (b *InMemoryBackend) sweepTimers() int {
	removedCount := 0
	for region, regionTimers := range b.timers {
		certs := b.certs[region]
		for arn, timer := range regionTimers {
			cert, ok := certs[arn]
			if !ok {
				timer.Stop()
				delete(regionTimers, arn)
				removedCount++

				continue
			}

			isPending := cert.Status == statusPendingValidation
			hasRenewal := cert.RenewalSummary != nil &&
				cert.RenewalSummary.RenewalStatus == renewalStatusPendingValidation

			if !isPending && !hasRenewal {
				timer.Stop()
				delete(regionTimers, arn)
				removedCount++
			}
		}
	}

	return removedCount
}
