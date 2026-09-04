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
	g := worker.NewGroup(ctx, "acm")
	g.Ticker("AcmJanitor", interval, 0, b.sweepIdempotencyMaps)

	<-ctx.Done()
	g.Stop()
}

func (b *InMemoryBackend) sweepIdempotencyMaps(ctx context.Context) {
	b.mu.Lock("sweepIdempotencyMaps")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	cutoffIdempotency := now.Add(-b.getIdempotencyRetentionLocked())
	removedCount := 0

	certCreatedAt := func(e certIdempotencyEntry) time.Time { return e.CreatedAt }
	accountCreatedAt := func(e accountIdempotencyEntry) time.Time { return e.CreatedAt }
	removedCount += sweepExpiredTokens(b.idempotencyMap, cutoffIdempotency, certCreatedAt)
	removedCount += sweepExpiredTokens(b.accountIdempotency, cutoffIdempotency, accountCreatedAt)
	// The ACME resource family's Create* idempotency tokens (endpoints/EABs/
	// domain-validations) previously had no TTL sweep at all -- unlike
	// idempotencyMap/accountIdempotency above, they grew unbounded for every
	// token-bearing Create call, and DeleteAcmeEndpoint's cascade delete
	// (acme_endpoints.go) only cleans its own endpointIdempotency entry, not
	// the eabIdempotency/domainValidationIdempotency entries of the children
	// it cascade-deletes -- so those were also orphaned with no cleanup path.
	acmeFingerprint := func(e acmeIdempotencyEntry) time.Time { return e.CreatedAt }
	removedCount += sweepExpiredTokens(b.endpointIdempotency, cutoffIdempotency, acmeFingerprint)
	removedCount += sweepExpiredTokens(b.eabIdempotency, cutoffIdempotency, acmeFingerprint)
	removedCount += sweepExpiredTokens(b.domainValidationIdempotency, cutoffIdempotency, acmeFingerprint)

	b.sweepStaleCerts(now)

	removedCount += b.sweepTimers()

	if removedCount > 0 {
		telemetry.RecordWorkerItems("acm", "AcmJanitor", removedCount)
		logger.Load(ctx).DebugContext(ctx, "ACM janitor: resources purged",
			"count", removedCount)
	}
	telemetry.RecordWorkerTask("acm", "AcmJanitor", "success")
}

// sweepExpiredTokens removes idempotency-token entries older than cutoff
// across all regions of a region-scoped token map, and returns the number
// removed. Shared by every idempotency-token family in this package
// (RequestCertificate, PutAccountConfiguration, and the ACME endpoint/EAB/
// domain-validation families).
func sweepExpiredTokens[T any](m map[string]map[string]T, cutoff time.Time, createdAt func(T) time.Time) int {
	removed := 0
	for _, regionTokens := range m {
		for token, entry := range regionTokens {
			if createdAt(entry).Before(cutoff) {
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

	for _, cert := range b.certs.All() {
		if cert.Status == statusPendingValidation && cert.CreatedAt.Before(cutoffPending) {
			cert.Status = statusValidationTimedOut
			cert.FailureReason = "VALIDATION_TIMED_OUT"
		}

		if cert.Status == statusIssued && !cert.NotAfter.IsZero() && cert.NotAfter.Before(now) {
			cert.Status = statusExpired
		}
	}
}

func (b *InMemoryBackend) sweepTimers() int {
	removedCount := 0
	for region, regionTimers := range b.timers {
		for arn, timer := range regionTimers {
			cert, ok := b.certs.Get(regionKey(region, arn))
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
