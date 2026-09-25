package acmpca

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestIdempotency_TTLBoundsMapGrowth locks in the fix for b.idempotency growing
// unbounded: idempotentResourceARN already treated an entry past its expiresAt as
// absent, but rememberIdempotency never deleted it, so a long-running backend fed
// unique idempotency tokens leaked memory forever. rememberIdempotency now sweeps
// expired entries on every write (see sweepIdempotencyLocked).
func TestIdempotency_TTLBoundsMapGrowth(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := NewInMemoryBackend("123456789012", "us-east-1")

		const (
			region = "us-east-1"
			op     = "CreateCertificateAuthority"
			arnOne = "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority/one"
			arnTwo = "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority/two"
		)

		b.mu.Lock("test")
		now := time.Now()
		b.rememberIdempotency(region, op, "tok-1", arnOne, now)
		assert.Len(t, b.idempotency, 1)
		b.mu.Unlock()

		// Still inside the window: the token still resolves to its cached ARN.
		time.Sleep(idempotencyWindow - time.Second)

		b.mu.Lock("test")
		gotARN, ok := b.idempotentResourceARN(region, op, "tok-1", time.Now())
		assert.True(t, ok, "replay inside the window must still hit")
		assert.Equal(t, arnOne, gotARN)
		b.mu.Unlock()

		// Cross the window and write a new token: the sweep must reclaim tok-1's
		// now-expired entry, leaving only the new one behind.
		time.Sleep(2 * time.Second)

		b.mu.Lock("test")
		b.rememberIdempotency(region, op, "tok-2", arnTwo, time.Now())
		assert.Len(t, b.idempotency, 1, "expired tok-1 entry must be swept, not accumulate")

		_, hasExpired := b.idempotency[idempotencyCacheKey(region, op, "tok-1")]
		assert.False(t, hasExpired, "expired entry must be deleted from the map")

		_, hitExpired := b.idempotentResourceARN(region, op, "tok-1", time.Now())
		assert.False(t, hitExpired, "an expired token must no longer resolve to its old ARN")
		b.mu.Unlock()
	})
}
