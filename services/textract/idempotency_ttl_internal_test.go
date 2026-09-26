package textract

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestClientTokenMaps_TTLBoundsMapGrowth locks in the fix for the four
// ClientRequestToken dedup maps (clientTokenToJobID, adapterClientTokenToID,
// expenseClientTokenToJobID, lendingClientTokenToJobID) growing unbounded: entries
// were never deleted, even once the maxJobHistory cap evicted the job/adapter they
// pointed at, so a long-running backend fed unique tokens leaked memory forever.
// touchClientToken now sweeps entries past clientTokenTTL on every write, and
// clientTokenFresh now honors that TTL on lookup.
func TestClientTokenMaps_TTLBoundsMapGrowth(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := NewInMemoryBackend("123456789012", "us-east-1")
		b.asyncJobDelay = 0 // synchronous completion, no background goroutines to track

		ctx := context.Background()

		first, err := b.StartDocumentTextDetectionWithOptions(ctx, "s3://bucket/doc.pdf", nil, nil, "", "tok-1")
		require.NoError(t, err)
		assert.Len(t, b.clientTokenToJobID["us-east-1"], 1, "one entry recorded after the first Start call")

		// Still inside the window: same token replays the original job.
		time.Sleep(clientTokenTTL - time.Second)

		replay, err := b.StartDocumentTextDetectionWithOptions(ctx, "s3://bucket/doc.pdf", nil, nil, "", "tok-1")
		require.NoError(t, err)
		assert.Equal(t, first.JobID, replay.JobID,
			"a replay inside the window must return the original job, not create a new one")
		assert.Len(t, b.clientTokenToJobID["us-east-1"], 1, "replay must not grow the map")

		// Cross the window and write a new token: the sweep must reclaim tok-1's
		// now-expired entry, leaving only the new one behind.
		time.Sleep(2 * time.Second)

		second, err := b.StartDocumentTextDetectionWithOptions(ctx, "s3://bucket/doc.pdf", nil, nil, "", "tok-2")
		require.NoError(t, err)
		assert.NotEqual(t, first.JobID, second.JobID)
		assert.Len(t, b.clientTokenToJobID["us-east-1"], 1, "expired tok-1 entry must be swept, not accumulate")

		_, hasExpired := b.clientTokenToJobID["us-east-1"]["tok-1"]
		assert.False(t, hasExpired, "expired entry must be deleted from the map")

		_, hasExpiredTS := b.clientTokenCreatedAt[clientTokenKey("job", "us-east-1", "tok-1")]
		assert.False(t, hasExpiredTS, "expired entry's timestamp must also be deleted")

		// A retry of the expired token now creates a genuinely new job instead of
		// replaying the (no longer remembered) original.
		third, err := b.StartDocumentTextDetectionWithOptions(ctx, "s3://bucket/doc.pdf", nil, nil, "", "tok-1")
		require.NoError(t, err)
		assert.NotEqual(t, first.JobID, third.JobID,
			"an expired token must not replay the original job")
	})
}
