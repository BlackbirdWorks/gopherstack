package ram_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

// pastRAMDeletedShareTTL is strictly greater than the modeled one-hour
// ramDeletedShareTTL, so pruneDeletedResourceSharesLocked always evicts a
// DELETED share by the time it fires.
const pastRAMDeletedShareTTL = time.Hour + time.Second

// TestDeleteResourceShare_EvictedAfterTTL locks in the fix for the
// unbounded-memory-growth leak DeleteResourceShare's soft-delete comment
// used to describe: a DELETED share was kept in b.resourceShares forever.
// GetResourceShare/ListResourceShares already exclude a DELETED share
// unconditionally (asserted below before the TTL elapses), so the fix here
// is invisible to every normal read path -- only ResourceShareCount, which
// counts the raw map, sees the eviction.
func TestDeleteResourceShare_EvictedAfterTTL(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := ram.NewInMemoryBackend("000000000000", "us-east-1")

		rs, err := b.CreateResourceShare("evict-share", false, nil, nil, nil)
		require.NoError(t, err)

		require.NoError(t, b.DeleteResourceShare(rs.ARN))

		_, err = b.GetResourceShare(rs.ARN)
		require.Error(t, err, "a DELETED share must already read as not-found")
		require.Equal(t, 1, ram.ResourceShareCount(b), "the tombstone row must still be in the map before the TTL")

		time.Sleep(pastRAMDeletedShareTTL)

		// The prune runs lazily on the next write-locked op.
		_, err = b.CreateResourceShare("evict-share-2", false, nil, nil, nil)
		require.NoError(t, err)

		assert.Equal(t, 1, ram.ResourceShareCount(b),
			"the expired DELETED share must be evicted, leaving only the new one")
	})
}

// TestDeleteResourceShare_KeptWithinTTL proves the eviction above does not
// fire early.
func TestDeleteResourceShare_KeptWithinTTL(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := ram.NewInMemoryBackend("000000000000", "us-east-1")

		rs, err := b.CreateResourceShare("keep-share", false, nil, nil, nil)
		require.NoError(t, err)

		require.NoError(t, b.DeleteResourceShare(rs.ARN))

		time.Sleep(time.Hour - time.Second)

		_, err = b.CreateResourceShare("keep-share-2", false, nil, nil, nil)
		require.NoError(t, err)

		assert.Equal(t, 2, ram.ResourceShareCount(b),
			"the DELETED share must still be in the map within the TTL")
	})
}
