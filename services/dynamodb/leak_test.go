package dynamodb_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	ddb "github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// TestShardIteratorStore_PutEvictsExpiredAboveThreshold verifies that once the
// store grows past the sweep threshold, inserting a new iterator opportunistically
// drops expired entries — bounding the store even when Sweep() is never called.
func TestShardIteratorStore_PutEvictsExpiredAboveThreshold(t *testing.T) {
	t.Parallel()

	store := ddb.NewShardIteratorStore()

	// Fill to the threshold so the next Put triggers the inline sweep.
	threshold := ddb.ShardIteratorSweepThresholdForTest
	for range threshold {
		_, err := store.Put("table", 0)
		require.NoError(t, err)
	}
	require.Equal(t, threshold, store.Size())

	// Backdate everything, then insert one more entry. The expired entries must
	// be swept, leaving only the freshly-inserted one.
	store.ExpireAllShardIteratorsForTest()

	_, err := store.Put("table", 1)
	require.NoError(t, err)

	require.Equal(t, 1, store.Size(),
		"expired iterators must be evicted on Put once past the threshold")
}

// TestShardIteratorStore_PutBelowThresholdKeepsEntries confirms the sweep is a
// no-op below the threshold so steady-state Puts stay cheap.
func TestShardIteratorStore_PutBelowThresholdKeepsEntries(t *testing.T) {
	t.Parallel()

	store := ddb.NewShardIteratorStore()

	_, err := store.Put("table", 0)
	require.NoError(t, err)
	store.ExpireAllShardIteratorsForTest()

	_, err = store.Put("table", 1)
	require.NoError(t, err)

	// Below threshold: no eager sweep, so the expired entry remains.
	require.Equal(t, 2, store.Size())
}
