package quicksight_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// TestDeleteAnalysis_EvictedAfterRecoveryWindow locks in the fix for the
// unbounded-memory-growth leak DeleteAnalysis used to have: DeletionTime was
// computed and returned to the caller but never stored, so nothing ever
// evicted the row -- the same leak class fixed for ec2/ecs/medialive/ram/
// acmpca's own delete-waiter tombstones.
func TestDeleteAnalysis_EvictedAfterRecoveryWindow(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := quicksight.NewInMemoryBackend(testAccountID, "us-east-1")

		_, err := b.CreateAnalysis(testAccountID, "evict-analysis", "evict", "", nil, nil, nil)
		require.NoError(t, err)

		deletionTime, err := b.DeleteAnalysis(testAccountID, "evict-analysis", false, 7)
		require.NoError(t, err)

		got, err := b.DescribeAnalysis(testAccountID, "evict-analysis")
		require.NoError(t, err, "a soft-deleted analysis must still describe within its recovery window")
		assert.Equal(t, "DELETED", got.Status)
		require.Equal(t, 1, quicksight.AnalysisCount(b))

		time.Sleep(time.Until(deletionTime) + time.Second)

		// The prune runs lazily on the next write-locked op.
		_, err = b.CreateAnalysis(testAccountID, "evict-analysis-2", "evict2", "", nil, nil, nil)
		require.NoError(t, err)

		assert.Equal(t, 1, quicksight.AnalysisCount(b),
			"the analysis past its DeletionTime must be evicted, leaving only the new one")
	})
}

// TestDeleteAnalysis_KeptWithinRecoveryWindow proves the eviction above does
// not fire early -- matching real AWS, which restores the analysis
// describable until its recovery window elapses.
func TestDeleteAnalysis_KeptWithinRecoveryWindow(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := quicksight.NewInMemoryBackend(testAccountID, "us-east-1")

		_, err := b.CreateAnalysis(testAccountID, "keep-analysis", "keep", "", nil, nil, nil)
		require.NoError(t, err)

		deletionTime, err := b.DeleteAnalysis(testAccountID, "keep-analysis", false, 7)
		require.NoError(t, err)

		time.Sleep(time.Until(deletionTime) - time.Second)

		got, err := b.DescribeAnalysis(testAccountID, "keep-analysis")
		require.NoError(t, err)
		assert.Equal(t, "DELETED", got.Status)
	})
}
