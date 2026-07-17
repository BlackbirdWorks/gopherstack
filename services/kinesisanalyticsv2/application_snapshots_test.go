package kinesisanalyticsv2_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

func TestBackend_SnapshotLifecycle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)
	_, err := b.CreateApplication(ctx, "snap-app", "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)

	_, err = b.StartApplication(ctx, "snap-app")
	require.NoError(t, err)

	// Create snapshot.
	snap, err := b.CreateApplicationSnapshot(ctx, "snap-app", "snap-1")
	require.NoError(t, err)
	assert.Equal(t, "snap-1", snap.SnapshotName)
	assert.Equal(t, "READY", snap.SnapshotStatus)

	// List snapshots.
	snaps, _, err := b.ListApplicationSnapshots(ctx, "snap-app", "")
	require.NoError(t, err)
	assert.Len(t, snaps, 1)

	// Duplicate snapshot name.
	_, err = b.CreateApplicationSnapshot(ctx, "snap-app", "snap-1")
	require.Error(t, err)

	// Delete snapshot.
	err = b.DeleteApplicationSnapshot(ctx, "snap-app", "snap-1")
	require.NoError(t, err)

	snaps, _, err = b.ListApplicationSnapshots(ctx, "snap-app", "")
	require.NoError(t, err)
	assert.Empty(t, snaps)
}

// TestBackend_DescribeApplicationSnapshot_DirectLookup exercises the direct
// snapshot-by-name lookup path, including the not-found case.
func TestBackend_DescribeApplicationSnapshot_DirectLookup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)

	_, err := b.CreateApplication(ctx, "snap-direct-app", "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)

	_, err = b.StartApplication(ctx, "snap-direct-app")
	require.NoError(t, err)

	_, err = b.CreateApplicationSnapshot(ctx, "snap-direct-app", "snap-direct")
	require.NoError(t, err)

	snap, err := b.DescribeApplicationSnapshot(ctx, "snap-direct-app", "snap-direct")
	require.NoError(t, err)
	assert.Equal(t, "snap-direct", snap.SnapshotName)

	_, err = b.DescribeApplicationSnapshot(ctx, "snap-direct-app", "missing-snap")
	require.ErrorIs(t, err, kinesisanalyticsv2.ErrNotFound)
}

func TestBackend_ListApplicationSnapshotsPagination(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	tests := []struct {
		name          string
		count         int
		wantNextToken bool
	}{
		{
			name:          "single_page",
			count:         5,
			wantNextToken: false,
		},
		{
			name:          "multi_page",
			count:         55, // exceeds kav2DefaultPageSize=50
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend(t)

			_, err := b.CreateApplication(ctx, "paged-snap-app", "FLINK-1_18", "", "", "", nil)
			require.NoError(t, err)

			_, err = b.StartApplication(ctx, "paged-snap-app")
			require.NoError(t, err)

			for i := range tt.count {
				_, err = b.CreateApplicationSnapshot(ctx, "paged-snap-app", fmt.Sprintf("snap-%04d", i))
				require.NoError(t, err)
			}

			snaps, outToken, err := b.ListApplicationSnapshots(ctx, "paged-snap-app", "")
			require.NoError(t, err)

			if tt.wantNextToken {
				assert.Len(t, snaps, 50)
				assert.NotEmpty(t, outToken)

				// Second page.
				var snaps2 []*kinesisanalyticsv2.Snapshot
				var outToken2 string
				snaps2, outToken2, err = b.ListApplicationSnapshots(ctx, "paged-snap-app", outToken)
				require.NoError(t, err)
				assert.Len(t, snaps2, tt.count-50)
				assert.Empty(t, outToken2)
			} else {
				assert.Len(t, snaps, tt.count)
				assert.Empty(t, outToken)
			}
		})
	}
}

// TestBackend_ListApplicationSnapshots_SortedByCreationTime verifies
// ListApplicationSnapshots orders results by creation time regardless of
// insertion/name order.
func TestBackend_ListApplicationSnapshots_SortedByCreationTime(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newTestBackend(t)

	_, err := b.CreateApplication(ctx, "sort-snap-app", "FLINK-1_18", "", "", "", nil)
	require.NoError(t, err)

	_, err = b.StartApplication(ctx, "sort-snap-app")
	require.NoError(t, err)

	for _, name := range []string{"snap-b", "snap-a", "snap-c"} {
		_, err = b.CreateApplicationSnapshot(ctx, "sort-snap-app", name)
		require.NoError(t, err)
	}

	snaps, _, err := b.ListApplicationSnapshots(ctx, "sort-snap-app", "")
	require.NoError(t, err)
	require.Len(t, snaps, 3)

	// Verify sorted by creation time (ascending)
	for i := 1; i < len(snaps); i++ {
		assert.False(t, snaps[i].SnapshotCreation.Before(snaps[i-1].SnapshotCreation),
			"snapshots should be sorted by creation time")
	}
}
