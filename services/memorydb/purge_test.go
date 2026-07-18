package memorydb_test

import (
	"context"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBackend_Purge tests that the Purge method removes old resources.
func TestBackend_Purge(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantClusters int
	}{
		{
			name:         "purges old clusters",
			wantClusters: 0,
		},
		{
			name:         "keeps recent clusters",
			wantClusters: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
			ctx := t.Context()

			_, err := b.CreateCluster(context.Background(), &memorydb.ExportedCreateClusterRequest{
				ClusterName: "old-cluster",
				NodeType:    "db.r6g.large",
				ACLName:     "open-access",
			})
			require.NoError(t, err)

			switch tt.name {
			case "purges old clusters":
				// Set cutoff to future - purge everything
				b.Purge(ctx, time.Now().Add(time.Hour))
				clusters := b.ListClusters()
				assert.Len(t, clusters, tt.wantClusters)
			case "keeps recent clusters":
				// Set cutoff to past - keep everything
				b.Purge(ctx, time.Now().Add(-time.Hour))
				clusters := b.ListClusters()
				assert.Len(t, clusters, tt.wantClusters)
			}
		})
	}
}

// TestBackend_Purge_CancelledContext tests that Purge respects context cancellation.
func TestBackend_Purge_CancelledContext(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	_, err := b.CreateCluster(context.Background(), &memorydb.ExportedCreateClusterRequest{
		ClusterName: "my-cluster",
		NodeType:    "db.r6g.large",
		ACLName:     "open-access",
	})
	require.NoError(t, err)

	// Purge with cancelled context should be a no-op
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	b.Purge(ctx, time.Now().Add(time.Hour))

	// Cluster should still be there
	clusters := b.ListClusters()
	assert.Len(t, clusters, 1)
}

// TestHandler_Purge tests the handler Purge delegation.
func TestHandler_Purge(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "purge-test",
		"NodeType":    "db.r6g.large",
		"ACLName":     "open-access",
	})

	// Purge through handler - should delegate to backend
	h.Purge(t.Context(), time.Now().Add(time.Hour))

	// Cluster should be gone
	rec := doRequest(t, h, "DescribeClusters", map[string]any{
		"ClusterName": "purge-test",
	})
	assert.Equal(t, 404, rec.Code)
}

// TestRefinement1_PurgeIncludesSnapshots verifies Purge removes snapshots.
func TestPurgeIncludesSnapshots(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	b.AddClusterInternal("purge-cluster", "db.r6g.large")
	b.AddSnapshotInternal("old-snap", "purge-cluster")

	// Purge with a future cutoff - should remove everything.
	b.Purge(t.Context(), time.Now().Add(time.Hour))

	assert.Equal(t, 0, memorydb.ClusterCount(b))
	assert.Equal(t, 0, memorydb.SnapshotCount(b))
}

// TestRefinement1_PurgeWithCancelledContext verifies Purge respects context cancellation.
func TestPurgeWithCancelledContext(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	b.AddClusterInternal("cancel-cluster", "db.r6g.large")

	ctx, cancel := context.WithCancel(t.Context())
	cancel() // cancel immediately

	// Purge with a cancelled context should be a no-op.
	b.Purge(ctx, time.Now().Add(time.Hour))

	// Cluster should still be present.
	assert.Equal(t, 1, memorydb.ClusterCount(b))
}
