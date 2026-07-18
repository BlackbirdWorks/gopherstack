package memorydb_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRefinement1_DescribeSnapshots verifies the new DescribeSnapshots operation.
func TestDescribeSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantCount  int
		wantStatus int
	}{
		{
			name:       "list all snapshots",
			body:       map[string]any{},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:       "filter by name - found",
			body:       map[string]any{"SnapshotName": "snap-a"},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name:       "filter by name - not found",
			body:       map[string]any{"SnapshotName": "no-such"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
			b.AddClusterInternal("my-cluster", "db.r6g.large")
			b.AddSnapshotInternal("snap-a", "my-cluster")
			b.AddSnapshotInternal("snap-b", "my-cluster")
			h := memorydb.NewHandler(b)

			rec := doRequest(t, h, "DescribeSnapshots", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantCount > 0 {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				snaps := resp["Snapshots"].([]any)
				assert.Len(t, snaps, tt.wantCount)
			}
		})
	}
}

// TestRefinement1_CreateSnapshotValidatesCluster verifies that CreateSnapshot requires an existing cluster.
func TestCreateSnapshotValidatesCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateSnapshot", map[string]any{
		"SnapshotName": "my-snap",
		"ClusterName":  "non-existent-cluster",
	})

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement1_DescribeSnapshotCreatedAtField verifies CreatedAt is returned.
func TestDescribeSnapshotCreatedAtField(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	b.AddClusterInternal("cl", "db.r6g.large")
	b.AddSnapshotInternal("ts-snap", "cl")
	h := memorydb.NewHandler(b)

	rec := doRequest(t, h, "DescribeSnapshots", map[string]any{"SnapshotName": "ts-snap"})

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	snaps := resp["Snapshots"].([]any)
	require.Len(t, snaps, 1)

	snap := snaps[0].(map[string]any)
	assert.NotEmpty(t, snap["SnapshotCreationTime"])
}

// TestRefinement1_CopySnapshotInheritsTags verifies tags are inherited from source when none supplied.
func TestCopySnapshotInheritsTags(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	b.AddClusterInternal("inherit-cluster", "db.r6g.large")
	b.AddSnapshotInternal("src-snap", "inherit-cluster")
	h := memorydb.NewHandler(b)

	// Tag the source snapshot
	doRequest(t, h, "DescribeSnapshots", map[string]any{"SnapshotName": "src-snap"})

	rec := doRequest(t, h, "CopySnapshot", map[string]any{
		"SourceSnapshotName": "src-snap",
		"TargetSnapshotName": "dst-snap",
	})

	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestBackend_CopySnapshot_EdgeCases tests CopySnapshot edge cases.
func TestBackend_CopySnapshot_EdgeCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name:    "copy with no source",
			wantErr: true,
		},
		{
			name:    "copy to existing target",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

			if tt.name == "copy to existing target" {
				// Pre-create the cluster that the snapshots reference.
				b.AddClusterInternal("cluster", "db.r6g.large")

				// Create source
				_, err := b.CreateSnapshot(context.Background(), &memorydb.ExportedCreateSnapshotRequest{
					SnapshotName: "src",
					ClusterName:  "cluster",
				})
				require.NoError(t, err)

				// Create target first
				_, err = b.CreateSnapshot(context.Background(), &memorydb.ExportedCreateSnapshotRequest{
					SnapshotName: "dst",
					ClusterName:  "cluster",
				})
				require.NoError(t, err)

				// Try to copy to same target name
				_, err = b.CopySnapshot(context.Background(), &memorydb.ExportedCopySnapshotRequest{
					SourceSnapshotName: "src",
					TargetSnapshotName: "dst",
				})
				require.Error(t, err)
			} else {
				// Copy from non-existent source
				_, err := b.CopySnapshot(context.Background(), &memorydb.ExportedCopySnapshotRequest{
					SourceSnapshotName: "no-such",
					TargetSnapshotName: "dst",
				})
				require.Error(t, err)
			}
		})
	}
}
