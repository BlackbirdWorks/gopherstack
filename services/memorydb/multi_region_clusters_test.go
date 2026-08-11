package memorydb_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBackend_DescribeMultiRegionParameterGroups_WithData tests filtering with existing data.
func TestBackend_DescribeMultiRegionParameterGroups_WithData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		filterName string
		wantCount  int
		wantErr    bool
	}{
		{
			name:      "all groups",
			wantCount: 4, // 4 default multi-region parameter groups are seeded on startup
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
			groups, err := b.DescribeMultiRegionParameterGroups(context.Background(), tt.filterName)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, groups, tt.wantCount)
		})
	}
}

// TestRefinement3_ListAllowedMultiRegionClusterUpdates_Backend tests backend directly.
func TestListAllowedMultiRegionClusterUpdates(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	_, err := b.CreateMultiRegionCluster(context.Background(), &memorydb.ExportedCreateMultiRegionClusterRequest{
		MultiRegionClusterNameSuffix: "mrc-test",
		NodeType:                     "db.r6g.large",
	})
	require.NoError(t, err)

	types, err := b.ListAllowedMultiRegionClusterUpdates(context.Background(), "virv-mrc-test")
	require.NoError(t, err)
	assert.NotEmpty(t, types)
}

// TestRefinement3_ListAllowedMultiRegionClusterUpdates_Backend_NotFound tests error for unknown cluster.
func TestListAllowedMultiRegionClusterUpdates_NotFound(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	_, err := b.ListAllowedMultiRegionClusterUpdates(context.Background(), "no-such-mrc")
	require.Error(t, err)
}

// TestRefinement3_UpdateMultiRegionCluster_Backend tests UpdateMultiRegionCluster backend directly.
func TestUpdateMultiRegionCluster(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	_, err := b.CreateMultiRegionCluster(context.Background(), &memorydb.ExportedCreateMultiRegionClusterRequest{
		MultiRegionClusterNameSuffix: "upd-test",
		NodeType:                     "db.r6g.large",
	})
	require.NoError(t, err)

	mrc, err := b.UpdateMultiRegionCluster(context.Background(), &memorydb.ExportedUpdateMultiRegionClusterRequest{
		MultiRegionClusterName: "virv-upd-test",
		Description:            "updated",
		NodeType:               "db.r6g.xlarge",
	})
	require.NoError(t, err)
	assert.Equal(t, "updated", mrc.Description)
	assert.Equal(t, "db.r6g.xlarge", mrc.NodeType)
}

// TestRefinement3_UpdateMultiRegionCluster_Backend_NotFound tests error for unknown cluster.
func TestUpdateMultiRegionCluster_NotFound(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	_, err := b.UpdateMultiRegionCluster(context.Background(), &memorydb.ExportedUpdateMultiRegionClusterRequest{
		MultiRegionClusterName: "no-such-mrc",
	})
	require.Error(t, err)
}

// TestRefinement3_UpdateMultiRegionCluster_NodeType verifies NodeType is updated.
func TestUpdateMultiRegionCluster_NodeType(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	_, err := b.CreateMultiRegionCluster(context.Background(), &memorydb.ExportedCreateMultiRegionClusterRequest{
		MultiRegionClusterNameSuffix: "nt-test",
		NodeType:                     "db.r6g.large",
	})
	require.NoError(t, err)

	mrc, err := b.UpdateMultiRegionCluster(context.Background(), &memorydb.ExportedUpdateMultiRegionClusterRequest{
		MultiRegionClusterName: "virv-nt-test",
		NodeType:               "db.r6g.2xlarge",
	})
	require.NoError(t, err)
	assert.Equal(t, "db.r6g.2xlarge", mrc.NodeType)
}

// TestCreateMultiRegionCluster_UnknownParameterGroup proves CreateMultiRegionCluster
// rejects a MultiRegionParameterGroupName naming no known parameter group, instead
// of silently storing the dangling reference.
func TestCreateMultiRegionCluster_UnknownParameterGroup(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	_, err := b.CreateMultiRegionCluster(context.Background(), &memorydb.ExportedCreateMultiRegionClusterRequest{
		MultiRegionClusterNameSuffix:  "bad-mrpg",
		NodeType:                      "db.r6g.large",
		MultiRegionParameterGroupName: "no-such-mrpg",
	})
	require.ErrorIs(t, err, memorydb.ErrMultiRegionParameterGroupNotFound)
}

// TestUpdateMultiRegionCluster_UnknownParameterGroup proves UpdateMultiRegionCluster
// rejects a MultiRegionParameterGroupName naming no known parameter group.
func TestUpdateMultiRegionCluster_UnknownParameterGroup(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	_, err := b.CreateMultiRegionCluster(context.Background(), &memorydb.ExportedCreateMultiRegionClusterRequest{
		MultiRegionClusterNameSuffix: "upd-bad-mrpg",
		NodeType:                     "db.r6g.large",
	})
	require.NoError(t, err)

	_, err = b.UpdateMultiRegionCluster(context.Background(), &memorydb.ExportedUpdateMultiRegionClusterRequest{
		MultiRegionClusterName:        "virv-upd-bad-mrpg",
		MultiRegionParameterGroupName: "no-such-mrpg",
	})
	require.ErrorIs(t, err, memorydb.ErrMultiRegionParameterGroupNotFound)
}

// TestRefinement1_MultiRegionParameterGroupNotFound verifies the named sentinel is used.
func TestMultiRegionParameterGroupNotFound(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	_, err := b.DescribeMultiRegionParameterGroups(context.Background(), "no-such")

	require.Error(t, err)
	require.ErrorIs(t, err, memorydb.ErrMultiRegionParameterGroupNotFound)
}

// TestBackend_MultiRegionParameterGroups tests DescribeMultiRegionParameterGroups.
func TestBackend_MultiRegionParameterGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		filterName string
		wantErr    bool
		wantCount  int
	}{
		{
			name:      "list returns seeded defaults",
			wantCount: 4,
		},
		{
			name:       "not found by name",
			filterName: "no-such",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
			groups, err := b.DescribeMultiRegionParameterGroups(context.Background(), tt.filterName)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, groups, tt.wantCount)
		})
	}
}
