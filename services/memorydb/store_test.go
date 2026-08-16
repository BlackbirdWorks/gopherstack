package memorydb_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

// TestRefinement1_Reset verifies that Reset clears all state.
func TestReset(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	b.AddClusterInternal("my-cluster", "db.r6g.large")
	b.AddACLInternal("my-acl")
	b.AddSnapshotInternal("my-snap", "my-cluster")
	b.AddUserInternal("my-user", "on ~*")
	b.AddSubnetGroupInternal("my-sg")
	b.AddParameterGroupInternal("my-pg", "memorydb_redis7")

	require.Equal(t, 1, memorydb.ClusterCount(b))
	require.Equal(t, 2, memorydb.ACLCount(b)) // open-access + my-acl

	b.Reset()

	assert.Equal(t, 0, memorydb.ClusterCount(b))
	assert.Equal(t, 1, memorydb.ACLCount(b)) // only open-access
	assert.Equal(t, 0, memorydb.SnapshotCount(b))
	assert.Equal(t, 0, memorydb.UserCount(b))
	assert.Equal(t, 0, memorydb.SubnetGroupCount(b))
	assert.Equal(t, 4, memorydb.ParameterGroupCount(b)) // 4 default parameter groups re-seeded
}

// TestRefinement1_SeedHelpers verifies the seed helper functions.
func TestInternalSeedHelpersUpdateCounts(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	tests := []struct {
		seed    func()
		wantFn  func() int
		name    string
		wantLen int
	}{
		{
			name:    "AddClusterInternal",
			seed:    func() { b.AddClusterInternal("c1", "db.r6g.large") },
			wantFn:  func() int { return memorydb.ClusterCount(b) },
			wantLen: 1,
		},
		{
			name:    "AddACLInternal",
			seed:    func() { b.AddACLInternal("a1") },
			wantFn:  func() int { return memorydb.ACLCount(b) },
			wantLen: 2, // open-access + a1
		},
		{
			name:    "AddSnapshotInternal",
			seed:    func() { b.AddSnapshotInternal("s1", "c1") },
			wantFn:  func() int { return memorydb.SnapshotCount(b) },
			wantLen: 1,
		},
		{
			name:    "AddUserInternal",
			seed:    func() { b.AddUserInternal("u1", "on ~*") },
			wantFn:  func() int { return memorydb.UserCount(b) },
			wantLen: 1,
		},
		{
			name:    "AddSubnetGroupInternal",
			seed:    func() { b.AddSubnetGroupInternal("sg1") },
			wantFn:  func() int { return memorydb.SubnetGroupCount(b) },
			wantLen: 1,
		},
		{
			name:    "AddParameterGroupInternal",
			seed:    func() { b.AddParameterGroupInternal("pg1", "memorydb_redis7") },
			wantFn:  func() int { return memorydb.ParameterGroupCount(b) },
			wantLen: 5, // 4 defaults + 1 custom
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.seed()
			assert.Equal(t, tt.wantLen, tt.wantFn())
		})
	}
}

// TestRefinement1_ExportHelpers verifies export count helpers return correct values.
func TestExportedCountHelpers(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	h := memorydb.NewHandler(b)

	b.AddClusterInternal("cl1", "db.r6g.large")

	b.AddEvent(&memorydb.ExportedEvent{
		Date:       time.Now(),
		SourceName: "cl1",
		SourceType: "cluster",
		Message:    "Created",
	})

	tests := []struct {
		name string
		got  int
		want int
	}{
		{"ClusterCount", memorydb.ClusterCount(b), 1},
		{"ACLCount", memorydb.ACLCount(b), 1}, // only open-access
		{"SnapshotCount", memorydb.SnapshotCount(b), 0},
		{"UserCount", memorydb.UserCount(b), 0},
		{"SubnetGroupCount", memorydb.SubnetGroupCount(b), 0},
		{"ParameterGroupCount", memorydb.ParameterGroupCount(b), 4}, // 4 default parameter groups seeded
		{"EventCount", memorydb.EventCount(b), 1},
		{"MultiRegionClusterCount", memorydb.MultiRegionClusterCount(b), 0},
		// 45, not 46: "ExportSnapshot" was removed from GetSupportedOperations
		// because it is not a real MemoryDB SDK operation — see handler.go.
		{"HandlerOpsLen", memorydb.HandlerOpsLen(h), 45},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.got)
		})
	}
}

// TestBackend_NewOps_Lifecycle tests the new backend operations end-to-end.
func TestSnapshotMultiRegionEngineEventsLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "snapshot_lifecycle"},
		{name: "multi_region_cluster_lifecycle"},
		{name: "engine_versions"},
		{name: "events"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

			switch tt.name {
			case "snapshot_lifecycle":
				// Pre-create the cluster the snapshot references.
				b.AddClusterInternal("my-cluster", "db.r6g.large")

				req := &memorydb.ExportedCreateSnapshotRequest{
					SnapshotName: "my-snap",
					ClusterName:  "my-cluster",
				}

				s, err := b.CreateSnapshot(context.Background(), req)
				require.NoError(t, err)
				assert.Equal(t, "my-snap", s.Name)
				assert.Equal(t, "my-cluster", s.ClusterName)
				assert.NotEmpty(t, s.ARN)
				assert.Equal(t, "available", s.Status)

				// duplicate
				_, err = b.CreateSnapshot(context.Background(), req)
				require.Error(t, err)

				// copy
				cp, err := b.CopySnapshot(context.Background(), &memorydb.ExportedCopySnapshotRequest{
					SourceSnapshotName: "my-snap",
					TargetSnapshotName: "copy-snap",
				})
				require.NoError(t, err)
				assert.Equal(t, "copy-snap", cp.Name)
				assert.Equal(t, "my-cluster", cp.ClusterName)

				// delete
				deleted, err := b.DeleteSnapshot(context.Background(), "my-snap")
				require.NoError(t, err)
				assert.Equal(t, "my-snap", deleted.Name)

				// delete again → error
				_, err = b.DeleteSnapshot(context.Background(), "my-snap")
				require.Error(t, err)

			case "multi_region_cluster_lifecycle":
				req := &memorydb.ExportedCreateMultiRegionClusterRequest{
					MultiRegionClusterNameSuffix: "my-mrc",
					NodeType:                     "db.r6g.large",
					EngineVersion:                "7.0",
				}

				mrc, err := b.CreateMultiRegionCluster(context.Background(), req)
				require.NoError(t, err)
				assert.Contains(t, mrc.MultiRegionClusterName, "my-mrc")
				assert.Equal(t, "available", mrc.Status)
				assert.Equal(t, "7.0", mrc.EngineVersion)

				// duplicate
				_, err = b.CreateMultiRegionCluster(context.Background(), req)
				require.Error(t, err)

				// describe
				mrcs, err := b.DescribeMultiRegionClusters(context.Background(), "")
				require.NoError(t, err)
				require.Len(t, mrcs, 1)

				// describe by name
				mrcs2, err := b.DescribeMultiRegionClusters(context.Background(), mrc.MultiRegionClusterName)
				require.NoError(t, err)
				require.Len(t, mrcs2, 1)

				// describe by bad name
				_, err = b.DescribeMultiRegionClusters(context.Background(), "no-such")
				require.Error(t, err)

				// delete
				deleted, err := b.DeleteMultiRegionCluster(context.Background(), mrc.MultiRegionClusterName)
				require.NoError(t, err)
				assert.Equal(t, mrc.MultiRegionClusterName, deleted.MultiRegionClusterName)

				// delete again → error
				_, err = b.DeleteMultiRegionCluster(context.Background(), mrc.MultiRegionClusterName)
				require.Error(t, err)

			case "engine_versions":
				versions, err := b.DescribeEngineVersions(
					context.Background(),
					&memorydb.ExportedDescribeEngineVersionsRequest{},
				)
				require.NoError(t, err)
				assert.NotEmpty(t, versions)

				// filter by family
				redis7, err := b.DescribeEngineVersions(
					context.Background(),
					&memorydb.ExportedDescribeEngineVersionsRequest{
						ParameterGroupFamily: "memorydb_redis7",
					},
				)
				require.NoError(t, err)

				for _, ev := range redis7 {
					assert.Equal(t, "memorydb_redis7", ev.ParameterGroupFamily)
				}

				// filter unknown family
				none, err := b.DescribeEngineVersions(
					context.Background(),
					&memorydb.ExportedDescribeEngineVersionsRequest{
						ParameterGroupFamily: "memorydb_redis99",
					},
				)
				require.NoError(t, err)
				assert.Empty(t, none)

			case "events":
				// empty initially
				events, err := b.DescribeEvents(context.Background(), &memorydb.ExportedDescribeEventsRequest{})
				require.NoError(t, err)
				assert.Empty(t, events)

				// add events
				b.AddEvent(&memorydb.ExportedEvent{
					SourceName: "my-cluster",
					SourceType: "cluster",
					Message:    "cluster created",
				})
				b.AddEvent(&memorydb.ExportedEvent{
					SourceName: "other-cluster",
					SourceType: "cluster",
					Message:    "other created",
				})

				// all events
				all, err := b.DescribeEvents(context.Background(), &memorydb.ExportedDescribeEventsRequest{})
				require.NoError(t, err)
				assert.Len(t, all, 2)

				// filter by source name
				filtered, err := b.DescribeEvents(context.Background(), &memorydb.ExportedDescribeEventsRequest{
					SourceName: "my-cluster",
				})
				require.NoError(t, err)
				assert.Len(t, filtered, 1)
				assert.Equal(t, "my-cluster", filtered[0].SourceName)

				// filter by source type
				byType, err := b.DescribeEvents(context.Background(), &memorydb.ExportedDescribeEventsRequest{
					SourceType: "cluster",
				})
				require.NoError(t, err)
				assert.Len(t, byType, 2)
			}
		})
	}
}
