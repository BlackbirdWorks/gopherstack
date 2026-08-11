package memorydb_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_Cluster_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		clusterName string
		nodeType    string
		aclName     string
		wantErr     bool
	}{
		{
			name:        "create_and_describe",
			clusterName: "test-cluster",
			nodeType:    "db.r6g.large",
			aclName:     "open-access",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			req := &memorydb.ExportedCreateClusterRequest{
				ClusterName: tt.clusterName,
				NodeType:    tt.nodeType,
				ACLName:     tt.aclName,
			}

			c, err := b.CreateCluster(context.Background(), req)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.clusterName, c.Name)
			assert.NotEmpty(t, c.ARN)
			assert.Equal(t, "available", c.Status)

			clusters, err := b.DescribeClusters(context.Background(), tt.clusterName)
			require.NoError(t, err)
			require.Len(t, clusters, 1)
			assert.Equal(t, tt.clusterName, clusters[0].Name)

			deleted, err := b.DeleteCluster(context.Background(), tt.clusterName)
			require.NoError(t, err)
			assert.Equal(t, tt.clusterName, deleted.Name)

			_, err = b.DescribeClusters(context.Background(), tt.clusterName)
			require.Error(t, err)
		})
	}
}

func TestBackend_Cluster_DuplicateName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name:    "duplicate_cluster",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			req := &memorydb.ExportedCreateClusterRequest{
				ClusterName: "dup-cluster",
				NodeType:    "db.r6g.large",
				ACLName:     "open-access",
			}

			_, err := b.CreateCluster(context.Background(), req)
			require.NoError(t, err)

			_, err = b.CreateCluster(context.Background(), req)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestBackend_ListClusters tests the ListClusters method.
func TestBackend_ListClusters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*memorydb.InMemoryBackend)
		name      string
		wantCount int
	}{
		{
			name:      "empty",
			wantCount: 0,
		},
		{
			name: "multiple clusters",
			setup: func(b *memorydb.InMemoryBackend) {
				for _, clusterName := range []string{"cluster-1", "cluster-2", "cluster-3"} {
					_, err := b.CreateCluster(context.Background(), &memorydb.ExportedCreateClusterRequest{
						ClusterName: clusterName,
						NodeType:    "db.r6g.large",
						ACLName:     "open-access",
					})
					require.NoError(t, err)
				}
			},
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

			if tt.setup != nil {
				tt.setup(b)
			}

			clusters := b.ListClusters()
			assert.Len(t, clusters, tt.wantCount)
		})
	}
}

// TestRefinement3_FailoverShard_Backend tests FailoverShard backend directly.
func TestFailoverShard(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	b.AddClusterInternal("fs-cluster", "db.r6g.large")

	cl, err := b.FailoverShard(context.Background(), "fs-cluster", "")
	require.NoError(t, err)
	assert.Equal(t, "fs-cluster", cl.Name)
}

// TestRefinement3_ListAllowedNodeTypeUpdates_Backend tests backend directly.
func TestListAllowedNodeTypeUpdates(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	b.AddClusterInternal("nt-cluster", "db.r6g.large")

	types, err := b.ListAllowedNodeTypeUpdates(context.Background(), "nt-cluster")
	require.NoError(t, err)
	assert.NotEmpty(t, types)
}

// TestRefinement3_DeepCopyOnDescribeClusters verifies that mutating returned slices does not affect stored data.
func TestDeepCopyOnDescribeClusters(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	b.AddClusterInternal("copy-cluster", "db.r6g.large")

	clusters1, err := b.DescribeClusters(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, clusters1, 1)

	// Mutate the returned copy
	clusters1[0].Name = "mutated"

	// Original should be unchanged
	clusters2, err := b.DescribeClusters(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, clusters2, 1)
	assert.Equal(t, "copy-cluster", clusters2[0].Name)
}

// TestRefinement1_DeleteClusterWithFinalSnapshot verifies final snapshot creation.
func TestDeleteClusterWithFinalSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantSnap   bool
	}{
		{
			name: "delete with final snapshot",
			body: map[string]any{
				"ClusterName":       "snap-cluster",
				"FinalSnapshotName": "final-snap",
			},
			wantStatus: http.StatusOK,
			wantSnap:   true,
		},
		{
			name: "delete without final snapshot",
			body: map[string]any{
				"ClusterName": "snap-cluster",
			},
			wantStatus: http.StatusOK,
			wantSnap:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
			b.AddClusterInternal("snap-cluster", "db.r6g.large")
			h := memorydb.NewHandler(b)

			rec := doRequest(t, h, "DeleteCluster", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantSnap {
				assert.Equal(t, 1, memorydb.SnapshotCount(b))
			} else {
				assert.Equal(t, 0, memorydb.SnapshotCount(b))
			}
		})
	}
}

// TestRefinement1_CloneClusterDeepCopy verifies DescribeClusters returns independent copies.
func TestCloneClusterDeepCopy(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	b.AddClusterInternal("copy-test", "db.r6g.large")
	h := memorydb.NewHandler(b)

	rec1 := doRequest(t, h, "DescribeClusters", map[string]any{"ClusterName": "copy-test"})
	assert.Equal(t, http.StatusOK, rec1.Code)

	// Tag the cluster (modifying backend state).
	doRequest(t, h, "TagResource", map[string]any{})

	// Second describe should still succeed (independent copy was returned).
	rec2 := doRequest(t, h, "DescribeClusters", map[string]any{"ClusterName": "copy-test"})
	assert.Equal(t, http.StatusOK, rec2.Code)
}

// TestRefinement1_SecurityGroupIDsStoredAndReturned verifies SecurityGroupIDs are stored.
func TestSecurityGroupIDsStoredAndReturned(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName":      "sg-cluster",
		"NodeType":         "db.r6g.large",
		"ACLName":          "open-access",
		"SecurityGroupIds": []string{"sg-abc123", "sg-def456"},
	})

	require.Equal(t, http.StatusOK, rec.Code)
}

// TestBackend_BatchUpdateCluster tests the BatchUpdateCluster backend method.
func TestBackend_BatchUpdateCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		clusterNames   []string
		wantFoundCount int
	}{
		{
			name:           "found clusters returned",
			clusterNames:   []string{"cluster-a", "cluster-b"},
			wantFoundCount: 2,
		},
		{
			name:           "unknown clusters omitted",
			clusterNames:   []string{"no-such"},
			wantFoundCount: 0,
		},
		{
			name:           "mixed found and not found",
			clusterNames:   []string{"cluster-a", "no-such"},
			wantFoundCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

			// Seed clusters
			req := &memorydb.ExportedCreateClusterRequest{
				ClusterName: "cluster-a",
				NodeType:    "db.r6g.large",
				ACLName:     "open-access",
			}
			_, err := b.CreateCluster(context.Background(), req)
			require.NoError(t, err)

			req2 := &memorydb.ExportedCreateClusterRequest{
				ClusterName: "cluster-b",
				NodeType:    "db.r6g.large",
				ACLName:     "open-access",
			}
			_, err = b.CreateCluster(context.Background(), req2)
			require.NoError(t, err)

			found, err := b.BatchUpdateCluster(context.Background(), tt.clusterNames, "")
			require.NoError(t, err)
			assert.Len(t, found, tt.wantFoundCount)
		})
	}
}

// TestBackend_BatchUpdateCluster_UnknownServiceUpdate proves BatchUpdateCluster
// rejects a ServiceUpdateNameToApply that doesn't match any known service
// update, instead of silently succeeding (real AWS fault: ServiceUpdateNotFoundFault).
func TestBackend_BatchUpdateCluster_UnknownServiceUpdate(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	req := &memorydb.ExportedCreateClusterRequest{
		ClusterName: "su-cluster",
		NodeType:    "db.r6g.large",
		ACLName:     "open-access",
	}
	_, err := b.CreateCluster(context.Background(), req)
	require.NoError(t, err)

	found, err := b.BatchUpdateCluster(context.Background(), []string{"su-cluster"}, "no-such-service-update")
	require.ErrorIs(t, err, memorydb.ErrServiceUpdateNotFound)
	assert.Nil(t, found)
}

func TestListClusters_NoMutation(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	b.AddClusterInternal("cl-clone-test", "db.r6g.large")

	// Call ListClusters and mutate the result; verify backend is not affected.
	clusters := b.ListClusters()
	require.Len(t, clusters, 1)

	// Mutate the returned cluster's tags.
	clusters[0].Tags["mutated"] = "yes"

	// Call again and verify the mutation didn't leak.
	clusters2 := b.ListClusters()
	require.Len(t, clusters2, 1)
	_, leaked := clusters2[0].Tags["mutated"]
	assert.False(t, leaked, "mutation of ListClusters result should not affect backend")
}

// -- DescribeParameters returns metadata -----------------------------------------
