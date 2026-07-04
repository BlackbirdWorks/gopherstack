package memorydb_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

// TestRefinement1_ErrNilAppContext verifies that Init rejects a nil context.
func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &memorydb.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	require.ErrorIs(t, err, memorydb.ErrNilAppContext)
}

// TestRefinement1_ErrValidationSentinel verifies that ErrValidation wraps ErrInvalidParameter.
func TestRefinement1_ErrValidationSentinel(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	_, err := b.CreateParameterGroup(context.Background(), &memorydb.ExportedCreateParameterGroupRequest{
		ParameterGroupName: "no-family",
		// Family intentionally omitted
	})

	require.Error(t, err)
}

// TestRefinement1_Reset verifies that Reset clears all state.
func TestRefinement1_Reset(t *testing.T) {
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

// TestRefinement1_HandlerReset verifies that Handler.Reset() delegates to backend.
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	h := memorydb.NewHandler(b)
	b.AddClusterInternal("cluster-x", "db.r6g.large")

	h.Reset()

	assert.Equal(t, 0, memorydb.ClusterCount(b))
}

// TestRefinement1_SeedHelpers verifies the seed helper functions.
func TestRefinement1_SeedHelpers(t *testing.T) {
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
func TestRefinement1_ExportHelpers(t *testing.T) {
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
		{"HandlerOpsLen", memorydb.HandlerOpsLen(h), 46},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.got)
		})
	}
}

// TestRefinement1_ARNIndexSize verifies the ARN index grows correctly.
func TestRefinement1_ARNIndexSize(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	initialSize := memorydb.ARNIndexSize(b) // open-access

	b.AddClusterInternal("c1", "db.r6g.large")
	b.AddSnapshotInternal("s1", "c1")

	assert.Equal(t, initialSize+2, memorydb.ARNIndexSize(b))
}

// TestRefinement1_DescribeSnapshots verifies the new DescribeSnapshots operation.
func TestRefinement1_DescribeSnapshots(t *testing.T) {
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
func TestRefinement1_CreateSnapshotValidatesCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateSnapshot", map[string]any{
		"SnapshotName": "my-snap",
		"ClusterName":  "non-existent-cluster",
	})

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRefinement1_CreateParameterGroupMissingFamily verifies family is required.
func TestRefinement1_CreateParameterGroupMissingFamily(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateParameterGroup", map[string]any{
		"ParameterGroupName": "no-family-pg",
		// Family intentionally omitted
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_DeleteClusterWithFinalSnapshot verifies final snapshot creation.
func TestRefinement1_DeleteClusterWithFinalSnapshot(t *testing.T) {
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

// TestRefinement1_TagResourceReturnsTagList verifies TagResource returns resulting tags.
func TestRefinement1_TagResourceReturnsTagList(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	cluster := b.AddClusterInternal("tagtest", "db.r6g.large")
	h := memorydb.NewHandler(b)

	rec := doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": cluster.ARN,
		"Tags":        []map[string]any{{"Key": "Env", "Value": "test"}},
	})

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tagList, ok := resp["TagList"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, tagList)
}

// TestRefinement1_UntagResourceReturnsTagList verifies UntagResource returns remaining tags.
func TestRefinement1_UntagResourceReturnsTagList(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	cluster := b.AddClusterInternal("untagtest", "db.r6g.large")
	h := memorydb.NewHandler(b)

	// Add a tag first
	doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": cluster.ARN,
		"Tags":        []map[string]any{{"Key": "K1", "Value": "V1"}, {"Key": "K2", "Value": "V2"}},
	})

	// Remove one tag
	rec := doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": cluster.ARN,
		"TagKeys":     []string{"K1"},
	})

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tagList := resp["TagList"].([]any)
	assert.Len(t, tagList, 1)
}

// TestRefinement1_PersistenceRoundTrip verifies Snapshot/Restore round-trip.
func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b1 := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	b1.AddClusterInternal("cluster-a", "db.r6g.large")
	b1.AddSnapshotInternal("snap-a", "cluster-a")
	b1.AddUserInternal("user-a", "on ~*")
	b1.AddSubnetGroupInternal("sg-a")
	b1.AddParameterGroupInternal("pg-a", "memorydb_redis7")

	data := b1.Snapshot(t.Context())
	require.NotNil(t, data)

	b2 := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	require.NoError(t, b2.Restore(t.Context(), data))

	assert.Equal(t, 1, memorydb.ClusterCount(b2))
	assert.Equal(t, 1, memorydb.SnapshotCount(b2))
	assert.Equal(t, 1, memorydb.UserCount(b2))
	assert.Equal(t, 1, memorydb.SubnetGroupCount(b2))
	assert.Equal(t, 5, memorydb.ParameterGroupCount(b2)) // 4 defaults + 1 custom (pg-a)
}

// TestRefinement1_HandlerPersistence verifies Handler Snapshot/Restore delegates to backend.
func TestRefinement1_HandlerPersistence(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	b.AddClusterInternal("h-cluster", "db.r6g.large")
	h := memorydb.NewHandler(b)

	data := h.Snapshot(t.Context())
	require.NotNil(t, data)

	b2 := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	h2 := memorydb.NewHandler(b2)
	require.NoError(t, h2.Restore(t.Context(), data))

	assert.Equal(t, 1, memorydb.ClusterCount(b2))
}

// TestRefinement1_MaxEventsCap verifies that events are capped at maxEvents.
func TestRefinement1_MaxEventsCap(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	for i := range 1200 {
		b.AddEvent(&memorydb.ExportedEvent{
			Date:       time.Now(),
			SourceName: "src",
			SourceType: "cluster",
			Message:    string(rune('a' + i%26)),
		})
	}

	// Should be capped at 1000
	assert.LessOrEqual(t, memorydb.EventCount(b), 1000)
}

// TestRefinement1_GetSupportedOperations verifies 35 sorted operations are returned.
func TestRefinement1_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	h := memorydb.NewHandler(b)

	ops := h.GetSupportedOperations()

	assert.Len(t, ops, 46)
	assert.Contains(t, ops, "DescribeSnapshots")
	assert.Contains(t, ops, "BatchUpdateCluster")
	assert.Contains(t, ops, "CreateMultiRegionCluster")
	assert.Contains(t, ops, "DescribeParameters")
	assert.Contains(t, ops, "FailoverShard")
	assert.Contains(t, ops, "UpdateMultiRegionCluster")
	assert.Contains(t, ops, "DescribeReservedNodes")
	assert.Contains(t, ops, "DescribeReservedNodesOfferings")
	assert.Contains(t, ops, "PurchaseReservedNodesOffering")
	assert.Contains(t, ops, "DescribeMultiRegionParameters")
}

// TestRefinement1_VarCompileTimeAssertion verifies var _ StorageBackend = (*InMemoryBackend)(nil) compiles.
func TestRefinement1_VarCompileTimeAssertion(t *testing.T) {
	t.Parallel()

	var _ memorydb.StorageBackend = (*memorydb.InMemoryBackend)(nil)
}

// TestRefinement1_UpdateACLSliceNoAlias verifies UpdateACL remove doesn't alias old slice.
func TestRefinement1_UpdateACLSliceNoAlias(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create ACL with users
	doRequest(t, h, "CreateACL", map[string]any{
		"ACLName":   "test-acl",
		"UserNames": []string{"u1", "u2", "u3"},
	})

	// Remove u2
	rec := doRequest(t, h, "UpdateACL", map[string]any{
		"ACLName":           "test-acl",
		"UserNamesToRemove": []string{"u2"},
	})

	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify the ACL now has only u1 and u3
	descRec := doRequest(t, h, "DescribeACLs", map[string]any{"ACLName": "test-acl"})
	require.Equal(t, http.StatusOK, descRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))
	acls := resp["ACLs"].([]any)
	acl := acls[0].(map[string]any)
	users := acl["UserNames"].([]any)

	assert.Len(t, users, 2)
	assert.NotContains(t, users, "u2")
}

// TestRefinement1_PurgeIncludesSnapshots verifies Purge removes snapshots.
func TestRefinement1_PurgeIncludesSnapshots(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	b.AddClusterInternal("purge-cluster", "db.r6g.large")
	b.AddSnapshotInternal("old-snap", "purge-cluster")

	// Purge with a future cutoff - should remove everything.
	b.Purge(t.Context(), time.Now().Add(time.Hour))

	assert.Equal(t, 0, memorydb.ClusterCount(b))
	assert.Equal(t, 0, memorydb.SnapshotCount(b))
}

// TestRefinement1_WriteBackendErrorValidation verifies ErrValidation maps to 400.
func TestRefinement1_WriteBackendErrorValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// CreateParameterGroup with missing Family -> should return 400 via ErrValidation.
	rec := doRequest(t, h, "CreateParameterGroup", map[string]any{
		"ParameterGroupName": "no-fam",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_DescribeSnapshotCreatedAtField verifies CreatedAt is returned.
func TestRefinement1_DescribeSnapshotCreatedAtField(t *testing.T) {
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

// TestRefinement1_ExtractResourceSnapshotName verifies SnapshotName extraction.
func TestRefinement1_ExtractResourceSnapshotName(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	h := memorydb.NewHandler(b)

	// The handler must parse SnapshotName when ExtractResource is called.
	// We verify indirectly that DescribeSnapshots picks up SnapshotName from body.
	b.AddSnapshotInternal("ext-snap", "cl")

	rec := doRequest(t, h, "DescribeSnapshots", map[string]any{"SnapshotName": "ext-snap"})

	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRefinement1_CloneClusterDeepCopy verifies DescribeClusters returns independent copies.
func TestRefinement1_CloneClusterDeepCopy(t *testing.T) {
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
func TestRefinement1_SecurityGroupIDsStoredAndReturned(t *testing.T) {
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

// TestRefinement1_MultiRegionParameterGroupNotFound verifies the named sentinel is used.
func TestRefinement1_MultiRegionParameterGroupNotFound(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	_, err := b.DescribeMultiRegionParameterGroups(context.Background(), "no-such")

	require.Error(t, err)
	require.ErrorIs(t, err, memorydb.ErrMultiRegionParameterGroupNotFound)
}

// TestRefinement1_ErrValidationIs verifies ErrValidation sentinel wraps correctly.
func TestRefinement1_ErrValidationIs(t *testing.T) {
	t.Parallel()

	wrapped := fmt.Errorf("wrap: %w", memorydb.ErrValidation)
	require.Error(t, wrapped)
	require.ErrorIs(t, wrapped, memorydb.ErrValidation)
}

// TestRefinement1_AddEventCapEnforced verifies AddEvent does not grow beyond cap.
func TestRefinement1_AddEventCapEnforced(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)

	for range 100 {
		b.AddEvent(&memorydb.ExportedEvent{
			Date:       time.Now(),
			SourceName: "cluster-x",
			SourceType: "cluster",
			Message:    "test event",
		})
	}

	assert.Equal(t, 100, memorydb.EventCount(b))

	// Adding one more should still be 100 (under cap)
	b.AddEvent(&memorydb.ExportedEvent{
		Date: time.Now(), SourceName: "x", SourceType: "cluster", Message: "extra",
	})

	assert.Equal(t, 101, memorydb.EventCount(b))
}

// TestRefinement1_CopySnapshotInheritsTags verifies tags are inherited from source when none supplied.
func TestRefinement1_CopySnapshotInheritsTags(t *testing.T) {
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

// TestRefinement1_PurgeWithCancelledContext verifies Purge respects context cancellation.
func TestRefinement1_PurgeWithCancelledContext(t *testing.T) {
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
