package elasticache_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticache"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *elasticache.InMemoryBackend) string
		verify func(t *testing.T, b *elasticache.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *elasticache.InMemoryBackend) string {
				cluster, err := b.CreateCluster(context.Background(), "test-cluster", "redis", "cache.t3.micro", 6379)
				if err != nil {
					return ""
				}

				return cluster.ClusterID
			},
			verify: func(t *testing.T, b *elasticache.InMemoryBackend, id string) {
				t.Helper()

				p, err := b.DescribeClusters(context.Background(), id, "", 0, false)
				clusters := p.Data
				require.NoError(t, err)
				require.Len(t, clusters, 1)
				assert.Equal(t, id, clusters[0].ClusterID)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *elasticache.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *elasticache.InMemoryBackend, _ string) {
				t.Helper()

				clusters := b.ListAll()
				assert.Empty(t, clusters)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := elasticache.NewInMemoryBackend("redis", "000000000000", "us-east-1", nil)
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := elasticache.NewInMemoryBackend("redis", "000000000000", "us-east-1", nil)
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend("redis", "000000000000", "us-east-1", nil)
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_SnapshotRestore_FullState seeds one instance of every
// resource type Phase 3.3 converted to pkgs/store, snapshots the backend,
// restores into a fresh backend, and checks every resource survived with its
// identity/fields intact. This guards against a table being silently dropped
// during the map -> store.Table conversion. Region-nested per-region table
// round-tripping (a second region within the same resource) is covered
// separately by the internal-package test TestSnapshotRestore_CrossRegion,
// which needs the unexported region-context key not reachable from here.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) { //nolint:maintidx // exhaustive seed+verify
	t.Parallel()

	ctx := t.Context()

	original := elasticache.NewInMemoryBackend("redis", "000000000000", "us-east-1", nil)

	_, err := original.CreateCluster(ctx, "fs-cluster", "redis", "cache.t3.micro", 6379)
	require.NoError(t, err)

	rg, err := original.CreateReplicationGroup(ctx, "fs-rg", "full state rg")
	require.NoError(t, err)

	_, err = original.CreateParameterGroup(ctx, "fs-pg", "redis7", "full state pg")
	require.NoError(t, err)

	_, err = original.CreateSubnetGroup(ctx, "fs-sng", "full state subnet group", []string{"subnet-1"})
	require.NoError(t, err)

	_, err = original.CreateSnapshot(ctx, "fs-snap", "fs-cluster", "")
	require.NoError(t, err)

	_, err = original.CreateCacheSecurityGroup(ctx, "fs-sg", "full state security group")
	require.NoError(t, err)
	_, err = original.AuthorizeCacheSecurityGroupIngress(ctx, "fs-sg", "some-ec2-sg", "000000000000")
	require.NoError(t, err)

	_, err = original.CreateGlobalReplicationGroup(ctx, "fsgrg", "full state global rg", rg.ReplicationGroupID)
	require.NoError(t, err)

	_, err = original.CreateServerlessCache(ctx, "fs-serverless", "full state serverless", "redis")
	require.NoError(t, err)
	_, err = original.CreateServerlessCacheSnapshot(ctx, "fs-serverless-snap", "fs-serverless", "")
	require.NoError(t, err)

	_, err = original.CreateUser(ctx, "fs-user", "fs-user-name", "on ~* &* +@all", "redis", true)
	require.NoError(t, err)
	_, err = original.CreateUserGroup(ctx, "fs-usergroup", "redis", []string{"fs-user"})
	require.NoError(t, err)

	// Hardcoded to match the builtin offering ID in backend_ops2.go's
	// builtinReservedOfferings (unexported, so not reachable from this
	// external test package by name).
	const builtinOfferingID = "31153cd5-4ce6-45a9-b6ce-7f0b6789b8fa"
	_, err = original.PurchaseReservedCacheNodesOffering(ctx, builtinOfferingID, "fs-reserved-node", 1)
	require.NoError(t, err)

	snap := original.Snapshot(ctx)
	require.NotNil(t, snap)

	fresh := elasticache.NewInMemoryBackend("redis", "000000000000", "us-east-1", nil)
	require.NoError(t, fresh.Restore(ctx, snap))

	clusters, err := fresh.DescribeClusters(ctx, "fs-cluster", "", 0, false)
	require.NoError(t, err)
	require.Len(t, clusters.Data, 1)

	rgs, err := fresh.DescribeReplicationGroups(ctx, "fs-rg", "", 0)
	require.NoError(t, err)
	require.Len(t, rgs.Data, 1)

	pgs, err := fresh.DescribeParameterGroups(ctx, "fs-pg", "", 0)
	require.NoError(t, err)
	require.Len(t, pgs.Data, 1)

	sngs, err := fresh.DescribeSubnetGroups(ctx, "fs-sng", "", 0)
	require.NoError(t, err)
	require.Len(t, sngs.Data, 1)

	snaps, err := fresh.DescribeSnapshots(ctx, "fs-snap", "", "", "", "", 0)
	require.NoError(t, err)
	require.Len(t, snaps.Data, 1)

	sgs, err := fresh.DescribeCacheSecurityGroups(ctx, "fs-sg", "", 0)
	require.NoError(t, err)
	require.Len(t, sgs.Data, 1)

	grgs, err := fresh.DescribeGlobalReplicationGroups(ctx, "ldgnf-fsgrg", "", 0)
	require.NoError(t, err)
	require.Len(t, grgs.Data, 1)

	scs, err := fresh.DescribeServerlessCaches(ctx, "fs-serverless", "", 0)
	require.NoError(t, err)
	require.Len(t, scs.Data, 1)

	scSnaps, err := fresh.DescribeServerlessCacheSnapshots(ctx, "fs-serverless", "fs-serverless-snap", "", 0)
	require.NoError(t, err)
	require.Len(t, scSnaps.Data, 1)

	users, err := fresh.DescribeUsers(ctx, "fs-user", "", 0)
	require.NoError(t, err)
	require.Len(t, users.Data, 1)

	userGroups, err := fresh.DescribeUserGroups(ctx, "fs-usergroup", "", 0)
	require.NoError(t, err)
	require.Len(t, userGroups.Data, 1)

	reservedNodes, err := fresh.DescribeReservedCacheNodes(ctx, "fs-reserved-node", "", "", "", 0)
	require.NoError(t, err)
	require.Len(t, reservedNodes.Data, 1)
}

// TestInMemoryBackend_Restore_IncompatibleVersion feeds Restore a
// well-formed-but-wrong-version snapshot (simulating an older/incompatible
// on-disk format) and checks it is discarded cleanly -- resetting the backend
// to empty rather than erroring or partially decoding the mismatched shape.
func TestInMemoryBackend_Restore_IncompatibleVersion(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	original := elasticache.NewInMemoryBackend("redis", "000000000000", "us-east-1", nil)
	_, err := original.CreateCluster(ctx, "versioned-cluster", "redis", "cache.t3.micro", 6379)
	require.NoError(t, err)

	// Craft a snapshot with an incompatible version by round-tripping through
	// a generic map: bump "version" to a value that can never match the
	// current elasticacheSnapshotVersion constant.
	var raw map[string]any

	require.NoError(t, json.Unmarshal(original.Snapshot(ctx), &raw))
	raw["version"] = -1

	badSnap, err := json.Marshal(raw)
	require.NoError(t, err)

	fresh := elasticache.NewInMemoryBackend("redis", "000000000000", "us-east-1", nil)
	_, err = fresh.CreateCluster(ctx, "pre-existing", "redis", "cache.t3.micro", 6379)
	require.NoError(t, err)

	require.NoError(t, fresh.Restore(ctx, badSnap))

	all := fresh.ListAll()
	assert.Empty(t, all,
		"incompatible-version restore must reset to empty, not keep pre-existing or partially decoded state")
}

func TestBackend_AddInternalSeedHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *elasticache.InMemoryBackend)
		name string
	}{
		{
			name: "AddCacheSecurityGroupInternal",
			run: func(t *testing.T, b *elasticache.InMemoryBackend) {
				t.Helper()
				b.AddCacheSecurityGroupInternal(&elasticache.CacheSecurityGroup{
					Name:        "seeded-sg",
					Description: "seeded",
					ARN:         "arn:aws:elasticache:us-east-1:000000000000:securitygroup:seeded-sg",
					OwnerID:     "000000000000",
				})
				assert.Equal(t, 1, elasticache.CacheSecurityGroupCount(b))
			},
		},
		{
			name: "AddGlobalReplicationGroupInternal",
			run: func(t *testing.T, b *elasticache.InMemoryBackend) {
				t.Helper()
				b.AddGlobalReplicationGroupInternal(&elasticache.GlobalReplicationGroup{
					GlobalReplicationGroupID: "ldgnf-seeded",
					Description:              "seeded",
					Status:                   "available",
					ARN:                      "arn:aws:elasticache:us-east-1:000000000000:globalreplicationgroup:ldgnf-seeded",
					Engine:                   "redis",
				})
				assert.Equal(t, 1, elasticache.GlobalReplicationGroupCount(b))
			},
		},
		{
			name: "AddServerlessCacheInternal",
			run: func(t *testing.T, b *elasticache.InMemoryBackend) {
				t.Helper()
				b.AddServerlessCacheInternal(&elasticache.ServerlessCache{
					Name:   "seeded-sc",
					Status: "available",
					ARN:    "arn:aws:elasticache:us-east-1:000000000000:serverlesscache:seeded-sc",
					Engine: "redis",
				})
				assert.Equal(t, 1, elasticache.ServerlessCacheCount(b))
			},
		},
		{
			name: "AddServerlessCacheSnapshotInternal",
			run: func(t *testing.T, b *elasticache.InMemoryBackend) {
				t.Helper()
				b.AddServerlessCacheSnapshotInternal(&elasticache.ServerlessCacheSnapshot{
					Name:                "seeded-snap",
					Status:              "available",
					ARN:                 "arn:aws:elasticache:us-east-1:000000000000:serverlesssnapshot:seeded-snap",
					ServerlessCacheName: "some-cache",
				})
				assert.Equal(t, 1, elasticache.ServerlessCacheSnapshotCount(b))
			},
		},
		{
			name: "AddUserInternal",
			run: func(t *testing.T, b *elasticache.InMemoryBackend) {
				t.Helper()
				b.AddUserInternal(&elasticache.User{
					UserID:       "seeded-user",
					UserName:     "seeded",
					Status:       "active",
					ARN:          "arn:aws:elasticache:us-east-1:000000000000:user:seeded-user",
					Engine:       "redis",
					AccessString: "on ~* +@all",
				})
				assert.Equal(t, 1, elasticache.UserCount(b))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
			b.Reset()
			tt.run(t, b)
		})
	}
}

func TestBackend_ExportCountHelpers_InitiallyZero(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
	b.Reset()

	assert.Equal(t, 0, elasticache.CacheSecurityGroupCount(b))
	assert.Equal(t, 0, elasticache.GlobalReplicationGroupCount(b))
	assert.Equal(t, 0, elasticache.ServerlessCacheCount(b))
	assert.Equal(t, 0, elasticache.ServerlessCacheSnapshotCount(b))
	assert.Equal(t, 0, elasticache.UserCount(b))
}

func TestHandler_Reset_ClearsExtendedResourceMaps(t *testing.T) {
	t.Parallel()

	backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
	backend.Reset()

	backend.AddCacheSecurityGroupInternal(&elasticache.CacheSecurityGroup{Name: "sg1", ARN: "arn:sg1"})
	backend.AddGlobalReplicationGroupInternal(
		&elasticache.GlobalReplicationGroup{GlobalReplicationGroupID: "grg1", ARN: "arn:grg1"},
	)
	backend.AddServerlessCacheInternal(&elasticache.ServerlessCache{Name: "sc1", ARN: "arn:sc1"})
	backend.AddServerlessCacheSnapshotInternal(&elasticache.ServerlessCacheSnapshot{Name: "snap1", ARN: "arn:snap1"})
	backend.AddUserInternal(&elasticache.User{UserID: "u1", ARN: "arn:u1"})

	assert.Equal(t, 1, elasticache.CacheSecurityGroupCount(backend))
	assert.Equal(t, 1, elasticache.GlobalReplicationGroupCount(backend))
	assert.Equal(t, 1, elasticache.ServerlessCacheCount(backend))
	assert.Equal(t, 1, elasticache.ServerlessCacheSnapshotCount(backend))
	assert.Equal(t, 1, elasticache.UserCount(backend))

	h := elasticache.NewHandler(backend)
	h.Reset()

	assert.Equal(t, 0, elasticache.CacheSecurityGroupCount(backend))
	assert.Equal(t, 0, elasticache.GlobalReplicationGroupCount(backend))
	assert.Equal(t, 0, elasticache.ServerlessCacheCount(backend))
	assert.Equal(t, 0, elasticache.ServerlessCacheSnapshotCount(backend))
	assert.Equal(t, 0, elasticache.UserCount(backend))
}

func TestBackend_Persistence_ExtendedResourcesRoundTrip(t *testing.T) {
	t.Parallel()

	backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	// Seed data.
	backend.AddCacheSecurityGroupInternal(&elasticache.CacheSecurityGroup{Name: "sg1", ARN: "arn:sg1"})
	backend.AddGlobalReplicationGroupInternal(&elasticache.GlobalReplicationGroup{
		GlobalReplicationGroupID: "ldgnf-grg1",
		ARN:                      "arn:grg1",
		Status:                   "available",
	})
	backend.AddServerlessCacheInternal(&elasticache.ServerlessCache{Name: "sc1", ARN: "arn:sc1"})
	backend.AddServerlessCacheSnapshotInternal(&elasticache.ServerlessCacheSnapshot{
		Name:   "snap1",
		ARN:    "arn:snap1",
		Status: "available",
	})
	backend.AddUserInternal(&elasticache.User{UserID: "u1", ARN: "arn:u1", Status: "active"})

	snap := backend.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
	err := b2.Restore(t.Context(), snap)
	require.NoError(t, err)

	assert.Equal(t, 1, elasticache.CacheSecurityGroupCount(b2))
	assert.Equal(t, 1, elasticache.GlobalReplicationGroupCount(b2))
	assert.Equal(t, 1, elasticache.ServerlessCacheCount(b2))
	assert.Equal(t, 1, elasticache.ServerlessCacheSnapshotCount(b2))
	assert.Equal(t, 1, elasticache.UserCount(b2))
}

func TestHandler_Persistence_Snapshot_Restore(t *testing.T) {
	t.Parallel()

	backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
	h := elasticache.NewHandler(backend)

	// Seed some data.
	backend.AddCacheSecurityGroupInternal(&elasticache.CacheSecurityGroup{Name: "sg1", ARN: "arn:sg1"})

	// Snapshot via handler.
	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	// New backend + handler, restore.
	b2 := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
	h2 := elasticache.NewHandler(b2)
	err := h2.Restore(t.Context(), snap)
	require.NoError(t, err)

	assert.Equal(t, 1, elasticache.CacheSecurityGroupCount(b2))
}

func TestBackend_Persistence_NewFieldsRoundTrip(t *testing.T) {
	t.Parallel()

	b1 := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b1.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID:                       "persist-rg",
		Description:              "persistence test",
		ClusterModeEnabled:       true,
		AuthTokenEnabled:         true,
		AuthToken:                "persist-token",
		KmsKeyID:                 "arn:aws:kms:us-east-1:123:key/abc",
		AtRestEncryptionEnabled:  true,
		TransitEncryptionEnabled: true,
		TransitEncryptionMode:    "preferred",
		DataTieringEnabled:       false,
		SnapshotRetentionLimit:   3,
		NotificationTopicArn:     "arn:aws:sns:us-east-1:123:topic",
	})
	require.NoError(t, err)

	snap := b1.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
	err = b2.Restore(t.Context(), snap)
	require.NoError(t, err)

	page, err := b2.DescribeReplicationGroups(context.Background(), "persist-rg", "", 0)
	require.NoError(t, err)
	require.Len(t, page.Data, 1)

	rg := page.Data[0]
	assert.True(t, rg.ClusterModeEnabled)
	assert.True(t, rg.AuthTokenEnabled)
	assert.True(t, rg.AtRestEncryptionEnabled)
	assert.True(t, rg.TransitEncryptionEnabled)
	assert.Equal(t, "preferred", rg.TransitEncryptionMode)
	assert.Equal(t, 3, rg.SnapshotRetentionLimit)
	assert.Equal(t, "arn:aws:sns:us-east-1:123:topic", rg.NotificationTopicArn)
}

// TestBackend_Persistence_gopherstack31dm_NewFields round-trips
// ServerlessCache.NetworkType and ReplicationGroup.Durability -- the two
// gopherstack-31dm fields with real, settable state (see
// handler_new_sdk_fields_test.go for the wire-shape half of this coverage)
// -- through Snapshot/Restore. Both are additive omitempty fields on structs
// that persist whole (persistence.go's backendSnapshot embeds *ServerlessCache/
// *ReplicationGroup directly), so no elasticacheSnapshotVersion bump is
// needed or permitted for this change.
func TestBackend_Persistence_gopherstack31dm_NewFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		seed  func(t *testing.T, b *elasticache.InMemoryBackend)
		check func(t *testing.T, b *elasticache.InMemoryBackend)
		name  string
	}{
		{
			name: "serverlesscache_networktype",
			seed: func(t *testing.T, b *elasticache.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateServerlessCacheFull(context.Background(), elasticache.ServerlessCreateOpts{
					Name:        "persist-sc-networktype",
					Engine:      "redis",
					NetworkType: "dual_stack",
				})
				require.NoError(t, err)
			},
			check: func(t *testing.T, b *elasticache.InMemoryBackend) {
				t.Helper()

				page, err := b.DescribeServerlessCaches(context.Background(), "persist-sc-networktype", "", 0)
				require.NoError(t, err)
				require.Len(t, page.Data, 1)
				assert.Equal(t, "dual_stack", page.Data[0].NetworkType)
			},
		},
		{
			name: "replicationgroup_durability",
			seed: func(t *testing.T, b *elasticache.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
					ID:          "persist-rg-durability",
					Description: "durability persistence test",
					Durability:  "sync",
				})
				require.NoError(t, err)
			},
			check: func(t *testing.T, b *elasticache.InMemoryBackend) {
				t.Helper()

				page, err := b.DescribeReplicationGroups(context.Background(), "persist-rg-durability", "", 0)
				require.NoError(t, err)
				require.Len(t, page.Data, 1)
				assert.Equal(t, "sync", page.Data[0].Durability)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b1 := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
			tt.seed(t, b1)

			snap := b1.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
			require.NoError(t, b2.Restore(t.Context(), snap))

			tt.check(t, b2)
		})
	}
}

// ----------------------------------------
// GetSupportedOperations includes new ops
// ----------------------------------------

func TestBackend_Persistence_UserGroupIds(t *testing.T) {
	t.Parallel()

	b1 := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b1.CreateUser(context.Background(), "persist-user", "persist-user", "on ~* +@all", "redis", false)
	require.NoError(t, err)

	_, err = b1.CreateUserGroup(context.Background(), "persist-ug", "redis", []string{"persist-user"})
	require.NoError(t, err)

	_, err = b1.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID:           "persist-ug-rg",
		Description:  "persist user groups",
		UserGroupIDs: []string{"persist-ug"},
	})
	require.NoError(t, err)

	snap := b1.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
	err = b2.Restore(t.Context(), snap)
	require.NoError(t, err)

	page, err := b2.DescribeReplicationGroups(context.Background(), "persist-ug-rg", "", 0)
	require.NoError(t, err)
	require.Len(t, page.Data, 1)
	assert.Contains(t, page.Data[0].UserGroupIDs, "persist-ug")
}

// TestBackend_Persistence_UserGroupServerlessCaches verifies that
// UserGroup.AssignedServerlessCacheIDs (wire ServerlessCaches, the reverse
// of ServerlessCache.UserGroupID) survives a Snapshot/Restore round trip.
// The field itself is never persisted (like AssignedReplicationGroupIDs, it
// is recomputed from the serverless caches map on every read), so this
// locks that the recomputation still finds the association after the
// underlying ServerlessCache.UserGroupID has round-tripped through
// persistence.
func TestBackend_Persistence_UserGroupServerlessCaches(t *testing.T) {
	t.Parallel()

	b1 := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b1.CreateUserGroup(context.Background(), "persist-ug-sc", "redis", nil)
	require.NoError(t, err)

	_, err = b1.CreateServerlessCacheFull(context.Background(), elasticache.ServerlessCreateOpts{
		Name:        "persist-sc",
		Engine:      "redis",
		UserGroupID: "persist-ug-sc",
	})
	require.NoError(t, err)

	snap := b1.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
	err = b2.Restore(t.Context(), snap)
	require.NoError(t, err)

	page, err := b2.DescribeUserGroups(context.Background(), "persist-ug-sc", "", 0)
	require.NoError(t, err)
	require.Len(t, page.Data, 1)
	assert.Contains(t, page.Data[0].AssignedServerlessCacheIDs, "persist-sc")
}
