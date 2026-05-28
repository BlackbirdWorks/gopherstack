package elasticache_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticache"
)

// ----------------------------------------
// CreateServerlessCacheFull
// ----------------------------------------

func TestBackend_CreateServerlessCacheFull_AllFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantKms    string
		wantGroup  string
		wantSnap   string
		opts       elasticache.ServerlessCreateOpts
		wantRetain int32
	}{
		{
			name: "with_kms_and_user_group",
			opts: elasticache.ServerlessCreateOpts{
				Name:                   "sc-kms",
				Engine:                 "redis",
				KmsKeyID:               "arn:aws:kms:us-east-1:123456789012:key/mrk-abc123",
				UserGroupID:            "my-group",
				DailySnapshotTime:      "05:00",
				SnapshotRetentionLimit: 7,
			},
			wantKms:    "arn:aws:kms:us-east-1:123456789012:key/mrk-abc123",
			wantGroup:  "my-group",
			wantSnap:   "05:00",
			wantRetain: 7,
		},
		{
			name: "valkey_major_version",
			opts: elasticache.ServerlessCreateOpts{
				Name:   "sc-valkey",
				Engine: "valkey",
			},
		},
		{
			name: "with_security_groups",
			opts: elasticache.ServerlessCreateOpts{
				Name:             "sc-sgs",
				Engine:           "redis",
				SecurityGroupIDs: []string{"sg-111", "sg-222"},
				SubnetIDs:        []string{"subnet-aaa", "subnet-bbb"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

			sc, err := b.CreateServerlessCacheFull(tt.opts)
			require.NoError(t, err)
			assert.Equal(t, tt.opts.Name, sc.Name)
			assert.NotEmpty(t, sc.ARN)
			assert.NotEmpty(t, sc.Status)

			if tt.wantKms != "" {
				assert.Equal(t, tt.wantKms, sc.KmsKeyID)
			}

			if tt.wantGroup != "" {
				assert.Equal(t, tt.wantGroup, sc.UserGroupID)
			}

			if tt.wantSnap != "" {
				assert.Equal(t, tt.wantSnap, sc.DailySnapshotTime)
			}

			if tt.wantRetain > 0 {
				assert.Equal(t, tt.wantRetain, sc.SnapshotRetentionLimit)
			}
		})
	}
}

func TestBackend_CreateServerlessCacheFull_AlreadyExists(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	opts := elasticache.ServerlessCreateOpts{Name: "dup-sc", Engine: "redis"}
	_, err := b.CreateServerlessCacheFull(opts)
	require.NoError(t, err)

	_, err = b.CreateServerlessCacheFull(opts)
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticache.ErrServerlessCacheAlreadyExists)
}

// ----------------------------------------
// ModifyServerlessCacheFull
// ----------------------------------------

func TestBackend_ModifyServerlessCacheFull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantUG  string
		opts    elasticache.ServerlessModifyOpts
		wantRet int32
		noUG    bool
	}{
		{
			name: "update_description_and_snapshot",
			opts: elasticache.ServerlessModifyOpts{
				Description:       "updated desc",
				DailySnapshotTime: "03:00",
				SnapshotRetentionLimit: func() *int32 {
					v := int32(14)

					return &v
				}(),
			},
			wantRet: 14,
		},
		{
			name: "add_user_group",
			opts: elasticache.ServerlessModifyOpts{
				UserGroupID: "grp-abc",
			},
			wantUG: "grp-abc",
		},
		{
			name: "remove_user_group",
			opts: elasticache.ServerlessModifyOpts{
				RemoveUserGroup: true,
			},
			noUG: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")
			_, err := b.CreateServerlessCache("mod-sc", "original", "redis")
			require.NoError(t, err)

			if tt.noUG {
				_, err = b.ModifyServerlessCacheFull(
					"mod-sc",
					elasticache.ServerlessModifyOpts{UserGroupID: "pre-group"},
				)
				require.NoError(t, err)
			}

			sc, err := b.ModifyServerlessCacheFull("mod-sc", tt.opts)
			require.NoError(t, err)

			if tt.wantRet > 0 {
				assert.Equal(t, tt.wantRet, sc.SnapshotRetentionLimit)
			}

			if tt.wantUG != "" {
				assert.Equal(t, tt.wantUG, sc.UserGroupID)
			}

			if tt.noUG {
				assert.Empty(t, sc.UserGroupID)
			}
		})
	}
}

// ----------------------------------------
// CreateSubnetGroupFull (with VpcId)
// ----------------------------------------

func TestBackend_CreateSubnetGroupFull_WithVpcId(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	sg, err := b.CreateSubnetGroupFull("sng-vpc", "with vpc", "vpc-0abc123", []string{"subnet-1", "subnet-2"})
	require.NoError(t, err)
	assert.Equal(t, "sng-vpc", sg.Name)
	assert.Equal(t, "vpc-0abc123", sg.VpcID)
	assert.Len(t, sg.SubnetIDs, 2)
	assert.NotEmpty(t, sg.ARN)
}

func TestBackend_CreateSubnetGroupFull_AlreadyExists(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateSubnetGroupFull("dup-sng", "dup", "vpc-111", nil)
	require.NoError(t, err)

	_, err = b.CreateSubnetGroupFull("dup-sng", "dup", "vpc-111", nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticache.ErrSubnetGroupAlreadyExists)
}

// ----------------------------------------
// CopySnapshotFull (with KmsKeyId)
// ----------------------------------------

func TestBackend_CopySnapshotFull_WithKmsKey(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateReplicationGroupFull(elasticache.ReplicationGroupCreateOpts{
		ID: "kms-rg", Description: "for copy",
	})
	require.NoError(t, err)

	_, err = b.CreateSnapshot("original-snap", "", "kms-rg")
	require.NoError(t, err)

	copied, err := b.CopySnapshotFull("original-snap", "encrypted-copy", "arn:aws:kms:us-east-1:000000000000:key/key-1")
	require.NoError(t, err)
	assert.Equal(t, "encrypted-copy", copied.SnapshotName)
	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/key-1", copied.KmsKeyID)
	assert.NotEmpty(t, copied.ARN)
}

func TestBackend_CopySnapshotFull_SourceNotFound(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CopySnapshotFull("no-such-snap", "target", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticache.ErrSnapshotNotFound)
}

func TestBackend_CopySnapshotFull_TargetAlreadyExists(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateReplicationGroupFull(elasticache.ReplicationGroupCreateOpts{
		ID: "dup-copy-rg", Description: "dup",
	})
	require.NoError(t, err)

	_, err = b.CreateSnapshot("src-snap", "", "dup-copy-rg")
	require.NoError(t, err)

	_, err = b.CreateSnapshot("dst-snap", "", "dup-copy-rg")
	require.NoError(t, err)

	_, err = b.CopySnapshotFull("src-snap", "dst-snap", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticache.ErrSnapshotAlreadyExists)
}

// ----------------------------------------
// CreateUserGroupValidated
// ----------------------------------------

func TestBackend_CreateUserGroupValidated_UsersExist(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateUser("u-valid-1", "u-valid-1", "on ~* +@all", "redis", true)
	require.NoError(t, err)
	_, err = b.CreateUser("u-valid-2", "u-valid-2", "on ~* +@all", "redis", true)
	require.NoError(t, err)

	ug, err := b.CreateUserGroupValidated("validated-ug", "validated", "redis", []string{"u-valid-1", "u-valid-2"})
	require.NoError(t, err)
	assert.Equal(t, "validated-ug", ug.UserGroupID)
	assert.Len(t, ug.UserIDs, 2)
}

func TestBackend_CreateUserGroupValidated_UserNotFound(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateUserGroupValidated("fail-ug", "fail", "redis", []string{"nonexistent-user"})
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticache.ErrGroupUserNotFound)
}

// ----------------------------------------
// DeleteUserSafe
// ----------------------------------------

func TestBackend_DeleteUserSafe_NotInGroup(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateUser("safe-del", "safe-del", "on ~* +@all", "redis", true)
	require.NoError(t, err)

	u, err := b.DeleteUserSafe("safe-del")
	require.NoError(t, err)
	assert.Equal(t, "safe-del", u.UserID)
}

func TestBackend_DeleteUserSafe_InGroup_Fails(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateUser("grp-member", "grp-member", "on ~* +@all", "redis", true)
	require.NoError(t, err)
	_, err = b.CreateUserGroup("owns-member", "", "redis", []string{"grp-member"})
	require.NoError(t, err)

	_, err = b.DeleteUserSafe("grp-member")
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticache.ErrUserNotInGroup)
}

func TestBackend_DeleteUserSafe_NotFound(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.DeleteUserSafe("no-such-user")
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticache.ErrUserNotFound)
}

// ----------------------------------------
// Service update tracking
// ----------------------------------------

func TestBackend_BatchApplyUpdateAction_TracksUpdateActions(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateReplicationGroupFull(elasticache.ReplicationGroupCreateOpts{
		ID: "track-rg", Description: "tracking",
	})
	require.NoError(t, err)

	_, err = b.BatchApplyUpdateAction(
		[]string{"track-rg"},
		nil,
		"20240101-001-security-patch",
	)
	require.NoError(t, err)

	actions := b.ListUpdateActionsByServiceUpdate("20240101-001-security-patch")
	require.Len(t, actions, 1)
	assert.Equal(t, "track-rg", actions[0].ReplicationGroupID)
	assert.Equal(t, "20240101-001-security-patch", actions[0].ServiceUpdateName)
}

func TestBackend_BatchApplyUpdateAction_MultipleTargets(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	for _, id := range []string{"multi-rg-1", "multi-rg-2"} {
		_, err := b.CreateReplicationGroupFull(elasticache.ReplicationGroupCreateOpts{
			ID: id, Description: "multi",
		})
		require.NoError(t, err)
	}

	_, err := b.CreateClusterWithOptions("multi-cl-1", "redis", "cache.t3.micro", "", "", "", 1, 0)
	require.NoError(t, err)

	_, err = b.BatchApplyUpdateAction(
		[]string{"multi-rg-1", "multi-rg-2"},
		[]string{"multi-cl-1"},
		"multi-patch",
	)
	require.NoError(t, err)

	actions := b.ListUpdateActionsByServiceUpdate("multi-patch")
	assert.Len(t, actions, 3)
}

// ----------------------------------------
// DescribeServiceUpdatesFull — seeded updates
// ----------------------------------------

func TestBackend_DescribeServiceUpdatesFull_SeededData(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	data, _, err := b.DescribeServiceUpdatesFull("", nil, "", 0)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(data), 1)
}

func TestBackend_DescribeServiceUpdatesFull_FilterByName(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	data, _, err := b.DescribeServiceUpdatesFull("20240101-001-security-patch", nil, "", 0)
	require.NoError(t, err)
	require.Len(t, data, 1)
	assert.Equal(t, "20240101-001-security-patch", data[0].ServiceUpdateName)
}

func TestBackend_DescribeServiceUpdatesFull_FilterByStatus(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	data, _, err := b.DescribeServiceUpdatesFull("", []string{"available"}, "", 0)
	require.NoError(t, err)
	for _, su := range data {
		assert.Equal(t, "available", su.Status)
	}
}

// ----------------------------------------
// DescribeUpdateActionsFull
// ----------------------------------------

func TestBackend_DescribeUpdateActionsFull_FilterByUpdateName(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateReplicationGroupFull(elasticache.ReplicationGroupCreateOpts{
		ID: "ua-filter-rg", Description: "filter",
	})
	require.NoError(t, err)

	_, err = b.BatchApplyUpdateAction([]string{"ua-filter-rg"}, nil, "patch-a")
	require.NoError(t, err)

	_, err = b.BatchApplyUpdateAction([]string{"ua-filter-rg"}, nil, "patch-b")
	require.NoError(t, err)

	data, _, err := b.DescribeUpdateActionsFull("patch-a", "", 0)
	require.NoError(t, err)
	require.Len(t, data, 1)
	assert.Equal(t, "patch-a", data[0].ServiceUpdateName)
}

// ----------------------------------------
// GlobalReplicationGroup NodeGroupCount
// ----------------------------------------

func TestBackend_IncreaseNodeGroupsInGRG_UpdatesCount(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	grg, err := b.CreateGlobalReplicationGroup("mygrg", "desc", "")
	require.NoError(t, err)
	initialCount := grg.NodeGroupCount

	updated, err := b.IncreaseNodeGroupsInGlobalReplicationGroup(grg.GlobalReplicationGroupID, 3)
	require.NoError(t, err)
	assert.Greater(t, updated.NodeGroupCount, initialCount)
	assert.Equal(t, int32(3), updated.NodeGroupCount)
}

func TestBackend_DecreaseNodeGroupsInGRG_UpdatesCount(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	grg, err := b.CreateGlobalReplicationGroup("dec-grg", "desc", "")
	require.NoError(t, err)

	_, err = b.IncreaseNodeGroupsInGlobalReplicationGroup(grg.GlobalReplicationGroupID, 5)
	require.NoError(t, err)

	updated, err := b.DecreaseNodeGroupsInGlobalReplicationGroup(grg.GlobalReplicationGroupID, 2)
	require.NoError(t, err)
	assert.Equal(t, int32(2), updated.NodeGroupCount)
}

// ----------------------------------------
// DescribeEngineDefaultParameters — real defaults
// ----------------------------------------

func TestBackend_DescribeEngineDefaultParameters_Redis(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	p, err := b.DescribeEngineDefaultParameters("redis7", "", 0)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(p.Data), 3)

	names := make(map[string]bool)
	for _, param := range p.Data {
		names[param.Name] = true
	}

	assert.True(t, names["maxmemory-policy"])
	assert.True(t, names["hz"])
}

func TestBackend_DescribeEngineDefaultParameters_Memcached(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	p, err := b.DescribeEngineDefaultParameters("memcached1.6", "", 0)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(p.Data), 2)

	names := make(map[string]bool)
	for _, param := range p.Data {
		names[param.Name] = true
	}

	assert.True(t, names["max_item_size"])
}

func TestBackend_DescribeEngineDefaultParameters_Valkey(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	p, err := b.DescribeEngineDefaultParameters("valkey8", "", 0)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(p.Data), 3)
}

// ----------------------------------------
// Reserved cache node ARN
// ----------------------------------------

func TestBackend_PurchaseReservedCacheNode_HasARN(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	rcn, err := b.PurchaseReservedCacheNodesOffering("31153cd5-4ce6-45a9-b6ce-7f0b6789b8fa", "", 1)
	require.NoError(t, err)
	assert.NotEmpty(t, rcn.ARN)
	assert.Contains(t, rcn.ARN, "arn:aws:elasticache")
	assert.Contains(t, rcn.ARN, "reserved-instance")
}

func TestBackend_PurchaseReservedCacheNode_AutoIDUnique(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	rcn1, err := b.PurchaseReservedCacheNodesOffering("31153cd5-4ce6-45a9-b6ce-7f0b6789b8fa", "", 1)
	require.NoError(t, err)

	rcn2, err := b.PurchaseReservedCacheNodesOffering("31153cd5-4ce6-45a9-b6ce-7f0b6789b8fa", "", 2)
	require.NoError(t, err)

	assert.NotEqual(t, rcn1.ReservedCacheNodeID, rcn2.ReservedCacheNodeID)
}

// ----------------------------------------
// AppendUpdateActions / ListUpdateActionsByServiceUpdate
// ----------------------------------------

func TestBackend_AppendUpdateActions(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	actions := []*elasticache.UpdateAction{
		{ReplicationGroupID: "rg-1", ServiceUpdateName: "upd-1", UpdateActionStatus: "scheduling"},
		{ReplicationGroupID: "rg-2", ServiceUpdateName: "upd-1", UpdateActionStatus: "scheduling"},
	}

	b.AppendUpdateActions(actions)

	got := b.ListUpdateActionsByServiceUpdate("upd-1")
	assert.Len(t, got, 2)

	none := b.ListUpdateActionsByServiceUpdate("nonexistent")
	assert.Empty(t, none)
}
