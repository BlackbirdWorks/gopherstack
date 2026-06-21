package elasticache_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticache"
)

// ----------------------------------------
// CacheCluster CRUD (issue: lifecycle)
// ----------------------------------------

func TestBackend_CreateCluster_DefaultEngine(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	cl, err := b.CreateClusterWithOptions(
		context.Background(),
		"default-cluster",
		"",
		"cache.t3.micro",
		"",
		"",
		"",
		1,
		0,
	)
	require.NoError(t, err)
	assert.Equal(t, "default-cluster", cl.ClusterID)
	assert.Equal(t, "available", cl.Status)
	assert.NotEmpty(t, cl.ARN)
	assert.Contains(t, cl.ARN, "arn:aws:elasticache:us-east-1:000000000000:cluster:default-cluster")
}

func TestBackend_CreateCluster_Redis(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	cl, err := b.CreateClusterWithOptions(
		context.Background(),
		"redis-cluster",
		"redis",
		"cache.r6g.large",
		"",
		"",
		"",
		1,
		0,
	)
	require.NoError(t, err)
	assert.Equal(t, "redis", cl.Engine)
}

func TestBackend_CreateCluster_Memcached(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	cl, err := b.CreateClusterWithOptions(
		context.Background(),
		"memcached-cluster",
		"memcached",
		"cache.t3.micro",
		"",
		"",
		"",
		3,
		0,
	)
	require.NoError(t, err)
	assert.Equal(t, "memcached", cl.Engine)
	assert.Equal(t, 3, cl.NumCacheNodes)
}

func TestBackend_CreateCluster_AlreadyExists(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateClusterWithOptions(
		context.Background(),
		"dup-cluster",
		"redis",
		"cache.t3.micro",
		"",
		"",
		"",
		1,
		0,
	)
	require.NoError(t, err)

	_, err = b.CreateClusterWithOptions(
		context.Background(),
		"dup-cluster",
		"redis",
		"cache.t3.micro",
		"",
		"",
		"",
		1,
		0,
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticache.ErrClusterAlreadyExists)
}

func TestBackend_DeleteCluster_NotFound(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	err := b.DeleteCluster(context.Background(), "nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticache.ErrClusterNotFound)
}

func TestBackend_DescribeClusters_Pagination(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	for i := range 5 {
		_, err := b.CreateClusterWithOptions(context.Background(),
			"cl-"+string(rune('a'+i)), "redis", "cache.t3.micro", "", "", "", 1, 0,
		)
		require.NoError(t, err)
	}

	p1, err := b.DescribeClusters(context.Background(), "", "", 3, false)
	require.NoError(t, err)
	assert.Len(t, p1.Data, 3)
	assert.NotEmpty(t, p1.Next)

	p2, err := b.DescribeClusters(context.Background(), "", p1.Next, 3, false)
	require.NoError(t, err)
	assert.Len(t, p2.Data, 2)
	assert.Empty(t, p2.Next)
}

func TestBackend_ModifyCluster_EngineVersion(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateClusterWithOptions(context.Background(), "mod-cl", "redis", "cache.t3.micro", "", "", "", 1, 0)
	require.NoError(t, err)

	cl, err := b.ModifyCluster(context.Background(), "mod-cl", "", "", "7.1.0", "", "", 0)
	require.NoError(t, err)
	assert.Equal(t, "7.1.0", cl.EngineVersion)
}

func TestBackend_ModifyCluster_NodeType(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateClusterWithOptions(context.Background(), "node-cl", "redis", "cache.t3.micro", "", "", "", 1, 0)
	require.NoError(t, err)

	cl, err := b.ModifyCluster(context.Background(), "node-cl", "cache.r6g.large", "", "", "", "", 0)
	require.NoError(t, err)
	assert.Equal(t, "cache.r6g.large", cl.NodeType)
}

// ----------------------------------------
// CacheParameterGroup CRUD (issue)
// ----------------------------------------

func TestBackend_CreateParameterGroup_Redis7(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	pg, err := b.CreateParameterGroup(context.Background(), "redis7-pg", "redis7", "Redis 7 group")
	require.NoError(t, err)
	assert.Equal(t, "redis7-pg", pg.Name)
	assert.Equal(t, "redis7", pg.Family)
	assert.Contains(t, pg.ARN, "arn:aws:elasticache:us-east-1:000000000000:parametergroup:redis7-pg")
}

func TestBackend_CreateParameterGroup_Valkey(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	pg, err := b.CreateParameterGroup(context.Background(), "valkey8-pg", "valkey8", "Valkey 8 group")
	require.NoError(t, err)
	assert.Equal(t, "valkey8", pg.Family)
}

func TestBackend_DescribeParameterGroups_FilterByName(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateParameterGroup(context.Background(), "pg-1", "redis7", "group 1")
	require.NoError(t, err)
	_, err = b.CreateParameterGroup(context.Background(), "pg-2", "redis7", "group 2")
	require.NoError(t, err)

	p, err := b.DescribeParameterGroups(context.Background(), "pg-1", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "pg-1", p.Data[0].Name)
}

func TestBackend_ModifyParameterGroup_SetValue(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateParameterGroup(context.Background(), "mod-pg", "redis7", "param group to modify")
	require.NoError(t, err)

	pg, err := b.ModifyParameterGroup(context.Background(), "mod-pg", map[string]string{
		"maxmemory-policy": "allkeys-lru",
	})
	require.NoError(t, err)
	assert.Equal(t, "allkeys-lru", pg.Parameters["maxmemory-policy"])
}

func TestBackend_ResetParameterGroup_All(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateParameterGroup(context.Background(), "reset-all-pg", "redis7", "for reset")
	require.NoError(t, err)

	_, err = b.ModifyParameterGroup(context.Background(), "reset-all-pg", map[string]string{
		"maxmemory-policy": "volatile-lru",
	})
	require.NoError(t, err)

	pg, err := b.ResetParameterGroup(context.Background(), "reset-all-pg", nil, true)
	require.NoError(t, err)
	assert.Equal(t, "reset-all-pg", pg.Name)
	// After reset, the custom parameter should be cleared.
	assert.Empty(t, pg.Parameters["maxmemory-policy"])
}

func TestBackend_ResetParameterGroup_Specific(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateParameterGroup(context.Background(), "reset-spec-pg", "redis7", "for selective reset")
	require.NoError(t, err)

	_, err = b.ModifyParameterGroup(context.Background(), "reset-spec-pg", map[string]string{
		"maxmemory-policy": "allkeys-lru",
		"activerehashing":  "yes",
	})
	require.NoError(t, err)

	_, err = b.ResetParameterGroup(context.Background(), "reset-spec-pg", []string{"maxmemory-policy"}, false)
	require.NoError(t, err)

	p, err := b.DescribeParameters(context.Background(), "reset-spec-pg", "", 0)
	require.NoError(t, err)
	// maxmemory-policy should be reset; activerehashing should remain.
	paramMap := make(map[string]string)
	for _, param := range p.Data {
		if param.Value != "" {
			paramMap[param.Name] = param.Value
		}
	}
	assert.Empty(t, paramMap["maxmemory-policy"])
	assert.Equal(t, "yes", paramMap["activerehashing"])
}

// ----------------------------------------
// CacheSubnetGroup CRUD (issue)
// ----------------------------------------

func TestBackend_CreateSubnetGroup(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	sg, err := b.CreateSubnetGroup(context.Background(), "my-sng", "my subnet group", []string{"subnet-1", "subnet-2"})
	require.NoError(t, err)
	assert.Equal(t, "my-sng", sg.Name)
	assert.Len(t, sg.SubnetIDs, 2)
	assert.Contains(t, sg.ARN, "arn:aws:elasticache:")
}

func TestBackend_ModifySubnetGroup_AddSubnet(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateSubnetGroup(context.Background(), "add-sng", "original", []string{"subnet-1"})
	require.NoError(t, err)

	sg, err := b.ModifySubnetGroup(
		context.Background(),
		"add-sng",
		"updated",
		[]string{"subnet-1", "subnet-2", "subnet-3"},
	)
	require.NoError(t, err)
	assert.Len(t, sg.SubnetIDs, 3)
	assert.Equal(t, "updated", sg.Description)
}

func TestBackend_DeleteSubnetGroup_NotFound(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	err := b.DeleteSubnetGroup(context.Background(), "nonexistent-sng")
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticache.ErrSubnetGroupNotFound)
}

// ----------------------------------------
// Snapshot CRUD + ExportServerlessCacheSnapshot (issue)
// ----------------------------------------

func TestBackend_CreateSnapshot_FromCluster(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateClusterWithOptions(
		context.Background(),
		"snap-src-cluster",
		"redis",
		"cache.t3.micro",
		"",
		"",
		"",
		1,
		0,
	)
	require.NoError(t, err)

	snap, err := b.CreateSnapshot(context.Background(), "my-snapshot", "snap-src-cluster", "")
	require.NoError(t, err)
	assert.Equal(t, "my-snapshot", snap.SnapshotName)
	assert.Equal(t, "snap-src-cluster", snap.CacheClusterID)
	assert.Equal(t, "available", snap.Status)
	assert.Contains(t, snap.ARN, "arn:aws:elasticache:")
}

func TestBackend_CreateSnapshot_FromReplicationGroup(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID:          "snap-rg-src",
		Description: "for snapshot",
	})
	require.NoError(t, err)

	snap, err := b.CreateSnapshot(context.Background(), "rg-snapshot", "", "snap-rg-src")
	require.NoError(t, err)
	assert.Equal(t, "snap-rg-src", snap.ReplicationGroupID)
}

func TestBackend_CreateSnapshot_InvalidSource(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateSnapshot(context.Background(), "bad-snap", "", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticache.ErrInvalidSnapshotSource)
}

func TestBackend_DescribeSnapshots_FilterByName(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateClusterWithOptions(
		context.Background(),
		"desc-snap-cl",
		"redis",
		"cache.t3.micro",
		"",
		"",
		"",
		1,
		0,
	)
	require.NoError(t, err)

	for i := range 3 {
		_, err = b.CreateSnapshot(context.Background(), "snap-"+string(rune('a'+i)), "desc-snap-cl", "")
		require.NoError(t, err)
	}

	p, err := b.DescribeSnapshots(context.Background(), "snap-a", "", "", "", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "snap-a", p.Data[0].SnapshotName)
}

func TestBackend_DeleteSnapshot(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateClusterWithOptions(
		context.Background(),
		"del-snap-cl",
		"redis",
		"cache.t3.micro",
		"",
		"",
		"",
		1,
		0,
	)
	require.NoError(t, err)

	_, err = b.CreateSnapshot(context.Background(), "to-delete-snap", "del-snap-cl", "")
	require.NoError(t, err)

	deleted, err := b.DeleteSnapshot(context.Background(), "to-delete-snap")
	require.NoError(t, err)
	assert.Equal(t, "to-delete-snap", deleted.SnapshotName)

	// Should be gone now.
	_, err = b.DescribeSnapshots(context.Background(), "to-delete-snap", "", "", "", "", 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticache.ErrSnapshotNotFound)
}

func TestBackend_CopySnapshot(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateClusterWithOptions(
		context.Background(),
		"copy-snap-src-cl",
		"redis",
		"cache.t3.micro",
		"",
		"",
		"",
		1,
		0,
	)
	require.NoError(t, err)

	_, err = b.CreateSnapshot(context.Background(), "copy-src-snap", "copy-snap-src-cl", "")
	require.NoError(t, err)

	copied, err := b.CopySnapshot(context.Background(), "copy-src-snap", "copy-dst-snap")
	require.NoError(t, err)
	assert.Equal(t, "copy-dst-snap", copied.SnapshotName)

	// Both exist.
	p, err := b.DescribeSnapshots(context.Background(), "", "", "", "", "", 0)
	require.NoError(t, err)
	assert.Len(t, p.Data, 2)
}

// ----------------------------------------
// User ACL (Redis 6+ / Valkey) CRUD (issue)
// ----------------------------------------

func TestBackend_CreateUser_Redis6ACL(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	u, err := b.CreateUser(context.Background(), "acl-user1", "acl-user1", "on ~* +@all", "redis", false)
	require.NoError(t, err)
	assert.Equal(t, "acl-user1", u.UserID)
	assert.Equal(t, "on ~* +@all", u.AccessString)
	assert.Equal(t, "redis", u.Engine)
	assert.Equal(t, "active", u.Status)
	assert.Contains(t, u.ARN, "arn:aws:elasticache:")
	assert.Contains(t, u.ARN, ":user:acl-user1")
}

func TestBackend_CreateUser_Valkey(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	u, err := b.CreateUser(context.Background(), "valkey-user", "valkey-user", "on ~cache:* +get", "valkey", false)
	require.NoError(t, err)
	assert.Equal(t, "valkey", u.Engine)
}

func TestBackend_CreateUser_AlreadyExists(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateUser(context.Background(), "dup-user", "dup-user", "on ~* +@all", "redis", false)
	require.NoError(t, err)

	_, err = b.CreateUser(context.Background(), "dup-user", "dup-user", "on ~* +@all", "redis", false)
	require.Error(t, err)
}

func TestBackend_DeleteUser_NotFound(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.DeleteUser(context.Background(), "nonexistent-user")
	require.Error(t, err)
}

func TestBackend_ModifyUser_AccessString(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateUser(context.Background(), "mod-user", "mod-user", "on ~* +@all", "redis", false)
	require.NoError(t, err)

	u, err := b.ModifyUser(context.Background(), "mod-user", "on ~limited:* +get", false)
	require.NoError(t, err)
	assert.Equal(t, "on ~limited:* +get", u.AccessString)
}

func TestBackend_DescribeUsers_All(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	for i := range 3 {
		_, err := b.CreateUser(context.Background(),
			"list-user-"+string(rune('a'+i)),
			"list-user-"+string(rune('a'+i)),
			"on ~* +@all",
			"redis",
			false,
		)
		require.NoError(t, err)
	}

	p, err := b.DescribeUsers(context.Background(), "", "", 0)
	require.NoError(t, err)
	assert.Len(t, p.Data, 3)
}

func TestBackend_DescribeUsers_FilterByID(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateUser(context.Background(), "filter-user", "filter-user", "on ~* +@all", "redis", false)
	require.NoError(t, err)
	_, err = b.CreateUser(context.Background(), "other-user", "other-user", "on ~* +@all", "redis", false)
	require.NoError(t, err)

	p, err := b.DescribeUsers(context.Background(), "filter-user", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "filter-user", p.Data[0].UserID)
}

// ----------------------------------------
// UserGroup CRUD (issue)
// ----------------------------------------

func TestBackend_CreateUserGroup(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateUser(context.Background(), "u-a", "u-a", "on ~* +@all", "redis", false)
	require.NoError(t, err)
	_, err = b.CreateUser(context.Background(), "u-b", "u-b", "on ~* +@all", "redis", false)
	require.NoError(t, err)

	ug, err := b.CreateUserGroup(context.Background(), "group-ab", "Group AB", "redis", []string{"u-a", "u-b"})
	require.NoError(t, err)
	assert.Equal(t, "group-ab", ug.UserGroupID)
	assert.ElementsMatch(t, []string{"u-a", "u-b"}, ug.UserIDs)
	assert.Contains(t, ug.ARN, "arn:aws:elasticache:")
}

func TestBackend_CreateUserGroup_AlreadyExists(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateUserGroup(context.Background(), "dup-group", "first", "redis", nil)
	require.NoError(t, err)

	_, err = b.CreateUserGroup(context.Background(), "dup-group", "second", "redis", nil)
	require.Error(t, err)
}

func TestBackend_ModifyUserGroup_AddUsers(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateUser(context.Background(), "gu-1", "gu-1", "on ~* +@all", "redis", false)
	require.NoError(t, err)
	_, err = b.CreateUser(context.Background(), "gu-2", "gu-2", "on ~* +@all", "redis", false)
	require.NoError(t, err)

	ug, err := b.CreateUserGroup(context.Background(), "mod-grp", "modify test", "redis", []string{"gu-1"})
	require.NoError(t, err)
	assert.Len(t, ug.UserIDs, 1)

	modified, err := b.ModifyUserGroup(context.Background(), "mod-grp", []string{"gu-2"}, nil)
	require.NoError(t, err)
	assert.Contains(t, modified.UserIDs, "gu-1")
	assert.Contains(t, modified.UserIDs, "gu-2")
}

func TestBackend_ModifyUserGroup_RemoveUsers(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateUser(context.Background(), "rm-u1", "rm-u1", "on ~* +@all", "redis", false)
	require.NoError(t, err)
	_, err = b.CreateUser(context.Background(), "rm-u2", "rm-u2", "on ~* +@all", "redis", false)
	require.NoError(t, err)

	_, err = b.CreateUserGroup(context.Background(), "remove-grp", "remove test", "redis", []string{"rm-u1", "rm-u2"})
	require.NoError(t, err)

	modified, err := b.ModifyUserGroup(context.Background(), "remove-grp", nil, []string{"rm-u1"})
	require.NoError(t, err)
	assert.NotContains(t, modified.UserIDs, "rm-u1")
	assert.Contains(t, modified.UserIDs, "rm-u2")
}

func TestBackend_DescribeUserGroups_FilterByID(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateUserGroup(context.Background(), "grp-x", "group x", "redis", nil)
	require.NoError(t, err)
	_, err = b.CreateUserGroup(context.Background(), "grp-y", "group y", "redis", nil)
	require.NoError(t, err)

	p, err := b.DescribeUserGroups(context.Background(), "grp-x", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "grp-x", p.Data[0].UserGroupID)
}

// ----------------------------------------
// GlobalReplicationGroup (issue #12)
// ----------------------------------------

func TestBackend_CreateGlobalReplicationGroup_FullFlow(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID:          "global-primary",
		Description: "primary for global RG",
	})
	require.NoError(t, err)

	grg, err := b.CreateGlobalReplicationGroup(context.Background(), "test-global", "test global", "global-primary")
	require.NoError(t, err)

	assert.Equal(t, "ldgnf-test-global", grg.GlobalReplicationGroupID)
	assert.Equal(t, "test global", grg.Description)
	assert.Equal(t, "available", grg.Status)
	assert.Equal(t, "us-east-1", grg.PrimaryReplicationGroupRegion)
	assert.NotNil(t, grg.SecondaryReplicationGroups)
	assert.NotEmpty(t, grg.ARN)
	assert.Contains(t, grg.ARN, "arn:aws:elasticache:")
}

func TestBackend_DescribeGlobalReplicationGroups_FilterByID(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	for _, id := range []string{"rg-primary-1", "rg-primary-2"} {
		_, err := b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
			ID: id, Description: "primary",
		})
		require.NoError(t, err)
	}

	_, err := b.CreateGlobalReplicationGroup(context.Background(), "g1", "global 1", "rg-primary-1")
	require.NoError(t, err)
	_, err = b.CreateGlobalReplicationGroup(context.Background(), "g2", "global 2", "rg-primary-2")
	require.NoError(t, err)

	p, err := b.DescribeGlobalReplicationGroups(context.Background(), "ldgnf-g1", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "ldgnf-g1", p.Data[0].GlobalReplicationGroupID)
}

func TestBackend_DeleteGlobalReplicationGroup(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID: "del-grg-primary", Description: "for deletion",
	})
	require.NoError(t, err)

	grg, err := b.CreateGlobalReplicationGroup(context.Background(), "to-del", "to delete", "del-grg-primary")
	require.NoError(t, err)
	assert.Equal(t, "ldgnf-to-del", grg.GlobalReplicationGroupID)

	deleted, err := b.DeleteGlobalReplicationGroup(context.Background(), "ldgnf-to-del", true)
	require.NoError(t, err)
	assert.Equal(t, "ldgnf-to-del", deleted.GlobalReplicationGroupID)
}

func TestBackend_ModifyGlobalReplicationGroup(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID: "mod-grg-primary", Description: "for modification",
	})
	require.NoError(t, err)

	_, err = b.CreateGlobalReplicationGroup(context.Background(), "mod", "original desc", "mod-grg-primary")
	require.NoError(t, err)

	grg, err := b.ModifyGlobalReplicationGroup(context.Background(), "ldgnf-mod", "updated description", "", false)
	require.NoError(t, err)
	assert.Equal(t, "updated description", grg.Description)
}

// ----------------------------------------
// ServerlessCache CRUD (issue)
// ----------------------------------------

func TestBackend_CreateServerlessCache(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	sc, err := b.CreateServerlessCache(context.Background(), "my-sc", "my serverless cache", "redis")
	require.NoError(t, err)
	assert.Equal(t, "my-sc", sc.Name)
	assert.Equal(t, "redis", sc.Engine)
	assert.Equal(t, "available", sc.Status)
	assert.Contains(t, sc.ARN, "arn:aws:elasticache:")
	assert.Contains(t, sc.ARN, ":serverlesscache:my-sc")
}

func TestBackend_CreateServerlessCache_Valkey(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	sc, err := b.CreateServerlessCache(context.Background(), "valkey-sc", "valkey serverless", "valkey")
	require.NoError(t, err)
	assert.Equal(t, "valkey", sc.Engine)
}

func TestBackend_CreateServerlessCache_AlreadyExists(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateServerlessCache(context.Background(), "dup-sc", "first", "redis")
	require.NoError(t, err)

	_, err = b.CreateServerlessCache(context.Background(), "dup-sc", "second", "redis")
	require.Error(t, err)
}

func TestBackend_DescribeServerlessCaches_FilterByName(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	for _, name := range []string{"sc-a", "sc-b", "sc-c"} {
		_, err := b.CreateServerlessCache(context.Background(), name, "cache "+name, "redis")
		require.NoError(t, err)
	}

	p, err := b.DescribeServerlessCaches(context.Background(), "sc-b", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "sc-b", p.Data[0].Name)
}

func TestBackend_ModifyServerlessCache(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateServerlessCache(context.Background(), "mod-sc", "original", "redis")
	require.NoError(t, err)

	sc, err := b.ModifyServerlessCache(context.Background(), "mod-sc", "modified description")
	require.NoError(t, err)
	assert.Equal(t, "modified description", sc.Description)
}

func TestBackend_DeleteServerlessCache(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateServerlessCache(context.Background(), "del-sc", "to be deleted", "redis")
	require.NoError(t, err)

	sc, err := b.DeleteServerlessCache(context.Background(), "del-sc")
	require.NoError(t, err)
	assert.Equal(t, "del-sc", sc.Name)

	_, err = b.DescribeServerlessCaches(context.Background(), "del-sc", "", 0)
	require.Error(t, err)
}

// ----------------------------------------
// ServerlessCacheSnapshot (issue)
// ----------------------------------------

func TestBackend_CreateServerlessCacheSnapshot(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateServerlessCache(context.Background(), "snap-sc", "cache for snapshot", "redis")
	require.NoError(t, err)

	snap, err := b.CreateServerlessCacheSnapshot(context.Background(), "sc-snap-1", "snap-sc")
	require.NoError(t, err)
	assert.Equal(t, "sc-snap-1", snap.Name)
	assert.Equal(t, "snap-sc", snap.ServerlessCacheName)
	assert.Equal(t, "available", snap.Status)
	assert.Contains(t, snap.ARN, "arn:aws:elasticache:")
}

func TestBackend_CopyServerlessCacheSnapshot(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateServerlessCache(context.Background(), "copy-sc-snap-cache", "cache", "redis")
	require.NoError(t, err)
	_, err = b.CreateServerlessCacheSnapshot(context.Background(), "copy-sc-src", "copy-sc-snap-cache")
	require.NoError(t, err)

	copied, err := b.CopyServerlessCacheSnapshot(context.Background(), "copy-sc-src", "copy-sc-dst")
	require.NoError(t, err)
	assert.Equal(t, "copy-sc-dst", copied.Name)
}

func TestBackend_ExportServerlessCacheSnapshot(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateServerlessCache(context.Background(), "export-sc", "cache for export", "redis")
	require.NoError(t, err)
	_, err = b.CreateServerlessCacheSnapshot(context.Background(), "export-sc-snap", "export-sc")
	require.NoError(t, err)

	snap, err := b.ExportServerlessCacheSnapshot(context.Background(), "export-sc-snap", "my-s3-bucket")
	require.NoError(t, err)
	assert.Equal(t, "export-sc-snap", snap.Name)
}

func TestBackend_DeleteServerlessCacheSnapshot(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateServerlessCache(context.Background(), "del-sc-snap-cache", "cache", "redis")
	require.NoError(t, err)
	_, err = b.CreateServerlessCacheSnapshot(context.Background(), "del-sc-snap", "del-sc-snap-cache")
	require.NoError(t, err)

	snap, err := b.DeleteServerlessCacheSnapshot(context.Background(), "del-sc-snap")
	require.NoError(t, err)
	assert.Equal(t, "del-sc-snap", snap.Name)
}

// ----------------------------------------
// ServiceUpdate + UpdateAction (issue)
// ----------------------------------------

func TestBackend_DescribeServiceUpdates_Empty(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	p, err := b.DescribeServiceUpdates(context.Background(), "", "", 0, nil)
	require.NoError(t, err)
	assert.NotNil(t, p.Data)
}

func TestBackend_DescribeUpdateActions_Empty(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	p, err := b.DescribeUpdateActions(context.Background(), "", "", 0)
	require.NoError(t, err)
	assert.NotNil(t, p.Data)
}

func TestBackend_BatchApplyUpdateAction_Processed(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID: "batch-rg-1", Description: "batch apply test",
	})
	require.NoError(t, err)

	result, err := b.BatchApplyUpdateAction(context.Background(),
		[]string{"batch-rg-1"},
		[]string{"missing-cluster"},
		"update-20260101",
	)
	require.NoError(t, err)
	assert.Len(t, result.ProcessedUpdateActions, 1)
	assert.Len(t, result.UnprocessedUpdateActions, 1)
}

func TestBackend_BatchStopUpdateAction(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateClusterWithOptions(
		context.Background(),
		"batch-stop-cl",
		"redis",
		"cache.t3.micro",
		"",
		"",
		"",
		1,
		0,
	)
	require.NoError(t, err)

	result, err := b.BatchStopUpdateAction(context.Background(),
		[]string{"missing-rg"},
		[]string{"batch-stop-cl"},
		"update-20260101",
	)
	require.NoError(t, err)
	assert.Len(t, result.ProcessedUpdateActions, 1)
	assert.Len(t, result.UnprocessedUpdateActions, 1)
}

// ----------------------------------------
// Tags — CRUD on multiple resource types
// ----------------------------------------

func TestBackend_Tags_ClusterCRUD(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	cl, err := b.CreateClusterWithOptions(
		context.Background(),
		"tag-test-cl",
		"redis",
		"cache.t3.micro",
		"",
		"",
		"",
		1,
		0,
	)
	require.NoError(t, err)

	err = b.AddTagsToResource(context.Background(), cl.ARN, map[string]string{
		"env":  "test",
		"team": "platform",
	})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(context.Background(), cl.ARN)
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])
	assert.Equal(t, "platform", tags["team"])

	err = b.RemoveTagsFromResource(context.Background(), cl.ARN, []string{"env"})
	require.NoError(t, err)

	tags, err = b.ListTagsForResource(context.Background(), cl.ARN)
	require.NoError(t, err)
	assert.NotContains(t, tags, "env")
	assert.Equal(t, "platform", tags["team"])
}

func TestBackend_Tags_ReplicationGroupCRUD(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	rg, err := b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID: "tag-rg", Description: "tag test rg",
	})
	require.NoError(t, err)

	err = b.AddTagsToResource(context.Background(), rg.ARN, map[string]string{"stage": "prod"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(context.Background(), rg.ARN)
	require.NoError(t, err)
	assert.Equal(t, "prod", tags["stage"])
}

func TestBackend_Tags_UserCRUD(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	u, err := b.CreateUser(context.Background(), "tag-user", "tag-user", "on ~* +@all", "redis", false)
	require.NoError(t, err)

	err = b.AddTagsToResource(context.Background(), u.ARN, map[string]string{"owner": "alice"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(context.Background(), u.ARN)
	require.NoError(t, err)
	assert.Equal(t, "alice", tags["owner"])
}

func TestBackend_Tags_SnapshotCRUD(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateClusterWithOptions(
		context.Background(),
		"tag-snap-cl",
		"redis",
		"cache.t3.micro",
		"",
		"",
		"",
		1,
		0,
	)
	require.NoError(t, err)

	snap, err := b.CreateSnapshot(context.Background(), "tag-snap", "tag-snap-cl", "")
	require.NoError(t, err)

	err = b.AddTagsToResource(context.Background(), snap.ARN, map[string]string{"retention": "30days"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(context.Background(), snap.ARN)
	require.NoError(t, err)
	assert.Equal(t, "30days", tags["retention"])
}

// ----------------------------------------
// Events — ring buffer behavior
// ----------------------------------------

func TestBackend_Events_AfterMultipleOps(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateClusterWithOptions(context.Background(), "event-cl", "redis", "cache.t3.micro", "", "", "", 1, 0)
	require.NoError(t, err)

	_, err = b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID: "event-rg", Description: "event test",
	})
	require.NoError(t, err)

	p, err := b.DescribeEvents(context.Background(), "", "", "", time.Time{}, time.Time{}, 0, 100)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(p.Data), 2)
}

// ----------------------------------------
// ReservedCacheNodes (issue)
// ----------------------------------------

func TestBackend_DescribeReservedCacheNodes_Empty(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	p, err := b.DescribeReservedCacheNodes(context.Background(), "", "", "", "", 0)
	require.NoError(t, err)
	assert.NotNil(t, p.Data)
}

func TestBackend_DescribeReservedCacheNodesOfferings_NonEmpty(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	p, err := b.DescribeReservedCacheNodesOfferings(context.Background(), "", "", "", "", 0)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(p.Data), 1)
}

func TestBackend_PurchaseReservedCacheNodesOffering(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	offerings, err := b.DescribeReservedCacheNodesOfferings(context.Background(), "", "", "", "", 0)
	require.NoError(t, err)
	require.NotEmpty(t, offerings.Data)

	offeringID := offerings.Data[0].OfferingID
	rcn, err := b.PurchaseReservedCacheNodesOffering(context.Background(), offeringID, "my-reserved-node-id", 1)
	require.NoError(t, err)
	assert.Equal(t, "my-reserved-node-id", rcn.ReservedCacheNodeID)
	assert.Equal(t, int32(1), rcn.CacheNodeCount)
}

// ----------------------------------------
// AllowedNodeTypeModifications
// ----------------------------------------

func TestBackend_ListAllowedNodeTypeModifications_ForCluster(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateClusterWithOptions(
		context.Background(),
		"nodemods-cl",
		"redis",
		"cache.t3.micro",
		"",
		"",
		"",
		1,
		0,
	)
	require.NoError(t, err)

	mods, err := b.ListAllowedNodeTypeModifications(context.Background(), "nodemods-cl", "")
	require.NoError(t, err)
	assert.NotNil(t, mods)
}

func TestBackend_ListAllowedNodeTypeModifications_ForRG(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID: "nodemods-rg", Description: "node mods test",
	})
	require.NoError(t, err)

	mods, err := b.ListAllowedNodeTypeModifications(context.Background(), "", "nodemods-rg")
	require.NoError(t, err)
	assert.NotNil(t, mods)
}

// ----------------------------------------
// EngineDefaultParameters
// ----------------------------------------

func TestBackend_DescribeEngineDefaultParameters_Redis7(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	p, err := b.DescribeEngineDefaultParameters(context.Background(), "redis7", "", 0)
	require.NoError(t, err)
	assert.NotNil(t, p.Data)
}

func TestBackend_DescribeEngineDefaultParameters_Valkey8(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	p, err := b.DescribeEngineDefaultParameters(context.Background(), "valkey8", "", 0)
	require.NoError(t, err)
	assert.NotNil(t, p.Data)
}

// ----------------------------------------
// CacheEngineVersions — all engines
// ----------------------------------------

func TestBackend_DescribeCacheEngineVersions_Redis(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	p, err := b.DescribeCacheEngineVersions(context.Background(), "redis", "", "", "", 0)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(p.Data), 2)
	for _, v := range p.Data {
		assert.Equal(t, "redis", v.Engine)
	}
}

func TestBackend_DescribeCacheEngineVersions_Memcached(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	p, err := b.DescribeCacheEngineVersions(context.Background(), "memcached", "", "", "", 0)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(p.Data), 1)
	for _, v := range p.Data {
		assert.Equal(t, "memcached", v.Engine)
	}
}

func TestBackend_DescribeCacheEngineVersions_Valkey(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	p, err := b.DescribeCacheEngineVersions(context.Background(), "valkey", "", "", "", 0)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(p.Data), 2)
	for _, v := range p.Data {
		assert.Equal(t, "valkey", v.Engine)
	}
}

func TestBackend_DescribeCacheEngineVersions_FilterByFamily(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	p, err := b.DescribeCacheEngineVersions(context.Background(), "", "valkey8", "", "", 0)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(p.Data), 1)
	for _, v := range p.Data {
		assert.Equal(t, "valkey8", v.CacheParameterGroupFamily)
	}
}

// ----------------------------------------
// CacheSecurityGroup (legacy) CRUD
// ----------------------------------------

func TestBackend_CacheSecurityGroup_CRUD(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	sg, err := b.CreateCacheSecurityGroup(context.Background(), "my-sg", "My security group")
	require.NoError(t, err)
	assert.Equal(t, "my-sg", sg.Name)
	assert.Contains(t, sg.ARN, "arn:aws:elasticache:")

	auth, err := b.AuthorizeCacheSecurityGroupIngress(context.Background(), "my-sg", "ec2-sg", "123456789012")
	require.NoError(t, err)
	assert.NotNil(t, auth)

	p, err := b.DescribeCacheSecurityGroups(context.Background(), "my-sg", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)

	revoked, err := b.RevokeCacheSecurityGroupIngress(context.Background(), "my-sg", "ec2-sg", "123456789012")
	require.NoError(t, err)
	assert.NotNil(t, revoked)

	err = b.DeleteCacheSecurityGroup(context.Background(), "my-sg")
	require.NoError(t, err)

	// After delete, lookup by name returns not-found error.
	_, err = b.DescribeCacheSecurityGroups(context.Background(), "my-sg", "", 0)
	require.Error(t, err)
}

// ----------------------------------------
// Reset clears all state
// ----------------------------------------

func TestBackend_Reset_ClearsAll(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateClusterWithOptions(context.Background(), "reset-cl", "redis", "cache.t3.micro", "", "", "", 1, 0)
	require.NoError(t, err)

	_, err = b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID: "reset-rg", Description: "for reset",
	})
	require.NoError(t, err)

	_, err = b.CreateUser(context.Background(), "reset-user", "reset-user", "on ~* +@all", "redis", false)
	require.NoError(t, err)

	b.Reset()

	// All resources should be gone.
	p1, err := b.DescribeClusters(context.Background(), "", "", 0, false)
	require.NoError(t, err)
	assert.Empty(t, p1.Data)

	p2, err := b.DescribeReplicationGroups(context.Background(), "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, p2.Data)

	p3, err := b.DescribeUsers(context.Background(), "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, p3.Data)
}
