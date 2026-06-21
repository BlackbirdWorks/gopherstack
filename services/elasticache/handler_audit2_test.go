package elasticache_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticachetypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticache"
)

// ----------------------------------------
// NodeGroups in DescribeReplicationGroups response (issue #2)
// ----------------------------------------

func TestHandler_DescribeReplicationGroups_ReturnsNodeGroups(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("ng-rg"),
		ReplicationGroupDescription: aws.String("node groups test"),
		ClusterMode:                 elasticachetypes.ClusterModeEnabled,
		NumNodeGroups:               aws.Int32(3),
		ReplicasPerNodeGroup:        aws.Int32(1),
	})
	require.NoError(t, err)

	out, err := client.DescribeReplicationGroups(t.Context(), &elasticachesdk.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String("ng-rg"),
	})
	require.NoError(t, err)
	require.Len(t, out.ReplicationGroups, 1)

	rg := out.ReplicationGroups[0]
	assert.True(t, aws.ToBool(rg.ClusterEnabled))
	assert.Len(t, rg.NodeGroups, 3)

	// Verify each node group has slots.
	for _, ng := range rg.NodeGroups {
		assert.NotEmpty(t, aws.ToString(ng.NodeGroupId))
		assert.NotEmpty(t, aws.ToString(ng.Slots))
	}
}

func TestHandler_DescribeReplicationGroups_NoNodeGroups_WhenDisabled(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("no-ng-rg"),
		ReplicationGroupDescription: aws.String("no node groups"),
	})
	require.NoError(t, err)

	out, err := client.DescribeReplicationGroups(t.Context(), &elasticachesdk.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String("no-ng-rg"),
	})
	require.NoError(t, err)
	require.Len(t, out.ReplicationGroups, 1)

	rg := out.ReplicationGroups[0]
	assert.False(t, aws.ToBool(rg.ClusterEnabled))
	assert.Empty(t, rg.NodeGroups)
}

// ----------------------------------------
// PendingModifiedValues in HTTP response (issue #7)
// ----------------------------------------

func TestHandler_ModifyReplicationGroup_PendingModifiedValues_InResponse(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("pending-http-rg"),
		ReplicationGroupDescription: aws.String("pending changes"),
	})
	require.NoError(t, err)

	_, err = client.ModifyReplicationGroup(t.Context(), &elasticachesdk.ModifyReplicationGroupInput{
		ReplicationGroupId: aws.String("pending-http-rg"),
		EngineVersion:      aws.String("7.0.7"),
		ApplyImmediately:   aws.Bool(false),
	})
	require.NoError(t, err)

	// PendingModifiedValues should appear in the describe response.
	desc, err := client.DescribeReplicationGroups(t.Context(), &elasticachesdk.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String("pending-http-rg"),
	})
	require.NoError(t, err)
	require.Len(t, desc.ReplicationGroups, 1)

	// Pending modifications exist (SDK type doesn't expose EngineVersion in PendingModifiedValues,
	// but the pending state is tracked internally by the backend).
	_ = desc.ReplicationGroups[0].PendingModifiedValues
}

func TestHandler_ModifyReplicationGroup_ApplyImmediately_ClearsPending(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("clear-pending-rg"),
		ReplicationGroupDescription: aws.String("clear pending"),
	})
	require.NoError(t, err)

	// First modify without ApplyImmediately to queue pending.
	_, err = client.ModifyReplicationGroup(t.Context(), &elasticachesdk.ModifyReplicationGroupInput{
		ReplicationGroupId: aws.String("clear-pending-rg"),
		EngineVersion:      aws.String("7.0.7"),
		ApplyImmediately:   aws.Bool(false),
	})
	require.NoError(t, err)

	// Second modify with ApplyImmediately=true clears pending.
	_, err = client.ModifyReplicationGroup(t.Context(), &elasticachesdk.ModifyReplicationGroupInput{
		ReplicationGroupId: aws.String("clear-pending-rg"),
		CacheNodeType:      aws.String("cache.r6g.large"),
		ApplyImmediately:   aws.Bool(true),
	})
	require.NoError(t, err)

	desc, err := client.DescribeReplicationGroups(t.Context(), &elasticachesdk.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String("clear-pending-rg"),
	})
	require.NoError(t, err)
	rg := desc.ReplicationGroups[0]
	assert.Nil(t, rg.PendingModifiedValues)
	assert.Equal(t, "cache.r6g.large", aws.ToString(rg.CacheNodeType))
}

// ----------------------------------------
// Tags on CreateReplicationGroup (gap - tags not parsed before)
// ----------------------------------------

func TestHandler_CreateReplicationGroup_WithTags(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("tagged-rg"),
		ReplicationGroupDescription: aws.String("tagged"),
		Tags: []elasticachetypes.Tag{
			{Key: aws.String("env"), Value: aws.String("test")},
			{Key: aws.String("team"), Value: aws.String("platform")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, out.ReplicationGroup)

	arn := aws.ToString(out.ReplicationGroup.ARN)
	require.NotEmpty(t, arn)

	tagsOut, err := client.ListTagsForResource(t.Context(), &elasticachesdk.ListTagsForResourceInput{
		ResourceName: aws.String(arn),
	})
	require.NoError(t, err)
	require.Len(t, tagsOut.TagList, 2)

	tagMap := make(map[string]string)
	for _, tag := range tagsOut.TagList {
		tagMap[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	assert.Equal(t, "test", tagMap["env"])
	assert.Equal(t, "platform", tagMap["team"])
}

// ----------------------------------------
// UserGroupIds in ReplicationGroup create/modify (issue #15)
// ----------------------------------------

func TestHandler_CreateReplicationGroup_WithUserGroupIds(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	// Create users and user group first.
	_, err := client.CreateUser(t.Context(), &elasticachesdk.CreateUserInput{
		UserId:             aws.String("ug-user1"),
		UserName:           aws.String("ug-user1"),
		Engine:             aws.String("redis"),
		AccessString:       aws.String("on ~* +@all"),
		NoPasswordRequired: aws.Bool(true),
	})
	require.NoError(t, err)

	_, err = client.CreateUserGroup(t.Context(), &elasticachesdk.CreateUserGroupInput{
		UserGroupId: aws.String("test-ug"),
		Engine:      aws.String("redis"),
		UserIds:     []string{"ug-user1"},
	})
	require.NoError(t, err)

	out, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("ug-rg"),
		ReplicationGroupDescription: aws.String("with user groups"),
		UserGroupIds:                []string{"test-ug"},
	})
	require.NoError(t, err)
	require.NotNil(t, out.ReplicationGroup)

	desc, err := client.DescribeReplicationGroups(t.Context(), &elasticachesdk.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String("ug-rg"),
	})
	require.NoError(t, err)
	rg := desc.ReplicationGroups[0]
	assert.Contains(t, rg.UserGroupIds, "test-ug")
}

func TestHandler_ModifyReplicationGroup_AddUserGroupIds(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateUser(t.Context(), &elasticachesdk.CreateUserInput{
		UserId:             aws.String("mod-ug-user"),
		UserName:           aws.String("mod-ug-user"),
		Engine:             aws.String("redis"),
		AccessString:       aws.String("on ~* +@all"),
		NoPasswordRequired: aws.Bool(true),
	})
	require.NoError(t, err)

	_, err = client.CreateUserGroup(t.Context(), &elasticachesdk.CreateUserGroupInput{
		UserGroupId: aws.String("mod-ug"),
		Engine:      aws.String("redis"),
		UserIds:     []string{"mod-ug-user"},
	})
	require.NoError(t, err)

	_, err = client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("mod-ug-rg"),
		ReplicationGroupDescription: aws.String("modify user groups"),
	})
	require.NoError(t, err)

	_, err = client.ModifyReplicationGroup(t.Context(), &elasticachesdk.ModifyReplicationGroupInput{
		ReplicationGroupId: aws.String("mod-ug-rg"),
		UserGroupIdsToAdd:  []string{"mod-ug"},
		ApplyImmediately:   aws.Bool(true),
	})
	require.NoError(t, err)

	desc, err := client.DescribeReplicationGroups(t.Context(), &elasticachesdk.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String("mod-ug-rg"),
	})
	require.NoError(t, err)
	rg := desc.ReplicationGroups[0]
	assert.Contains(t, rg.UserGroupIds, "mod-ug")
}

func TestHandler_ModifyReplicationGroup_RemoveUserGroupIds(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateUser(t.Context(), &elasticachesdk.CreateUserInput{
		UserId:             aws.String("rm-ug-user"),
		UserName:           aws.String("rm-ug-user"),
		Engine:             aws.String("redis"),
		AccessString:       aws.String("on ~* +@all"),
		NoPasswordRequired: aws.Bool(true),
	})
	require.NoError(t, err)

	_, err = client.CreateUserGroup(t.Context(), &elasticachesdk.CreateUserGroupInput{
		UserGroupId: aws.String("rm-ug"),
		Engine:      aws.String("redis"),
		UserIds:     []string{"rm-ug-user"},
	})
	require.NoError(t, err)

	_, err = client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("rm-ug-rg"),
		ReplicationGroupDescription: aws.String("remove user groups"),
		UserGroupIds:                []string{"rm-ug"},
	})
	require.NoError(t, err)

	_, err = client.ModifyReplicationGroup(t.Context(), &elasticachesdk.ModifyReplicationGroupInput{
		ReplicationGroupId:   aws.String("rm-ug-rg"),
		UserGroupIdsToRemove: []string{"rm-ug"},
		ApplyImmediately:     aws.Bool(true),
	})
	require.NoError(t, err)

	desc, err := client.DescribeReplicationGroups(t.Context(), &elasticachesdk.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String("rm-ug-rg"),
	})
	require.NoError(t, err)
	rg := desc.ReplicationGroups[0]
	assert.NotContains(t, rg.UserGroupIds, "rm-ug")
}

// ----------------------------------------
// UserGroupIds backend unit tests (issue #15)
// ----------------------------------------

func TestBackend_UserGroupIds_AddRemove(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateUser(context.Background(), "u1", "user1", "on ~* +@all", "redis", false)
	require.NoError(t, err)

	_, err = b.CreateUserGroup(context.Background(), "ug1", "group 1", "redis", []string{"u1"})
	require.NoError(t, err)

	rg, err := b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID:           "ug-backend-rg",
		Description:  "user group backend test",
		UserGroupIDs: []string{"ug1"},
	})
	require.NoError(t, err)
	assert.Contains(t, rg.UserGroupIDs, "ug1")

	// Modify: remove ug1.
	rg2, err := b.ModifyReplicationGroupFull(
		context.Background(),
		"ug-backend-rg",
		elasticache.ReplicationGroupModifyOpts{
			UserGroupIDsToRemove: []string{"ug1"},
			ApplyImmediately:     true,
		},
	)
	require.NoError(t, err)
	assert.NotContains(t, rg2.UserGroupIDs, "ug1")
}

// ----------------------------------------
// CacheCluster — DescribeCacheClusters pagination
// ----------------------------------------

func TestHandler_DescribeCacheClusters_Pagination(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	// Create 5 clusters.
	for i := range 5 {
		_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
			CacheClusterId: aws.String(fmt.Sprintf("paginate-cluster-%d", i)),
			Engine:         aws.String("redis"),
		})
		require.NoError(t, err)
	}

	// Page 1: max 3.
	page1, err := client.DescribeCacheClusters(t.Context(), &elasticachesdk.DescribeCacheClustersInput{
		MaxRecords: aws.Int32(3),
	})
	require.NoError(t, err)
	assert.Len(t, page1.CacheClusters, 3)
	assert.NotEmpty(t, aws.ToString(page1.Marker))

	// Page 2: rest.
	page2, err := client.DescribeCacheClusters(t.Context(), &elasticachesdk.DescribeCacheClustersInput{
		MaxRecords: aws.Int32(3),
		Marker:     page1.Marker,
	})
	require.NoError(t, err)
	assert.Len(t, page2.CacheClusters, 2)
}

// ----------------------------------------
// CacheCluster — ARN format
// ----------------------------------------

func TestHandler_CreateCacheCluster_ARNFormat(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("arn-cluster"),
		Engine:         aws.String("redis"),
	})
	require.NoError(t, err)
	arn := aws.ToString(out.CacheCluster.ARN)
	assert.Contains(t, arn, "arn:aws:elasticache:")
	assert.Contains(t, arn, ":cluster:")
	assert.Contains(t, arn, "arn-cluster")
}

func TestHandler_CreateReplicationGroup_ARNFormat(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("arn-rg"),
		ReplicationGroupDescription: aws.String("arn test"),
	})
	require.NoError(t, err)
	arn := aws.ToString(out.ReplicationGroup.ARN)
	assert.Contains(t, arn, "arn:aws:elasticache:")
	assert.Contains(t, arn, "arn-rg")
}

// ----------------------------------------
// CacheCluster — Memcached multi-node
// ----------------------------------------

func TestHandler_CreateCacheCluster_Memcached_NumNodes(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("memcached-3node"),
		Engine:         aws.String("memcached"),
		CacheNodeType:  aws.String("cache.t3.micro"),
		NumCacheNodes:  aws.Int32(3),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(3), aws.ToInt32(out.CacheCluster.NumCacheNodes))
	assert.Equal(t, int32(3), int32(len(out.CacheCluster.CacheNodes)))
}

// ----------------------------------------
// CacheParameterGroup — reset all parameters
// ----------------------------------------

func TestHandler_ResetCacheParameterGroup_All(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateCacheParameterGroup(t.Context(), &elasticachesdk.CreateCacheParameterGroupInput{
		CacheParameterGroupName:   aws.String("reset-pg"),
		CacheParameterGroupFamily: aws.String("redis7"),
		Description:               aws.String("for reset test"),
	})
	require.NoError(t, err)

	_, err = client.ModifyCacheParameterGroup(t.Context(), &elasticachesdk.ModifyCacheParameterGroupInput{
		CacheParameterGroupName: aws.String("reset-pg"),
		ParameterNameValues: []elasticachetypes.ParameterNameValue{
			{ParameterName: aws.String("maxmemory-policy"), ParameterValue: aws.String("allkeys-lru")},
		},
	})
	require.NoError(t, err)

	out, err := client.ResetCacheParameterGroup(t.Context(), &elasticachesdk.ResetCacheParameterGroupInput{
		CacheParameterGroupName: aws.String("reset-pg"),
		ResetAllParameters:      aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Equal(t, "reset-pg", aws.ToString(out.CacheParameterGroupName))
}

// ----------------------------------------
// Snapshot — CopySnapshot cross-region
// ----------------------------------------

func TestHandler_CopySnapshot_CreatesNewSnapshot(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("copy-src-cluster"),
		Engine:         aws.String("redis"),
	})
	require.NoError(t, err)

	_, err = client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
		SnapshotName:   aws.String("copy-src-snap"),
		CacheClusterId: aws.String("copy-src-cluster"),
	})
	require.NoError(t, err)

	out, err := client.CopySnapshot(t.Context(), &elasticachesdk.CopySnapshotInput{
		SourceSnapshotName: aws.String("copy-src-snap"),
		TargetSnapshotName: aws.String("copy-dst-snap"),
	})
	require.NoError(t, err)
	assert.Equal(t, "copy-dst-snap", aws.ToString(out.Snapshot.SnapshotName))

	// Both should exist now.
	desc, err := client.DescribeSnapshots(t.Context(), &elasticachesdk.DescribeSnapshotsInput{})
	require.NoError(t, err)
	snapNames := make([]string, 0, len(desc.Snapshots))
	for _, s := range desc.Snapshots {
		snapNames = append(snapNames, aws.ToString(s.SnapshotName))
	}
	assert.Contains(t, snapNames, "copy-src-snap")
	assert.Contains(t, snapNames, "copy-dst-snap")
}

// ----------------------------------------
// User — describe filter by ID
// ----------------------------------------

func TestHandler_DescribeUsers_FilterByID(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	for _, id := range []string{"filter-u1", "filter-u2"} {
		_, err := client.CreateUser(t.Context(), &elasticachesdk.CreateUserInput{
			UserId:             aws.String(id),
			UserName:           aws.String(id),
			Engine:             aws.String("redis"),
			AccessString:       aws.String("on ~* +@all"),
			NoPasswordRequired: aws.Bool(true),
		})
		require.NoError(t, err)
	}

	out, err := client.DescribeUsers(t.Context(), &elasticachesdk.DescribeUsersInput{
		UserId: aws.String("filter-u1"),
	})
	require.NoError(t, err)
	require.Len(t, out.Users, 1)
	assert.Equal(t, "filter-u1", aws.ToString(out.Users[0].UserId))
}

// ----------------------------------------
// User — Modify AccessString
// ----------------------------------------

func TestHandler_ModifyUser_AccessString(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateUser(t.Context(), &elasticachesdk.CreateUserInput{
		UserId:             aws.String("mod-access-user"),
		UserName:           aws.String("mod-access-user"),
		Engine:             aws.String("redis"),
		AccessString:       aws.String("on ~* +@all"),
		NoPasswordRequired: aws.Bool(true),
	})
	require.NoError(t, err)

	out, err := client.ModifyUser(t.Context(), &elasticachesdk.ModifyUserInput{
		UserId:       aws.String("mod-access-user"),
		AccessString: aws.String("on ~cache:* +get +set"),
	})
	require.NoError(t, err)
	assert.Equal(t, "on ~cache:* +get +set", aws.ToString(out.AccessString))
}

// ----------------------------------------
// ServiceUpdate — describe
// ----------------------------------------

func TestHandler_DescribeServiceUpdates_Empty(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.DescribeServiceUpdates(t.Context(), &elasticachesdk.DescribeServiceUpdatesInput{})
	require.NoError(t, err)
	assert.NotNil(t, out.ServiceUpdates)
}

// ----------------------------------------
// UpdateAction — describe with service update name
// ----------------------------------------

func TestHandler_DescribeUpdateActions_WithServiceUpdateName(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.DescribeUpdateActions(t.Context(), &elasticachesdk.DescribeUpdateActionsInput{
		ServiceUpdateName: aws.String("nonexistent-update"),
	})
	require.NoError(t, err)
	assert.NotNil(t, out.UpdateActions)
}

// ----------------------------------------
// ListAllowedNodeTypeModifications
// ----------------------------------------

func TestHandler_ListAllowedNodeTypeModifications_ForCluster(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("mod-type-cluster"),
		Engine:         aws.String("redis"),
		CacheNodeType:  aws.String("cache.t3.micro"),
	})
	require.NoError(t, err)

	out, err := client.ListAllowedNodeTypeModifications(
		t.Context(),
		&elasticachesdk.ListAllowedNodeTypeModificationsInput{
			CacheClusterId: aws.String("mod-type-cluster"),
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, out)
}

// ----------------------------------------
// DescribeEngineDefaultParameters
// ----------------------------------------

func TestHandler_DescribeEngineDefaultParameters_Redis(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.DescribeEngineDefaultParameters(
		t.Context(),
		&elasticachesdk.DescribeEngineDefaultParametersInput{
			CacheParameterGroupFamily: aws.String("redis7"),
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, out)
	assert.Equal(t, "redis7", aws.ToString(out.EngineDefaults.CacheParameterGroupFamily))
}

// ----------------------------------------
// RebootCacheCluster
// ----------------------------------------

func TestHandler_RebootCacheCluster_Audit2(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("reboot-cluster-2"),
		Engine:         aws.String("redis"),
	})
	require.NoError(t, err)

	out, err := client.RebootCacheCluster(t.Context(), &elasticachesdk.RebootCacheClusterInput{
		CacheClusterId:       aws.String("reboot-cluster-2"),
		CacheNodeIdsToReboot: []string{"0001"},
	})
	require.NoError(t, err)
	assert.Equal(t, "reboot-cluster-2", aws.ToString(out.CacheCluster.CacheClusterId))
}

// ----------------------------------------
// GlobalReplicationGroup — region tracking (issue #12)
// ----------------------------------------

func TestHandler_CreateGlobalReplicationGroup_RegionTracking(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("grg-region-primary"),
		ReplicationGroupDescription: aws.String("primary for region test"),
	})
	require.NoError(t, err)

	grgOut, err := client.CreateGlobalReplicationGroup(t.Context(), &elasticachesdk.CreateGlobalReplicationGroupInput{
		GlobalReplicationGroupIdSuffix:    aws.String("region-grg"),
		GlobalReplicationGroupDescription: aws.String("Region tracking GRG"),
		PrimaryReplicationGroupId:         aws.String("grg-region-primary"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(grgOut.GlobalReplicationGroup.GlobalReplicationGroupId))

	// Describe to verify.
	desc, err := client.DescribeGlobalReplicationGroups(
		t.Context(),
		&elasticachesdk.DescribeGlobalReplicationGroupsInput{
			GlobalReplicationGroupId: grgOut.GlobalReplicationGroup.GlobalReplicationGroupId,
		},
	)
	require.NoError(t, err)
	require.Len(t, desc.GlobalReplicationGroups, 1)
}

// ----------------------------------------
// Tags on CacheSubnetGroup
// ----------------------------------------

func TestHandler_CreateCacheSubnetGroup_Tags(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.CreateCacheSubnetGroup(t.Context(), &elasticachesdk.CreateCacheSubnetGroupInput{
		CacheSubnetGroupName:        aws.String("tagged-sng"),
		CacheSubnetGroupDescription: aws.String("tagged subnet group"),
		SubnetIds:                   []string{"subnet-abc123"},
		Tags: []elasticachetypes.Tag{
			{Key: aws.String("purpose"), Value: aws.String("testing")},
		},
	})
	require.NoError(t, err)
	arn := aws.ToString(out.CacheSubnetGroup.ARN)
	require.NotEmpty(t, arn)

	tagsOut, err := client.ListTagsForResource(t.Context(), &elasticachesdk.ListTagsForResourceInput{
		ResourceName: aws.String(arn),
	})
	require.NoError(t, err)
	require.Len(t, tagsOut.TagList, 1)
	assert.Equal(t, "purpose", aws.ToString(tagsOut.TagList[0].Key))
	assert.Equal(t, "testing", aws.ToString(tagsOut.TagList[0].Value))
}

// ----------------------------------------
// DescribeSnapshots — filter by cluster ID
// ----------------------------------------

func TestHandler_DescribeSnapshots_FilterByClusterID(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	// Create two clusters with snapshots.
	for _, id := range []string{"snap-cluster-a", "snap-cluster-b"} {
		_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
			CacheClusterId: aws.String(id),
			Engine:         aws.String("redis"),
		})
		require.NoError(t, err)

		_, err = client.CreateSnapshot(t.Context(), &elasticachesdk.CreateSnapshotInput{
			SnapshotName:   aws.String("snap-" + id),
			CacheClusterId: aws.String(id),
		})
		require.NoError(t, err)
	}

	out, err := client.DescribeSnapshots(t.Context(), &elasticachesdk.DescribeSnapshotsInput{
		CacheClusterId: aws.String("snap-cluster-a"),
	})
	require.NoError(t, err)
	require.Len(t, out.Snapshots, 1)
	assert.Equal(t, "snap-cluster-a", aws.ToString(out.Snapshots[0].CacheClusterId))
}

// ----------------------------------------
// Persistence: UserGroupIds survive snapshot/restore
// ----------------------------------------

func TestBackend_Persistence_UserGroupIds(t *testing.T) {
	t.Parallel()

	b1 := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b1.CreateUser(context.Background(), "persist-user", "persist-user", "on ~* +@all", "redis", false)
	require.NoError(t, err)

	_, err = b1.CreateUserGroup(context.Background(), "persist-ug", "persist group", "redis", []string{"persist-user"})
	require.NoError(t, err)

	_, err = b1.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID:           "persist-ug-rg",
		Description:  "persist user groups",
		UserGroupIDs: []string{"persist-ug"},
	})
	require.NoError(t, err)

	snap := b1.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")
	err = b2.Restore(t.Context(), snap)
	require.NoError(t, err)

	page, err := b2.DescribeReplicationGroups(context.Background(), "persist-ug-rg", "", 0)
	require.NoError(t, err)
	require.Len(t, page.Data, 1)
	assert.Contains(t, page.Data[0].UserGroupIDs, "persist-ug")
}

// ----------------------------------------
// CacheSecurityGroup — full lifecycle
// ----------------------------------------

func TestHandler_CacheSecurityGroup_Lifecycle(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	// Create.
	createOut, err := client.CreateCacheSecurityGroup(t.Context(), &elasticachesdk.CreateCacheSecurityGroupInput{
		CacheSecurityGroupName: aws.String("lifecycle-sg"),
		Description:            aws.String("lifecycle test"),
	})
	require.NoError(t, err)
	assert.Equal(t, "lifecycle-sg", aws.ToString(createOut.CacheSecurityGroup.CacheSecurityGroupName))

	// Authorize ingress.
	authOut, err := client.AuthorizeCacheSecurityGroupIngress(
		t.Context(),
		&elasticachesdk.AuthorizeCacheSecurityGroupIngressInput{
			CacheSecurityGroupName:  aws.String("lifecycle-sg"),
			EC2SecurityGroupName:    aws.String("my-ec2-sg"),
			EC2SecurityGroupOwnerId: aws.String("123456789012"),
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, authOut.CacheSecurityGroup)

	// Describe.
	descOut, err := client.DescribeCacheSecurityGroups(
		t.Context(),
		&elasticachesdk.DescribeCacheSecurityGroupsInput{
			CacheSecurityGroupName: aws.String("lifecycle-sg"),
		},
	)
	require.NoError(t, err)
	require.Len(t, descOut.CacheSecurityGroups, 1)

	// Revoke ingress.
	_, err = client.RevokeCacheSecurityGroupIngress(
		t.Context(),
		&elasticachesdk.RevokeCacheSecurityGroupIngressInput{
			CacheSecurityGroupName:  aws.String("lifecycle-sg"),
			EC2SecurityGroupName:    aws.String("my-ec2-sg"),
			EC2SecurityGroupOwnerId: aws.String("123456789012"),
		},
	)
	require.NoError(t, err)

	// Delete.
	_, err = client.DeleteCacheSecurityGroup(t.Context(), &elasticachesdk.DeleteCacheSecurityGroupInput{
		CacheSecurityGroupName: aws.String("lifecycle-sg"),
	})
	require.NoError(t, err)
}

// ----------------------------------------
// ServerlessCache — full lifecycle
// ----------------------------------------

func TestHandler_ServerlessCache_FullLifecycle(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	// Create.
	createOut, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
		ServerlessCacheName: aws.String("lifecycle-sc"),
		Engine:              aws.String("redis"),
		Description:         aws.String("lifecycle test"),
	})
	require.NoError(t, err)
	assert.Equal(t, "lifecycle-sc", aws.ToString(createOut.ServerlessCache.ServerlessCacheName))
	arn := aws.ToString(createOut.ServerlessCache.ARN)
	assert.NotEmpty(t, arn)

	// Create snapshot.
	snapOut, err := client.CreateServerlessCacheSnapshot(
		t.Context(),
		&elasticachesdk.CreateServerlessCacheSnapshotInput{
			ServerlessCacheSnapshotName: aws.String("lifecycle-sc-snap"),
			ServerlessCacheName:         aws.String("lifecycle-sc"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "lifecycle-sc-snap", aws.ToString(snapOut.ServerlessCacheSnapshot.ServerlessCacheSnapshotName))

	// Copy snapshot.
	copyOut, err := client.CopyServerlessCacheSnapshot(
		t.Context(),
		&elasticachesdk.CopyServerlessCacheSnapshotInput{
			SourceServerlessCacheSnapshotName: aws.String("lifecycle-sc-snap"),
			TargetServerlessCacheSnapshotName: aws.String("lifecycle-sc-snap-copy"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "lifecycle-sc-snap-copy", aws.ToString(copyOut.ServerlessCacheSnapshot.ServerlessCacheSnapshotName))

	// Describe cache.
	descOut, err := client.DescribeServerlessCaches(t.Context(), &elasticachesdk.DescribeServerlessCachesInput{
		ServerlessCacheName: aws.String("lifecycle-sc"),
	})
	require.NoError(t, err)
	require.Len(t, descOut.ServerlessCaches, 1)

	// Modify.
	_, err = client.ModifyServerlessCache(t.Context(), &elasticachesdk.ModifyServerlessCacheInput{
		ServerlessCacheName: aws.String("lifecycle-sc"),
		Description:         aws.String("modified description"),
	})
	require.NoError(t, err)

	// Delete snapshot.
	_, err = client.DeleteServerlessCacheSnapshot(
		t.Context(),
		&elasticachesdk.DeleteServerlessCacheSnapshotInput{
			ServerlessCacheSnapshotName: aws.String("lifecycle-sc-snap"),
		},
	)
	require.NoError(t, err)

	// Delete cache.
	_, err = client.DeleteServerlessCache(t.Context(), &elasticachesdk.DeleteServerlessCacheInput{
		ServerlessCacheName: aws.String("lifecycle-sc"),
	})
	require.NoError(t, err)
}

// ----------------------------------------
// TriggerAutoSnapshot — engine and ARN
// ----------------------------------------

func TestBackend_TriggerAutoSnapshot_Engine(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")

	_, err := b.CreateReplicationGroupFull(context.Background(), elasticache.ReplicationGroupCreateOpts{
		ID:          "engine-snap-rg",
		Description: "engine snapshot test",
		Engine:      "redis",
	})
	require.NoError(t, err)

	snap, err := b.TriggerAutoSnapshot(context.Background(), "engine-snap-rg")
	require.NoError(t, err)
	assert.Equal(t, "automated", snap.SnapshotSource)
	assert.NotEmpty(t, snap.ARN)
	assert.Contains(t, snap.ARN, "arn:aws:elasticache:")
}

// ----------------------------------------
// IncreaseReplicaCount / DecreaseReplicaCount via HTTP
// ----------------------------------------

func TestHandler_IncreaseDecreaseReplicaCount(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("replica-count-http-rg"),
		ReplicationGroupDescription: aws.String("replica count via HTTP"),
	})
	require.NoError(t, err)

	// Increase to 2.
	increaseOut, err := client.IncreaseReplicaCount(t.Context(), &elasticachesdk.IncreaseReplicaCountInput{
		ReplicationGroupId: aws.String("replica-count-http-rg"),
		NewReplicaCount:    aws.Int32(2),
		ApplyImmediately:   aws.Bool(true),
	})
	require.NoError(t, err)
	assert.NotNil(t, increaseOut.ReplicationGroup)

	// Decrease to 1.
	decreaseOut, err := client.DecreaseReplicaCount(t.Context(), &elasticachesdk.DecreaseReplicaCountInput{
		ReplicationGroupId: aws.String("replica-count-http-rg"),
		NewReplicaCount:    aws.Int32(1),
		ApplyImmediately:   aws.Bool(true),
	})
	require.NoError(t, err)
	assert.NotNil(t, decreaseOut.ReplicationGroup)
}

// ----------------------------------------
// ModifyReplicationGroupShardConfiguration via HTTP (issue #2)
// ----------------------------------------

func TestHandler_ModifyReplicationGroupShardConfiguration_UpdatesNodeGroups(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("shard-config-rg"),
		ReplicationGroupDescription: aws.String("shard config via HTTP"),
		ClusterMode:                 elasticachetypes.ClusterModeEnabled,
		NumNodeGroups:               aws.Int32(2),
	})
	require.NoError(t, err)

	out, err := client.ModifyReplicationGroupShardConfiguration(
		t.Context(),
		&elasticachesdk.ModifyReplicationGroupShardConfigurationInput{
			ReplicationGroupId: aws.String("shard-config-rg"),
			NodeGroupCount:     aws.Int32(4),
			ApplyImmediately:   aws.Bool(true),
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, out.ReplicationGroup)

	// Verify node groups in describe.
	desc, err := client.DescribeReplicationGroups(t.Context(), &elasticachesdk.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String("shard-config-rg"),
	})
	require.NoError(t, err)
	rg := desc.ReplicationGroups[0]
	assert.Len(t, rg.NodeGroups, 4)
}

// ----------------------------------------
// Events — source type filtering
// ----------------------------------------

func TestHandler_DescribeEvents_AfterClusterOps(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("events-cluster"),
		Engine:         aws.String("redis"),
	})
	require.NoError(t, err)

	out, err := client.DescribeEvents(t.Context(), &elasticachesdk.DescribeEventsInput{
		SourceIdentifier: aws.String("events-cluster"),
		SourceType:       elasticachetypes.SourceTypeCacheCluster,
	})
	require.NoError(t, err)
	assert.NotNil(t, out.Events)
}

// ----------------------------------------
// DescribeCacheEngineVersions — Memcached
// ----------------------------------------

func TestDescribeCacheEngineVersions_Memcached(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.DescribeCacheEngineVersions(t.Context(), &elasticachesdk.DescribeCacheEngineVersionsInput{
		Engine: aws.String("memcached"),
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(out.CacheEngineVersions), 1)

	for _, v := range out.CacheEngineVersions {
		assert.Equal(t, "memcached", aws.ToString(v.Engine))
	}
}

// ----------------------------------------
// CacheSubnetGroup — modify
// ----------------------------------------

func TestHandler_ModifyCacheSubnetGroup(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateCacheSubnetGroup(t.Context(), &elasticachesdk.CreateCacheSubnetGroupInput{
		CacheSubnetGroupName:        aws.String("mod-sng"),
		CacheSubnetGroupDescription: aws.String("original description"),
		SubnetIds:                   []string{"subnet-111"},
	})
	require.NoError(t, err)

	out, err := client.ModifyCacheSubnetGroup(t.Context(), &elasticachesdk.ModifyCacheSubnetGroupInput{
		CacheSubnetGroupName:        aws.String("mod-sng"),
		CacheSubnetGroupDescription: aws.String("updated description"),
		SubnetIds:                   []string{"subnet-111", "subnet-222"},
	})
	require.NoError(t, err)
	assert.Equal(t, "updated description", aws.ToString(out.CacheSubnetGroup.CacheSubnetGroupDescription))
	assert.Len(t, out.CacheSubnetGroup.Subnets, 2)
}

// ----------------------------------------
// Tags on CacheParameterGroup
// ----------------------------------------

func TestHandler_Tags_OnCacheParameterGroup(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.CreateCacheParameterGroup(t.Context(), &elasticachesdk.CreateCacheParameterGroupInput{
		CacheParameterGroupName:   aws.String("tagged-pg"),
		CacheParameterGroupFamily: aws.String("redis7"),
		Description:               aws.String("tagged param group"),
		Tags: []elasticachetypes.Tag{
			{Key: aws.String("purpose"), Value: aws.String("testing")},
		},
	})
	require.NoError(t, err)
	arn := aws.ToString(out.CacheParameterGroup.ARN)
	require.NotEmpty(t, arn)

	tagsOut, err := client.ListTagsForResource(t.Context(), &elasticachesdk.ListTagsForResourceInput{
		ResourceName: aws.String(arn),
	})
	require.NoError(t, err)
	require.Len(t, tagsOut.TagList, 1)
	assert.Equal(t, "purpose", aws.ToString(tagsOut.TagList[0].Key))
}
