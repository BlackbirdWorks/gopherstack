package elasticache_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticachetypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_AvailabilityZonesSecurityGroupsAndParameters drives the
// gopherstack-xhu2t slice-13 elasticache tier-1 fixes through the real
// aws-sdk-go-v2 client (newTestStack, shared with this package's other
// handler tests).
func TestRealClient_AvailabilityZonesSecurityGroupsAndParameters(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testCreateCacheClusterAvailabilityZonesRealClient, "create_cache_cluster_availability_zones"},
		{testDeleteServerlessCacheFinalSnapshotRealClient, "delete_serverless_cache_final_snapshot"},
		{testDescribeCacheEngineVersionsDefaultOnlyRealClient, "describe_cache_engine_versions_default_only"},
		{testDescribeCacheParametersSourceRealClient, "describe_cache_parameters_source"},
		{testModifyCacheClusterAuthAndSecurityGroupsRealClient, "modify_cache_cluster_auth_and_security_groups"},
		{testModifyReplicationGroupSecurityGroupsRealClient, "modify_replication_group_security_groups"},
		{
			testModifyReplicationGroupShardConfigurationReshardingRealClient,
			"modify_replication_group_shard_configuration_resharding",
		},
		{testCopyServerlessCacheSnapshotTagsRealClient, "copy_serverless_cache_snapshot_tags"},
		{testCacheSecurityGroupsPaginationRealClient, "cache_security_groups_pagination"},
		{testServerlessCachesPaginationRealClient, "serverless_caches_pagination"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

func testCreateCacheClusterAvailabilityZonesRealClient(t *testing.T) {
	t.Helper()

	client := newTestStack(t)
	ctx := t.Context()

	single, err := client.CreateCacheCluster(ctx, &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId:            aws.String("az-single-cluster"),
		Engine:                    aws.String("redis"),
		CacheNodeType:             aws.String("cache.t3.micro"),
		NumCacheNodes:             aws.Int32(1),
		PreferredAvailabilityZone: aws.String("us-east-1c"),
	})
	require.NoError(t, err)
	assert.Equal(t, "us-east-1c", aws.ToString(single.CacheCluster.PreferredAvailabilityZone))
	require.Len(t, single.CacheCluster.CacheNodes, 1)
	assert.Equal(t, "us-east-1c", aws.ToString(single.CacheCluster.CacheNodes[0].CustomerAvailabilityZone))

	multi, err := client.CreateCacheCluster(ctx, &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("az-multi-cluster"),
		Engine:         aws.String("memcached"),
		CacheNodeType:  aws.String("cache.t3.micro"),
		NumCacheNodes:  aws.Int32(2),
		PreferredAvailabilityZones: []string{
			"us-east-1a",
			"us-east-1b",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "Multiple", aws.ToString(multi.CacheCluster.PreferredAvailabilityZone))
	require.Len(t, multi.CacheCluster.CacheNodes, 2)
	assert.Equal(t, "us-east-1a", aws.ToString(multi.CacheCluster.CacheNodes[0].CustomerAvailabilityZone))
	assert.Equal(t, "us-east-1b", aws.ToString(multi.CacheCluster.CacheNodes[1].CustomerAvailabilityZone))
}

func testDeleteServerlessCacheFinalSnapshotRealClient(t *testing.T) {
	t.Helper()

	client := newTestStack(t)
	ctx := t.Context()

	_, err := client.CreateServerlessCache(ctx, &elasticachesdk.CreateServerlessCacheInput{
		ServerlessCacheName: aws.String("final-snap-cache"),
		Engine:              aws.String("redis"),
	})
	require.NoError(t, err)

	_, err = client.DeleteServerlessCache(ctx, &elasticachesdk.DeleteServerlessCacheInput{
		ServerlessCacheName: aws.String("final-snap-cache"),
		FinalSnapshotName:   aws.String("final-snap-on-delete"),
	})
	require.NoError(t, err)

	snapOut, err := client.DescribeServerlessCacheSnapshots(ctx, &elasticachesdk.DescribeServerlessCacheSnapshotsInput{
		ServerlessCacheSnapshotName: aws.String("final-snap-on-delete"),
	})
	require.NoError(t, err)
	require.Len(t, snapOut.ServerlessCacheSnapshots, 1)
	require.NotNil(t, snapOut.ServerlessCacheSnapshots[0].ServerlessCacheConfiguration)
	assert.Equal(
		t,
		"final-snap-cache",
		aws.ToString(snapOut.ServerlessCacheSnapshots[0].ServerlessCacheConfiguration.ServerlessCacheName),
	)
}

func testDescribeCacheEngineVersionsDefaultOnlyRealClient(t *testing.T) {
	t.Helper()

	client := newTestStack(t)
	ctx := t.Context()

	all, err := client.DescribeCacheEngineVersions(ctx, &elasticachesdk.DescribeCacheEngineVersionsInput{
		Engine: aws.String("redis"),
	})
	require.NoError(t, err)
	require.Greater(t, len(all.CacheEngineVersions), 1)

	defaultOnly, err := client.DescribeCacheEngineVersions(ctx, &elasticachesdk.DescribeCacheEngineVersionsInput{
		Engine:      aws.String("redis"),
		DefaultOnly: aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, defaultOnly.CacheEngineVersions, 1)
}

func testDescribeCacheParametersSourceRealClient(t *testing.T) {
	t.Helper()

	client := newTestStack(t)
	ctx := t.Context()

	_, err := client.CreateCacheParameterGroup(ctx, &elasticachesdk.CreateCacheParameterGroupInput{
		CacheParameterGroupName:   aws.String("source-filter-pg"),
		CacheParameterGroupFamily: aws.String("redis7"),
		Description:               aws.String("source filter test"),
	})
	require.NoError(t, err)

	_, err = client.ModifyCacheParameterGroup(ctx, &elasticachesdk.ModifyCacheParameterGroupInput{
		CacheParameterGroupName: aws.String("source-filter-pg"),
		ParameterNameValues: []elasticachetypes.ParameterNameValue{
			{ParameterName: aws.String("maxmemory-policy"), ParameterValue: aws.String("allkeys-lru")},
		},
	})
	require.NoError(t, err)

	userOnly, err := client.DescribeCacheParameters(ctx, &elasticachesdk.DescribeCacheParametersInput{
		CacheParameterGroupName: aws.String("source-filter-pg"),
		Source:                  aws.String("user"),
	})
	require.NoError(t, err)
	require.Len(t, userOnly.Parameters, 1)
	assert.Equal(t, "maxmemory-policy", aws.ToString(userOnly.Parameters[0].ParameterName))

	defaultOnly, err := client.DescribeCacheParameters(ctx, &elasticachesdk.DescribeCacheParametersInput{
		CacheParameterGroupName: aws.String("source-filter-pg"),
		Source:                  aws.String("engine-default"),
	})
	require.NoError(t, err)
	require.Greater(t, len(defaultOnly.Parameters), 1)
	for _, p := range defaultOnly.Parameters {
		assert.NotEqual(t, "maxmemory-policy", aws.ToString(p.ParameterName))
	}
}

func testModifyCacheClusterAuthAndSecurityGroupsRealClient(t *testing.T) {
	t.Helper()

	client := newTestStack(t)
	ctx := t.Context()

	_, err := client.CreateCacheSecurityGroup(ctx, &elasticachesdk.CreateCacheSecurityGroupInput{
		CacheSecurityGroupName: aws.String("modify-cluster-sg"),
		Description:            aws.String("for modify cache cluster test"),
	})
	require.NoError(t, err)

	_, err = client.CreateCacheCluster(ctx, &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("modify-auth-cluster"),
		Engine:         aws.String("redis"),
		CacheNodeType:  aws.String("cache.t3.micro"),
		NumCacheNodes:  aws.Int32(1),
	})
	require.NoError(t, err)

	modified, err := client.ModifyCacheCluster(ctx, &elasticachesdk.ModifyCacheClusterInput{
		CacheClusterId:          aws.String("modify-auth-cluster"),
		AuthToken:               aws.String("a-very-long-auth-token-1234567890"),
		AuthTokenUpdateStrategy: elasticachetypes.AuthTokenUpdateStrategyTypeSet,
		CacheSecurityGroupNames: []string{"modify-cluster-sg"},
		ApplyImmediately:        aws.Bool(false),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(modified.CacheCluster.AuthTokenEnabled))
	require.Len(t, modified.CacheCluster.CacheSecurityGroups, 1)
	assert.Equal(
		t,
		"modify-cluster-sg",
		aws.ToString(modified.CacheCluster.CacheSecurityGroups[0].CacheSecurityGroupName),
	)

	_, err = client.ModifyCacheCluster(ctx, &elasticachesdk.ModifyCacheClusterInput{
		CacheClusterId:          aws.String("modify-auth-cluster"),
		CacheSecurityGroupNames: []string{"no-such-security-group"},
	})
	require.Error(t, err)
}

func testModifyReplicationGroupSecurityGroupsRealClient(t *testing.T) {
	t.Helper()

	client := newTestStack(t)
	ctx := t.Context()

	_, err := client.CreateCacheSecurityGroup(ctx, &elasticachesdk.CreateCacheSecurityGroupInput{
		CacheSecurityGroupName: aws.String("modify-rg-sg"),
		Description:            aws.String("for modify replication group test"),
	})
	require.NoError(t, err)

	_, err = client.CreateReplicationGroup(ctx, &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("modify-rg-sg-rg"),
		ReplicationGroupDescription: aws.String("sg propagation test"),
		Engine:                      aws.String("redis"),
		CacheNodeType:               aws.String("cache.t3.micro"),
		NumCacheClusters:            aws.Int32(1),
	})
	require.NoError(t, err)

	// This backend doesn't spawn member CacheClusters from
	// CreateReplicationGroup itself (a pre-existing structural
	// simplification): join one explicitly via CreateCacheCluster's
	// ReplicationGroupId so there is a member to observe the propagated
	// security group on.
	_, err = client.CreateCacheCluster(ctx, &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId:     aws.String("modify-rg-sg-member"),
		ReplicationGroupId: aws.String("modify-rg-sg-rg"),
	})
	require.NoError(t, err)

	_, err = client.ModifyReplicationGroup(ctx, &elasticachesdk.ModifyReplicationGroupInput{
		ReplicationGroupId:      aws.String("modify-rg-sg-rg"),
		CacheSecurityGroupNames: []string{"modify-rg-sg"},
		ApplyImmediately:        aws.Bool(true),
	})
	require.NoError(t, err)

	clustersOut, err := client.DescribeCacheClusters(ctx, &elasticachesdk.DescribeCacheClustersInput{})
	require.NoError(t, err)
	var found bool
	for _, cl := range clustersOut.CacheClusters {
		if aws.ToString(cl.ReplicationGroupId) != "modify-rg-sg-rg" {
			continue
		}
		found = true
		require.Len(t, cl.CacheSecurityGroups, 1)
		assert.Equal(t, "modify-rg-sg", aws.ToString(cl.CacheSecurityGroups[0].CacheSecurityGroupName))
	}
	assert.True(t, found, "expected to find a member cluster of modify-rg-sg-rg")
}

func testModifyReplicationGroupShardConfigurationReshardingRealClient(t *testing.T) {
	t.Helper()

	client := newTestStack(t)
	ctx := t.Context()

	_, err := client.CreateReplicationGroup(ctx, &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("resharding-rg"),
		ReplicationGroupDescription: aws.String("resharding test"),
		Engine:                      aws.String("redis"),
		CacheNodeType:               aws.String("cache.t3.micro"),
		ClusterMode:                 elasticachetypes.ClusterModeEnabled,
		NumNodeGroups:               aws.Int32(1),
		ReplicasPerNodeGroup:        aws.Int32(1),
		AutomaticFailoverEnabled:    aws.Bool(true),
	})
	require.NoError(t, err)

	out, err := client.ModifyReplicationGroupShardConfiguration(
		ctx,
		&elasticachesdk.ModifyReplicationGroupShardConfigurationInput{
			ReplicationGroupId: aws.String("resharding-rg"),
			NodeGroupCount:     aws.Int32(2),
			ApplyImmediately:   aws.Bool(true),
			ReshardingConfiguration: []elasticachetypes.ReshardingConfiguration{
				{PreferredAvailabilityZones: []string{"us-east-1f"}},
			},
		},
	)
	require.NoError(t, err)
	require.Len(t, out.ReplicationGroup.NodeGroups, 2)
	newGroup := out.ReplicationGroup.NodeGroups[1]
	require.Len(t, newGroup.NodeGroupMembers, 1)
	assert.Equal(t, "us-east-1f", aws.ToString(newGroup.NodeGroupMembers[0].PreferredAvailabilityZone))
}

func testCopyServerlessCacheSnapshotTagsRealClient(t *testing.T) {
	t.Helper()

	client := newTestStack(t)
	ctx := t.Context()

	_, err := client.CreateServerlessCache(ctx, &elasticachesdk.CreateServerlessCacheInput{
		ServerlessCacheName: aws.String("copy-snap-source-cache"),
		Engine:              aws.String("redis"),
	})
	require.NoError(t, err)

	_, err = client.CreateServerlessCacheSnapshot(ctx, &elasticachesdk.CreateServerlessCacheSnapshotInput{
		ServerlessCacheSnapshotName: aws.String("copy-snap-source"),
		ServerlessCacheName:         aws.String("copy-snap-source-cache"),
	})
	require.NoError(t, err)

	copied, err := client.CopyServerlessCacheSnapshot(ctx, &elasticachesdk.CopyServerlessCacheSnapshotInput{
		SourceServerlessCacheSnapshotName: aws.String("copy-snap-source"),
		TargetServerlessCacheSnapshotName: aws.String("copy-snap-target"),
		Tags: []elasticachetypes.Tag{
			{Key: aws.String("env"), Value: aws.String("test")},
		},
	})
	require.NoError(t, err)

	tagsOut, err := client.ListTagsForResource(ctx, &elasticachesdk.ListTagsForResourceInput{
		ResourceName: copied.ServerlessCacheSnapshot.ARN,
	})
	require.NoError(t, err)
	require.Len(t, tagsOut.TagList, 1)
	assert.Equal(t, "env", aws.ToString(tagsOut.TagList[0].Key))
	assert.Equal(t, "test", aws.ToString(tagsOut.TagList[0].Value))
}

func testCacheSecurityGroupsPaginationRealClient(t *testing.T) {
	t.Helper()

	client := newTestStack(t)
	ctx := t.Context()

	// AWS enforces MaxRecords in [20,100] for this operation (unlike
	// MaxResults on the newer serverless ops), so proving truncation needs
	// more than 20 groups.
	const groupCount = 21
	for i := range groupCount {
		_, err := client.CreateCacheSecurityGroup(ctx, &elasticachesdk.CreateCacheSecurityGroupInput{
			CacheSecurityGroupName: aws.String(fmt.Sprintf("page-sg-%02d", i)),
			Description:            aws.String("pagination test"),
		})
		require.NoError(t, err)
	}

	first, err := client.DescribeCacheSecurityGroups(ctx, &elasticachesdk.DescribeCacheSecurityGroupsInput{
		MaxRecords: aws.Int32(20),
	})
	require.NoError(t, err)
	assert.Len(t, first.CacheSecurityGroups, 20)
	require.NotEmpty(t, aws.ToString(first.Marker))
}

func testServerlessCachesPaginationRealClient(t *testing.T) {
	t.Helper()

	client := newTestStack(t)
	ctx := t.Context()

	for _, name := range []string{"page-sc-a", "page-sc-b"} {
		_, err := client.CreateServerlessCache(ctx, &elasticachesdk.CreateServerlessCacheInput{
			ServerlessCacheName: aws.String(name),
			Engine:              aws.String("redis"),
		})
		require.NoError(t, err)
	}

	first, err := client.DescribeServerlessCaches(ctx, &elasticachesdk.DescribeServerlessCachesInput{
		MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	assert.Len(t, first.ServerlessCaches, 1)
	require.NotEmpty(t, aws.ToString(first.NextToken))
}
