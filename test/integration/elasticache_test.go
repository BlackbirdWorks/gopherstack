package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticachetypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_ElastiCache_ClusterLifecycle tests create, describe, and delete.
func TestIntegration_ElastiCache_ClusterLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createElastiCacheClient(t)
	ctx := t.Context()

	clusterID := "test-cluster-" + uuid.NewString()[:8]

	// CreateCacheCluster
	createOut, err := client.CreateCacheCluster(ctx, &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String(clusterID),
		Engine:         aws.String("redis"),
		CacheNodeType:  aws.String("cache.t3.micro"),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.CacheCluster)
	assert.Equal(t, clusterID, aws.ToString(createOut.CacheCluster.CacheClusterId))
	assert.Equal(t, "available", aws.ToString(createOut.CacheCluster.CacheClusterStatus))
	assert.Equal(t, "redis", aws.ToString(createOut.CacheCluster.Engine))

	// Verify endpoint returned (embedded mode)
	require.NotEmpty(t, createOut.CacheCluster.CacheNodes)
	ep := createOut.CacheCluster.CacheNodes[0].Endpoint
	require.NotNil(t, ep)
	assert.Contains(t, aws.ToString(ep.Address), ".cache.amazonaws.com")
	assert.Positive(t, aws.ToInt32(ep.Port))

	// DescribeCacheClusters — specific cluster
	descOut, err := client.DescribeCacheClusters(ctx, &elasticachesdk.DescribeCacheClustersInput{
		CacheClusterId: aws.String(clusterID),
	})
	require.NoError(t, err)
	require.Len(t, descOut.CacheClusters, 1)
	assert.Equal(t, clusterID, aws.ToString(descOut.CacheClusters[0].CacheClusterId))

	// ListTagsForResource
	arn := aws.ToString(createOut.CacheCluster.ARN)
	tagsOut, err := client.ListTagsForResource(ctx, &elasticachesdk.ListTagsForResourceInput{
		ResourceName: aws.String(arn),
	})
	require.NoError(t, err)
	assert.NotNil(t, tagsOut)

	// DeleteCacheCluster
	delOut, err := client.DeleteCacheCluster(ctx, &elasticachesdk.DeleteCacheClusterInput{
		CacheClusterId: aws.String(clusterID),
	})
	require.NoError(t, err)
	require.NotNil(t, delOut.CacheCluster)
	assert.Equal(t, "deleting", aws.ToString(delOut.CacheCluster.CacheClusterStatus))

	// Verify gone
	_, err = client.DescribeCacheClusters(ctx, &elasticachesdk.DescribeCacheClustersInput{
		CacheClusterId: aws.String(clusterID),
	})
	assert.Error(t, err)
}

// TestIntegration_ElastiCache_DescribeAll tests describe without filter returns all clusters.
func TestIntegration_ElastiCache_DescribeAll(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createElastiCacheClient(t)
	ctx := t.Context()

	ids := []string{
		"int-cluster-a-" + uuid.NewString()[:8],
		"int-cluster-b-" + uuid.NewString()[:8],
	}

	for _, id := range ids {
		_, err := client.CreateCacheCluster(ctx, &elasticachesdk.CreateCacheClusterInput{
			CacheClusterId: aws.String(id),
			Engine:         aws.String("redis"),
		})
		require.NoError(t, err)
	}

	out, err := client.DescribeCacheClusters(ctx, &elasticachesdk.DescribeCacheClustersInput{})
	require.NoError(t, err)
	// At least 2 clusters (others may be created by parallel tests)
	assert.GreaterOrEqual(t, len(out.CacheClusters), 2)

	// Clean up
	for _, id := range ids {
		_, delErr := client.DeleteCacheCluster(ctx, &elasticachesdk.DeleteCacheClusterInput{
			CacheClusterId: aws.String(id),
		})
		require.NoError(t, delErr)
	}
}

// TestIntegration_ElastiCache_ReplicationGroupLifecycle tests replication group CRUD.
func TestIntegration_ElastiCache_ReplicationGroupLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createElastiCacheClient(t)
	ctx := t.Context()

	rgID := "test-rg-" + uuid.NewString()[:8]

	// CreateReplicationGroup
	createOut, err := client.CreateReplicationGroup(ctx, &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String(rgID),
		ReplicationGroupDescription: aws.String("integration test replication group"),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.ReplicationGroup)
	assert.Equal(t, rgID, aws.ToString(createOut.ReplicationGroup.ReplicationGroupId))
	assert.Equal(t, "available", aws.ToString(createOut.ReplicationGroup.Status))

	// DescribeReplicationGroups
	descOut, err := client.DescribeReplicationGroups(ctx, &elasticachesdk.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String(rgID),
	})
	require.NoError(t, err)
	require.Len(t, descOut.ReplicationGroups, 1)
	assert.Equal(t, rgID, aws.ToString(descOut.ReplicationGroups[0].ReplicationGroupId))

	// DeleteReplicationGroup
	delOut, err := client.DeleteReplicationGroup(ctx, &elasticachesdk.DeleteReplicationGroupInput{
		ReplicationGroupId: aws.String(rgID),
	})
	require.NoError(t, err)
	require.NotNil(t, delOut.ReplicationGroup)
	assert.Equal(t, "deleting", aws.ToString(delOut.ReplicationGroup.Status))
}

// TestIntegration_ElastiCache_UserLifecycle tests user CRUD operations.
func TestIntegration_ElastiCache_UserLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createElastiCacheClient(t)
	ctx := t.Context()

	userID := "test-user-" + uuid.NewString()[:8]

	// CreateUser - the output fields are flat (no User wrapper in SDK v2).
	createOut, err := client.CreateUser(ctx, &elasticachesdk.CreateUserInput{
		UserId:             aws.String(userID),
		UserName:           aws.String(userID),
		Engine:             aws.String("Redis"),
		NoPasswordRequired: aws.Bool(true),
		AccessString:       aws.String("on ~* +@all"),
	})
	require.NoError(t, err)
	assert.Equal(t, userID, aws.ToString(createOut.UserId))
	assert.Equal(t, "active", aws.ToString(createOut.Status))

	// DescribeUsers
	descOut, err := client.DescribeUsers(ctx, &elasticachesdk.DescribeUsersInput{
		UserId: aws.String(userID),
	})
	require.NoError(t, err)
	require.Len(t, descOut.Users, 1)
	assert.Equal(t, userID, aws.ToString(descOut.Users[0].UserId))

	// DeleteUser - output fields also flat in SDK v2.
	delOut, err := client.DeleteUser(ctx, &elasticachesdk.DeleteUserInput{
		UserId: aws.String(userID),
	})
	require.NoError(t, err)
	assert.Equal(t, userID, aws.ToString(delOut.UserId))
}

// TestIntegration_ElastiCache_UserGroupLifecycle tests user group CRUD operations.
func TestIntegration_ElastiCache_UserGroupLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createElastiCacheClient(t)
	ctx := t.Context()

	groupID := "test-ug-" + uuid.NewString()[:8]

	// CreateUserGroup
	createOut, err := client.CreateUserGroup(ctx, &elasticachesdk.CreateUserGroupInput{
		UserGroupId: aws.String(groupID),
		Engine:      aws.String("Redis"),
	})
	require.NoError(t, err)
	assert.Equal(t, groupID, aws.ToString(createOut.UserGroupId))
	assert.Equal(t, "active", aws.ToString(createOut.Status))

	// DescribeUserGroups
	descOut, err := client.DescribeUserGroups(ctx, &elasticachesdk.DescribeUserGroupsInput{
		UserGroupId: aws.String(groupID),
	})
	require.NoError(t, err)
	require.Len(t, descOut.UserGroups, 1)
	assert.Equal(t, groupID, aws.ToString(descOut.UserGroups[0].UserGroupId))

	// DeleteUserGroup
	delOut, err := client.DeleteUserGroup(ctx, &elasticachesdk.DeleteUserGroupInput{
		UserGroupId: aws.String(groupID),
	})
	require.NoError(t, err)
	assert.Equal(t, groupID, aws.ToString(delOut.UserGroupId))
}

// TestIntegration_ElastiCache_ServerlessCacheLifecycle tests serverless cache CRUD.
func TestIntegration_ElastiCache_ServerlessCacheLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createElastiCacheClient(t)
	ctx := t.Context()

	name := "test-sl-" + uuid.NewString()[:8]

	// CreateServerlessCache
	createOut, err := client.CreateServerlessCache(ctx, &elasticachesdk.CreateServerlessCacheInput{
		ServerlessCacheName: aws.String(name),
		Engine:              aws.String("redis"),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.ServerlessCache)
	assert.Equal(t, name, aws.ToString(createOut.ServerlessCache.ServerlessCacheName))
	assert.Equal(t, "available", aws.ToString(createOut.ServerlessCache.Status))

	// DescribeServerlessCaches
	descOut, err := client.DescribeServerlessCaches(ctx, &elasticachesdk.DescribeServerlessCachesInput{
		ServerlessCacheName: aws.String(name),
	})
	require.NoError(t, err)
	require.Len(t, descOut.ServerlessCaches, 1)
	assert.Equal(t, name, aws.ToString(descOut.ServerlessCaches[0].ServerlessCacheName))

	// DeleteServerlessCache
	delOut, err := client.DeleteServerlessCache(ctx, &elasticachesdk.DeleteServerlessCacheInput{
		ServerlessCacheName: aws.String(name),
	})
	require.NoError(t, err)
	require.NotNil(t, delOut.ServerlessCache)
	assert.Equal(t, name, aws.ToString(delOut.ServerlessCache.ServerlessCacheName))
}

// TestIntegration_ElastiCache_ReservedCacheNodesLifecycle tests reserved cache node operations.
func TestIntegration_ElastiCache_ReservedCacheNodesLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createElastiCacheClient(t)
	ctx := t.Context()

	// DescribeReservedCacheNodesOfferings
	offeringsOut, err := client.DescribeReservedCacheNodesOfferings(
		ctx,
		&elasticachesdk.DescribeReservedCacheNodesOfferingsInput{},
	)
	require.NoError(t, err)
	require.NotEmpty(t, offeringsOut.ReservedCacheNodesOfferings)

	offeringID := aws.ToString(offeringsOut.ReservedCacheNodesOfferings[0].ReservedCacheNodesOfferingId)
	reservationID := "test-rcn-" + uuid.NewString()[:8]

	// PurchaseReservedCacheNodesOffering
	purchaseOut, err := client.PurchaseReservedCacheNodesOffering(
		ctx,
		&elasticachesdk.PurchaseReservedCacheNodesOfferingInput{
			ReservedCacheNodesOfferingId: aws.String(offeringID),
			ReservedCacheNodeId:          aws.String(reservationID),
			CacheNodeCount:               aws.Int32(1),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, purchaseOut.ReservedCacheNode)
	assert.Equal(t, reservationID, aws.ToString(purchaseOut.ReservedCacheNode.ReservedCacheNodeId))
	assert.Equal(t, "active", aws.ToString(purchaseOut.ReservedCacheNode.State))

	// DescribeReservedCacheNodes
	descOut, err := client.DescribeReservedCacheNodes(
		ctx,
		&elasticachesdk.DescribeReservedCacheNodesInput{
			ReservedCacheNodeId: aws.String(reservationID),
		},
	)
	require.NoError(t, err)
	require.Len(t, descOut.ReservedCacheNodes, 1)
	assert.Equal(t, reservationID, aws.ToString(descOut.ReservedCacheNodes[0].ReservedCacheNodeId))
}

// TestIntegration_ElastiCache_Events tests event recording via DescribeEvents.
func TestIntegration_ElastiCache_Events(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createElastiCacheClient(t)
	ctx := t.Context()

	clusterID := "test-evt-" + uuid.NewString()[:8]

	// Create a cluster (generates event)
	_, err := client.CreateCacheCluster(ctx, &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String(clusterID),
		Engine:         aws.String("redis"),
		CacheNodeType:  aws.String("cache.t3.micro"),
		NumCacheNodes:  aws.Int32(1),
	})
	require.NoError(t, err)

	// DescribeEvents should return at least one event for this cluster
	eventsOut, err := client.DescribeEvents(ctx, &elasticachesdk.DescribeEventsInput{
		SourceIdentifier: aws.String(clusterID),
		SourceType:       elasticachetypes.SourceTypeCacheCluster,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, eventsOut.Events)

	// Cleanup
	_, _ = client.DeleteCacheCluster(ctx, &elasticachesdk.DeleteCacheClusterInput{
		CacheClusterId: aws.String(clusterID),
	})
}
