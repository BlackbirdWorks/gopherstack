package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
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
	assert.Equal(t, "localhost", aws.ToString(ep.Address))
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
