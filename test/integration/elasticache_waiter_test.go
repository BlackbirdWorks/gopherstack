package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_ElastiCache_CacheClusterAvailableWaiter verifies that
// CacheClusterAvailableWaiter succeeds immediately after CreateCacheCluster
// because the status is "available" from creation.
func TestIntegration_ElastiCache_CacheClusterAvailableWaiter(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createElastiCacheClient(t)
	ctx := t.Context()

	clusterID := "waiter-ec-" + uuid.NewString()[:8]

	createOut, err := client.CreateCacheCluster(ctx, &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String(clusterID),
		Engine:         aws.String("redis"),
		CacheNodeType:  aws.String("cache.t3.micro"),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.CacheCluster)

	t.Cleanup(func() {
		_, _ = client.DeleteCacheCluster(ctx, &elasticachesdk.DeleteCacheClusterInput{
			CacheClusterId: aws.String(clusterID),
		})
	})

	waiter := elasticachesdk.NewCacheClusterAvailableWaiter(client)
	start := time.Now()
	err = waiter.Wait(ctx, &elasticachesdk.DescribeCacheClustersInput{
		CacheClusterId: aws.String(clusterID),
	}, 5*time.Second)
	elapsed := time.Since(start)

	require.NoError(t, err, "CacheClusterAvailableWaiter should succeed after cluster is created")
	assert.Less(t, elapsed, 2*time.Second, "CacheClusterAvailableWaiter should complete quickly, took %v", elapsed)
}
