package elasticache_test

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/elasticache"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func newTestStack(t *testing.T) (*echo.Echo, *elasticachesdk.Client) {
	t.Helper()

	backend := elasticache.NewInMemoryBackend(elasticache.EngineEmbedded, "000000000000", "us-east-1")
	handler := elasticache.NewHandler(backend, nil)

	e := echo.New()
	registry := service.NewRegistry(slog.Default())
	_ = registry.Register(handler)
	router := service.NewServiceRouter(registry)
	e.Use(router.RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(context.Background(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := elasticachesdk.NewFromConfig(cfg, func(o *elasticachesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	return e, client
}

func TestCreateDescribeDeleteCacheCluster(t *testing.T) {
	_, client := newTestStack(t)

	// Create cluster
	out, err := client.CreateCacheCluster(context.Background(), &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("my-cluster"),
		Engine:         aws.String("redis"),
		CacheNodeType:  aws.String("cache.t3.micro"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.CacheCluster)
	assert.Equal(t, "my-cluster", aws.ToString(out.CacheCluster.CacheClusterId))
	assert.Equal(t, "available", aws.ToString(out.CacheCluster.CacheClusterStatus))
	assert.Equal(t, "redis", aws.ToString(out.CacheCluster.Engine))

	// Verify endpoint is populated (embedded mode starts miniredis)
	require.NotEmpty(t, out.CacheCluster.CacheNodes)
	ep := out.CacheCluster.CacheNodes[0].Endpoint
	require.NotNil(t, ep)
	assert.Equal(t, "localhost", aws.ToString(ep.Address))
	assert.Greater(t, aws.ToInt32(ep.Port), int32(0))

	// Describe cluster
	descOut, err := client.DescribeCacheClusters(context.Background(), &elasticachesdk.DescribeCacheClustersInput{
		CacheClusterId: aws.String("my-cluster"),
	})
	require.NoError(t, err)
	require.Len(t, descOut.CacheClusters, 1)
	assert.Equal(t, "my-cluster", aws.ToString(descOut.CacheClusters[0].CacheClusterId))

	// Delete cluster
	delOut, err := client.DeleteCacheCluster(context.Background(), &elasticachesdk.DeleteCacheClusterInput{
		CacheClusterId: aws.String("my-cluster"),
	})
	require.NoError(t, err)
	require.NotNil(t, delOut.CacheCluster)
	assert.Equal(t, "deleting", aws.ToString(delOut.CacheCluster.CacheClusterStatus))

	// Describe after delete → not found
	_, err = client.DescribeCacheClusters(context.Background(), &elasticachesdk.DescribeCacheClustersInput{
		CacheClusterId: aws.String("my-cluster"),
	})
	assert.Error(t, err)
}

func TestDescribeAllClusters(t *testing.T) {
	_, client := newTestStack(t)

	_, err := client.CreateCacheCluster(context.Background(), &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("cluster-a"),
		Engine:         aws.String("redis"),
	})
	require.NoError(t, err)

	_, err = client.CreateCacheCluster(context.Background(), &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("cluster-b"),
		Engine:         aws.String("redis"),
	})
	require.NoError(t, err)

	out, err := client.DescribeCacheClusters(context.Background(), &elasticachesdk.DescribeCacheClustersInput{})
	require.NoError(t, err)
	assert.Len(t, out.CacheClusters, 2)
}

func TestCreateClusterAlreadyExists(t *testing.T) {
	_, client := newTestStack(t)

	_, err := client.CreateCacheCluster(context.Background(), &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("dup"),
		Engine:         aws.String("redis"),
	})
	require.NoError(t, err)

	_, err = client.CreateCacheCluster(context.Background(), &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("dup"),
		Engine:         aws.String("redis"),
	})
	assert.Error(t, err)
}

func TestReplicationGroupCRUD(t *testing.T) {
	_, client := newTestStack(t)

	// Create replication group
	createOut, err := client.CreateReplicationGroup(context.Background(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("my-rg"),
		ReplicationGroupDescription: aws.String("test replication group"),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.ReplicationGroup)
	assert.Equal(t, "my-rg", aws.ToString(createOut.ReplicationGroup.ReplicationGroupId))
	assert.Equal(t, "available", aws.ToString(createOut.ReplicationGroup.Status))

	// Describe replication group
	descOut, err := client.DescribeReplicationGroups(context.Background(), &elasticachesdk.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String("my-rg"),
	})
	require.NoError(t, err)
	require.Len(t, descOut.ReplicationGroups, 1)
	assert.Equal(t, "my-rg", aws.ToString(descOut.ReplicationGroups[0].ReplicationGroupId))

	// Delete replication group
	delOut, err := client.DeleteReplicationGroup(context.Background(), &elasticachesdk.DeleteReplicationGroupInput{
		ReplicationGroupId: aws.String("my-rg"),
	})
	require.NoError(t, err)
	require.NotNil(t, delOut.ReplicationGroup)
	assert.Equal(t, "deleting", aws.ToString(delOut.ReplicationGroup.Status))
}

func TestStubEngineMode(t *testing.T) {
	backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")
	cluster, err := backend.CreateCluster("stub-cluster", "redis", "cache.t3.micro", 0)
	require.NoError(t, err)
	assert.Equal(t, "localhost", cluster.Endpoint)
	assert.Equal(t, 6379, cluster.Port)
	assert.Equal(t, "available", cluster.Status)
}

func TestListTagsForResource(t *testing.T) {
	_, client := newTestStack(t)

	createOut, err := client.CreateCacheCluster(context.Background(), &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("tag-cluster"),
		Engine:         aws.String("redis"),
	})
	require.NoError(t, err)

	arn := aws.ToString(createOut.CacheCluster.ARN)
	out, err := client.ListTagsForResource(context.Background(), &elasticachesdk.ListTagsForResourceInput{
		ResourceName: aws.String(arn),
	})
	require.NoError(t, err)
	assert.NotNil(t, out)
	// No tags added, so TagList is empty
	assert.Empty(t, out.TagList)
}

// newTestHandler creates a raw handler for internal tests.
func newTestHandler(t *testing.T) *elasticache.Handler {
	t.Helper()
	backend := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1")
	return elasticache.NewHandler(backend, nil)
}

func TestHandlerName(t *testing.T) {
	h := newTestHandler(t)
	assert.Equal(t, "ElastiCache", h.Name())
}

func TestHandlerSupportedOperations(t *testing.T) {
	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateCacheCluster")
	assert.Contains(t, ops, "DeleteCacheCluster")
	assert.Contains(t, ops, "DescribeCacheClusters")
	assert.Contains(t, ops, "ListTagsForResource")
	assert.Contains(t, ops, "CreateReplicationGroup")
	assert.Contains(t, ops, "DeleteReplicationGroup")
	assert.Contains(t, ops, "DescribeReplicationGroups")
}

func TestRouteMatcher_NonPost(t *testing.T) {
	h := newTestHandler(t)
	e := echo.New()
	req := httptest.NewRequest("GET", "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	matcher := h.RouteMatcher()
	assert.False(t, matcher(c))
}

func TestRouteMatcher_WrongVersion(t *testing.T) {
	h := newTestHandler(t)
	e := echo.New()
	body := strings.NewReader("Action=CreateCacheCluster&Version=2012-01-01")
	req := httptest.NewRequest("POST", "/", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	matcher := h.RouteMatcher()
	assert.False(t, matcher(c))
}
