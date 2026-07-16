package elasticache_test

import (
	"testing"

	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticachetypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_DescribeEvents(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.DescribeEvents(t.Context(), &elasticachesdk.DescribeEventsInput{})

	require.NoError(t, err)
	assert.NotNil(t, out.Events)
}

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
