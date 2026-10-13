package elasticache_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticache"
)

func TestSDK_CacheClusterReportsParameterGroupStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		engine   string
		version  *string
		explicit string
		want     string
	}{
		{name: "redis default", engine: "redis", want: "default.redis7"},
		{name: "valkey default", engine: "valkey", want: "default.valkey8"},
		{name: "memcached default", engine: "memcached", want: "default.memcached1.6"},
		{name: "redis 6", engine: "redis", version: aws.String("6.2"), want: "default.redis6.x"},
		{name: "explicit", engine: "redis", explicit: "custom-pg", want: "custom-pg"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)
			ctx := t.Context()

			in := &elasticachesdk.CreateCacheClusterInput{
				CacheClusterId: aws.String("pg-cluster"), Engine: aws.String(tt.engine),
				CacheNodeType: aws.String("cache.t3.micro"), NumCacheNodes: aws.Int32(1),
				EngineVersion: tt.version,
			}
			if tt.explicit != "" {
				_, err := client.CreateCacheParameterGroup(ctx, &elasticachesdk.CreateCacheParameterGroupInput{
					CacheParameterGroupName: aws.String(tt.explicit), CacheParameterGroupFamily: aws.String("redis7"),
					Description: aws.String("d"),
				})
				require.NoError(t, err)

				in.CacheParameterGroupName = aws.String(tt.explicit)
			}

			out, err := client.CreateCacheCluster(ctx, in)
			require.NoError(t, err)
			require.NotNil(t, out.CacheCluster.CacheParameterGroup)
			assert.Equal(t, tt.want, aws.ToString(out.CacheCluster.CacheParameterGroup.CacheParameterGroupName))
			assert.Equal(t, "in-sync", aws.ToString(out.CacheCluster.CacheParameterGroup.ParameterApplyStatus))
		})
	}
}

func TestSDK_NodeGroupStatusFollowsReplicationGroup(t *testing.T) {
	t.Parallel()

	clock := newFakeClock()
	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
	b.SetClock(clock.now)
	b.SetLifecycleDelay(time.Minute)
	client := newTestStackSeeded(t, b)
	ctx := t.Context()

	ngStatuses := func() (string, []string) {
		out, err := client.DescribeReplicationGroups(ctx, &elasticachesdk.DescribeReplicationGroupsInput{
			ReplicationGroupId: aws.String("rg"),
		})
		require.NoError(t, err)

		ngs := make([]string, 0, len(out.ReplicationGroups[0].NodeGroups))
		for _, ng := range out.ReplicationGroups[0].NodeGroups {
			ngs = append(ngs, aws.ToString(ng.Status))
		}

		return aws.ToString(out.ReplicationGroups[0].Status), ngs
	}

	_, err := client.CreateReplicationGroup(ctx, &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId: aws.String("rg"), ReplicationGroupDescription: aws.String("d"),
		Engine: aws.String("redis"), CacheNodeType: aws.String("cache.t3.micro"),
		NumNodeGroups: aws.Int32(2), ReplicasPerNodeGroup: aws.Int32(1),
	})
	require.NoError(t, err)

	status, ngs := ngStatuses()
	assert.Equal(t, "creating", status)
	assert.Equal(t, []string{"creating", "creating"}, ngs)

	clock.advance(2 * time.Minute)

	status, ngs = ngStatuses()
	assert.Equal(t, "available", status)
	assert.Equal(t, []string{"available", "available"}, ngs)

	_, err = client.ModifyReplicationGroup(ctx, &elasticachesdk.ModifyReplicationGroupInput{
		ReplicationGroupId: aws.String(
			"rg",
		), CacheNodeType: aws.String("cache.t3.small"), ApplyImmediately: aws.Bool(true),
	})
	require.NoError(t, err)

	status, ngs = ngStatuses()
	assert.Equal(t, "modifying", status)
	assert.Equal(t, []string{"modifying", "modifying"}, ngs)
}

func TestSDK_CreateCacheClusterEngineVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		engine  string
		version string
		want    string
		wantErr bool
	}{
		{name: "default", engine: "redis", want: "7.1.0"},
		{name: "major minor", engine: "redis", version: "6.2", want: "6.2.6"},
		{name: "wildcard", engine: "redis", version: "6.x", want: "6.2.6"},
		{name: "exact", engine: "redis", version: "7.0.7", want: "7.0.7"},
		{name: "memcached", engine: "memcached", version: "1.5.16", want: "1.5.16"},
		{name: "valkey", engine: "valkey", version: "7.2", want: "7.2.7"},
		{name: "unknown", engine: "redis", version: "9.9", wantErr: true},
		{name: "wrong engine", engine: "memcached", version: "7.0.7", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)
			ctx := t.Context()

			in := &elasticachesdk.CreateCacheClusterInput{
				CacheClusterId: aws.String("ver-cluster"), Engine: aws.String(tt.engine),
				CacheNodeType: aws.String("cache.t3.micro"), NumCacheNodes: aws.Int32(1),
			}
			if tt.version != "" {
				in.EngineVersion = aws.String(tt.version)
			}

			out, err := client.CreateCacheCluster(ctx, in)
			if tt.wantErr {
				require.ErrorContains(t, err, "InvalidParameterValue")

				_, descErr := client.DescribeCacheClusters(ctx, &elasticachesdk.DescribeCacheClustersInput{
					CacheClusterId: aws.String("ver-cluster"),
				})
				require.ErrorContains(t, descErr, "CacheClusterNotFound", "rejected create must not leave a cluster")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, aws.ToString(out.CacheCluster.EngineVersion))

			desc, err := client.DescribeCacheClusters(ctx, &elasticachesdk.DescribeCacheClustersInput{
				CacheClusterId: aws.String("ver-cluster"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, aws.ToString(desc.CacheClusters[0].EngineVersion))
		})
	}
}
