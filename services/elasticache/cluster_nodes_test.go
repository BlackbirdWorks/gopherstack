package elasticache_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticachetypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticache"
)

func requireErrCode(t *testing.T, err error, code string) {
	t.Helper()

	var apiErr smithy.APIError

	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, code, apiErr.ErrorCode())
}

func TestCreateCacheCluster_PlacementAndParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(t *testing.T, c elasticachetypes.CacheCluster)
		in       elasticachesdk.CreateCacheClusterInput
		name     string
		wantCode string
	}{
		{
			name: "cross_az_spreads_nodes",
			in: elasticachesdk.CreateCacheClusterInput{
				Engine: aws.String("memcached"), NumCacheNodes: aws.Int32(3), AZMode: elasticachetypes.AZModeCrossAz,
			},
			check: func(t *testing.T, c elasticachetypes.CacheCluster) {
				t.Helper()
				require.Len(t, c.CacheNodes, 3)
				assert.Equal(t, "us-east-1a", aws.ToString(c.CacheNodes[0].CustomerAvailabilityZone))
				assert.Equal(t, "us-east-1b", aws.ToString(c.CacheNodes[1].CustomerAvailabilityZone))
				assert.Equal(t, "us-east-1c", aws.ToString(c.CacheNodes[2].CustomerAvailabilityZone))
				assert.Equal(t, "Multiple", aws.ToString(c.PreferredAvailabilityZone))
				require.NotNil(t, c.ConfigurationEndpoint)
			},
		},
		{
			name: "single_az_keeps_nodes_together",
			in: elasticachesdk.CreateCacheClusterInput{
				Engine: aws.String("memcached"), NumCacheNodes: aws.Int32(2), AZMode: elasticachetypes.AZModeSingleAz,
			},
			check: func(t *testing.T, c elasticachetypes.CacheCluster) {
				t.Helper()
				assert.Equal(t, aws.ToString(c.CacheNodes[0].CustomerAvailabilityZone),
					aws.ToString(c.CacheNodes[1].CustomerAvailabilityZone))
			},
		},
		{
			name: "outpost_arn_echoed",
			in: elasticachesdk.CreateCacheClusterInput{
				Engine:              aws.String("redis"),
				OutpostMode:         elasticachetypes.OutpostModeSingleOutpost,
				PreferredOutpostArn: aws.String("arn:aws:outposts:us-east-1:000000000000:outpost/op-0123456789abcdef0"),
			},
			check: func(t *testing.T, c elasticachetypes.CacheCluster) {
				t.Helper()
				assert.Equal(t, "arn:aws:outposts:us-east-1:000000000000:outpost/op-0123456789abcdef0",
					aws.ToString(c.PreferredOutpostArn))
			},
		},
		{
			name: "az_mode_on_redis",
			in: elasticachesdk.CreateCacheClusterInput{
				Engine: aws.String("redis"),
				AZMode: elasticachetypes.AZModeCrossAz,
			},
			wantCode: "InvalidParameterCombination",
		},
		{
			name: "zones_must_match_node_count",
			in: elasticachesdk.CreateCacheClusterInput{
				Engine: aws.String("memcached"), NumCacheNodes: aws.Int32(2),
				PreferredAvailabilityZones: []string{"us-east-1a"},
			},
			wantCode: "InvalidParameterCombination",
		},
		{
			name: "bad_outpost_arn",
			in: elasticachesdk.CreateCacheClusterInput{
				Engine: aws.String("redis"), PreferredOutpostArn: aws.String("not-an-arn"),
			},
			wantCode: "InvalidParameterValue",
		},
		{
			name: "bad_az_mode",
			in: elasticachesdk.CreateCacheClusterInput{
				Engine: aws.String("memcached"), AZMode: elasticachetypes.AZMode("diagonal"),
			},
			wantCode: "InvalidParameterValue",
		},
		{
			name:     "redis_multiple_nodes",
			in:       elasticachesdk.CreateCacheClusterInput{Engine: aws.String("redis"), NumCacheNodes: aws.Int32(2)},
			wantCode: "InvalidParameterValue",
		},
		{
			name:     "port_out_of_range",
			in:       elasticachesdk.CreateCacheClusterInput{Engine: aws.String("redis"), Port: aws.Int32(70000)},
			wantCode: "InvalidParameterValue",
		},
		{
			name: "snapshot_arns_ok",
			in: elasticachesdk.CreateCacheClusterInput{
				Engine: aws.String("redis"), SnapshotArns: []string{"arn:aws:s3:::bucket/snapshot1.rdb"},
			},
		},
		{
			name: "snapshot_arns_not_s3",
			in: elasticachesdk.CreateCacheClusterInput{
				Engine: aws.String("redis"), SnapshotArns: []string{"arn:aws:sqs:us-east-1:000000000000:q"},
			},
			wantCode: "InvalidParameterValue",
		},
		{
			name: "snapshot_arns_memcached",
			in: elasticachesdk.CreateCacheClusterInput{
				Engine: aws.String("memcached"), SnapshotArns: []string{"arn:aws:s3:::bucket/snapshot1.rdb"},
			},
			wantCode: "InvalidParameterCombination",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)
			in := tt.in
			in.CacheClusterId = aws.String("placement-" + tt.name[:min(len(tt.name), 8)])

			out, err := client.CreateCacheCluster(t.Context(), &in)
			if tt.wantCode != "" {
				requireErrCode(t, err, tt.wantCode)

				_, descErr := client.DescribeCacheClusters(t.Context(), &elasticachesdk.DescribeCacheClustersInput{
					CacheClusterId: in.CacheClusterId,
				})
				requireErrCode(t, descErr, "CacheClusterNotFound")

				return
			}

			require.NoError(t, err)

			if tt.check != nil {
				tt.check(t, *out.CacheCluster)
			}
		})
	}
}

func TestModifyCacheCluster_MemcachedNodeScaling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		in        elasticachesdk.ModifyCacheClusterInput
		wantCode  string
		wantNodes []string
		wantAZs   []string
	}{
		{
			name: "add_nodes_in_zones",
			in: elasticachesdk.ModifyCacheClusterInput{
				NumCacheNodes: aws.Int32(4), NewAvailabilityZones: []string{"us-east-1c", "us-east-1b"},
			},
			wantNodes: []string{"0001", "0002", "0003", "0004"},
			wantAZs:   []string{"us-east-1a", "us-east-1a", "us-east-1c", "us-east-1b"},
		},
		{
			name: "add_nodes_zone_count_mismatch",
			in: elasticachesdk.ModifyCacheClusterInput{
				NumCacheNodes: aws.Int32(4), NewAvailabilityZones: []string{"us-east-1c"},
			},
			wantCode: "InvalidParameterCombination",
		},
		{
			name: "remove_named_node",
			in: elasticachesdk.ModifyCacheClusterInput{
				NumCacheNodes: aws.Int32(1), CacheNodeIdsToRemove: []string{"0001"},
			},
			wantNodes: []string{"0002"},
			wantAZs:   []string{"us-east-1a"},
		},
		{
			name: "remove_count_mismatch",
			in: elasticachesdk.ModifyCacheClusterInput{
				NumCacheNodes: aws.Int32(1),
			},
			wantCode: "InvalidParameterValue",
		},
		{
			name: "remove_unknown_node",
			in: elasticachesdk.ModifyCacheClusterInput{
				NumCacheNodes: aws.Int32(1), CacheNodeIdsToRemove: []string{"0009"},
			},
			wantCode: "InvalidParameterValue",
		},
		{
			name: "remove_ids_without_shrink",
			in: elasticachesdk.ModifyCacheClusterInput{
				NumCacheNodes: aws.Int32(2), CacheNodeIdsToRemove: []string{"0001"},
			},
			wantCode: "InvalidParameterCombination",
		},
		{
			name:     "too_many_nodes",
			in:       elasticachesdk.ModifyCacheClusterInput{NumCacheNodes: aws.Int32(41)},
			wantCode: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)
			ctx := t.Context()

			_, err := client.CreateCacheCluster(ctx, &elasticachesdk.CreateCacheClusterInput{
				CacheClusterId: aws.String("scale-mc"), Engine: aws.String("memcached"), NumCacheNodes: aws.Int32(2),
			})
			require.NoError(t, err)

			in := tt.in
			in.CacheClusterId = aws.String("scale-mc")

			out, err := client.ModifyCacheCluster(ctx, &in)
			if tt.wantCode != "" {
				requireErrCode(t, err, tt.wantCode)

				return
			}

			require.NoError(t, err)

			var ids, azs []string
			for _, n := range out.CacheCluster.CacheNodes {
				ids = append(ids, aws.ToString(n.CacheNodeId))
				azs = append(azs, aws.ToString(n.CustomerAvailabilityZone))
			}

			assert.Equal(t, tt.wantNodes, ids)
			assert.Equal(t, tt.wantAZs, azs)
			assert.Equal(t, int32(len(tt.wantNodes)), aws.ToInt32(out.CacheCluster.NumCacheNodes))
		})
	}
}

func TestModifyCacheCluster_RedisRejectsMemcachedOnlyMembers(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)
	ctx := t.Context()

	_, err := client.CreateCacheCluster(ctx, &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("redis-only"), Engine: aws.String("redis"),
	})
	require.NoError(t, err)

	_, err = client.ModifyCacheCluster(ctx, &elasticachesdk.ModifyCacheClusterInput{
		CacheClusterId: aws.String("redis-only"), NewAvailabilityZones: []string{"us-east-1a"},
	})
	requireErrCode(t, err, "InvalidParameterCombination")

	_, err = client.ModifyCacheCluster(ctx, &elasticachesdk.ModifyCacheClusterInput{
		CacheClusterId: aws.String("redis-only"), NumCacheNodes: aws.Int32(2),
	})
	requireErrCode(t, err, "InvalidParameterValue")
}

func TestClusterPersistenceKeepsPlacementAndGroupLink(t *testing.T) {
	t.Parallel()

	src := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
	ctx := context.Background()

	_, err := src.CreateClusterWithOptions(ctx, "persist-mc", "memcached", "cache.t3.micro", "", "", "", 2, 0)
	require.NoError(t, err)
	require.NoError(t, src.SetClusterPlacement(ctx, "persist-mc", elasticache.ClusterPlacement{
		AZs: []string{"us-east-1b", "us-east-1c"}, AZMode: "cross-az",
	}))
	require.NoError(t, src.SetClusterSubnetGroupName(ctx, "persist-mc", "sng-x"))

	limit := 7
	require.NoError(t, src.SetClusterSnapshotRetentionLimit(ctx, "persist-mc", &limit))

	dst := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
	require.NoError(t, dst.Restore(ctx, src.Snapshot(ctx)))

	got, err := dst.DescribeClusters(ctx, "persist-mc", "", 0, false)
	require.NoError(t, err)
	require.Len(t, got.Data, 1)

	c := got.Data[0]
	assert.Equal(t, "sng-x", c.SubnetGroupName)
	assert.Equal(t, 7, c.SnapshotRetentionLimit)
	assert.Equal(t, []string{"us-east-1b", "us-east-1c"}, c.PreferredAvailabilityZones)
	assert.Equal(t, []string{"us-east-1b", "us-east-1c"}, c.CacheNodeAZs)
	assert.Equal(t, "cross-az", c.AZMode)
}
