package elasticache_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticachetypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func memberRoles(ng elasticachetypes.NodeGroup) map[string]string {
	out := map[string]string{}
	for _, m := range ng.NodeGroupMembers {
		out[aws.ToString(m.CacheClusterId)] = aws.ToString(m.CurrentRole)
	}

	return out
}

func createGroup(
	t *testing.T,
	client *elasticachesdk.Client,
	in elasticachesdk.CreateReplicationGroupInput,
) *elasticachetypes.ReplicationGroup {
	t.Helper()

	in.ReplicationGroupDescription = aws.String("d")

	out, err := client.CreateReplicationGroup(t.Context(), &in)
	require.NoError(t, err)

	return out.ReplicationGroup
}

func TestCreateReplicationGroup_Topology(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(t *testing.T, rg *elasticachetypes.ReplicationGroup)
		name     string
		wantCode string
		in       elasticachesdk.CreateReplicationGroupInput
	}{
		{
			name: "num_cache_clusters_with_zones",
			in: elasticachesdk.CreateReplicationGroupInput{
				NumCacheClusters:         aws.Int32(3),
				PreferredCacheClusterAZs: []string{"us-east-1b", "us-east-1c", "us-east-1a"},
			},
			check: func(t *testing.T, rg *elasticachetypes.ReplicationGroup) {
				t.Helper()
				require.Len(t, rg.NodeGroups, 1)
				ng := rg.NodeGroups[0]
				assert.Equal(t, map[string]string{"tg-001": "primary", "tg-002": "replica", "tg-003": "replica"},
					memberRoles(ng))
				assert.Equal(t, "us-east-1b", aws.ToString(ng.NodeGroupMembers[0].PreferredAvailabilityZone))
				assert.Equal(t, "us-east-1a", aws.ToString(ng.NodeGroupMembers[2].PreferredAvailabilityZone))
				assert.Equal(t, []string{"tg-001", "tg-002", "tg-003"}, rg.MemberClusters)
				require.NotNil(t, ng.PrimaryEndpoint)
				require.NotNil(t, ng.ReaderEndpoint)
				assert.Nil(t, rg.ConfigurationEndpoint)
				assert.NotEqual(t, aws.ToString(ng.PrimaryEndpoint.Address), aws.ToString(ng.ReaderEndpoint.Address))
			},
		},
		{
			name: "node_group_configuration",
			in: elasticachesdk.CreateReplicationGroupInput{
				ClusterMode: elasticachetypes.ClusterModeEnabled,
				NodeGroupConfiguration: []elasticachetypes.NodeGroupConfiguration{
					{
						NodeGroupId:  aws.String("a1"),
						Slots:        aws.String("0-8191"),
						ReplicaCount: aws.Int32(1),
						PrimaryAvailabilityZone: aws.String(
							"us-east-1c",
						),
						ReplicaAvailabilityZones: []string{"us-east-1d"},
					},
					{NodeGroupId: aws.String("b2"), Slots: aws.String("8192-16383"), ReplicaCount: aws.Int32(0)},
				},
			},
			check: func(t *testing.T, rg *elasticachetypes.ReplicationGroup) {
				t.Helper()
				require.Len(t, rg.NodeGroups, 2)
				assert.Equal(t, "a1", aws.ToString(rg.NodeGroups[0].NodeGroupId))
				assert.Equal(t, "0-8191", aws.ToString(rg.NodeGroups[0].Slots))
				assert.Equal(
					t,
					map[string]string{"tg-a1-001": "primary", "tg-a1-002": "replica"},
					memberRoles(rg.NodeGroups[0]),
				)
				assert.Equal(
					t,
					"us-east-1d",
					aws.ToString(rg.NodeGroups[0].NodeGroupMembers[1].PreferredAvailabilityZone),
				)
				assert.Equal(t, map[string]string{"tg-b2-001": "primary"}, memberRoles(rg.NodeGroups[1]))
				require.NotNil(t, rg.ConfigurationEndpoint)
				assert.Nil(t, rg.NodeGroups[0].PrimaryEndpoint)
				assert.Len(t, rg.MemberClusters, 3)
			},
		},
		{
			name: "num_node_groups_even_slots",
			in: elasticachesdk.CreateReplicationGroupInput{
				NumNodeGroups: aws.Int32(2), ReplicasPerNodeGroup: aws.Int32(1),
			},
			check: func(t *testing.T, rg *elasticachetypes.ReplicationGroup) {
				t.Helper()
				require.Len(t, rg.NodeGroups, 2)
				assert.Equal(t, "0-8191", aws.ToString(rg.NodeGroups[0].Slots))
				assert.Equal(t, "8192-16383", aws.ToString(rg.NodeGroups[1].Slots))
				assert.True(t, aws.ToBool(rg.ClusterEnabled))
			},
		},
		{
			name: "zones_count_mismatch",
			in: elasticachesdk.CreateReplicationGroupInput{
				NumCacheClusters:         aws.Int32(2),
				PreferredCacheClusterAZs: []string{"us-east-1a"},
			},
			wantCode: "InvalidParameterCombination",
		},
		{
			name: "num_cache_clusters_with_shards",
			in: elasticachesdk.CreateReplicationGroupInput{
				NumCacheClusters: aws.Int32(2),
				NumNodeGroups:    aws.Int32(2),
			},
			wantCode: "InvalidParameterCombination",
		},
		{
			name: "num_cache_clusters_with_replicas_per_group",
			in: elasticachesdk.CreateReplicationGroupInput{
				NumCacheClusters:     aws.Int32(2),
				ReplicasPerNodeGroup: aws.Int32(1),
			},
			wantCode: "InvalidParameterCombination",
		},
		{
			name:     "too_many_clusters",
			in:       elasticachesdk.CreateReplicationGroupInput{NumCacheClusters: aws.Int32(7)},
			wantCode: "InvalidParameterValue",
		},
		{
			name:     "too_many_replicas_per_group",
			in:       elasticachesdk.CreateReplicationGroupInput{ReplicasPerNodeGroup: aws.Int32(6)},
			wantCode: "InvalidParameterValue",
		},
		{
			name: "replica_zone_count_mismatch",
			in: elasticachesdk.CreateReplicationGroupInput{
				NodeGroupConfiguration: []elasticachetypes.NodeGroupConfiguration{
					{ReplicaCount: aws.Int32(2), ReplicaAvailabilityZones: []string{"us-east-1a"}},
				},
			},
			wantCode: "InvalidParameterCombination",
		},
		{
			name: "num_node_groups_mismatch_config",
			in: elasticachesdk.CreateReplicationGroupInput{
				NumNodeGroups:          aws.Int32(3),
				NodeGroupConfiguration: []elasticachetypes.NodeGroupConfiguration{{NodeGroupId: aws.String("x")}},
			},
			wantCode: "InvalidParameterCombination",
		},
		{
			name:     "unknown_subnet_group",
			in:       elasticachesdk.CreateReplicationGroupInput{CacheSubnetGroupName: aws.String("nope")},
			wantCode: "CacheSubnetGroupNotFoundFault",
		},
		{
			name:     "unknown_global_group",
			in:       elasticachesdk.CreateReplicationGroupInput{GlobalReplicationGroupId: aws.String("ldgnf-nope")},
			wantCode: "GlobalReplicationGroupNotFoundFault",
		},
		{
			name:     "unknown_serverless_snapshot",
			in:       elasticachesdk.CreateReplicationGroupInput{ServerlessCacheSnapshotName: aws.String("nope")},
			wantCode: "ServerlessCacheSnapshotNotFoundFault",
		},
		{
			name:     "unknown_primary_cluster",
			in:       elasticachesdk.CreateReplicationGroupInput{PrimaryClusterId: aws.String("nope")},
			wantCode: "CacheClusterNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)
			in := tt.in
			in.ReplicationGroupId = aws.String("tg")
			in.ReplicationGroupDescription = aws.String("d")

			out, err := client.CreateReplicationGroup(t.Context(), &in)
			if tt.wantCode != "" {
				requireErrCode(t, err, tt.wantCode)

				clusters, descErr := client.DescribeCacheClusters(
					t.Context(),
					&elasticachesdk.DescribeCacheClustersInput{},
				)
				require.NoError(t, descErr)
				assert.Empty(t, clusters.CacheClusters, "a rejected create must leave no clusters behind")

				return
			}

			require.NoError(t, err)
			tt.check(t, out.ReplicationGroup)
		})
	}
}

func TestReplicationGroup_MembersAreRealClusters(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)
	ctx := t.Context()

	_, err := client.CreateCacheSubnetGroup(ctx, &elasticachesdk.CreateCacheSubnetGroupInput{
		CacheSubnetGroupName: aws.String("members-sng"), CacheSubnetGroupDescription: aws.String("d"),
		SubnetIds: []string{"subnet-1"},
	})
	require.NoError(t, err)

	rg := createGroup(t, client, elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:     aws.String("mem"),
		NumCacheClusters:       aws.Int32(2),
		CacheSubnetGroupName:   aws.String("members-sng"),
		SecurityGroupIds:       []string{"sg-1"},
		SnapshotRetentionLimit: aws.Int32(3),
		Engine:                 aws.String("redis"),
		EngineVersion:          aws.String("7.0"),
	})
	assert.Equal(t, []string{"mem-001", "mem-002"}, rg.MemberClusters)

	described, err := client.DescribeCacheClusters(ctx, &elasticachesdk.DescribeCacheClustersInput{})
	require.NoError(t, err)
	require.Len(t, described.CacheClusters, 2)

	for _, c := range described.CacheClusters {
		assert.Equal(t, "mem", aws.ToString(c.ReplicationGroupId))
		assert.Equal(t, "members-sng", aws.ToString(c.CacheSubnetGroupName))
		assert.Equal(t, "7.0", aws.ToString(c.EngineVersion))
		assert.Equal(t, int32(3), aws.ToInt32(c.SnapshotRetentionLimit))
		require.Len(t, c.SecurityGroups, 1)
		assert.Equal(t, "sg-1", aws.ToString(c.SecurityGroups[0].SecurityGroupId))
	}

	standalone, err := client.DescribeCacheClusters(ctx, &elasticachesdk.DescribeCacheClustersInput{
		ShowCacheClustersNotInReplicationGroups: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Empty(t, standalone.CacheClusters)

	_, err = client.ModifyReplicationGroup(ctx, &elasticachesdk.ModifyReplicationGroupInput{
		ReplicationGroupId: aws.String(
			"mem",
		),
		SecurityGroupIds: []string{"sg-2", "sg-3"},
		ApplyImmediately: aws.Bool(true),
	})
	require.NoError(t, err)

	one, err := client.DescribeCacheClusters(
		ctx,
		&elasticachesdk.DescribeCacheClustersInput{CacheClusterId: aws.String("mem-002")},
	)
	require.NoError(t, err)
	require.Len(t, one.CacheClusters[0].SecurityGroups, 2)
}

func TestCreateReplicationGroup_AdoptsPrimaryCluster(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)
	ctx := t.Context()

	_, err := client.CreateCacheCluster(ctx, &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("lonely"), Engine: aws.String("redis"), CacheNodeType: aws.String("cache.m5.large"),
	})
	require.NoError(t, err)

	rg := createGroup(t, client, elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId: aws.String("adopt"), PrimaryClusterId: aws.String("lonely"), NumCacheClusters: aws.Int32(2),
	})

	require.Len(t, rg.NodeGroups, 1)
	assert.Equal(t, map[string]string{"lonely": "primary", "adopt-002": "replica"}, memberRoles(rg.NodeGroups[0]))
	assert.Equal(t, "cache.m5.large", aws.ToString(rg.CacheNodeType))

	one, err := client.DescribeCacheClusters(
		ctx,
		&elasticachesdk.DescribeCacheClustersInput{CacheClusterId: aws.String("lonely")},
	)
	require.NoError(t, err)
	assert.Equal(t, "adopt", aws.ToString(one.CacheClusters[0].ReplicationGroupId))

	_, err = client.CreateReplicationGroup(ctx, &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId: aws.String("adopt-again"), ReplicationGroupDescription: aws.String("d"),
		PrimaryClusterId: aws.String("lonely"),
	})
	requireErrCode(t, err, "InvalidCacheClusterState")
}

func TestDeleteReplicationGroup_CascadesToMembers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantClusters []string
		retain       bool
	}{
		{name: "removes_all_members"},
		{name: "retains_primary", retain: true, wantClusters: []string{"del-001"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)
			ctx := t.Context()

			createGroup(t, client, elasticachesdk.CreateReplicationGroupInput{
				ReplicationGroupId: aws.String("del"), NumCacheClusters: aws.Int32(3),
			})

			_, err := client.DeleteReplicationGroup(ctx, &elasticachesdk.DeleteReplicationGroupInput{
				ReplicationGroupId: aws.String("del"), RetainPrimaryCluster: aws.Bool(tt.retain),
			})
			require.NoError(t, err)

			out, err := client.DescribeCacheClusters(ctx, &elasticachesdk.DescribeCacheClustersInput{})
			require.NoError(t, err)

			ids := make([]string, 0, len(out.CacheClusters))

			for _, c := range out.CacheClusters {
				ids = append(ids, aws.ToString(c.CacheClusterId))
				assert.Empty(t, aws.ToString(c.ReplicationGroupId), "retained primary becomes standalone")
			}

			assert.ElementsMatch(t, tt.wantClusters, ids)
		})
	}
}

func increaseBy(in elasticachesdk.IncreaseReplicaCountInput) replicaRun {
	return func(ctx context.Context, c *elasticachesdk.Client) (*elasticachetypes.ReplicationGroup, error) {
		in.ReplicationGroupId, in.ApplyImmediately = aws.String("rc"), aws.Bool(true)

		out, err := c.IncreaseReplicaCount(ctx, &in)
		if err != nil {
			return nil, err
		}

		return out.ReplicationGroup, nil
	}
}

func decreaseBy(in elasticachesdk.DecreaseReplicaCountInput) replicaRun {
	return func(ctx context.Context, c *elasticachesdk.Client) (*elasticachetypes.ReplicationGroup, error) {
		in.ReplicationGroupId, in.ApplyImmediately = aws.String("rc"), aws.Bool(true)

		out, err := c.DecreaseReplicaCount(ctx, &in)
		if err != nil {
			return nil, err
		}

		return out.ReplicationGroup, nil
	}
}

type replicaRun func(ctx context.Context, c *elasticachesdk.Client) (*elasticachetypes.ReplicationGroup, error)

func configureShard(id string, count int32, zones ...string) elasticachetypes.ConfigureShard {
	return elasticachetypes.ConfigureShard{
		NodeGroupId: aws.String(id), NewReplicaCount: aws.Int32(count), PreferredAvailabilityZones: zones,
	}
}

func TestReplicaCountChanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run       replicaRun
		name      string
		wantCode  string
		wantNodes int
	}{
		{name: "increase_to_count", run: increaseBy(elasticachesdk.IncreaseReplicaCountInput{
			NewReplicaCount: aws.Int32(3),
		}), wantNodes: 4},
		{name: "increase_same_count_is_noop_fault", run: increaseBy(elasticachesdk.IncreaseReplicaCountInput{
			NewReplicaCount: aws.Int32(1),
		}), wantCode: "NoOperationFault"},
		{name: "increase_with_configuration_zones", run: increaseBy(elasticachesdk.IncreaseReplicaCountInput{
			ReplicaConfiguration: []elasticachetypes.ConfigureShard{configureShard("0001", 2, "us-east-1f")},
		}), wantNodes: 3},
		{name: "increase_both_forms", run: increaseBy(elasticachesdk.IncreaseReplicaCountInput{
			NewReplicaCount:      aws.Int32(2),
			ReplicaConfiguration: []elasticachetypes.ConfigureShard{configureShard("0001", 2)},
		}), wantCode: "InvalidParameterCombination"},
		{name: "increase_unknown_node_group", run: increaseBy(elasticachesdk.IncreaseReplicaCountInput{
			ReplicaConfiguration: []elasticachetypes.ConfigureShard{configureShard("9999", 2)},
		}), wantCode: "NodeGroupNotFoundFault"},
		{name: "increase_above_limit", run: increaseBy(elasticachesdk.IncreaseReplicaCountInput{
			NewReplicaCount: aws.Int32(6),
		}), wantCode: "InvalidParameterValue"},
		{name: "decrease_to_count", run: func(ctx context.Context, c *elasticachesdk.Client) (
			*elasticachetypes.ReplicationGroup, error,
		) {
			if _, err := increaseBy(
				elasticachesdk.IncreaseReplicaCountInput{NewReplicaCount: aws.Int32(3)},
			)(
				ctx,
				c,
			); err != nil {
				return nil, err
			}

			return decreaseBy(elasticachesdk.DecreaseReplicaCountInput{NewReplicaCount: aws.Int32(0)})(ctx, c)
		}, wantNodes: 1},
		{name: "decrease_named_replica", run: decreaseBy(elasticachesdk.DecreaseReplicaCountInput{
			ReplicasToRemove: []string{"rc-002"},
		}), wantNodes: 1},
		{name: "decrease_primary_is_not_a_replica", run: decreaseBy(elasticachesdk.DecreaseReplicaCountInput{
			ReplicasToRemove: []string{"rc-001"},
		}), wantCode: "InvalidParameterValue"},
		{name: "decrease_above_current", run: decreaseBy(elasticachesdk.DecreaseReplicaCountInput{
			NewReplicaCount: aws.Int32(3),
		}), wantCode: "InvalidParameterValue"},
		{name: "decrease_same_count_is_noop_fault", run: decreaseBy(elasticachesdk.DecreaseReplicaCountInput{
			NewReplicaCount: aws.Int32(1),
		}), wantCode: "NoOperationFault"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)
			createGroup(t, client, elasticachesdk.CreateReplicationGroupInput{
				ReplicationGroupId: aws.String("rc"), NumCacheClusters: aws.Int32(2),
			})

			rg, err := tt.run(t.Context(), client)
			if tt.wantCode != "" {
				requireErrCode(t, err, tt.wantCode)

				return
			}

			require.NoError(t, err)
			require.Len(t, rg.NodeGroups, 1)
			assert.Len(t, rg.NodeGroups[0].NodeGroupMembers, tt.wantNodes)
			assert.Len(t, rg.MemberClusters, tt.wantNodes)

			clusters, err := client.DescribeCacheClusters(t.Context(), &elasticachesdk.DescribeCacheClustersInput{})
			require.NoError(t, err)
			assert.Len(t, clusters.CacheClusters, tt.wantNodes, "member cluster rows track the node group")
		})
	}
}

func TestModifyReplicationGroupShardConfiguration_RemoveAndRetain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode string
		wantIDs  []string
		wantSlot []string
		in       elasticachesdk.ModifyReplicationGroupShardConfigurationInput
	}{
		{
			name: "remove",
			in: elasticachesdk.ModifyReplicationGroupShardConfigurationInput{
				NodeGroupCount: aws.Int32(2), NodeGroupsToRemove: []string{"0002"},
			},
			wantIDs:  []string{"0001", "0003"},
			wantSlot: []string{"0-8191", "8192-16383"},
		},
		{
			name: "retain",
			in: elasticachesdk.ModifyReplicationGroupShardConfigurationInput{
				NodeGroupCount: aws.Int32(1), NodeGroupsToRetain: []string{"0003"},
			},
			wantIDs:  []string{"0003"},
			wantSlot: []string{"0-16383"},
		},
		{
			name: "grow",
			in: elasticachesdk.ModifyReplicationGroupShardConfigurationInput{
				NodeGroupCount: aws.Int32(4),
			},
			wantIDs:  []string{"0001", "0002", "0003", "0004"},
			wantSlot: []string{"0-4095", "4096-8191", "8192-12287", "12288-16383"},
		},
		{
			name: "shrink_needs_selection",
			in: elasticachesdk.ModifyReplicationGroupShardConfigurationInput{
				NodeGroupCount: aws.Int32(2),
			},
			wantCode: "InvalidParameterCombination",
		},
		{
			name: "both_selections",
			in: elasticachesdk.ModifyReplicationGroupShardConfigurationInput{
				NodeGroupCount: aws.Int32(
					2,
				),
				NodeGroupsToRemove: []string{"0001"},
				NodeGroupsToRetain: []string{"0002", "0003"},
			},
			wantCode: "InvalidParameterCombination",
		},
		{
			name: "unknown_group",
			in: elasticachesdk.ModifyReplicationGroupShardConfigurationInput{
				NodeGroupCount: aws.Int32(2), NodeGroupsToRemove: []string{"0099"},
			},
			wantCode: "NodeGroupNotFoundFault",
		},
		{
			name: "count_mismatch",
			in: elasticachesdk.ModifyReplicationGroupShardConfigurationInput{
				NodeGroupCount: aws.Int32(2), NodeGroupsToRemove: []string{"0001", "0002"},
			},
			wantCode: "InvalidParameterValue",
		},
		{
			name: "same_count",
			in: elasticachesdk.ModifyReplicationGroupShardConfigurationInput{
				NodeGroupCount: aws.Int32(3),
			},
			wantCode: "NoOperationFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)
			createGroup(t, client, elasticachesdk.CreateReplicationGroupInput{
				ReplicationGroupId: aws.String("sh"), NumNodeGroups: aws.Int32(3), ReplicasPerNodeGroup: aws.Int32(1),
			})

			in := tt.in
			in.ReplicationGroupId = aws.String("sh")
			in.ApplyImmediately = aws.Bool(true)

			out, err := client.ModifyReplicationGroupShardConfiguration(t.Context(), &in)
			if tt.wantCode != "" {
				requireErrCode(t, err, tt.wantCode)

				return
			}

			require.NoError(t, err)

			var ids, slots []string
			for _, ng := range out.ReplicationGroup.NodeGroups {
				ids = append(ids, aws.ToString(ng.NodeGroupId))
				slots = append(slots, aws.ToString(ng.Slots))
				assert.Len(t, ng.NodeGroupMembers, 2, "every shard keeps its replica count")
			}

			assert.Equal(t, tt.wantIDs, ids)
			assert.Equal(t, tt.wantSlot, slots)

			clusters, err := client.DescribeCacheClusters(t.Context(), &elasticachesdk.DescribeCacheClustersInput{})
			require.NoError(t, err)
			assert.Len(t, clusters.CacheClusters, 2*len(tt.wantIDs))
		})
	}
}

func TestReplicationGroup_FailoverAndPromotion(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)
	ctx := t.Context()

	createGroup(t, client, elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId: aws.String("fo"), NumCacheClusters: aws.Int32(3),
	})

	out, err := client.TestFailover(ctx, &elasticachesdk.TestFailoverInput{
		ReplicationGroupId: aws.String("fo"), NodeGroupId: aws.String("0001"),
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"fo-001": "replica", "fo-002": "primary", "fo-003": "replica"},
		memberRoles(out.ReplicationGroup.NodeGroups[0]))

	_, err = client.TestFailover(ctx, &elasticachesdk.TestFailoverInput{
		ReplicationGroupId: aws.String("fo"), NodeGroupId: aws.String("0042"),
	})
	requireErrCode(t, err, "NodeGroupNotFoundFault")

	modified, err := client.ModifyReplicationGroup(ctx, &elasticachesdk.ModifyReplicationGroupInput{
		ReplicationGroupId: aws.String("fo"), PrimaryClusterId: aws.String("fo-003"), ApplyImmediately: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Equal(t, "primary", memberRoles(modified.ReplicationGroup.NodeGroups[0])["fo-003"])

	_, err = client.ModifyReplicationGroup(ctx, &elasticachesdk.ModifyReplicationGroupInput{
		ReplicationGroupId: aws.String("fo"), PrimaryClusterId: aws.String("ghost"), ApplyImmediately: aws.Bool(true),
	})
	requireErrCode(t, err, "CacheClusterNotFound")

	_, err = client.DeleteCacheCluster(
		ctx,
		&elasticachesdk.DeleteCacheClusterInput{CacheClusterId: aws.String("fo-003")},
	)
	requireErrCode(t, err, "InvalidCacheClusterState")

	_, err = client.DeleteCacheCluster(
		ctx,
		&elasticachesdk.DeleteCacheClusterInput{CacheClusterId: aws.String("fo-001")},
	)
	require.NoError(t, err, "the former primary is a replica now and can be removed")
}

func TestReplicationGroup_FailoverWithoutReplica(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	createGroup(t, client, elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId: aws.String("solo"), NumCacheClusters: aws.Int32(1),
	})

	_, err := client.TestFailover(t.Context(), &elasticachesdk.TestFailoverInput{
		ReplicationGroupId: aws.String("solo"), NodeGroupId: aws.String("0001"),
	})
	requireErrCode(t, err, "InvalidReplicationGroupState")
}

func TestReplicationGroup_MultiAZMembersCannotBeDeleted(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	createGroup(t, client, elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId: aws.String("maz"), NumCacheClusters: aws.Int32(2),
		MultiAZEnabled: aws.Bool(true), AutomaticFailoverEnabled: aws.Bool(true),
	})

	_, err := client.DeleteCacheCluster(
		t.Context(),
		&elasticachesdk.DeleteCacheClusterInput{CacheClusterId: aws.String("maz-002")},
	)
	requireErrCode(t, err, "InvalidCacheClusterState")

	_, err = client.DecreaseReplicaCount(t.Context(), &elasticachesdk.DecreaseReplicaCountInput{
		ReplicationGroupId: aws.String("maz"), NewReplicaCount: aws.Int32(0), ApplyImmediately: aws.Bool(true),
	})
	requireErrCode(t, err, "InvalidParameterValue")
}

func TestReplicationGroup_StorageEncryptionAndDurability(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantEncryption elasticachetypes.StorageEncryptionType
		wantDurability elasticachetypes.EffectiveDurability
		in             elasticachesdk.CreateReplicationGroupInput
	}{
		{name: "none", in: elasticachesdk.CreateReplicationGroupInput{}, wantEncryption: "none"},
		{
			name:           "service_managed",
			in:             elasticachesdk.CreateReplicationGroupInput{AtRestEncryptionEnabled: aws.Bool(true)},
			wantEncryption: "sse-elasticache",
		},
		{
			name: "customer_key",
			in: elasticachesdk.CreateReplicationGroupInput{
				AtRestEncryptionEnabled: aws.Bool(true), KmsKeyId: aws.String("kms-1"),
			},
			wantEncryption: "sse-kms",
		},
		{
			name:           "explicit_durability",
			in:             elasticachesdk.CreateReplicationGroupInput{Durability: elasticachetypes.DurabilitySync},
			wantEncryption: "none",
			wantDurability: elasticachetypes.EffectiveDurabilitySync,
		},
		{
			name:           "default_durability_is_unresolved",
			in:             elasticachesdk.CreateReplicationGroupInput{Durability: elasticachetypes.DurabilityDefault},
			wantEncryption: "none",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)
			in := tt.in
			in.ReplicationGroupId = aws.String("enc")

			rg := createGroup(t, client, in)
			assert.Equal(t, tt.wantEncryption, rg.StorageEncryptionType)
			assert.Equal(t, tt.wantDurability, rg.EffectiveDurability)
		})
	}
}

func TestSnapshot_NodeGroupConfigAndRestoreTopology(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)
	ctx := t.Context()

	createGroup(t, client, elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId: aws.String("snap-src"), ClusterMode: elasticachetypes.ClusterModeEnabled,
		Durability: elasticachetypes.DurabilityAsync,
		NodeGroupConfiguration: []elasticachetypes.NodeGroupConfiguration{
			{
				NodeGroupId:              aws.String("s1"),
				ReplicaCount:             aws.Int32(1),
				PrimaryAvailabilityZone:  aws.String("us-east-1b"),
				ReplicaAvailabilityZones: []string{"us-east-1c"},
			},
			{NodeGroupId: aws.String("s2"), ReplicaCount: aws.Int32(0)},
		},
	})

	_, err := client.CreateSnapshot(ctx, &elasticachesdk.CreateSnapshotInput{
		SnapshotName: aws.String("topo-snap"), ReplicationGroupId: aws.String("snap-src"),
	})
	require.NoError(t, err)

	plain, err := client.DescribeSnapshots(
		ctx,
		&elasticachesdk.DescribeSnapshotsInput{SnapshotName: aws.String("topo-snap")},
	)
	require.NoError(t, err)
	snap := plain.Snapshots[0]
	assert.Equal(t, int32(2), aws.ToInt32(snap.NumNodeGroups))
	assert.Equal(t, elasticachetypes.DurabilityAsync, snap.Durability)
	require.Len(t, snap.NodeSnapshots, 3)

	for _, ns := range snap.NodeSnapshots {
		assert.Nil(t, ns.NodeGroupConfiguration, "configuration only with ShowNodeGroupConfig")
	}

	shown, err := client.DescribeSnapshots(ctx, &elasticachesdk.DescribeSnapshotsInput{
		SnapshotName: aws.String("topo-snap"), ShowNodeGroupConfig: aws.Bool(true),
	})
	require.NoError(t, err)

	cfgs := map[string]elasticachetypes.NodeGroupConfiguration{}
	for _, ns := range shown.Snapshots[0].NodeSnapshots {
		require.NotNil(t, ns.NodeGroupConfiguration)
		cfgs[aws.ToString(ns.NodeGroupId)] = *ns.NodeGroupConfiguration
	}

	assert.Equal(t, int32(1), aws.ToInt32(cfgs["s1"].ReplicaCount))
	assert.Equal(t, "us-east-1b", aws.ToString(cfgs["s1"].PrimaryAvailabilityZone))
	assert.Equal(t, []string{"us-east-1c"}, cfgs["s1"].ReplicaAvailabilityZones)
	assert.Equal(t, int32(0), aws.ToInt32(cfgs["s2"].ReplicaCount))

	restored := createGroup(t, client, elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId: aws.String("snap-copy"), SnapshotName: aws.String("topo-snap"),
	})
	require.Len(t, restored.NodeGroups, 2)
	assert.Equal(t, "s1", aws.ToString(restored.NodeGroups[0].NodeGroupId))
	assert.Len(t, restored.NodeGroups[0].NodeGroupMembers, 2)
	assert.Len(t, restored.NodeGroups[1].NodeGroupMembers, 1)
	assert.True(t, aws.ToBool(restored.ClusterEnabled))
}

func TestClusterSnapshot_ListsNodes(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)
	ctx := t.Context()

	_, err := client.CreateCacheCluster(ctx, &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("snap-cluster"), Engine: aws.String("redis"),
	})
	require.NoError(t, err)

	_, err = client.CreateSnapshot(ctx, &elasticachesdk.CreateSnapshotInput{
		SnapshotName: aws.String("c-snap"), CacheClusterId: aws.String("snap-cluster"),
	})
	require.NoError(t, err)

	out, err := client.DescribeSnapshots(
		ctx,
		&elasticachesdk.DescribeSnapshotsInput{SnapshotName: aws.String("c-snap")},
	)
	require.NoError(t, err)
	assert.Equal(t, int32(1), aws.ToInt32(out.Snapshots[0].NumCacheNodes))
	require.Len(t, out.Snapshots[0].NodeSnapshots, 1)
	assert.Equal(t, "0001", aws.ToString(out.Snapshots[0].NodeSnapshots[0].CacheNodeId))
}
