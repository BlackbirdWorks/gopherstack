package elasticache_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticache"
)

func TestSDK_CacheClusterKeepsSettings(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)
	ctx := t.Context()

	created, err := client.CreateCacheCluster(ctx, &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId:          aws.String("settings"),
		Engine:                  aws.String("redis"),
		NotificationTopicArn:    aws.String("arn:aws:sns:us-east-1:000000000000:topic"),
		SecurityGroupIds:        []string{"sg-123"},
		NetworkType:             types.NetworkTypeDualStack,
		IpDiscovery:             types.IpDiscoveryIpv6,
		AutoMinorVersionUpgrade: aws.Bool(false),
		LogDeliveryConfigurations: []types.LogDeliveryConfigurationRequest{{
			LogType:         types.LogTypeSlowLog,
			DestinationType: types.DestinationTypeCloudWatchLogs,
			LogFormat:       types.LogFormatJson,
			DestinationDetails: &types.DestinationDetails{
				CloudWatchLogsDetails: &types.CloudWatchLogsDestinationDetails{
					LogGroup: aws.String("/elasticache/slow"),
				},
			},
		}},
	})
	require.NoError(t, err)

	got, err := client.DescribeCacheClusters(ctx, &elasticachesdk.DescribeCacheClustersInput{
		CacheClusterId: created.CacheCluster.CacheClusterId,
	})
	require.NoError(t, err)
	require.Len(t, got.CacheClusters, 1)

	cl := got.CacheClusters[0]
	require.NotNil(t, cl.NotificationConfiguration)
	assert.Equal(t, "arn:aws:sns:us-east-1:000000000000:topic", aws.ToString(cl.NotificationConfiguration.TopicArn))
	assert.Equal(t, "active", aws.ToString(cl.NotificationConfiguration.TopicStatus))
	require.Len(t, cl.SecurityGroups, 1)
	assert.Equal(t, "sg-123", aws.ToString(cl.SecurityGroups[0].SecurityGroupId))
	assert.Equal(t, types.NetworkTypeDualStack, cl.NetworkType)
	assert.Equal(t, types.IpDiscoveryIpv6, cl.IpDiscovery)
	assert.False(t, aws.ToBool(cl.AutoMinorVersionUpgrade))
	require.Len(t, cl.LogDeliveryConfigurations, 1)
	assert.Equal(
		t,
		"/elasticache/slow",
		aws.ToString(cl.LogDeliveryConfigurations[0].DestinationDetails.CloudWatchLogsDetails.LogGroup),
	)

	modified, err := client.ModifyCacheCluster(ctx, &elasticachesdk.ModifyCacheClusterInput{
		CacheClusterId:          created.CacheCluster.CacheClusterId,
		NotificationTopicStatus: aws.String("inactive"),
		SecurityGroupIds:        []string{"sg-456"},
		AutoMinorVersionUpgrade: aws.Bool(true),
		ApplyImmediately:        aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Equal(t, "inactive", aws.ToString(modified.CacheCluster.NotificationConfiguration.TopicStatus))
	assert.Equal(t, "sg-456", aws.ToString(modified.CacheCluster.SecurityGroups[0].SecurityGroupId))
	assert.True(t, aws.ToBool(modified.CacheCluster.AutoMinorVersionUpgrade))
}

func TestSDK_CreateCacheClusterUnknownCacheSecurityGroup(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId:          aws.String("c"),
		Engine:                  aws.String("redis"),
		CacheSecurityGroupNames: []string{"missing"},
	})

	var nf *types.CacheSecurityGroupNotFoundFault
	require.ErrorAs(t, err, &nf)
}

func TestSDK_ReplicationGroupKeepsSettings(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)
	ctx := t.Context()

	_, err := client.CreateReplicationGroup(ctx, &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("rg"),
		ReplicationGroupDescription: aws.String("d"),
		NetworkType:                 types.NetworkTypeIpv6,
		IpDiscovery:                 types.IpDiscoveryIpv6,
		AutoMinorVersionUpgrade:     aws.Bool(false),
		ClusterMode:                 types.ClusterModeCompatible,
		UserGroupIds:                []string{"ug"},
	})
	require.NoError(t, err)

	got, err := client.DescribeReplicationGroups(ctx, &elasticachesdk.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String("rg"),
	})
	require.NoError(t, err)

	rg := got.ReplicationGroups[0]
	assert.Equal(t, types.NetworkTypeIpv6, rg.NetworkType)
	assert.Equal(t, types.IpDiscoveryIpv6, rg.IpDiscovery)
	assert.False(t, aws.ToBool(rg.AutoMinorVersionUpgrade))
	assert.Equal(t, types.ClusterModeCompatible, rg.ClusterMode)

	mod, err := client.ModifyReplicationGroup(ctx, &elasticachesdk.ModifyReplicationGroupInput{
		ReplicationGroupId:    aws.String("rg"),
		ClusterMode:           types.ClusterModeEnabled,
		SnapshottingClusterId: aws.String("rg-001"),
		RemoveUserGroups:      aws.Bool(true),
		ApplyImmediately:      aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ClusterModeEnabled, mod.ReplicationGroup.ClusterMode)
	assert.True(t, aws.ToBool(mod.ReplicationGroup.ClusterEnabled))
	assert.Equal(t, "rg-001", aws.ToString(mod.ReplicationGroup.SnapshottingClusterId))
	assert.Empty(t, mod.ReplicationGroup.UserGroupIds)
}

func TestSDK_SnapshotKmsKeyAndCopyTags(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)
	ctx := t.Context()

	_, err := client.CreateCacheCluster(ctx, &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("c"), Engine: aws.String("redis"),
	})
	require.NoError(t, err)

	snap, err := client.CreateSnapshot(ctx, &elasticachesdk.CreateSnapshotInput{
		SnapshotName: aws.String("s1"), CacheClusterId: aws.String("c"), KmsKeyId: aws.String("kms-1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "kms-1", aws.ToString(snap.Snapshot.KmsKeyId))

	cp, err := client.CopySnapshot(ctx, &elasticachesdk.CopySnapshotInput{
		SourceSnapshotName: aws.String("s1"), TargetSnapshotName: aws.String("s2"), KmsKeyId: aws.String("kms-2"),
		Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
	})
	require.NoError(t, err)
	assert.Equal(t, "kms-2", aws.ToString(cp.Snapshot.KmsKeyId))

	tags, err := client.ListTagsForResource(
		ctx,
		&elasticachesdk.ListTagsForResourceInput{ResourceName: cp.Snapshot.ARN},
	)
	require.NoError(t, err)
	require.Len(t, tags.TagList, 1)
	assert.Equal(t, "env", aws.ToString(tags.TagList[0].Key))
}

func TestSDK_DeleteTakesFinalSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		rg   bool
	}{
		{name: "cluster"},
		{name: "replication_group", rg: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)
			ctx := t.Context()

			var err error

			if tt.rg {
				_, err = client.CreateReplicationGroup(ctx, &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId: aws.String("src"), ReplicationGroupDescription: aws.String("d"),
				})
				require.NoError(t, err)

				_, err = client.DeleteReplicationGroup(ctx, &elasticachesdk.DeleteReplicationGroupInput{
					ReplicationGroupId: aws.String("src"), FinalSnapshotIdentifier: aws.String("final"),
				})
			} else {
				_, err = client.CreateCacheCluster(ctx, &elasticachesdk.CreateCacheClusterInput{
					CacheClusterId: aws.String("src"), Engine: aws.String("redis"),
				})
				require.NoError(t, err)

				_, err = client.DeleteCacheCluster(ctx, &elasticachesdk.DeleteCacheClusterInput{
					CacheClusterId: aws.String("src"), FinalSnapshotIdentifier: aws.String("final"),
				})
			}

			require.NoError(t, err)

			snaps, err := client.DescribeSnapshots(
				ctx,
				&elasticachesdk.DescribeSnapshotsInput{SnapshotName: aws.String("final")},
			)
			require.NoError(t, err)
			require.Len(t, snaps.Snapshots, 1)
		})
	}
}

func TestSDK_DeleteWithExistingFinalSnapshotNameKeepsSource(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)
	ctx := t.Context()

	_, err := client.CreateCacheCluster(ctx, &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("src"), Engine: aws.String("redis"),
	})
	require.NoError(t, err)

	_, err = client.CreateSnapshot(ctx, &elasticachesdk.CreateSnapshotInput{
		SnapshotName: aws.String("final"), CacheClusterId: aws.String("src"),
	})
	require.NoError(t, err)

	_, err = client.DeleteCacheCluster(ctx, &elasticachesdk.DeleteCacheClusterInput{
		CacheClusterId: aws.String("src"), FinalSnapshotIdentifier: aws.String("final"),
	})

	var exists *types.SnapshotAlreadyExistsFault
	require.ErrorAs(t, err, &exists)

	got, err := client.DescribeCacheClusters(
		ctx,
		&elasticachesdk.DescribeCacheClustersInput{CacheClusterId: aws.String("src")},
	)
	require.NoError(t, err)
	assert.Len(t, got.CacheClusters, 1)
}

func TestSDK_ReservedNodeOfferingFilterAndTags(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)
	ctx := t.Context()

	offerings, err := client.DescribeReservedCacheNodesOfferings(
		ctx,
		&elasticachesdk.DescribeReservedCacheNodesOfferingsInput{},
	)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(offerings.ReservedCacheNodesOfferings), 2)

	first := aws.ToString(offerings.ReservedCacheNodesOfferings[0].ReservedCacheNodesOfferingId)
	second := aws.ToString(offerings.ReservedCacheNodesOfferings[1].ReservedCacheNodesOfferingId)

	bought, err := client.PurchaseReservedCacheNodesOffering(
		ctx,
		&elasticachesdk.PurchaseReservedCacheNodesOfferingInput{
			ReservedCacheNodesOfferingId: aws.String(first),
			Tags:                         []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
		},
	)
	require.NoError(t, err)

	tags, err := client.ListTagsForResource(
		ctx,
		&elasticachesdk.ListTagsForResourceInput{ResourceName: bought.ReservedCacheNode.ReservationARN},
	)
	require.NoError(t, err)
	require.Len(t, tags.TagList, 1)

	for id, want := range map[string]int{first: 1, second: 0} {
		got, descErr := client.DescribeReservedCacheNodes(ctx, &elasticachesdk.DescribeReservedCacheNodesInput{
			ReservedCacheNodesOfferingId: aws.String(id),
		})
		require.NoError(t, descErr)
		assert.Len(t, got.ReservedCacheNodes, want, id)
	}
}

func TestSDK_DescribeUpdateActionsServiceUpdateStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status types.ServiceUpdateStatus
		want   int
	}{
		{name: "expired_none", status: types.ServiceUpdateStatusExpired, want: 1},
		{name: "available_none", status: types.ServiceUpdateStatusAvailable, want: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, client := newTestStackWithBackend(t)
			b.AppendUpdateActions([]*elasticache.UpdateAction{
				{
					CacheClusterID:     "c",
					ServiceUpdateName:  "20240101-001-security-patch",
					UpdateActionStatus: "not-applied",
				},
				{
					CacheClusterID:     "c",
					ServiceUpdateName:  "20240201-002-engine-upgrade",
					UpdateActionStatus: "not-applied",
				},
				{
					CacheClusterID:     "c",
					ServiceUpdateName:  "20231101-003-security-hotfix",
					UpdateActionStatus: "not-applied",
				},
			})

			got, err := client.DescribeUpdateActions(t.Context(), &elasticachesdk.DescribeUpdateActionsInput{
				ServiceUpdateStatus: []types.ServiceUpdateStatus{tt.status},
			})
			require.NoError(t, err)
			assert.Len(t, got.UpdateActions, tt.want)
		})
	}
}

func TestPersistence_ClusterSettingsRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	b, client := newTestStackWithBackend(t)

	_, err := client.CreateCacheCluster(ctx, &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId:       aws.String("c"),
		Engine:               aws.String("redis"),
		NotificationTopicArn: aws.String("arn:aws:sns:us-east-1:000000000000:t"),
		SecurityGroupIds:     []string{"sg-1"},
		NetworkType:          types.NetworkTypeIpv6,
	})
	require.NoError(t, err)

	fresh := elasticache.NewInMemoryBackend("redis", "000000000000", "us-east-1", nil)
	require.NoError(t, fresh.Restore(ctx, b.Snapshot(ctx)))

	got, err := fresh.DescribeClusters(ctx, "c", "", 0, false)
	require.NoError(t, err)
	require.Len(t, got.Data, 1)
	assert.Equal(t, "arn:aws:sns:us-east-1:000000000000:t", got.Data[0].NotificationTopicArn)
	assert.Equal(t, []string{"sg-1"}, got.Data[0].SecurityGroupIDs)
	assert.Equal(t, "ipv6", got.Data[0].NetworkType)
}
