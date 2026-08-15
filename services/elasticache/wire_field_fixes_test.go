package elasticache_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeReplicationGroups_CreateTime_RealClient covers a layer-2 bug:
// gopherstack emitted the replication group's creation timestamp under the
// XML element "CreatingDate", but the real elasticache@v1.56.4 deserializer
// (awsAwsquery_deserializeDocumentReplicationGroup) reads
// "ReplicationGroupCreateTime" -- a completely different name, not just a
// casing difference, so EqualFold could never bridge it. Pre-fix, a real
// client's ReplicationGroup.ReplicationGroupCreateTime was always nil
// regardless of when the group was actually created.
func TestDescribeReplicationGroups_CreateTime_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)
	ctx := t.Context()

	_, err := client.CreateReplicationGroup(ctx, &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("wire-fixes-rg"),
		ReplicationGroupDescription: aws.String("wire fixes test"),
	})
	require.NoError(t, err)

	out, err := client.DescribeReplicationGroups(
		ctx,
		&elasticachesdk.DescribeReplicationGroupsInput{
			ReplicationGroupId: aws.String("wire-fixes-rg"),
		},
	)
	require.NoError(t, err)
	require.Len(t, out.ReplicationGroups, 1)

	rg := out.ReplicationGroups[0]
	require.NotNil(
		t,
		rg.ReplicationGroupCreateTime,
		"pre-fix the wire tag was the wrong element name (CreatingDate), so a real client never decoded it",
	)
	assert.WithinDuration(t, time.Now(), aws.ToTime(rg.ReplicationGroupCreateTime), time.Minute)
}

// TestDescribeSnapshots_CacheClusterCreateTime_RealClient covers a layer-2
// bug: gopherstack emitted the snapshot's own creation timestamp under XML
// element "SnapshotCreateTime" at the top level of Snapshot. The real
// elasticache@v1.56.4 deserializer (awsAwsquery_deserializeDocumentSnapshot)
// has no top-level "SnapshotCreateTime" case at all -- that name only exists
// on the nested NodeSnapshot type. Snapshot's real top-level time field is
// "CacheClusterCreateTime", which AWS documents as the *source cluster's*
// creation time, not the snapshot's. Fixed by capturing the source
// CacheCluster's own CreatedAt at CreateSnapshot time (snapshots.go) and
// wiring it to the correctly-named element. Pre-fix, a real client's
// Snapshot.CacheClusterCreateTime was always nil for every snapshot.
func TestDescribeSnapshots_CacheClusterCreateTime_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)
	ctx := t.Context()

	_, err := client.CreateCacheCluster(ctx, &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("wire-fixes-cluster"),
		Engine:         aws.String("redis"),
		CacheNodeType:  aws.String("cache.t3.micro"),
		NumCacheNodes:  aws.Int32(1),
	})
	require.NoError(t, err)

	described, err := client.DescribeCacheClusters(ctx, &elasticachesdk.DescribeCacheClustersInput{
		CacheClusterId: aws.String("wire-fixes-cluster"),
	})
	require.NoError(t, err)
	require.Len(t, described.CacheClusters, 1)
	wantCreateTime := aws.ToTime(described.CacheClusters[0].CacheClusterCreateTime)

	_, err = client.CreateSnapshot(ctx, &elasticachesdk.CreateSnapshotInput{
		SnapshotName:   aws.String("wire-fixes-snap"),
		CacheClusterId: aws.String("wire-fixes-cluster"),
	})
	require.NoError(t, err)

	out, err := client.DescribeSnapshots(ctx, &elasticachesdk.DescribeSnapshotsInput{
		SnapshotName: aws.String("wire-fixes-snap"),
	})
	require.NoError(t, err)
	require.Len(t, out.Snapshots, 1)

	snap := out.Snapshots[0]
	require.NotNil(
		t,
		snap.CacheClusterCreateTime,
		"pre-fix the wire tag was the wrong element name (SnapshotCreateTime), absent from the real top-level shape",
	)
	assert.Equal(t, wantCreateTime, aws.ToTime(snap.CacheClusterCreateTime))
}
