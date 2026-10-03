package docdb_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	docdbsdk "github.com/aws/aws-sdk-go-v2/service/docdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_ResourceIDsAndRestorableTimes(t *testing.T) {
	t.Parallel()

	tests := []struct{ name string }{{name: "cluster and instance"}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()

			c1, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
				DBClusterIdentifier: aws.String("rid-a"), Engine: aws.String("docdb"),
			})
			require.NoError(t, err)
			c2, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
				DBClusterIdentifier: aws.String("rid-b"), Engine: aws.String("docdb"),
			})
			require.NoError(t, err)

			id1 := aws.ToString(c1.DBCluster.DbClusterResourceId)
			assert.Regexp(t, `^cluster-[A-Z2-7]{26}$`, id1)
			assert.NotEqual(t, id1, aws.ToString(c2.DBCluster.DbClusterResourceId))

			desc, err := client.DescribeDBClusters(ctx, &docdbsdk.DescribeDBClustersInput{
				DBClusterIdentifier: aws.String("rid-a"),
			})
			require.NoError(t, err)
			got := desc.DBClusters[0]
			assert.Equal(t, id1, aws.ToString(got.DbClusterResourceId))
			require.NotNil(t, got.EarliestRestorableTime)
			require.NotNil(t, got.LatestRestorableTime)
			assert.False(t, got.LatestRestorableTime.Before(*got.EarliestRestorableTime))
			assert.WithinDuration(t, time.Now(), *got.LatestRestorableTime, time.Minute)

			inst, err := client.CreateDBInstance(ctx, &docdbsdk.CreateDBInstanceInput{
				DBInstanceIdentifier: aws.String("rid-i"),
				DBInstanceClass:      aws.String("db.r5.large"),
				Engine:               aws.String("docdb"),
				DBClusterIdentifier:  aws.String("rid-a"),
			})
			require.NoError(t, err)
			assert.Regexp(t, `^db-[A-Z2-7]{26}$`, aws.ToString(inst.DBInstance.DbiResourceId))
			require.NotNil(t, inst.DBInstance.LatestRestorableTime)
		})
	}
}

func TestRealClient_SnapshotStorageType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		storageType string
	}{
		{name: "standard", storageType: "standard"},
		{name: "iopt1", storageType: "iopt1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
				DBClusterIdentifier: aws.String("snap-c"),
				Engine:              aws.String("docdb"),
				StorageType:         aws.String(tt.storageType),
			})
			require.NoError(t, err)

			_, err = client.CreateDBClusterSnapshot(ctx, &docdbsdk.CreateDBClusterSnapshotInput{
				DBClusterSnapshotIdentifier: aws.String("snap-1"),
				DBClusterIdentifier:         aws.String("snap-c"),
			})
			require.NoError(t, err)

			cp, err := client.CopyDBClusterSnapshot(ctx, &docdbsdk.CopyDBClusterSnapshotInput{
				SourceDBClusterSnapshotIdentifier: aws.String("snap-1"),
				TargetDBClusterSnapshotIdentifier: aws.String("snap-2"),
			})
			require.NoError(t, err)

			out, err := client.DescribeDBClusterSnapshots(ctx, &docdbsdk.DescribeDBClusterSnapshotsInput{})
			require.NoError(t, err)
			require.Len(t, out.DBClusterSnapshots, 2)
			assert.Equal(t, "snap-2", aws.ToString(cp.DBClusterSnapshot.DBClusterSnapshotIdentifier))

			for _, s := range out.DBClusterSnapshots {
				assert.Equal(t, tt.storageType, aws.ToString(s.StorageType))
			}
		})
	}
}

func TestRealClient_CreateGlobalClusterOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		protect        bool
		encrypted      bool
		wantDeleteFail bool
	}{
		{name: "protected encrypted", protect: true, encrypted: true, wantDeleteFail: true},
		{name: "plain"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()

			out, err := client.CreateGlobalCluster(ctx, &docdbsdk.CreateGlobalClusterInput{
				GlobalClusterIdentifier: aws.String("gc-opts"),
				Engine:                  aws.String("docdb"),
				DatabaseName:            aws.String("appdb"),
				DeletionProtection:      aws.Bool(tt.protect),
				StorageEncrypted:        aws.Bool(tt.encrypted),
			})
			require.NoError(t, err)

			gc := out.GlobalCluster
			assert.Equal(t, "appdb", aws.ToString(gc.DatabaseName))
			assert.Equal(t, tt.protect, aws.ToBool(gc.DeletionProtection))
			assert.Equal(t, tt.encrypted, aws.ToBool(gc.StorageEncrypted))
			assert.Regexp(t, `^cluster-[A-Z2-7]{26}$`, aws.ToString(gc.GlobalClusterResourceId))

			desc, err := client.DescribeGlobalClusters(ctx, &docdbsdk.DescribeGlobalClustersInput{
				GlobalClusterIdentifier: aws.String("gc-opts"),
			})
			require.NoError(t, err)
			assert.Equal(t,
				aws.ToString(gc.GlobalClusterResourceId),
				aws.ToString(desc.GlobalClusters[0].GlobalClusterResourceId),
			)

			_, err = client.DeleteGlobalCluster(ctx, &docdbsdk.DeleteGlobalClusterInput{
				GlobalClusterIdentifier: aws.String("gc-opts"),
			})
			assert.Equal(t, tt.wantDeleteFail, err != nil)
		})
	}
}
