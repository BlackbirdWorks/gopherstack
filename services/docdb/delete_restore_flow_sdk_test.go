package docdb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	docdbsdk "github.com/aws/aws-sdk-go-v2/service/docdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_DeleteReportsDeleting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		kind string
	}{
		{name: "instance", kind: "instance"},
		{name: "cluster", kind: "cluster"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
				DBClusterIdentifier: aws.String("del"), Engine: aws.String("docdb"),
			})
			require.NoError(t, err)

			if tt.kind == "instance" {
				_, err = client.CreateDBInstance(ctx, &docdbsdk.CreateDBInstanceInput{
					DBInstanceIdentifier: aws.String("del-a"), DBInstanceClass: aws.String("db.r5.large"),
					Engine: aws.String("docdb"), DBClusterIdentifier: aws.String("del"),
				})
				require.NoError(t, err)

				out, delErr := client.DeleteDBInstance(ctx, &docdbsdk.DeleteDBInstanceInput{
					DBInstanceIdentifier: aws.String("del-a"),
				})
				require.NoError(t, delErr)
				assert.Equal(t, "deleting", aws.ToString(out.DBInstance.DBInstanceStatus))

				return
			}

			out, delErr := client.DeleteDBCluster(ctx, &docdbsdk.DeleteDBClusterInput{
				DBClusterIdentifier: aws.String("del"), SkipFinalSnapshot: aws.Bool(true),
			})
			require.NoError(t, delErr)
			assert.Equal(t, "deleting", aws.ToString(out.DBCluster.Status))
		})
	}
}

func TestRealClient_RestoreKeepsSnapshotConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		via  string
	}{
		{name: "manual snapshot", via: "manual"},
		{name: "final snapshot", via: "final"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateDBCluster(ctx, &docdbsdk.CreateDBClusterInput{
				DBClusterIdentifier: aws.String("src"), Engine: aws.String("docdb"),
				MasterUsername: aws.String("adm"), MasterUserPassword: aws.String("Passw0rd!x1"),
				Port: aws.Int32(27018),
			})
			require.NoError(t, err)

			if tt.via == "manual" {
				_, err = client.CreateDBClusterSnapshot(ctx, &docdbsdk.CreateDBClusterSnapshotInput{
					DBClusterIdentifier: aws.String("src"), DBClusterSnapshotIdentifier: aws.String("snap"),
				})
			} else {
				_, err = client.DeleteDBCluster(ctx, &docdbsdk.DeleteDBClusterInput{
					DBClusterIdentifier: aws.String("src"), FinalDBSnapshotIdentifier: aws.String("snap"),
				})
			}
			require.NoError(t, err)

			snaps, err := client.DescribeDBClusterSnapshots(ctx, &docdbsdk.DescribeDBClusterSnapshotsInput{
				DBClusterSnapshotIdentifier: aws.String("snap"),
			})
			require.NoError(t, err)
			require.Len(t, snaps.DBClusterSnapshots, 1)
			snap := snaps.DBClusterSnapshots[0]
			assert.Contains(t, aws.ToString(snap.DBClusterSnapshotArn), ":cluster-snapshot:snap")
			assert.Equal(t, "adm", aws.ToString(snap.MasterUsername))
			assert.Equal(t, int32(27018), aws.ToInt32(snap.Port))
			assert.NotNil(t, snap.ClusterCreateTime)

			out, err := client.RestoreDBClusterFromSnapshot(ctx, &docdbsdk.RestoreDBClusterFromSnapshotInput{
				DBClusterIdentifier: aws.String("restored"), SnapshotIdentifier: aws.String("snap"),
				Engine: aws.String("docdb"),
			})
			require.NoError(t, err)
			assert.Equal(t, "adm", aws.ToString(out.DBCluster.MasterUsername))
			assert.Equal(t, int32(27018), aws.ToInt32(out.DBCluster.Port))
		})
	}
}
