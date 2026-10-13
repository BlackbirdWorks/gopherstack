package rds_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

func TestRealClient_ClusterRestoreInheritsSourceAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		port         *int32
		deletionProt *bool
		name         string
		wantPort     int32
		viaSnapshot  bool
		wantDelProt  bool
	}{
		{name: "snapshot inherits", viaSnapshot: true, wantPort: 3307},
		{
			name: "snapshot overrides", viaSnapshot: true, port: aws.Int32(4000),
			deletionProt: aws.Bool(true), wantPort: 4000, wantDelProt: true,
		},
		{name: "pitr uses engine port", wantPort: 3306},
		{name: "pitr overrides", port: aws.Int32(4001), wantPort: 4001},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend, client := newRealClientBackendAndClient(t)
			ctx := t.Context()
			srcOpts := rds.DBClusterOptions{
				EngineVersion: "8.0.mysql_aurora.3.04.0", KmsKeyID: "key-1", StorageEncrypted: true,
				BackupRetentionPeriod: 5, AllocatedStorage: 100,
			}
			_, err := backend.CreateDBCluster("src", "aurora-mysql", "admin", "appdb", "", 3307, nil, srcOpts)
			require.NoError(t, err)

			var out *rdssdk.RestoreDBClusterFromSnapshotOutput
			var pitr *rdssdk.RestoreDBClusterToPointInTimeOutput
			if tt.viaSnapshot {
				_, err = client.CreateDBClusterSnapshot(ctx, &rdssdk.CreateDBClusterSnapshotInput{
					DBClusterIdentifier: aws.String("src"), DBClusterSnapshotIdentifier: aws.String("snap"),
				})
				require.NoError(t, err)
				out, err = client.RestoreDBClusterFromSnapshot(ctx, &rdssdk.RestoreDBClusterFromSnapshotInput{
					DBClusterIdentifier: aws.String("dst"), SnapshotIdentifier: aws.String("snap"),
					Engine: aws.String("aurora-mysql"), Port: tt.port, DeletionProtection: tt.deletionProt,
				})
			} else {
				pitr, err = client.RestoreDBClusterToPointInTime(ctx, &rdssdk.RestoreDBClusterToPointInTimeInput{
					DBClusterIdentifier: aws.String("dst"), SourceDBClusterIdentifier: aws.String("src"),
					UseLatestRestorableTime: aws.Bool(true), Port: tt.port,
				})
			}
			require.NoError(t, err)

			got, err := client.DescribeDBClusters(ctx, &rdssdk.DescribeDBClustersInput{
				DBClusterIdentifier: aws.String("dst"),
			})
			require.NoError(t, err)
			require.Len(t, got.DBClusters, 1)
			c := got.DBClusters[0]
			if out != nil {
				require.NotNil(t, out.DBCluster)
			} else {
				require.NotNil(t, pitr.DBCluster)
			}
			assert.Equal(t, "8.0.mysql_aurora.3.04.0", aws.ToString(c.EngineVersion))
			assert.Equal(t, "admin", aws.ToString(c.MasterUsername))
			assert.Equal(t, "key-1", aws.ToString(c.KmsKeyId))
			assert.True(t, aws.ToBool(c.StorageEncrypted))
			assert.Equal(t, int32(5), aws.ToInt32(c.BackupRetentionPeriod))
			assert.Equal(t, tt.wantPort, aws.ToInt32(c.Port))
			assert.Equal(t, tt.wantDelProt, aws.ToBool(c.DeletionProtection))
			assert.NotEmpty(t, aws.ToString(c.DbClusterResourceId))
			assert.NotEmpty(t, aws.ToString(c.ReaderEndpoint))
			assert.NotNil(t, c.ClusterCreateTime)
		})
	}
}
