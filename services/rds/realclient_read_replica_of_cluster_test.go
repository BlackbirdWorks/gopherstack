package rds_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

func TestRealClient_ReadReplicaOfCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		srcCluster string
		wantErr    string
		retention  int
	}{
		{name: "replica of cluster", retention: 3, srcCluster: "src"},
		{name: "cluster by arn", retention: 3, srcCluster: "arn:aws:rds:us-east-1:123456789012:cluster:src"},
		{name: "unknown cluster", retention: 3, srcCluster: "ghost", wantErr: "DBClusterNotFoundFault"},
		{name: "backups disabled", retention: 0, srcCluster: "src", wantErr: "InvalidDBClusterStateFault"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend, client := newRealClientBackendAndClient(t)
			ctx := t.Context()
			_, err := backend.CreateDBCluster("src", "mysql", "admin", "", "", 3306, nil, rds.DBClusterOptions{
				EngineVersion: "8.0.35", BackupRetentionPeriod: tt.retention, AllocatedStorage: 100,
			})
			require.NoError(t, err)

			out, err := client.CreateDBInstanceReadReplica(ctx, &rdssdk.CreateDBInstanceReadReplicaInput{
				DBInstanceIdentifier:      aws.String("rep"),
				SourceDBClusterIdentifier: aws.String(tt.srcCluster),
				DBInstanceClass:           aws.String("db.m5.large"),
			})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, "src", aws.ToString(out.DBInstance.ReadReplicaSourceDBClusterIdentifier))
			assert.Equal(t, "mysql", aws.ToString(out.DBInstance.Engine))
			assert.Equal(t, "8.0.35", aws.ToString(out.DBInstance.EngineVersion))

			clusters, err := client.DescribeDBClusters(ctx, &rdssdk.DescribeDBClustersInput{
				DBClusterIdentifier: aws.String("src"),
			})
			require.NoError(t, err)
			assert.Equal(t, []string{"rep"}, clusters.DBClusters[0].ReadReplicaIdentifiers)

			_, err = client.PromoteReadReplica(ctx, &rdssdk.PromoteReadReplicaInput{
				DBInstanceIdentifier: aws.String("rep"),
			})
			require.NoError(t, err)
			clusters, err = client.DescribeDBClusters(ctx, &rdssdk.DescribeDBClustersInput{
				DBClusterIdentifier: aws.String("src"),
			})
			require.NoError(t, err)
			assert.Empty(t, clusters.DBClusters[0].ReadReplicaIdentifiers)
		})
	}
}

func TestRealClient_ReadReplicaSourceConflict(t *testing.T) {
	t.Parallel()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()
	_, err := backend.CreateDBCluster("src", "mysql", "admin", "", "", 3306, nil, rds.DBClusterOptions{
		BackupRetentionPeriod: 3,
	})
	require.NoError(t, err)

	_, err = client.CreateDBInstanceReadReplica(ctx, &rdssdk.CreateDBInstanceReadReplicaInput{
		DBInstanceIdentifier:       aws.String("rep"),
		SourceDBClusterIdentifier:  aws.String("src"),
		SourceDBInstanceIdentifier: aws.String("other"),
	})
	require.ErrorContains(t, err, "InvalidParameterCombination")
}
