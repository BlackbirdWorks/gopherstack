package rds_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

func TestRealClient_RestoreToPointInTimeTimeConflict(t *testing.T) {
	t.Parallel()

	when := aws.Time(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	tests := []struct {
		restore   *time.Time
		name      string
		useLatest bool
		wantErr   bool
	}{
		{name: "both rejected", restore: when, useLatest: true, wantErr: true},
		{name: "latest only", useLatest: true},
		{name: "time only", restore: when},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend, client := newRealClientBackendAndClient(t)
			ctx := t.Context()

			_, err := backend.CreateDBInstance(
				"src-i", "mysql", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{},
			)
			require.NoError(t, err)
			waitForInstanceAvailable(t, backend, "src-i")
			_, err = backend.CreateDBCluster(
				"src-c", "aurora-mysql", "admin", "mydb", "", 3306, nil, rds.DBClusterOptions{},
			)
			require.NoError(t, err)

			_, ierr := client.RestoreDBInstanceToPointInTime(ctx, &rdssdk.RestoreDBInstanceToPointInTimeInput{
				SourceDBInstanceIdentifier: aws.String("src-i"), TargetDBInstanceIdentifier: aws.String("tgt-i"),
				RestoreTime: tt.restore, UseLatestRestorableTime: aws.Bool(tt.useLatest),
			})
			_, cerr := client.RestoreDBClusterToPointInTime(ctx, &rdssdk.RestoreDBClusterToPointInTimeInput{
				SourceDBClusterIdentifier: aws.String("src-c"), DBClusterIdentifier: aws.String("tgt-c"),
				RestoreToTime: tt.restore, UseLatestRestorableTime: aws.Bool(tt.useLatest),
			})

			insts, err := client.DescribeDBInstances(ctx, &rdssdk.DescribeDBInstancesInput{})
			require.NoError(t, err)
			clusters, err := client.DescribeDBClusters(ctx, &rdssdk.DescribeDBClustersInput{})
			require.NoError(t, err)

			if tt.wantErr {
				require.ErrorContains(t, ierr, "InvalidParameterValue")
				require.ErrorContains(t, cerr, "InvalidParameterValue")
				assert.Len(t, insts.DBInstances, 1)
				assert.Len(t, clusters.DBClusters, 1)

				return
			}
			require.NoError(t, ierr)
			require.NoError(t, cerr)
			assert.Len(t, insts.DBInstances, 2)
			assert.Len(t, clusters.DBClusters, 2)
		})
	}
}
