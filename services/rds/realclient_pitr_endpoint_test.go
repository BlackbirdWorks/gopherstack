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

func TestRealClient_PointInTimeRestoreWindow(t *testing.T) {
	t.Parallel()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBInstance("pitr-src", "mysql", "db.t3.micro", "", "admin", "", 20,
		rds.DBInstanceOptions{BackupRetentionPeriod: 7})
	require.NoError(t, err)
	waitForInstanceAvailable(t, backend, "pitr-src")

	got, err := client.DescribeDBInstances(ctx, &rdssdk.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String("pitr-src"),
	})
	require.NoError(t, err)
	require.NotNil(t, got.DBInstances[0].LatestRestorableTime)

	tests := []struct {
		at      time.Time
		name    string
		wantErr string
	}{
		{name: "before creation", at: time.Now().Add(-48 * time.Hour), wantErr: "InvalidRestoreFault"},
		{name: "future", at: time.Now().Add(time.Hour), wantErr: "InvalidRestoreFault"},
		{name: "now", at: time.Now()},
	}

	for i, tt := range tests {
		_, rerr := client.RestoreDBInstanceToPointInTime(ctx, &rdssdk.RestoreDBInstanceToPointInTimeInput{
			SourceDBInstanceIdentifier: aws.String("pitr-src"),
			TargetDBInstanceIdentifier: aws.String("pitr-tgt-" + string(rune('a'+i))),
			RestoreTime:                aws.Time(tt.at),
		})

		if tt.wantErr != "" {
			require.ErrorContains(t, rerr, tt.wantErr, tt.name)

			continue
		}

		require.NoError(t, rerr, tt.name)
	}
}

func TestRealClient_ClusterEndpointMembersAndRestoreWindow(t *testing.T) {
	t.Parallel()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBCluster("ep-c", "aurora-mysql", "admin", "db", "", 3306, nil,
		rds.DBClusterOptions{BackupRetentionPeriod: 3})
	require.NoError(t, err)
	_, err = backend.CreateDBInstance("ep-w", "aurora-mysql", "db.r5.large", "", "admin", "", 0,
		rds.DBInstanceOptions{DBClusterIdentifier: "ep-c"})
	require.NoError(t, err)

	out, err := client.CreateDBClusterEndpoint(ctx, &rdssdk.CreateDBClusterEndpointInput{
		DBClusterEndpointIdentifier: aws.String("ep1"),
		DBClusterIdentifier:         aws.String("ep-c"),
		EndpointType:                aws.String("ANY"),
		StaticMembers:               []string{"ep-w"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"ep-w"}, out.StaticMembers)
	assert.Empty(t, out.ExcludedMembers)

	mod, err := client.ModifyDBClusterEndpoint(ctx, &rdssdk.ModifyDBClusterEndpointInput{
		DBClusterEndpointIdentifier: aws.String("ep1"),
		ExcludedMembers:             []string{"ep-w"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"ep-w"}, mod.ExcludedMembers)
	assert.Empty(t, mod.StaticMembers)

	_, err = client.CreateDBClusterEndpoint(ctx, &rdssdk.CreateDBClusterEndpointInput{
		DBClusterEndpointIdentifier: aws.String("ep2"),
		DBClusterIdentifier:         aws.String("ep-c"),
		EndpointType:                aws.String("READER"),
		StaticMembers:               []string{"ep-w"},
		ExcludedMembers:             []string{"ep-w"},
	})
	require.ErrorContains(t, err, "InvalidParameterCombination")

	clusters, err := client.DescribeDBClusters(ctx, &rdssdk.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String("ep-c"),
	})
	require.NoError(t, err)
	assert.NotNil(t, clusters.DBClusters[0].EarliestRestorableTime)
	assert.NotNil(t, clusters.DBClusters[0].LatestRestorableTime)
}
