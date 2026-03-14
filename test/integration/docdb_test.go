package integration_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/docdb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_DocDB_ClusterLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createDocDBClient(t)
	ctx := t.Context()

	clusterID := fmt.Sprintf("test-cluster-%s", uuid.NewString()[:8])

	// CreateDBCluster
	createOut, err := client.CreateDBCluster(ctx, &docdb.CreateDBClusterInput{
		DBClusterIdentifier: aws.String(clusterID),
		Engine:              aws.String("docdb"),
		MasterUsername:      aws.String("admin"),
		MasterUserPassword:  aws.String("Password123!"),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.DBCluster)
	assert.Equal(t, clusterID, aws.ToString(createOut.DBCluster.DBClusterIdentifier))
	assert.Equal(t, "docdb", aws.ToString(createOut.DBCluster.Engine))

	t.Cleanup(func() {
		_, _ = client.DeleteDBCluster(ctx, &docdb.DeleteDBClusterInput{
			DBClusterIdentifier: aws.String(clusterID),
			SkipFinalSnapshot:   aws.Bool(true),
		})
	})

	// DescribeDBClusters
	descOut, err := client.DescribeDBClusters(ctx, &docdb.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String(clusterID),
	})
	require.NoError(t, err)
	require.Len(t, descOut.DBClusters, 1)
	assert.Equal(t, clusterID, aws.ToString(descOut.DBClusters[0].DBClusterIdentifier))

	// DescribeDBClusters - list all
	listOut, err := client.DescribeDBClusters(ctx, &docdb.DescribeDBClustersInput{})
	require.NoError(t, err)

	found := false

	for _, c := range listOut.DBClusters {
		if aws.ToString(c.DBClusterIdentifier) == clusterID {
			found = true

			break
		}
	}

	assert.True(t, found, "created cluster should appear in list")

	// DeleteDBCluster
	delOut, err := client.DeleteDBCluster(ctx, &docdb.DeleteDBClusterInput{
		DBClusterIdentifier: aws.String(clusterID),
		SkipFinalSnapshot:   aws.Bool(true),
	})
	require.NoError(t, err)
	require.NotNil(t, delOut.DBCluster)
	assert.Equal(t, clusterID, aws.ToString(delOut.DBCluster.DBClusterIdentifier))
}

func TestIntegration_DocDB_DBInstanceLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createDocDBClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	clusterID := fmt.Sprintf("inst-cluster-%s", suffix)
	instanceID := fmt.Sprintf("test-inst-%s", suffix)

	// Create cluster first
	_, err := client.CreateDBCluster(ctx, &docdb.CreateDBClusterInput{
		DBClusterIdentifier: aws.String(clusterID),
		Engine:              aws.String("docdb"),
		MasterUsername:      aws.String("admin"),
		MasterUserPassword:  aws.String("Password123!"),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = client.DeleteDBInstance(ctx, &docdb.DeleteDBInstanceInput{
			DBInstanceIdentifier: aws.String(instanceID),
		})
		_, _ = client.DeleteDBCluster(ctx, &docdb.DeleteDBClusterInput{
			DBClusterIdentifier: aws.String(clusterID),
			SkipFinalSnapshot:   aws.Bool(true),
		})
	})

	// CreateDBInstance
	createOut, err := client.CreateDBInstance(ctx, &docdb.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String(instanceID),
		DBClusterIdentifier:  aws.String(clusterID),
		DBInstanceClass:      aws.String("db.r5.large"),
		Engine:               aws.String("docdb"),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.DBInstance)
	assert.Equal(t, instanceID, aws.ToString(createOut.DBInstance.DBInstanceIdentifier))
	assert.Equal(t, clusterID, aws.ToString(createOut.DBInstance.DBClusterIdentifier))

	// DescribeDBInstances
	descOut, err := client.DescribeDBInstances(ctx, &docdb.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(instanceID),
	})
	require.NoError(t, err)
	require.Len(t, descOut.DBInstances, 1)
	assert.Equal(t, instanceID, aws.ToString(descOut.DBInstances[0].DBInstanceIdentifier))

	// DeleteDBInstance
	delOut, err := client.DeleteDBInstance(ctx, &docdb.DeleteDBInstanceInput{
		DBInstanceIdentifier: aws.String(instanceID),
	})
	require.NoError(t, err)
	require.NotNil(t, delOut.DBInstance)
	assert.Equal(t, instanceID, aws.ToString(delOut.DBInstance.DBInstanceIdentifier))

	// Verify deleted
	descOut2, err := client.DescribeDBInstances(ctx, &docdb.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(instanceID),
	})
	require.NoError(t, err)
	assert.Empty(t, descOut2.DBInstances)
}
