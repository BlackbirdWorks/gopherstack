package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	neptunesdk "github.com/aws/aws-sdk-go-v2/service/neptune"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_Neptune_ClusterAndInstanceLifecycle exercises the core
// Neptune control-plane workflow via the AWS SDK v2: create a DB cluster,
// describe/list it, create a DB instance in that cluster, describe instances,
// then delete instance and cluster. Primary integration coverage for the
// Neptune XML query-string handler.
func TestIntegration_Neptune_ClusterAndInstanceLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createNeptuneClient(t)
	ctx := t.Context()

	const (
		clusterID     = "it-neptune-cluster"
		instanceID    = "it-neptune-instance"
		instanceClass = "db.r5.large"
		engine        = "neptune"
	)

	// CreateDBCluster.
	createOut, err := client.CreateDBCluster(ctx, &neptunesdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String(clusterID),
		Engine:              aws.String(engine),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.DBCluster)
	assert.Equal(t, clusterID, aws.ToString(createOut.DBCluster.DBClusterIdentifier))

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteDBCluster(cleanupCtx, &neptunesdk.DeleteDBClusterInput{
			DBClusterIdentifier: aws.String(clusterID),
			SkipFinalSnapshot:   aws.Bool(true),
		})
	})

	// DescribeDBClusters with explicit ID.
	descOut, err := client.DescribeDBClusters(ctx, &neptunesdk.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String(clusterID),
	})
	require.NoError(t, err)
	require.Len(t, descOut.DBClusters, 1)
	assert.Equal(t, clusterID, aws.ToString(descOut.DBClusters[0].DBClusterIdentifier))

	// DescribeDBClusters with no filter should also contain it.
	listOut, err := client.DescribeDBClusters(ctx, &neptunesdk.DescribeDBClustersInput{})
	require.NoError(t, err)

	foundCluster := false

	for _, c := range listOut.DBClusters {
		if aws.ToString(c.DBClusterIdentifier) == clusterID {
			foundCluster = true

			break
		}
	}

	assert.True(t, foundCluster, "newly created cluster should be listed")

	// CreateDBInstance bound to the cluster.
	createInstOut, err := client.CreateDBInstance(ctx, &neptunesdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String(instanceID),
		DBClusterIdentifier:  aws.String(clusterID),
		DBInstanceClass:      aws.String(instanceClass),
		Engine:               aws.String(engine),
	})
	require.NoError(t, err)
	require.NotNil(t, createInstOut.DBInstance)
	assert.Equal(t, instanceID, aws.ToString(createInstOut.DBInstance.DBInstanceIdentifier))

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteDBInstance(cleanupCtx, &neptunesdk.DeleteDBInstanceInput{
			DBInstanceIdentifier: aws.String(instanceID),
			SkipFinalSnapshot:    aws.Bool(true),
		})
	})

	// DescribeDBInstances.
	descInstOut, err := client.DescribeDBInstances(ctx, &neptunesdk.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(instanceID),
	})
	require.NoError(t, err)
	require.Len(t, descInstOut.DBInstances, 1)
	assert.Equal(t, instanceID, aws.ToString(descInstOut.DBInstances[0].DBInstanceIdentifier))
	assert.Equal(t, instanceClass, aws.ToString(descInstOut.DBInstances[0].DBInstanceClass))
}
