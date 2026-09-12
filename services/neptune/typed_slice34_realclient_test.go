package neptune_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	neptunesdk "github.com/aws/aws-sdk-go-v2/service/neptune"
	"github.com/aws/aws-sdk-go-v2/service/neptune/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
)

// createSlice34Cluster creates a minimal Neptune DB cluster and returns its
// identifier and ARN.
func createSlice34Cluster(t *testing.T, client *neptunesdk.Client, id string) string {
	t.Helper()

	out, err := client.CreateDBCluster(t.Context(), &neptunesdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String(id),
		Engine:              aws.String("neptune"),
	})
	require.NoError(t, err)

	return aws.ToString(out.DBCluster.DBClusterIdentifier)
}

// TestDBClusterLifecycleAndRoles_RealClient drives ModifyDBCluster,
// StopDBCluster, StartDBCluster, AddRoleToDBCluster, RemoveRoleFromDBCluster
// and PromoteReadReplicaDBCluster through a real client.
func TestDBClusterLifecycleAndRoles_RealClient(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	client := newTestNeptuneClient(t, neptune.NewHandler(backend))

	clusterID := createSlice34Cluster(t, client, "slice34-cluster")

	t.Run("ModifyDBCluster", func(t *testing.T) {
		t.Parallel()

		modified, err := client.ModifyDBCluster(t.Context(), &neptunesdk.ModifyDBClusterInput{
			DBClusterIdentifier:   aws.String(clusterID),
			PreferredBackupWindow: aws.String("04:00-05:00"),
		})
		require.NoError(t, err)
		assert.Equal(t, "04:00-05:00", aws.ToString(modified.DBCluster.PreferredBackupWindow))
	})

	t.Run("AddRoleToDBCluster + RemoveRoleFromDBCluster", func(t *testing.T) {
		t.Parallel()

		roleBackend := neptune.NewInMemoryBackend("000000000000", testRegion)
		roleClient := newTestNeptuneClient(t, neptune.NewHandler(roleBackend))
		roleClusterID := createSlice34Cluster(t, roleClient, "slice34-role-cluster")

		roleArn := "arn:aws:iam::000000000000:role/slice34-neptune-role"

		_, err := roleClient.AddRoleToDBCluster(t.Context(), &neptunesdk.AddRoleToDBClusterInput{
			DBClusterIdentifier: aws.String(roleClusterID),
			RoleArn:             aws.String(roleArn),
		})
		require.NoError(t, err)

		described, descErr := roleClient.DescribeDBClusters(t.Context(), &neptunesdk.DescribeDBClustersInput{
			DBClusterIdentifier: aws.String(roleClusterID),
		})
		require.NoError(t, descErr)
		require.Len(t, described.DBClusters, 1)
		require.Len(t, described.DBClusters[0].AssociatedRoles, 1)
		assert.Equal(t, roleArn, aws.ToString(described.DBClusters[0].AssociatedRoles[0].RoleArn))

		_, err = roleClient.RemoveRoleFromDBCluster(t.Context(), &neptunesdk.RemoveRoleFromDBClusterInput{
			DBClusterIdentifier: aws.String(roleClusterID),
			RoleArn:             aws.String(roleArn),
		})
		require.NoError(t, err)

		after, afterErr := roleClient.DescribeDBClusters(t.Context(), &neptunesdk.DescribeDBClustersInput{
			DBClusterIdentifier: aws.String(roleClusterID),
		})
		require.NoError(t, afterErr)
		assert.Empty(t, after.DBClusters[0].AssociatedRoles)
	})

	t.Run("StopDBCluster + StartDBCluster", func(t *testing.T) {
		t.Parallel()

		lifecycleBackend := neptune.NewInMemoryBackend("000000000000", testRegion)
		lifecycleClient := newTestNeptuneClient(t, neptune.NewHandler(lifecycleBackend))
		lifecycleClusterID := createSlice34Cluster(t, lifecycleClient, "slice34-stopstart-cluster")

		stopped, stopErr := lifecycleClient.StopDBCluster(t.Context(), &neptunesdk.StopDBClusterInput{
			DBClusterIdentifier: aws.String(lifecycleClusterID),
		})
		require.NoError(t, stopErr)
		assert.Equal(t, "stopped", aws.ToString(stopped.DBCluster.Status))

		started, startErr := lifecycleClient.StartDBCluster(t.Context(), &neptunesdk.StartDBClusterInput{
			DBClusterIdentifier: aws.String(lifecycleClusterID),
		})
		require.NoError(t, startErr)
		assert.Equal(t, "available", aws.ToString(started.DBCluster.Status))
	})

	t.Run("PromoteReadReplicaDBCluster", func(t *testing.T) {
		t.Parallel()

		promoted, err := client.PromoteReadReplicaDBCluster(
			t.Context(),
			&neptunesdk.PromoteReadReplicaDBClusterInput{DBClusterIdentifier: aws.String(clusterID)},
		)
		require.NoError(t, err)
		assert.Equal(t, clusterID, aws.ToString(promoted.DBCluster.DBClusterIdentifier))
	})
}

// TestDBClusterFailoverAndRestore_RealClient drives FailoverDBCluster (which
// requires a writer plus at least one reader member) and
// RestoreDBClusterToPointInTime through a real client.
func TestDBClusterFailoverAndRestore_RealClient(t *testing.T) {
	t.Parallel()

	t.Run("FailoverDBCluster", func(t *testing.T) {
		t.Parallel()

		backend := neptune.NewInMemoryBackend("000000000000", testRegion)
		client := newTestNeptuneClient(t, neptune.NewHandler(backend))
		clusterID := createSlice34Cluster(t, client, "slice34-failover-cluster")

		_, err := client.CreateDBInstance(t.Context(), &neptunesdk.CreateDBInstanceInput{
			DBInstanceIdentifier: aws.String("slice34-writer"),
			DBClusterIdentifier:  aws.String(clusterID),
			DBInstanceClass:      aws.String("db.r5.large"),
			Engine:               aws.String("neptune"),
		})
		require.NoError(t, err)

		_, err = client.CreateDBInstance(t.Context(), &neptunesdk.CreateDBInstanceInput{
			DBInstanceIdentifier: aws.String("slice34-reader"),
			DBClusterIdentifier:  aws.String(clusterID),
			DBInstanceClass:      aws.String("db.r5.large"),
			Engine:               aws.String("neptune"),
		})
		require.NoError(t, err)

		out, failErr := client.FailoverDBCluster(t.Context(), &neptunesdk.FailoverDBClusterInput{
			DBClusterIdentifier:        aws.String(clusterID),
			TargetDBInstanceIdentifier: aws.String("slice34-reader"),
		})
		require.NoError(t, failErr)

		var writerFound bool
		for _, m := range out.DBCluster.DBClusterMembers {
			if aws.ToString(m.DBInstanceIdentifier) == "slice34-reader" {
				writerFound = aws.ToBool(m.IsClusterWriter)
			}
		}
		assert.True(t, writerFound, "target instance must become the cluster writer after failover")
	})

	t.Run("RestoreDBClusterToPointInTime", func(t *testing.T) {
		t.Parallel()

		backend := neptune.NewInMemoryBackend("000000000000", testRegion)
		client := newTestNeptuneClient(t, neptune.NewHandler(backend))
		sourceID := createSlice34Cluster(t, client, "slice34-pitr-source")

		out, err := client.RestoreDBClusterToPointInTime(t.Context(), &neptunesdk.RestoreDBClusterToPointInTimeInput{
			SourceDBClusterIdentifier: aws.String(sourceID),
			DBClusterIdentifier:       aws.String("slice34-pitr-target"),
		})
		require.NoError(t, err)
		assert.Equal(t, "slice34-pitr-target", aws.ToString(out.DBCluster.DBClusterIdentifier))
	})

	t.Run("ModifyDBClusterEndpoint", func(t *testing.T) {
		t.Parallel()

		backend := neptune.NewInMemoryBackend("000000000000", testRegion)
		client := newTestNeptuneClient(t, neptune.NewHandler(backend))
		clusterID := createSlice34Cluster(t, client, "slice34-endpoint-cluster")

		_, err := client.CreateDBClusterEndpoint(t.Context(), &neptunesdk.CreateDBClusterEndpointInput{
			DBClusterEndpointIdentifier: aws.String("slice34-endpoint"),
			DBClusterIdentifier:         aws.String(clusterID),
			EndpointType:                aws.String("READER"),
		})
		require.NoError(t, err)

		out, modErr := client.ModifyDBClusterEndpoint(t.Context(), &neptunesdk.ModifyDBClusterEndpointInput{
			DBClusterEndpointIdentifier: aws.String("slice34-endpoint"),
			StaticMembers:               []string{"slice34-member-a", "slice34-member-b"},
		})
		require.NoError(t, modErr)
		assert.Equal(t, "slice34-endpoint", aws.ToString(out.DBClusterEndpointIdentifier))
		assert.ElementsMatch(t, []string{"slice34-member-a", "slice34-member-b"}, out.StaticMembers)
	})
}

// TestDBInstanceOps_RealClient drives ModifyDBInstance, RebootDBInstance,
// DescribeDBEngineVersions, DescribeOrderableDBInstanceOptions,
// DescribeValidDBInstanceModifications and DescribePendingMaintenanceActions
// through a real client.
func TestDBInstanceOps_RealClient(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	client := newTestNeptuneClient(t, neptune.NewHandler(backend))
	clusterID := createSlice34Cluster(t, client, "slice34-inst-cluster")

	_, err := client.CreateDBInstance(t.Context(), &neptunesdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String("slice34-instance"),
		DBClusterIdentifier:  aws.String(clusterID),
		DBInstanceClass:      aws.String("db.r5.large"),
		Engine:               aws.String("neptune"),
	})
	require.NoError(t, err)

	t.Run("ModifyDBInstance", func(t *testing.T) {
		t.Parallel()

		modified, modErr := client.ModifyDBInstance(t.Context(), &neptunesdk.ModifyDBInstanceInput{
			DBInstanceIdentifier: aws.String("slice34-instance"),
			DBInstanceClass:      aws.String("db.r5.xlarge"),
		})
		require.NoError(t, modErr)
		assert.Equal(t, "db.r5.xlarge", aws.ToString(modified.DBInstance.DBInstanceClass))
	})

	t.Run("RebootDBInstance", func(t *testing.T) {
		t.Parallel()

		out, rebootErr := client.RebootDBInstance(t.Context(), &neptunesdk.RebootDBInstanceInput{
			DBInstanceIdentifier: aws.String("slice34-instance"),
		})
		require.NoError(t, rebootErr)
		assert.Equal(t, "slice34-instance", aws.ToString(out.DBInstance.DBInstanceIdentifier))
	})

	t.Run("DescribeDBEngineVersions", func(t *testing.T) {
		t.Parallel()

		out, descErr := client.DescribeDBEngineVersions(t.Context(), &neptunesdk.DescribeDBEngineVersionsInput{})
		require.NoError(t, descErr)
		require.NotEmpty(t, out.DBEngineVersions)
		assert.Equal(t, "neptune", aws.ToString(out.DBEngineVersions[0].Engine))
	})

	t.Run("DescribeOrderableDBInstanceOptions", func(t *testing.T) {
		t.Parallel()

		out, descErr := client.DescribeOrderableDBInstanceOptions(
			t.Context(),
			&neptunesdk.DescribeOrderableDBInstanceOptionsInput{Engine: aws.String("neptune")},
		)
		require.NoError(t, descErr)
		assert.NotEmpty(t, out.OrderableDBInstanceOptions)
	})

	t.Run("DescribeValidDBInstanceModifications", func(t *testing.T) {
		t.Parallel()

		out, descErr := client.DescribeValidDBInstanceModifications(
			t.Context(),
			&neptunesdk.DescribeValidDBInstanceModificationsInput{DBInstanceIdentifier: aws.String("slice34-instance")},
		)
		require.NoError(t, descErr)
		assert.NotNil(t, out.ValidDBInstanceModificationsMessage)
	})

	t.Run("DescribePendingMaintenanceActions", func(t *testing.T) {
		t.Parallel()

		out, descErr := client.DescribePendingMaintenanceActions(
			t.Context(),
			&neptunesdk.DescribePendingMaintenanceActionsInput{},
		)
		require.NoError(t, descErr)
		assert.NotNil(t, out.PendingMaintenanceActions)
	})
}

// TestDBClusterParameterGroupLifecycle_RealClient drives
// CopyDBClusterParameterGroup, DeleteDBClusterParameterGroup,
// DescribeDBClusterParameterGroups, DescribeDBClusterParameters,
// ModifyDBClusterParameterGroup and ResetDBClusterParameterGroup through a
// real client.
func TestDBClusterParameterGroupLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	client := newTestNeptuneClient(t, neptune.NewHandler(backend))

	_, err := client.CreateDBClusterParameterGroup(t.Context(), &neptunesdk.CreateDBClusterParameterGroupInput{
		DBClusterParameterGroupName: aws.String("slice34-cpg"),
		DBParameterGroupFamily:      aws.String("neptune1.3"),
		Description:                 aws.String("slice34 cluster parameter group"),
	})
	require.NoError(t, err)

	t.Run("DescribeDBClusterParameterGroups", func(t *testing.T) {
		t.Parallel()

		listed, listErr := client.DescribeDBClusterParameterGroups(
			t.Context(),
			&neptunesdk.DescribeDBClusterParameterGroupsInput{DBClusterParameterGroupName: aws.String("slice34-cpg")},
		)
		require.NoError(t, listErr)
		require.Len(t, listed.DBClusterParameterGroups, 1)
		assert.Equal(t, "neptune1.3", aws.ToString(listed.DBClusterParameterGroups[0].DBParameterGroupFamily))
	})

	t.Run("ModifyDBClusterParameterGroup", func(t *testing.T) {
		t.Parallel()

		modified, modErr := client.ModifyDBClusterParameterGroup(
			t.Context(),
			&neptunesdk.ModifyDBClusterParameterGroupInput{
				DBClusterParameterGroupName: aws.String("slice34-cpg"),
				Parameters: []types.Parameter{
					{
						ParameterName:  aws.String("neptune_query_timeout"),
						ParameterValue: aws.String("15000"),
						ApplyMethod:    types.ApplyMethodPendingReboot,
					},
				},
			},
		)
		require.NoError(t, modErr)
		assert.Equal(t, "slice34-cpg", aws.ToString(modified.DBClusterParameterGroupName))

		params, paramErr := client.DescribeDBClusterParameters(
			t.Context(),
			&neptunesdk.DescribeDBClusterParametersInput{DBClusterParameterGroupName: aws.String("slice34-cpg")},
		)
		require.NoError(t, paramErr)

		var found bool
		for _, p := range params.Parameters {
			if aws.ToString(p.ParameterName) == "neptune_query_timeout" {
				found = true
				assert.Equal(t, "15000", aws.ToString(p.ParameterValue))
			}
		}
		assert.True(t, found, "modified parameter must appear in DescribeDBClusterParameters")
	})

	t.Run("ResetDBClusterParameterGroup", func(t *testing.T) {
		t.Parallel()

		reset, resetErr := client.ResetDBClusterParameterGroup(
			t.Context(),
			&neptunesdk.ResetDBClusterParameterGroupInput{
				DBClusterParameterGroupName: aws.String("slice34-cpg"),
				ResetAllParameters:          aws.Bool(true),
			},
		)
		require.NoError(t, resetErr)
		assert.Equal(t, "slice34-cpg", aws.ToString(reset.DBClusterParameterGroupName))
	})

	t.Run("CopyDBClusterParameterGroup + DeleteDBClusterParameterGroup", func(t *testing.T) {
		t.Parallel()

		copied, copyErr := client.CopyDBClusterParameterGroup(t.Context(), &neptunesdk.CopyDBClusterParameterGroupInput{
			SourceDBClusterParameterGroupIdentifier:  aws.String("slice34-cpg"),
			TargetDBClusterParameterGroupIdentifier:  aws.String("slice34-cpg-copy"),
			TargetDBClusterParameterGroupDescription: aws.String("slice34 copy"),
		})
		require.NoError(t, copyErr)
		assert.Equal(t, "slice34-cpg-copy", aws.ToString(copied.DBClusterParameterGroup.DBClusterParameterGroupName))

		_, delErr := client.DeleteDBClusterParameterGroup(t.Context(), &neptunesdk.DeleteDBClusterParameterGroupInput{
			DBClusterParameterGroupName: aws.String("slice34-cpg-copy"),
		})
		require.NoError(t, delErr)

		copyInput := &neptunesdk.DescribeDBClusterParameterGroupsInput{
			DBClusterParameterGroupName: aws.String("slice34-cpg-copy"),
		}

		_, afterErr := client.DescribeDBClusterParameterGroups(t.Context(), copyInput)
		require.Error(t, afterErr, "parameter group must be gone after delete")
	})
}

// TestDBParameterGroupLifecycle_RealClient drives CopyDBParameterGroup,
// DeleteDBParameterGroup, DescribeDBParameterGroups, DescribeDBParameters and
// ResetDBParameterGroup through a real client.
func TestDBParameterGroupLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	client := newTestNeptuneClient(t, neptune.NewHandler(backend))

	_, err := client.CreateDBParameterGroup(t.Context(), &neptunesdk.CreateDBParameterGroupInput{
		DBParameterGroupName:   aws.String("slice34-pg"),
		DBParameterGroupFamily: aws.String("neptune1.3"),
		Description:            aws.String("slice34 parameter group"),
	})
	require.NoError(t, err)

	t.Run("DescribeDBParameterGroups", func(t *testing.T) {
		t.Parallel()

		listed, listErr := client.DescribeDBParameterGroups(
			t.Context(),
			&neptunesdk.DescribeDBParameterGroupsInput{DBParameterGroupName: aws.String("slice34-pg")},
		)
		require.NoError(t, listErr)
		require.Len(t, listed.DBParameterGroups, 1)
	})

	t.Run("DescribeDBParameters + ResetDBParameterGroup", func(t *testing.T) {
		t.Parallel()

		params, paramErr := client.DescribeDBParameters(
			t.Context(),
			&neptunesdk.DescribeDBParametersInput{DBParameterGroupName: aws.String("slice34-pg")},
		)
		require.NoError(t, paramErr)
		assert.NotNil(t, params.Parameters)

		reset, resetErr := client.ResetDBParameterGroup(t.Context(), &neptunesdk.ResetDBParameterGroupInput{
			DBParameterGroupName: aws.String("slice34-pg"),
			ResetAllParameters:   aws.Bool(true),
		})
		require.NoError(t, resetErr)
		assert.Equal(t, "slice34-pg", aws.ToString(reset.DBParameterGroupName))
	})

	t.Run("CopyDBParameterGroup + DeleteDBParameterGroup", func(t *testing.T) {
		t.Parallel()

		copied, copyErr := client.CopyDBParameterGroup(t.Context(), &neptunesdk.CopyDBParameterGroupInput{
			SourceDBParameterGroupIdentifier:  aws.String("slice34-pg"),
			TargetDBParameterGroupIdentifier:  aws.String("slice34-pg-copy"),
			TargetDBParameterGroupDescription: aws.String("slice34 pg copy"),
		})
		require.NoError(t, copyErr)
		assert.Equal(t, "slice34-pg-copy", aws.ToString(copied.DBParameterGroup.DBParameterGroupName))

		_, delErr := client.DeleteDBParameterGroup(t.Context(), &neptunesdk.DeleteDBParameterGroupInput{
			DBParameterGroupName: aws.String("slice34-pg-copy"),
		})
		require.NoError(t, delErr)
	})
}

// TestDBClusterSnapshotLifecycle_RealClient drives CopyDBClusterSnapshot,
// DeleteDBClusterSnapshot and DescribeDBClusterSnapshots through a real
// client.
func TestDBClusterSnapshotLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	client := newTestNeptuneClient(t, neptune.NewHandler(backend))
	clusterID := createSlice34Cluster(t, client, "slice34-snap-cluster")

	_, err := client.CreateDBClusterSnapshot(t.Context(), &neptunesdk.CreateDBClusterSnapshotInput{
		DBClusterSnapshotIdentifier: aws.String("slice34-snap"),
		DBClusterIdentifier:         aws.String(clusterID),
	})
	require.NoError(t, err)

	listed, listErr := client.DescribeDBClusterSnapshots(
		t.Context(),
		&neptunesdk.DescribeDBClusterSnapshotsInput{DBClusterSnapshotIdentifier: aws.String("slice34-snap")},
	)
	require.NoError(t, listErr)
	require.Len(t, listed.DBClusterSnapshots, 1)

	copied, copyErr := client.CopyDBClusterSnapshot(t.Context(), &neptunesdk.CopyDBClusterSnapshotInput{
		SourceDBClusterSnapshotIdentifier: aws.String("slice34-snap"),
		TargetDBClusterSnapshotIdentifier: aws.String("slice34-snap-copy"),
	})
	require.NoError(t, copyErr)
	assert.Equal(t, "slice34-snap-copy", aws.ToString(copied.DBClusterSnapshot.DBClusterSnapshotIdentifier))

	_, delErr := client.DeleteDBClusterSnapshot(t.Context(), &neptunesdk.DeleteDBClusterSnapshotInput{
		DBClusterSnapshotIdentifier: aws.String("slice34-snap-copy"),
	})
	require.NoError(t, delErr)

	_, afterErr := client.DescribeDBClusterSnapshots(
		t.Context(),
		&neptunesdk.DescribeDBClusterSnapshotsInput{DBClusterSnapshotIdentifier: aws.String("slice34-snap-copy")},
	)
	require.Error(t, afterErr, "snapshot must be gone after delete")
}

// TestEventSubscriptionLifecycle_RealClient drives
// AddSourceIdentifierToSubscription, RemoveSourceIdentifierFromSubscription,
// DeleteEventSubscription and DescribeEventCategories through a real client.
func TestEventSubscriptionLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	client := newTestNeptuneClient(t, neptune.NewHandler(backend))
	clusterID := createSlice34Cluster(t, client, "slice34-sub-cluster")

	_, err := client.CreateEventSubscription(t.Context(), &neptunesdk.CreateEventSubscriptionInput{
		SubscriptionName: aws.String("slice34-sub"),
		SnsTopicArn:      aws.String("arn:aws:sns:us-east-1:000000000000:slice34-topic"),
		SourceType:       aws.String("db-cluster"),
	})
	require.NoError(t, err)

	t.Run("DescribeEventCategories", func(t *testing.T) {
		t.Parallel()

		out, catErr := client.DescribeEventCategories(t.Context(), &neptunesdk.DescribeEventCategoriesInput{})
		require.NoError(t, catErr)
		assert.NotEmpty(t, out.EventCategoriesMapList)
	})

	t.Run("AddSourceIdentifierToSubscription + RemoveSourceIdentifierFromSubscription", func(t *testing.T) {
		t.Parallel()

		added, addErr := client.AddSourceIdentifierToSubscription(
			t.Context(),
			&neptunesdk.AddSourceIdentifierToSubscriptionInput{
				SubscriptionName: aws.String("slice34-sub"),
				SourceIdentifier: aws.String(clusterID),
			},
		)
		require.NoError(t, addErr)
		assert.Contains(t, added.EventSubscription.SourceIdsList, clusterID)

		removed, removeErr := client.RemoveSourceIdentifierFromSubscription(
			t.Context(),
			&neptunesdk.RemoveSourceIdentifierFromSubscriptionInput{
				SubscriptionName: aws.String("slice34-sub"),
				SourceIdentifier: aws.String(clusterID),
			},
		)
		require.NoError(t, removeErr)
		assert.NotContains(t, removed.EventSubscription.SourceIdsList, clusterID)
	})

	t.Run("DeleteEventSubscription", func(t *testing.T) {
		t.Parallel()

		delBackend := neptune.NewInMemoryBackend("000000000000", testRegion)
		delClient := newTestNeptuneClient(t, neptune.NewHandler(delBackend))

		_, delCreateErr := delClient.CreateEventSubscription(t.Context(), &neptunesdk.CreateEventSubscriptionInput{
			SubscriptionName: aws.String("slice34-del-sub"),
			SnsTopicArn:      aws.String("arn:aws:sns:us-east-1:000000000000:slice34-topic"),
			SourceType:       aws.String("db-cluster"),
		})
		require.NoError(t, delCreateErr)

		_, delErr := delClient.DeleteEventSubscription(t.Context(), &neptunesdk.DeleteEventSubscriptionInput{
			SubscriptionName: aws.String("slice34-del-sub"),
		})
		require.NoError(t, delErr)

		_, afterErr := delClient.DescribeEventSubscriptions(
			t.Context(),
			&neptunesdk.DescribeEventSubscriptionsInput{SubscriptionName: aws.String("slice34-del-sub")},
		)
		require.Error(t, afterErr, "subscription must be gone after delete")
	})
}

// TestDBSubnetGroupLifecycle_RealClient drives DeleteDBSubnetGroup,
// DescribeDBSubnetGroups and ModifyDBSubnetGroup through a real client.
func TestDBSubnetGroupLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	client := newTestNeptuneClient(t, neptune.NewHandler(backend))

	_, err := client.CreateDBSubnetGroup(t.Context(), &neptunesdk.CreateDBSubnetGroupInput{
		DBSubnetGroupName:        aws.String("slice34-sng"),
		DBSubnetGroupDescription: aws.String("slice34 subnet group"),
		SubnetIds:                []string{"subnet-slice34a", "subnet-slice34b"},
	})
	require.NoError(t, err)

	listed, listErr := client.DescribeDBSubnetGroups(
		t.Context(),
		&neptunesdk.DescribeDBSubnetGroupsInput{DBSubnetGroupName: aws.String("slice34-sng")},
	)
	require.NoError(t, listErr)
	require.Len(t, listed.DBSubnetGroups, 1)
	assert.Len(t, listed.DBSubnetGroups[0].Subnets, 2)

	modified, modErr := client.ModifyDBSubnetGroup(t.Context(), &neptunesdk.ModifyDBSubnetGroupInput{
		DBSubnetGroupName: aws.String("slice34-sng"),
		SubnetIds:         []string{"subnet-slice34a", "subnet-slice34b", "subnet-slice34c"},
	})
	require.NoError(t, modErr)
	assert.Len(t, modified.DBSubnetGroup.Subnets, 3)

	_, delErr := client.DeleteDBSubnetGroup(t.Context(), &neptunesdk.DeleteDBSubnetGroupInput{
		DBSubnetGroupName: aws.String("slice34-sng"),
	})
	require.NoError(t, delErr)

	_, afterErr := client.DescribeDBSubnetGroups(
		t.Context(),
		&neptunesdk.DescribeDBSubnetGroupsInput{DBSubnetGroupName: aws.String("slice34-sng")},
	)
	require.Error(t, afterErr, "subnet group must be gone after delete")
}

// TestTagsRoundTrip_RealClient drives AddTagsToResource and
// RemoveTagsFromResource through a real client.
func TestTagsRoundTrip_RealClient(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	client := newTestNeptuneClient(t, neptune.NewHandler(backend))
	clusterID := createSlice34Cluster(t, client, "slice34-tags-cluster")

	described, err := client.DescribeDBClusters(t.Context(), &neptunesdk.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String(clusterID),
	})
	require.NoError(t, err)
	clusterArn := aws.ToString(described.DBClusters[0].DBClusterArn)

	_, addErr := client.AddTagsToResource(t.Context(), &neptunesdk.AddTagsToResourceInput{
		ResourceName: aws.String(clusterArn),
		Tags:         []types.Tag{{Key: aws.String("env"), Value: aws.String("slice34")}},
	})
	require.NoError(t, addErr)

	tagged, tagErr := client.ListTagsForResource(
		t.Context(),
		&neptunesdk.ListTagsForResourceInput{ResourceName: aws.String(clusterArn)},
	)
	require.NoError(t, tagErr)
	require.Len(t, tagged.TagList, 1)
	assert.Equal(t, "env", aws.ToString(tagged.TagList[0].Key))

	_, removeErr := client.RemoveTagsFromResource(t.Context(), &neptunesdk.RemoveTagsFromResourceInput{
		ResourceName: aws.String(clusterArn),
		TagKeys:      []string{"env"},
	})
	require.NoError(t, removeErr)

	after, afterErr := client.ListTagsForResource(
		t.Context(),
		&neptunesdk.ListTagsForResourceInput{ResourceName: aws.String(clusterArn)},
	)
	require.NoError(t, afterErr)
	assert.Empty(t, after.TagList)
}
