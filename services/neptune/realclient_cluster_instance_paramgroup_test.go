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

// createTestCluster creates a minimal Neptune DB cluster and returns its
// identifier and ARN.
func createTestCluster(t *testing.T, client *neptunesdk.Client, id string) string {
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
// and PromoteReadReplicaDBCluster through a real client. Each case gets its
// own backend/cluster so parallel cases never mutate shared state.
func TestDBClusterLifecycleAndRoles_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "ModifyDBCluster",
			run: func(t *testing.T) {
				t.Helper()

				backend := neptune.NewInMemoryBackend("000000000000", testRegion)
				client := newTestNeptuneClient(t, neptune.NewHandler(backend))
				clusterID := createTestCluster(t, client, "modify-cluster")

				modified, err := client.ModifyDBCluster(t.Context(), &neptunesdk.ModifyDBClusterInput{
					DBClusterIdentifier:   aws.String(clusterID),
					PreferredBackupWindow: aws.String("04:00-05:00"),
				})
				require.NoError(t, err)
				assert.Equal(t, "04:00-05:00", aws.ToString(modified.DBCluster.PreferredBackupWindow))
			},
		},
		{
			name: "AddRoleToDBCluster + RemoveRoleFromDBCluster",
			run: func(t *testing.T) {
				t.Helper()

				roleBackend := neptune.NewInMemoryBackend("000000000000", testRegion)
				roleClient := newTestNeptuneClient(t, neptune.NewHandler(roleBackend))
				roleClusterID := createTestCluster(t, roleClient, "role-cluster")

				roleArn := "arn:aws:iam::000000000000:role/neptune-role"

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
			},
		},
		{
			name: "StopDBCluster + StartDBCluster",
			run: func(t *testing.T) {
				t.Helper()

				lifecycleBackend := neptune.NewInMemoryBackend("000000000000", testRegion)
				lifecycleClient := newTestNeptuneClient(t, neptune.NewHandler(lifecycleBackend))
				lifecycleClusterID := createTestCluster(t, lifecycleClient, "stopstart-cluster")

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
			},
		},
		{
			name: "PromoteReadReplicaDBCluster",
			run: func(t *testing.T) {
				t.Helper()

				backend := neptune.NewInMemoryBackend("000000000000", testRegion)
				client := newTestNeptuneClient(t, neptune.NewHandler(backend))
				clusterID := createTestCluster(t, client, "promote-cluster")

				promoted, err := client.PromoteReadReplicaDBCluster(
					t.Context(),
					&neptunesdk.PromoteReadReplicaDBClusterInput{DBClusterIdentifier: aws.String(clusterID)},
				)
				require.NoError(t, err)
				assert.Equal(t, clusterID, aws.ToString(promoted.DBCluster.DBClusterIdentifier))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// TestDBClusterFailoverAndRestore_RealClient drives FailoverDBCluster (which
// requires a writer plus at least one reader member) and
// RestoreDBClusterToPointInTime through a real client.
func TestDBClusterFailoverAndRestore_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "FailoverDBCluster",
			run: func(t *testing.T) {
				t.Helper()

				backend := neptune.NewInMemoryBackend("000000000000", testRegion)
				client := newTestNeptuneClient(t, neptune.NewHandler(backend))
				clusterID := createTestCluster(t, client, "failover-cluster")

				_, err := client.CreateDBInstance(t.Context(), &neptunesdk.CreateDBInstanceInput{
					DBInstanceIdentifier: aws.String("writer"),
					DBClusterIdentifier:  aws.String(clusterID),
					DBInstanceClass:      aws.String("db.r5.large"),
					Engine:               aws.String("neptune"),
				})
				require.NoError(t, err)

				_, err = client.CreateDBInstance(t.Context(), &neptunesdk.CreateDBInstanceInput{
					DBInstanceIdentifier: aws.String("reader"),
					DBClusterIdentifier:  aws.String(clusterID),
					DBInstanceClass:      aws.String("db.r5.large"),
					Engine:               aws.String("neptune"),
				})
				require.NoError(t, err)

				out, failErr := client.FailoverDBCluster(t.Context(), &neptunesdk.FailoverDBClusterInput{
					DBClusterIdentifier:        aws.String(clusterID),
					TargetDBInstanceIdentifier: aws.String("reader"),
				})
				require.NoError(t, failErr)

				var writerFound bool
				for _, m := range out.DBCluster.DBClusterMembers {
					if aws.ToString(m.DBInstanceIdentifier) == "reader" {
						writerFound = aws.ToBool(m.IsClusterWriter)
					}
				}
				assert.True(t, writerFound, "target instance must become the cluster writer after failover")
			},
		},
		{
			name: "RestoreDBClusterToPointInTime",
			run: func(t *testing.T) {
				t.Helper()

				backend := neptune.NewInMemoryBackend("000000000000", testRegion)
				client := newTestNeptuneClient(t, neptune.NewHandler(backend))
				sourceID := createTestCluster(t, client, "pitr-source")

				out, err := client.RestoreDBClusterToPointInTime(
					t.Context(),
					&neptunesdk.RestoreDBClusterToPointInTimeInput{
						SourceDBClusterIdentifier: aws.String(sourceID),
						DBClusterIdentifier:       aws.String("pitr-target"),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, "pitr-target", aws.ToString(out.DBCluster.DBClusterIdentifier))
			},
		},
		{
			name: "ModifyDBClusterEndpoint",
			run: func(t *testing.T) {
				t.Helper()

				backend := neptune.NewInMemoryBackend("000000000000", testRegion)
				client := newTestNeptuneClient(t, neptune.NewHandler(backend))
				clusterID := createTestCluster(t, client, "endpoint-cluster")

				_, err := client.CreateDBClusterEndpoint(t.Context(), &neptunesdk.CreateDBClusterEndpointInput{
					DBClusterEndpointIdentifier: aws.String("endpoint"),
					DBClusterIdentifier:         aws.String(clusterID),
					EndpointType:                aws.String("READER"),
				})
				require.NoError(t, err)

				out, modErr := client.ModifyDBClusterEndpoint(t.Context(), &neptunesdk.ModifyDBClusterEndpointInput{
					DBClusterEndpointIdentifier: aws.String("endpoint"),
					StaticMembers:               []string{"member-a", "member-b"},
				})
				require.NoError(t, modErr)
				assert.Equal(t, "endpoint", aws.ToString(out.DBClusterEndpointIdentifier))
				assert.ElementsMatch(t, []string{"member-a", "member-b"}, out.StaticMembers)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// TestDBInstanceOps_RealClient drives ModifyDBInstance, RebootDBInstance,
// DescribeDBEngineVersions, DescribeOrderableDBInstanceOptions,
// DescribeValidDBInstanceModifications and DescribePendingMaintenanceActions
// through a real client. Each case gets its own backend/cluster/instance so
// parallel cases never mutate shared state.
func TestDBInstanceOps_RealClient(t *testing.T) {
	t.Parallel()

	newInstance := func(t *testing.T, suffix string) (*neptunesdk.Client, string) {
		t.Helper()

		backend := neptune.NewInMemoryBackend("000000000000", testRegion)
		client := newTestNeptuneClient(t, neptune.NewHandler(backend))
		clusterID := createTestCluster(t, client, "inst-cluster-"+suffix)

		instanceID := "instance-" + suffix
		_, err := client.CreateDBInstance(t.Context(), &neptunesdk.CreateDBInstanceInput{
			DBInstanceIdentifier: aws.String(instanceID),
			DBClusterIdentifier:  aws.String(clusterID),
			DBInstanceClass:      aws.String("db.r5.large"),
			Engine:               aws.String("neptune"),
		})
		require.NoError(t, err)

		return client, instanceID
	}

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "ModifyDBInstance",
			run: func(t *testing.T) {
				t.Helper()

				client, instanceID := newInstance(t, "modify")

				modified, modErr := client.ModifyDBInstance(t.Context(), &neptunesdk.ModifyDBInstanceInput{
					DBInstanceIdentifier: aws.String(instanceID),
					DBInstanceClass:      aws.String("db.r5.xlarge"),
				})
				require.NoError(t, modErr)
				assert.Equal(t, "db.r5.xlarge", aws.ToString(modified.DBInstance.DBInstanceClass))
			},
		},
		{
			name: "RebootDBInstance",
			run: func(t *testing.T) {
				t.Helper()

				client, instanceID := newInstance(t, "reboot")

				out, rebootErr := client.RebootDBInstance(t.Context(), &neptunesdk.RebootDBInstanceInput{
					DBInstanceIdentifier: aws.String(instanceID),
				})
				require.NoError(t, rebootErr)
				assert.Equal(t, instanceID, aws.ToString(out.DBInstance.DBInstanceIdentifier))
			},
		},
		{
			name: "DescribeDBEngineVersions",
			run: func(t *testing.T) {
				t.Helper()

				client, _ := newInstance(t, "engineversions")

				out, descErr := client.DescribeDBEngineVersions(
					t.Context(),
					&neptunesdk.DescribeDBEngineVersionsInput{},
				)
				require.NoError(t, descErr)
				require.NotEmpty(t, out.DBEngineVersions)
				assert.Equal(t, "neptune", aws.ToString(out.DBEngineVersions[0].Engine))
			},
		},
		{
			name: "DescribeOrderableDBInstanceOptions",
			run: func(t *testing.T) {
				t.Helper()

				client, _ := newInstance(t, "orderableoptions")

				out, descErr := client.DescribeOrderableDBInstanceOptions(
					t.Context(),
					&neptunesdk.DescribeOrderableDBInstanceOptionsInput{Engine: aws.String("neptune")},
				)
				require.NoError(t, descErr)
				assert.NotEmpty(t, out.OrderableDBInstanceOptions)
			},
		},
		{
			name: "DescribeValidDBInstanceModifications",
			run: func(t *testing.T) {
				t.Helper()

				client, instanceID := newInstance(t, "validmods")

				out, descErr := client.DescribeValidDBInstanceModifications(
					t.Context(),
					&neptunesdk.DescribeValidDBInstanceModificationsInput{DBInstanceIdentifier: aws.String(instanceID)},
				)
				require.NoError(t, descErr)
				assert.NotNil(t, out.ValidDBInstanceModificationsMessage)
			},
		},
		{
			name: "DescribePendingMaintenanceActions",
			run: func(t *testing.T) {
				t.Helper()

				client, _ := newInstance(t, "pendingmaint")

				out, descErr := client.DescribePendingMaintenanceActions(
					t.Context(),
					&neptunesdk.DescribePendingMaintenanceActionsInput{},
				)
				require.NoError(t, descErr)
				assert.NotNil(t, out.PendingMaintenanceActions)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// TestDBClusterParameterGroupLifecycle_RealClient drives
// CopyDBClusterParameterGroup, DeleteDBClusterParameterGroup,
// DescribeDBClusterParameterGroups, DescribeDBClusterParameters,
// ModifyDBClusterParameterGroup and ResetDBClusterParameterGroup through a
// real client. Each case gets its own parameter group (Reset and Modify
// racing on a shared group previously made this flaky under -race: Reset
// could restore defaults between Modify's write and its own Describe).
func TestDBClusterParameterGroupLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	newGroup := func(t *testing.T, name string) *neptunesdk.Client {
		t.Helper()

		backend := neptune.NewInMemoryBackend("000000000000", testRegion)
		client := newTestNeptuneClient(t, neptune.NewHandler(backend))

		_, err := client.CreateDBClusterParameterGroup(t.Context(), &neptunesdk.CreateDBClusterParameterGroupInput{
			DBClusterParameterGroupName: aws.String(name),
			DBParameterGroupFamily:      aws.String("neptune1.3"),
			Description:                 aws.String("cluster parameter group"),
		})
		require.NoError(t, err)

		return client
	}

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "DescribeDBClusterParameterGroups",
			run: func(t *testing.T) {
				t.Helper()

				client := newGroup(t, "cpg-describe")

				listed, listErr := client.DescribeDBClusterParameterGroups(
					t.Context(),
					&neptunesdk.DescribeDBClusterParameterGroupsInput{
						DBClusterParameterGroupName: aws.String("cpg-describe"),
					},
				)
				require.NoError(t, listErr)
				require.Len(t, listed.DBClusterParameterGroups, 1)
				assert.Equal(t, "neptune1.3", aws.ToString(listed.DBClusterParameterGroups[0].DBParameterGroupFamily))
			},
		},
		{
			name: "ModifyDBClusterParameterGroup",
			run: func(t *testing.T) {
				t.Helper()

				client := newGroup(t, "cpg-modify")

				modified, modErr := client.ModifyDBClusterParameterGroup(
					t.Context(),
					&neptunesdk.ModifyDBClusterParameterGroupInput{
						DBClusterParameterGroupName: aws.String("cpg-modify"),
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
				assert.Equal(t, "cpg-modify", aws.ToString(modified.DBClusterParameterGroupName))

				params, paramErr := client.DescribeDBClusterParameters(
					t.Context(),
					&neptunesdk.DescribeDBClusterParametersInput{DBClusterParameterGroupName: aws.String("cpg-modify")},
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
			},
		},
		{
			name: "ResetDBClusterParameterGroup",
			run: func(t *testing.T) {
				t.Helper()

				client := newGroup(t, "cpg-reset")

				reset, resetErr := client.ResetDBClusterParameterGroup(
					t.Context(),
					&neptunesdk.ResetDBClusterParameterGroupInput{
						DBClusterParameterGroupName: aws.String("cpg-reset"),
						ResetAllParameters:          aws.Bool(true),
					},
				)
				require.NoError(t, resetErr)
				assert.Equal(t, "cpg-reset", aws.ToString(reset.DBClusterParameterGroupName))
			},
		},
		{
			name: "CopyDBClusterParameterGroup + DeleteDBClusterParameterGroup",
			run: func(t *testing.T) {
				t.Helper()

				client := newGroup(t, "cpg-copysrc")

				copied, copyErr := client.CopyDBClusterParameterGroup(
					t.Context(),
					&neptunesdk.CopyDBClusterParameterGroupInput{
						SourceDBClusterParameterGroupIdentifier:  aws.String("cpg-copysrc"),
						TargetDBClusterParameterGroupIdentifier:  aws.String("cpg-copysrc-copy"),
						TargetDBClusterParameterGroupDescription: aws.String("copy"),
					},
				)
				require.NoError(t, copyErr)
				assert.Equal(
					t,
					"cpg-copysrc-copy",
					aws.ToString(copied.DBClusterParameterGroup.DBClusterParameterGroupName),
				)

				_, delErr := client.DeleteDBClusterParameterGroup(
					t.Context(),
					&neptunesdk.DeleteDBClusterParameterGroupInput{
						DBClusterParameterGroupName: aws.String("cpg-copysrc-copy"),
					},
				)
				require.NoError(t, delErr)

				copyInput := &neptunesdk.DescribeDBClusterParameterGroupsInput{
					DBClusterParameterGroupName: aws.String("cpg-copysrc-copy"),
				}

				_, afterErr := client.DescribeDBClusterParameterGroups(t.Context(), copyInput)
				require.Error(t, afterErr, "parameter group must be gone after delete")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// TestDBParameterGroupLifecycle_RealClient drives CopyDBParameterGroup,
// DeleteDBParameterGroup, DescribeDBParameterGroups, DescribeDBParameters and
// ResetDBParameterGroup through a real client. Each case gets its own
// parameter group so parallel cases never mutate shared state.
func TestDBParameterGroupLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	newGroup := func(t *testing.T, name string) *neptunesdk.Client {
		t.Helper()

		backend := neptune.NewInMemoryBackend("000000000000", testRegion)
		client := newTestNeptuneClient(t, neptune.NewHandler(backend))

		_, err := client.CreateDBParameterGroup(t.Context(), &neptunesdk.CreateDBParameterGroupInput{
			DBParameterGroupName:   aws.String(name),
			DBParameterGroupFamily: aws.String("neptune1.3"),
			Description:            aws.String("parameter group"),
		})
		require.NoError(t, err)

		return client
	}

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "DescribeDBParameterGroups",
			run: func(t *testing.T) {
				t.Helper()

				client := newGroup(t, "pg-describe")

				listed, listErr := client.DescribeDBParameterGroups(
					t.Context(),
					&neptunesdk.DescribeDBParameterGroupsInput{DBParameterGroupName: aws.String("pg-describe")},
				)
				require.NoError(t, listErr)
				require.Len(t, listed.DBParameterGroups, 1)
			},
		},
		{
			name: "DescribeDBParameters + ResetDBParameterGroup",
			run: func(t *testing.T) {
				t.Helper()

				client := newGroup(t, "pg-reset")

				params, paramErr := client.DescribeDBParameters(
					t.Context(),
					&neptunesdk.DescribeDBParametersInput{DBParameterGroupName: aws.String("pg-reset")},
				)
				require.NoError(t, paramErr)
				assert.NotNil(t, params.Parameters)

				reset, resetErr := client.ResetDBParameterGroup(t.Context(), &neptunesdk.ResetDBParameterGroupInput{
					DBParameterGroupName: aws.String("pg-reset"),
					ResetAllParameters:   aws.Bool(true),
				})
				require.NoError(t, resetErr)
				assert.Equal(t, "pg-reset", aws.ToString(reset.DBParameterGroupName))
			},
		},
		{
			name: "CopyDBParameterGroup + DeleteDBParameterGroup",
			run: func(t *testing.T) {
				t.Helper()

				client := newGroup(t, "pg-copysrc")

				copied, copyErr := client.CopyDBParameterGroup(t.Context(), &neptunesdk.CopyDBParameterGroupInput{
					SourceDBParameterGroupIdentifier:  aws.String("pg-copysrc"),
					TargetDBParameterGroupIdentifier:  aws.String("pg-copysrc-copy"),
					TargetDBParameterGroupDescription: aws.String("pg copy"),
				})
				require.NoError(t, copyErr)
				assert.Equal(t, "pg-copysrc-copy", aws.ToString(copied.DBParameterGroup.DBParameterGroupName))

				_, delErr := client.DeleteDBParameterGroup(t.Context(), &neptunesdk.DeleteDBParameterGroupInput{
					DBParameterGroupName: aws.String("pg-copysrc-copy"),
				})
				require.NoError(t, delErr)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// TestDBClusterSnapshotLifecycle_RealClient drives CopyDBClusterSnapshot,
// DeleteDBClusterSnapshot and DescribeDBClusterSnapshots through a real
// client.
func TestDBClusterSnapshotLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	client := newTestNeptuneClient(t, neptune.NewHandler(backend))
	clusterID := createTestCluster(t, client, "snap-cluster")

	_, err := client.CreateDBClusterSnapshot(t.Context(), &neptunesdk.CreateDBClusterSnapshotInput{
		DBClusterSnapshotIdentifier: aws.String("snap"),
		DBClusterIdentifier:         aws.String(clusterID),
	})
	require.NoError(t, err)

	listed, listErr := client.DescribeDBClusterSnapshots(
		t.Context(),
		&neptunesdk.DescribeDBClusterSnapshotsInput{DBClusterSnapshotIdentifier: aws.String("snap")},
	)
	require.NoError(t, listErr)
	require.Len(t, listed.DBClusterSnapshots, 1)

	copied, copyErr := client.CopyDBClusterSnapshot(t.Context(), &neptunesdk.CopyDBClusterSnapshotInput{
		SourceDBClusterSnapshotIdentifier: aws.String("snap"),
		TargetDBClusterSnapshotIdentifier: aws.String("snap-copy"),
	})
	require.NoError(t, copyErr)
	assert.Equal(t, "snap-copy", aws.ToString(copied.DBClusterSnapshot.DBClusterSnapshotIdentifier))

	_, delErr := client.DeleteDBClusterSnapshot(t.Context(), &neptunesdk.DeleteDBClusterSnapshotInput{
		DBClusterSnapshotIdentifier: aws.String("snap-copy"),
	})
	require.NoError(t, delErr)

	_, afterErr := client.DescribeDBClusterSnapshots(
		t.Context(),
		&neptunesdk.DescribeDBClusterSnapshotsInput{DBClusterSnapshotIdentifier: aws.String("snap-copy")},
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
	clusterID := createTestCluster(t, client, "sub-cluster")

	_, err := client.CreateEventSubscription(t.Context(), &neptunesdk.CreateEventSubscriptionInput{
		SubscriptionName: aws.String("sub"),
		SnsTopicArn:      aws.String("arn:aws:sns:us-east-1:000000000000:topic"),
		SourceType:       aws.String("db-cluster"),
	})
	require.NoError(t, err)

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "DescribeEventCategories",
			run: func(t *testing.T) {
				t.Helper()

				out, catErr := client.DescribeEventCategories(t.Context(), &neptunesdk.DescribeEventCategoriesInput{})
				require.NoError(t, catErr)
				assert.NotEmpty(t, out.EventCategoriesMapList)
			},
		},
		{
			name: "AddSourceIdentifierToSubscription + RemoveSourceIdentifierFromSubscription",
			run: func(t *testing.T) {
				t.Helper()

				added, addErr := client.AddSourceIdentifierToSubscription(
					t.Context(),
					&neptunesdk.AddSourceIdentifierToSubscriptionInput{
						SubscriptionName: aws.String("sub"),
						SourceIdentifier: aws.String(clusterID),
					},
				)
				require.NoError(t, addErr)
				assert.Contains(t, added.EventSubscription.SourceIdsList, clusterID)

				removed, removeErr := client.RemoveSourceIdentifierFromSubscription(
					t.Context(),
					&neptunesdk.RemoveSourceIdentifierFromSubscriptionInput{
						SubscriptionName: aws.String("sub"),
						SourceIdentifier: aws.String(clusterID),
					},
				)
				require.NoError(t, removeErr)
				assert.NotContains(t, removed.EventSubscription.SourceIdsList, clusterID)
			},
		},
		{
			name: "DeleteEventSubscription",
			run: func(t *testing.T) {
				t.Helper()

				delBackend := neptune.NewInMemoryBackend("000000000000", testRegion)
				delClient := newTestNeptuneClient(t, neptune.NewHandler(delBackend))

				_, delCreateErr := delClient.CreateEventSubscription(
					t.Context(),
					&neptunesdk.CreateEventSubscriptionInput{
						SubscriptionName: aws.String("del-sub"),
						SnsTopicArn:      aws.String("arn:aws:sns:us-east-1:000000000000:topic"),
						SourceType:       aws.String("db-cluster"),
					},
				)
				require.NoError(t, delCreateErr)

				_, delErr := delClient.DeleteEventSubscription(t.Context(), &neptunesdk.DeleteEventSubscriptionInput{
					SubscriptionName: aws.String("del-sub"),
				})
				require.NoError(t, delErr)

				_, afterErr := delClient.DescribeEventSubscriptions(
					t.Context(),
					&neptunesdk.DescribeEventSubscriptionsInput{SubscriptionName: aws.String("del-sub")},
				)
				require.Error(t, afterErr, "subscription must be gone after delete")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// TestDBSubnetGroupLifecycle_RealClient drives DeleteDBSubnetGroup,
// DescribeDBSubnetGroups and ModifyDBSubnetGroup through a real client.
func TestDBSubnetGroupLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	client := newTestNeptuneClient(t, neptune.NewHandler(backend))

	_, err := client.CreateDBSubnetGroup(t.Context(), &neptunesdk.CreateDBSubnetGroupInput{
		DBSubnetGroupName:        aws.String("sng"),
		DBSubnetGroupDescription: aws.String("subnet group"),
		SubnetIds:                []string{"subnet-a", "subnet-b"},
	})
	require.NoError(t, err)

	listed, listErr := client.DescribeDBSubnetGroups(
		t.Context(),
		&neptunesdk.DescribeDBSubnetGroupsInput{DBSubnetGroupName: aws.String("sng")},
	)
	require.NoError(t, listErr)
	require.Len(t, listed.DBSubnetGroups, 1)
	assert.Len(t, listed.DBSubnetGroups[0].Subnets, 2)

	modified, modErr := client.ModifyDBSubnetGroup(t.Context(), &neptunesdk.ModifyDBSubnetGroupInput{
		DBSubnetGroupName: aws.String("sng"),
		SubnetIds:         []string{"subnet-a", "subnet-b", "subnet-c"},
	})
	require.NoError(t, modErr)
	assert.Len(t, modified.DBSubnetGroup.Subnets, 3)

	_, delErr := client.DeleteDBSubnetGroup(t.Context(), &neptunesdk.DeleteDBSubnetGroupInput{
		DBSubnetGroupName: aws.String("sng"),
	})
	require.NoError(t, delErr)

	_, afterErr := client.DescribeDBSubnetGroups(
		t.Context(),
		&neptunesdk.DescribeDBSubnetGroupsInput{DBSubnetGroupName: aws.String("sng")},
	)
	require.Error(t, afterErr, "subnet group must be gone after delete")
}

// TestTagsRoundTrip_RealClient drives AddTagsToResource and
// RemoveTagsFromResource through a real client.
func TestTagsRoundTrip_RealClient(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", testRegion)
	client := newTestNeptuneClient(t, neptune.NewHandler(backend))
	clusterID := createTestCluster(t, client, "tags-cluster")

	described, err := client.DescribeDBClusters(t.Context(), &neptunesdk.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String(clusterID),
	})
	require.NoError(t, err)
	clusterArn := aws.ToString(described.DBClusters[0].DBClusterArn)

	_, addErr := client.AddTagsToResource(t.Context(), &neptunesdk.AddTagsToResourceInput{
		ResourceName: aws.String(clusterArn),
		Tags:         []types.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
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
