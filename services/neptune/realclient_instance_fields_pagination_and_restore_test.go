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

// TestReqFieldDiffTier1_RealClient drives the bd gopherstack-xhu2t tier-1
// dropped-parameter fixes through a real aws-sdk-go-v2 neptune client,
// asserting each field's observable effect rather than just NoError.
func TestReqFieldDiffTier1_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "inherits_backup_retention_and_vpc_sgs",
			run: func(t *testing.T) {
				t.Helper()

				backend := neptune.NewInMemoryBackend("000000000000", testRegion)
				client := newTestNeptuneClient(t, neptune.NewHandler(backend))

				_, err := client.CreateDBCluster(t.Context(), &neptunesdk.CreateDBClusterInput{
					DBClusterIdentifier:   aws.String("inherit-cluster"),
					Engine:                aws.String("neptune"),
					BackupRetentionPeriod: aws.Int32(14),
					VpcSecurityGroupIds:   []string{"sg-aaaa", "sg-bbbb"},
				})
				require.NoError(t, err)

				created, err := client.CreateDBInstance(
					t.Context(),
					&neptunesdk.CreateDBInstanceInput{
						DBInstanceIdentifier: aws.String("inherit-instance"),
						DBClusterIdentifier:  aws.String("inherit-cluster"),
						DBInstanceClass:      aws.String("db.r5.large"),
						Engine:               aws.String("neptune"),
					},
				)
				require.NoError(t, err)
				assert.EqualValues(t, 14, aws.ToInt32(created.DBInstance.BackupRetentionPeriod))
				require.Len(t, created.DBInstance.VpcSecurityGroups, 2)
				assert.Equal(
					t,
					"sg-aaaa",
					aws.ToString(created.DBInstance.VpcSecurityGroups[0].VpcSecurityGroupId),
				)
				assert.Equal(
					t,
					"sg-bbbb",
					aws.ToString(created.DBInstance.VpcSecurityGroups[1].VpcSecurityGroupId),
				)
			},
		},
		{
			name: "dbsecuritygroups_monitoring_iops",
			run: func(t *testing.T) {
				t.Helper()

				backend := neptune.NewInMemoryBackend("000000000000", testRegion)
				client := newTestNeptuneClient(t, neptune.NewHandler(backend))
				clusterID := createTestCluster(t, client, "monitoring-cluster")

				created, err := client.CreateDBInstance(
					t.Context(),
					&neptunesdk.CreateDBInstanceInput{
						DBInstanceIdentifier: aws.String("monitoring-instance"),
						DBClusterIdentifier:  aws.String(clusterID),
						DBInstanceClass:      aws.String("db.r5.large"),
						Engine:               aws.String("neptune"),
						DBSecurityGroups:     []string{"default"},
						MonitoringInterval:   aws.Int32(15),
						MonitoringRoleArn:    aws.String("arn:aws:iam:123456789012:role/emaccess"),
						Iops:                 aws.Int32(1000),
					},
				)
				require.NoError(t, err)
				require.Len(t, created.DBInstance.DBSecurityGroups, 1)
				assert.Equal(
					t,
					"default",
					aws.ToString(created.DBInstance.DBSecurityGroups[0].DBSecurityGroupName),
				)
				assert.EqualValues(t, 15, aws.ToInt32(created.DBInstance.MonitoringInterval))
				assert.Equal(
					t, "arn:aws:iam:123456789012:role/emaccess",
					aws.ToString(created.DBInstance.MonitoringRoleArn),
				)
				assert.EqualValues(t, 1000, aws.ToInt32(created.DBInstance.Iops))
			},
		},
		{
			name: "rejects_invalid_monitoring_interval",
			run: func(t *testing.T) {
				t.Helper()

				backend := neptune.NewInMemoryBackend("000000000000", testRegion)
				client := newTestNeptuneClient(t, neptune.NewHandler(backend))
				clusterID := createTestCluster(t, client, "bad-monitoring-cluster")

				_, err := client.CreateDBInstance(t.Context(), &neptunesdk.CreateDBInstanceInput{
					DBInstanceIdentifier: aws.String("bad-monitoring-instance"),
					DBClusterIdentifier:  aws.String(clusterID),
					DBInstanceClass:      aws.String("db.r5.large"),
					Engine:               aws.String("neptune"),
					MonitoringInterval:   aws.Int32(7),
				})
				require.Error(t, err)
			},
		},
		{
			name: "modify_port_iops_monitoring",
			run: func(t *testing.T) {
				t.Helper()

				backend := neptune.NewInMemoryBackend("000000000000", testRegion)
				client := newTestNeptuneClient(t, neptune.NewHandler(backend))
				clusterID := createTestCluster(t, client, "modify-instance-cluster")
				_, err := client.CreateDBInstance(t.Context(), &neptunesdk.CreateDBInstanceInput{
					DBInstanceIdentifier: aws.String("modify-instance"),
					DBClusterIdentifier:  aws.String(clusterID),
					DBInstanceClass:      aws.String("db.r5.large"),
					Engine:               aws.String("neptune"),
				})
				require.NoError(t, err)

				modified, err := client.ModifyDBInstance(
					t.Context(),
					&neptunesdk.ModifyDBInstanceInput{
						DBInstanceIdentifier: aws.String("modify-instance"),
						DBPortNumber:         aws.Int32(8183),
						Iops:                 aws.Int32(2000),
						MonitoringInterval:   aws.Int32(30),
						// ApplyImmediately=false is read for wire-declaration parity but
						// this backend still applies changes immediately.
						ApplyImmediately: aws.Bool(false),
					},
				)
				require.NoError(t, err)
				assert.EqualValues(t, 8183, aws.ToInt32(modified.DBInstance.DbInstancePort))
				assert.EqualValues(t, 2000, aws.ToInt32(modified.DBInstance.Iops))
				const applyImmediatelyNote = "ApplyImmediately=false is read for wire-declaration parity " +
					"but this backend still applies changes immediately"
				assert.EqualValues(
					t,
					30,
					aws.ToInt32(modified.DBInstance.MonitoringInterval),
					applyImmediatelyNote,
				)
			},
		},
		{
			name: "modify_rejects_bad_port",
			run: func(t *testing.T) {
				t.Helper()

				backend := neptune.NewInMemoryBackend("000000000000", testRegion)
				client := newTestNeptuneClient(t, neptune.NewHandler(backend))
				clusterID := createTestCluster(t, client, "bad-port-cluster")
				_, err := client.CreateDBInstance(t.Context(), &neptunesdk.CreateDBInstanceInput{
					DBInstanceIdentifier: aws.String("bad-port-instance"),
					DBClusterIdentifier:  aws.String(clusterID),
					DBInstanceClass:      aws.String("db.r5.large"),
					Engine:               aws.String("neptune"),
				})
				require.NoError(t, err)

				_, err = client.ModifyDBInstance(t.Context(), &neptunesdk.ModifyDBInstanceInput{
					DBInstanceIdentifier: aws.String("bad-port-instance"),
					DBPortNumber:         aws.Int32(80),
				})
				require.Error(t, err)
			},
		},
		{
			name: "modify_cluster_propagates_instance_pg_name",
			run: func(t *testing.T) {
				t.Helper()

				backend := neptune.NewInMemoryBackend("000000000000", testRegion)
				client := newTestNeptuneClient(t, neptune.NewHandler(backend))
				clusterID := createTestCluster(t, client, "ipg-cluster")
				for _, id := range []string{"ipg-instance-1", "ipg-instance-2"} {
					_, err := client.CreateDBInstance(
						t.Context(),
						&neptunesdk.CreateDBInstanceInput{
							DBInstanceIdentifier: aws.String(id),
							DBClusterIdentifier:  aws.String(clusterID),
							DBInstanceClass:      aws.String("db.r5.large"),
							Engine:               aws.String("neptune"),
						},
					)
					require.NoError(t, err)
				}

				_, err := client.ModifyDBCluster(t.Context(), &neptunesdk.ModifyDBClusterInput{
					DBClusterIdentifier:          aws.String(clusterID),
					DBInstanceParameterGroupName: aws.String("custom-instance-pg"),
					ApplyImmediately:             aws.Bool(false),
				})
				require.NoError(t, err)

				for _, id := range []string{"ipg-instance-1", "ipg-instance-2"} {
					described, describeErr := client.DescribeDBInstances(
						t.Context(),
						&neptunesdk.DescribeDBInstancesInput{DBInstanceIdentifier: aws.String(id)},
					)
					require.NoError(t, describeErr)
					require.Len(t, described.DBInstances, 1)
					require.Len(t, described.DBInstances[0].DBParameterGroups, 1)
					assert.Equal(
						t,
						"custom-instance-pg",
						aws.ToString(
							described.DBInstances[0].DBParameterGroups[0].DBParameterGroupName,
						),
						"instance %s must inherit DBInstanceParameterGroupName",
						id,
					)
				}
			},
		},
		{
			name: "delete_rejects_snapshot_id_with_skip_true",
			run: func(t *testing.T) {
				t.Helper()

				backend := neptune.NewInMemoryBackend("000000000000", testRegion)
				client := newTestNeptuneClient(t, neptune.NewHandler(backend))
				clusterID := createTestCluster(t, client, "del-instance-cluster")
				_, err := client.CreateDBInstance(t.Context(), &neptunesdk.CreateDBInstanceInput{
					DBInstanceIdentifier: aws.String("del-instance-1"),
					DBClusterIdentifier:  aws.String(clusterID),
					DBInstanceClass:      aws.String("db.r5.large"),
					Engine:               aws.String("neptune"),
				})
				require.NoError(t, err)
				_, err = client.CreateDBInstance(t.Context(), &neptunesdk.CreateDBInstanceInput{
					DBInstanceIdentifier: aws.String("del-instance-2"),
					DBClusterIdentifier:  aws.String(clusterID),
					DBInstanceClass:      aws.String("db.r5.large"),
					Engine:               aws.String("neptune"),
				})
				require.NoError(t, err)

				_, err = client.DeleteDBInstance(t.Context(), &neptunesdk.DeleteDBInstanceInput{
					DBInstanceIdentifier:      aws.String("del-instance-1"),
					SkipFinalSnapshot:         aws.Bool(true),
					FinalDBSnapshotIdentifier: aws.String("should-not-be-allowed"),
				})
				require.Error(t, err)

				_, err = client.DeleteDBInstance(t.Context(), &neptunesdk.DeleteDBInstanceInput{
					DBInstanceIdentifier: aws.String("del-instance-1"),
					SkipFinalSnapshot:    aws.Bool(true),
				})
				require.NoError(t, err)
			},
		},
		{
			name: "copy_snapshot_copytags_true",
			run: func(t *testing.T) {
				t.Helper()

				backend := neptune.NewInMemoryBackend("000000000000", testRegion)
				client := newTestNeptuneClient(t, neptune.NewHandler(backend))
				clusterID := createTestCluster(t, client, "copytags-cluster")

				snap, err := client.CreateDBClusterSnapshot(
					t.Context(),
					&neptunesdk.CreateDBClusterSnapshotInput{
						DBClusterIdentifier:         aws.String(clusterID),
						DBClusterSnapshotIdentifier: aws.String("copytags-source-snap"),
					},
				)
				require.NoError(t, err)
				sourceArn := aws.ToString(snap.DBClusterSnapshot.DBClusterSnapshotArn)

				_, err = client.AddTagsToResource(t.Context(), &neptunesdk.AddTagsToResourceInput{
					ResourceName: aws.String(sourceArn),
					Tags:         []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				copied, err := client.CopyDBClusterSnapshot(
					t.Context(),
					&neptunesdk.CopyDBClusterSnapshotInput{
						SourceDBClusterSnapshotIdentifier: aws.String("copytags-source-snap"),
						TargetDBClusterSnapshotIdentifier: aws.String("copytags-target-snap"),
						CopyTags:                          aws.Bool(true),
					},
				)
				require.NoError(t, err)
				targetArn := aws.ToString(copied.DBClusterSnapshot.DBClusterSnapshotArn)

				tagged, err := client.ListTagsForResource(
					t.Context(),
					&neptunesdk.ListTagsForResourceInput{ResourceName: aws.String(targetArn)},
				)
				require.NoError(t, err)
				require.Len(t, tagged.TagList, 1)
				assert.Equal(t, "env", aws.ToString(tagged.TagList[0].Key))
			},
		},
		{
			name: "copy_snapshot_copytags_false",
			run: func(t *testing.T) {
				t.Helper()

				backend := neptune.NewInMemoryBackend("000000000000", testRegion)
				client := newTestNeptuneClient(t, neptune.NewHandler(backend))
				clusterID := createTestCluster(t, client, "nocopytags-cluster")

				snap, err := client.CreateDBClusterSnapshot(
					t.Context(),
					&neptunesdk.CreateDBClusterSnapshotInput{
						DBClusterIdentifier:         aws.String(clusterID),
						DBClusterSnapshotIdentifier: aws.String("nocopytags-source-snap"),
					},
				)
				require.NoError(t, err)
				_, err = client.AddTagsToResource(t.Context(), &neptunesdk.AddTagsToResourceInput{
					ResourceName: aws.String(
						aws.ToString(snap.DBClusterSnapshot.DBClusterSnapshotArn),
					),
					Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				copied, err := client.CopyDBClusterSnapshot(
					t.Context(),
					&neptunesdk.CopyDBClusterSnapshotInput{
						SourceDBClusterSnapshotIdentifier: aws.String("nocopytags-source-snap"),
						TargetDBClusterSnapshotIdentifier: aws.String("nocopytags-target-snap"),
					},
				)
				require.NoError(t, err)

				tagged, err := client.ListTagsForResource(
					t.Context(),
					&neptunesdk.ListTagsForResourceInput{
						ResourceName: aws.String(
							aws.ToString(copied.DBClusterSnapshot.DBClusterSnapshotArn),
						),
					},
				)
				require.NoError(t, err)
				assert.Empty(t, tagged.TagList)
			},
		},
		{
			name: "restore_pit_time_field_validation",
			run: func(t *testing.T) {
				t.Helper()

				backend := neptune.NewInMemoryBackend("000000000000", testRegion)
				client := newTestNeptuneClient(t, neptune.NewHandler(backend))
				sourceID := createTestCluster(t, client, "pitr-validation-source")

				_, err := client.RestoreDBClusterToPointInTime(
					t.Context(), &neptunesdk.RestoreDBClusterToPointInTimeInput{
						SourceDBClusterIdentifier: aws.String(sourceID),
						DBClusterIdentifier:       aws.String("pitr-validation-neither"),
					},
				)
				require.Error(t, err, "must specify RestoreToTime or UseLatestRestorableTime")

				out, err := client.RestoreDBClusterToPointInTime(
					t.Context(), &neptunesdk.RestoreDBClusterToPointInTimeInput{
						SourceDBClusterIdentifier: aws.String(sourceID),
						DBClusterIdentifier:       aws.String("pitr-validation-ok"),
						UseLatestRestorableTime:   aws.Bool(true),
					},
				)
				require.NoError(t, err)
				assert.Equal(
					t,
					"pitr-validation-ok",
					aws.ToString(out.DBCluster.DBClusterIdentifier),
				)
			},
		},
		{
			name: "engine_versions_default_only",
			run: func(t *testing.T) {
				t.Helper()

				backend := neptune.NewInMemoryBackend("000000000000", testRegion)
				client := newTestNeptuneClient(t, neptune.NewHandler(backend))

				all, err := client.DescribeDBEngineVersions(
					t.Context(),
					&neptunesdk.DescribeDBEngineVersionsInput{},
				)
				require.NoError(t, err)
				require.Greater(t, len(all.DBEngineVersions), 1)

				defaultOnly, err := client.DescribeDBEngineVersions(
					t.Context(),
					&neptunesdk.DescribeDBEngineVersionsInput{DefaultOnly: aws.Bool(true)},
				)
				require.NoError(t, err)
				require.Len(t, defaultOnly.DBEngineVersions, 1)
				assert.Equal(
					t,
					"1.3.0.0",
					aws.ToString(defaultOnly.DBEngineVersions[0].EngineVersion),
				)

				paged, err := client.DescribeDBEngineVersions(
					t.Context(),
					&neptunesdk.DescribeDBEngineVersionsInput{MaxRecords: aws.Int32(1)},
				)
				require.NoError(t, err)
				assert.Len(t, paged.DBEngineVersions, 1)
				assert.NotEmpty(t, aws.ToString(paged.Marker))
			},
		},
		{
			name: "cluster_parameters_max_records_pagination",
			run: func(t *testing.T) {
				t.Helper()

				backend := neptune.NewInMemoryBackend("000000000000", testRegion)
				client := newTestNeptuneClient(t, neptune.NewHandler(backend))
				_, err := client.CreateDBClusterParameterGroup(
					t.Context(), &neptunesdk.CreateDBClusterParameterGroupInput{
						DBClusterParameterGroupName: aws.String("page-cluster-pg"),
						DBParameterGroupFamily:      aws.String("neptune1.3"),
						Description:                 aws.String("d"),
					},
				)
				require.NoError(t, err)

				full, err := client.DescribeDBClusterParameters(
					t.Context(),
					&neptunesdk.DescribeDBClusterParametersInput{
						DBClusterParameterGroupName: aws.String("page-cluster-pg"),
					},
				)
				require.NoError(t, err)
				require.Greater(
					t,
					len(full.Parameters),
					1,
					"catalog must have more than 1 parameter",
				)

				paged, err := client.DescribeDBClusterParameters(
					t.Context(),
					&neptunesdk.DescribeDBClusterParametersInput{
						DBClusterParameterGroupName: aws.String("page-cluster-pg"),
						MaxRecords:                  aws.Int32(1),
					},
				)
				require.NoError(t, err)
				assert.Len(t, paged.Parameters, 1)
				assert.NotEmpty(t, aws.ToString(paged.Marker))
			},
		},
		{
			name: "global_clusters_max_records_pagination",
			run: func(t *testing.T) {
				t.Helper()

				backend := neptune.NewInMemoryBackend("000000000000", testRegion)
				client := newTestNeptuneClient(t, neptune.NewHandler(backend))
				for _, id := range []string{"page-gc-1", "page-gc-2"} {
					_, err := client.CreateGlobalCluster(
						t.Context(),
						&neptunesdk.CreateGlobalClusterInput{
							GlobalClusterIdentifier: aws.String(id),
						},
					)
					require.NoError(t, err)
				}

				paged, err := client.DescribeGlobalClusters(
					t.Context(),
					&neptunesdk.DescribeGlobalClustersInput{
						MaxRecords: aws.Int32(1),
					},
				)
				require.NoError(t, err)
				assert.Len(t, paged.GlobalClusters, 1)
				assert.NotEmpty(t, aws.ToString(paged.Marker))
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
