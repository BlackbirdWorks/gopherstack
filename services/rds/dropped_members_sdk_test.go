package rds_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func putSubnetGroup(t *testing.T, c *rdssdk.Client, name string) {
	t.Helper()

	_, err := c.CreateDBSubnetGroup(t.Context(), &rdssdk.CreateDBSubnetGroupInput{
		DBSubnetGroupName: aws.String(name), DBSubnetGroupDescription: aws.String("d"),
		SubnetIds: []string{"subnet-1", "subnet-2"},
	})
	require.NoError(t, err)
}

func TestSDK_DroppedMembersApplied(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, c *rdssdk.Client)
		name string
	}{
		{
			name: "cluster_network_and_storage_members",
			run: func(t *testing.T, c *rdssdk.Client) {
				t.Helper()
				putSubnetGroup(t, c, "sg1")

				_, err := c.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("c1"), Engine: aws.String("aurora-postgresql"),
					MasterUsername: aws.String("a"), DBSubnetGroupName: aws.String("sg1"),
					VpcSecurityGroupIds: []string{"sg-111", "sg-222"},
					AllocatedStorage:    aws.Int32(100), Iops: aws.Int32(3000),
				})
				require.NoError(t, err)

				_, err = c.ModifyDBCluster(t.Context(), &rdssdk.ModifyDBClusterInput{
					DBClusterIdentifier: aws.String("c1"), VpcSecurityGroupIds: []string{"sg-333"},
					AllocatedStorage: aws.Int32(200), Iops: aws.Int32(4000), ApplyImmediately: aws.Bool(true),
				})
				require.NoError(t, err)

				out, err := c.DescribeDBClusters(t.Context(), &rdssdk.DescribeDBClustersInput{})
				require.NoError(t, err)
				require.Len(t, out.DBClusters, 1)

				cl := out.DBClusters[0]
				assert.Equal(t, "sg1", aws.ToString(cl.DBSubnetGroup))
				require.Len(t, cl.VpcSecurityGroups, 1)
				assert.Equal(t, "sg-333", aws.ToString(cl.VpcSecurityGroups[0].VpcSecurityGroupId))
				assert.Equal(t, int32(200), aws.ToInt32(cl.AllocatedStorage))
				assert.Equal(t, int32(4000), aws.ToInt32(cl.Iops))

				_, err = c.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("c2"), Engine: aws.String("aurora-postgresql"),
					MasterUsername: aws.String("a"), DBSubnetGroupName: aws.String("missing"),
				})
				require.ErrorContains(t, err, "DBSubnetGroupNotFound")
			},
		},
		{
			name: "cluster_major_upgrade_guard",
			run: func(t *testing.T, c *rdssdk.Client) {
				t.Helper()

				_, err := c.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("c1"), Engine: aws.String("aurora-postgresql"),
					MasterUsername: aws.String("a"), EngineVersion: aws.String("14.9"),
				})
				require.NoError(t, err)

				_, err = c.ModifyDBCluster(t.Context(), &rdssdk.ModifyDBClusterInput{
					DBClusterIdentifier: aws.String("c1"), EngineVersion: aws.String("15.4"),
				})
				require.ErrorContains(t, err, "InvalidParameterCombination")

				out, err := c.ModifyDBCluster(t.Context(), &rdssdk.ModifyDBClusterInput{
					DBClusterIdentifier: aws.String("c1"), EngineVersion: aws.String("15.4"),
					AllowMajorVersionUpgrade: aws.Bool(true),
				})
				require.NoError(t, err)
				assert.Equal(t, "15.4", aws.ToString(out.DBCluster.EngineVersion))
			},
		},
		{
			name: "copy_ops_apply_tags",
			run: func(t *testing.T, c *rdssdk.Client) {
				t.Helper()

				tags := []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}}

				_, err := c.CreateDBParameterGroup(t.Context(), &rdssdk.CreateDBParameterGroupInput{
					DBParameterGroupName: aws.String("pg1"), DBParameterGroupFamily: aws.String("postgres15"),
					Description: aws.String("d"),
				})
				require.NoError(t, err)

				cp, err := c.CopyDBParameterGroup(t.Context(), &rdssdk.CopyDBParameterGroupInput{
					SourceDBParameterGroupIdentifier:  aws.String("pg1"),
					TargetDBParameterGroupIdentifier:  aws.String("pg2"),
					TargetDBParameterGroupDescription: aws.String("d"), Tags: tags,
				})
				require.NoError(t, err)

				got, err := c.ListTagsForResource(t.Context(), &rdssdk.ListTagsForResourceInput{
					ResourceName: cp.DBParameterGroup.DBParameterGroupArn,
				})
				require.NoError(t, err)
				assert.Equal(t, tags, got.TagList)

				_, err = c.CreateOptionGroup(t.Context(), &rdssdk.CreateOptionGroupInput{
					OptionGroupName: aws.String("og1"), EngineName: aws.String("mysql"),
					MajorEngineVersion: aws.String("8.0"), OptionGroupDescription: aws.String("d"),
				})
				require.NoError(t, err)

				og, err := c.CopyOptionGroup(t.Context(), &rdssdk.CopyOptionGroupInput{
					SourceOptionGroupIdentifier:  aws.String("og1"),
					TargetOptionGroupIdentifier:  aws.String("og2"),
					TargetOptionGroupDescription: aws.String("d"), Tags: tags,
				})
				require.NoError(t, err)

				got, err = c.ListTagsForResource(t.Context(), &rdssdk.ListTagsForResourceInput{
					ResourceName: og.OptionGroup.OptionGroupArn,
				})
				require.NoError(t, err)
				assert.Equal(t, tags, got.TagList)
			},
		},
		{
			name: "copy_cluster_snapshot_kms_and_tags",
			run: func(t *testing.T, c *rdssdk.Client) {
				t.Helper()

				_, err := c.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("c1"), Engine: aws.String("aurora-postgresql"),
					MasterUsername: aws.String("a"),
				})
				require.NoError(t, err)

				_, err = c.CreateDBClusterSnapshot(t.Context(), &rdssdk.CreateDBClusterSnapshotInput{
					DBClusterIdentifier: aws.String("c1"), DBClusterSnapshotIdentifier: aws.String("s1"),
				})
				require.NoError(t, err)

				cp, err := c.CopyDBClusterSnapshot(t.Context(), &rdssdk.CopyDBClusterSnapshotInput{
					SourceDBClusterSnapshotIdentifier: aws.String("s1"),
					TargetDBClusterSnapshotIdentifier: aws.String("s2"),
					KmsKeyId:                          aws.String("alias/k"),
					Tags:                              []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
				})
				require.NoError(t, err)
				assert.Equal(t, "alias/k", aws.ToString(cp.DBClusterSnapshot.KmsKeyId))
				assert.True(t, aws.ToBool(cp.DBClusterSnapshot.StorageEncrypted))

				got, err := c.ListTagsForResource(t.Context(), &rdssdk.ListTagsForResourceInput{
					ResourceName: cp.DBClusterSnapshot.DBClusterSnapshotArn,
				})
				require.NoError(t, err)
				require.Len(t, got.TagList, 1)
			},
		},
		{
			name: "global_cluster_membership",
			run: func(t *testing.T, c *rdssdk.Client) {
				t.Helper()

				cl, err := c.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("c1"), Engine: aws.String("aurora-postgresql"),
					MasterUsername: aws.String("a"), EngineVersion: aws.String("15.4"),
				})
				require.NoError(t, err)

				g, err := c.CreateGlobalCluster(t.Context(), &rdssdk.CreateGlobalClusterInput{
					GlobalClusterIdentifier:   aws.String("g1"),
					SourceDBClusterIdentifier: cl.DBCluster.DBClusterArn,
					DatabaseName:              aws.String("appdb"),
				})
				require.NoError(t, err)
				assert.Equal(t, "appdb", aws.ToString(g.GlobalCluster.DatabaseName))
				assert.Equal(t, "15.4", aws.ToString(g.GlobalCluster.EngineVersion))
				require.Len(t, g.GlobalCluster.GlobalClusterMembers, 1)
				assert.True(t, aws.ToBool(g.GlobalCluster.GlobalClusterMembers[0].IsWriter))

				_, err = c.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("c2"), Engine: aws.String("aurora-postgresql"),
					MasterUsername: aws.String("a"), GlobalClusterIdentifier: aws.String("g1"),
				})
				require.NoError(t, err)

				got, err := c.DescribeGlobalClusters(t.Context(), &rdssdk.DescribeGlobalClustersInput{})
				require.NoError(t, err)
				require.Len(t, got.GlobalClusters, 1)
				assert.Len(t, got.GlobalClusters[0].GlobalClusterMembers, 2)

				_, err = c.DeleteDBCluster(t.Context(), &rdssdk.DeleteDBClusterInput{
					DBClusterIdentifier: aws.String("c2"), SkipFinalSnapshot: aws.Bool(true),
				})
				require.NoError(t, err)

				got, err = c.DescribeGlobalClusters(t.Context(), &rdssdk.DescribeGlobalClustersInput{})
				require.NoError(t, err)
				assert.Len(t, got.GlobalClusters[0].GlobalClusterMembers, 1)

				_, err = c.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("c3"), Engine: aws.String("aurora-postgresql"),
					MasterUsername: aws.String("a"), GlobalClusterIdentifier: aws.String("nope"),
				})
				require.ErrorContains(t, err, "GlobalClusterNotFound")
			},
		},
		{
			name: "instance_network_type_and_replica_members",
			run: func(t *testing.T, c *rdssdk.Client) {
				t.Helper()
				putSubnetGroup(t, c, "sg1")

				src, err := c.CreateDBInstance(t.Context(), &rdssdk.CreateDBInstanceInput{
					DBInstanceIdentifier: aws.String("src"), Engine: aws.String("postgres"),
					DBInstanceClass: aws.String("db.m5.large"), MasterUsername: aws.String("a"),
					MasterUserPassword: aws.String("password123"), AllocatedStorage: aws.Int32(20),
					NetworkType: aws.String("DUAL"), MaxAllocatedStorage: aws.Int32(100),
				})
				require.NoError(t, err)
				assert.Equal(t, "DUAL", aws.ToString(src.DBInstance.NetworkType))
				assert.Equal(t, int32(100), aws.ToInt32(src.DBInstance.MaxAllocatedStorage))

				_, err = c.CreateDBInstance(t.Context(), &rdssdk.CreateDBInstanceInput{
					DBInstanceIdentifier: aws.String("bad"), Engine: aws.String("postgres"),
					DBInstanceClass: aws.String("db.m5.large"), MasterUsername: aws.String("a"),
					MasterUserPassword: aws.String("password123"), AllocatedStorage: aws.Int32(20),
					NetworkType: aws.String("IPV6"),
				})
				require.Error(t, err)

				mod, err := c.ModifyDBInstance(t.Context(), &rdssdk.ModifyDBInstanceInput{
					DBInstanceIdentifier: aws.String("src"), NetworkType: aws.String("IPV4"),
					DBSubnetGroupName: aws.String("sg1"), ApplyImmediately: aws.Bool(true),
				})
				require.NoError(t, err)
				assert.Equal(t, "IPV4", aws.ToString(mod.DBInstance.NetworkType))
				assert.Equal(t, "sg1", aws.ToString(mod.DBInstance.DBSubnetGroup.DBSubnetGroupName))

				rep, err := c.CreateDBInstanceReadReplica(t.Context(), &rdssdk.CreateDBInstanceReadReplicaInput{
					DBInstanceIdentifier: aws.String("rep"), SourceDBInstanceIdentifier: aws.String("src"),
					DBInstanceClass: aws.String("db.m5.xlarge"), StorageType: aws.String("gp3"),
					MultiAZ: aws.Bool(true), PubliclyAccessible: aws.Bool(true), KmsKeyId: aws.String("k"),
					MonitoringInterval: aws.Int32(60), DeletionProtection: aws.Bool(true),
					CopyTagsToSnapshot: aws.Bool(true), EnablePerformanceInsights: aws.Bool(true),
					EnableCloudwatchLogsExports: []string{"postgresql"}, NetworkType: aws.String("DUAL"),
					AvailabilityZone: aws.String("us-east-1b"), Port: aws.Int32(5555), Iops: aws.Int32(3000),
				})
				require.NoError(t, err)

				i := rep.DBInstance
				assert.Equal(t, "db.m5.xlarge", aws.ToString(i.DBInstanceClass))
				assert.Equal(t, "sg1", aws.ToString(i.DBSubnetGroup.DBSubnetGroupName), "inherited from source")
				assert.Equal(t, "gp3", aws.ToString(i.StorageType))
				assert.True(t, aws.ToBool(i.MultiAZ))
				assert.True(t, aws.ToBool(i.PubliclyAccessible))
				assert.Equal(t, "k", aws.ToString(i.KmsKeyId))
				assert.True(t, aws.ToBool(i.StorageEncrypted))
				assert.Equal(t, int32(60), aws.ToInt32(i.MonitoringInterval))
				assert.True(t, aws.ToBool(i.DeletionProtection))
				assert.True(t, aws.ToBool(i.CopyTagsToSnapshot))
				assert.True(t, aws.ToBool(i.PerformanceInsightsEnabled))
				assert.Equal(t, []string{"postgresql"}, i.EnabledCloudwatchLogsExports)
				assert.Equal(t, "DUAL", aws.ToString(i.NetworkType))
				assert.Equal(t, "us-east-1b", aws.ToString(i.AvailabilityZone))
				assert.Equal(t, int32(5555), aws.ToInt32(i.Endpoint.Port))
				assert.Equal(t, int32(3000), aws.ToInt32(i.Iops))
			},
		},
		{
			name: "restore_ops_apply_members",
			run: func(t *testing.T, c *rdssdk.Client) {
				t.Helper()
				putSubnetGroup(t, c, "sg1")

				tags := []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}}

				_, err := c.CreateDBCluster(t.Context(), &rdssdk.CreateDBClusterInput{
					DBClusterIdentifier: aws.String("c1"), Engine: aws.String("aurora-postgresql"),
					MasterUsername: aws.String("a"),
				})
				require.NoError(t, err)

				_, err = c.CreateDBClusterSnapshot(t.Context(), &rdssdk.CreateDBClusterSnapshotInput{
					DBClusterIdentifier: aws.String("c1"), DBClusterSnapshotIdentifier: aws.String("s1"),
				})
				require.NoError(t, err)

				fromSnap, err := c.RestoreDBClusterFromSnapshot(t.Context(), &rdssdk.RestoreDBClusterFromSnapshotInput{
					DBClusterIdentifier: aws.String("r1"), SnapshotIdentifier: aws.String("s1"),
					Engine: aws.String("aurora-postgresql"), DBSubnetGroupName: aws.String("sg1"),
					VpcSecurityGroupIds:         []string{"sg-1"},
					EnableCloudwatchLogsExports: []string{"postgresql"}, Tags: tags,
				})
				require.NoError(t, err)
				assert.Equal(t, "sg1", aws.ToString(fromSnap.DBCluster.DBSubnetGroup))
				require.Len(t, fromSnap.DBCluster.VpcSecurityGroups, 1)
				assert.Equal(t, []string{"postgresql"}, fromSnap.DBCluster.EnabledCloudwatchLogsExports)

				got, err := c.ListTagsForResource(t.Context(), &rdssdk.ListTagsForResourceInput{
					ResourceName: fromSnap.DBCluster.DBClusterArn,
				})
				require.NoError(t, err)
				assert.Equal(t, tags, got.TagList)

				pitr, err := c.RestoreDBClusterToPointInTime(t.Context(), &rdssdk.RestoreDBClusterToPointInTimeInput{
					DBClusterIdentifier: aws.String("r2"), SourceDBClusterIdentifier: aws.String("c1"),
					UseLatestRestorableTime: aws.Bool(true), DBSubnetGroupName: aws.String("sg1"), Tags: tags,
				})
				require.NoError(t, err)
				assert.Equal(t, "sg1", aws.ToString(pitr.DBCluster.DBSubnetGroup))

				_, err = c.RestoreDBClusterToPointInTime(t.Context(), &rdssdk.RestoreDBClusterToPointInTimeInput{
					DBClusterIdentifier: aws.String("r3"), SourceDBClusterIdentifier: aws.String("c1"),
					UseLatestRestorableTime: aws.Bool(true), DBSubnetGroupName: aws.String("missing"),
				})
				require.ErrorContains(t, err, "DBSubnetGroupNotFound")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newRealClientBackendAndClient(t)
			tt.run(t, client)
		})
	}
}
