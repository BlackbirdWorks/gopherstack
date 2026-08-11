package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_RDS_DBInstanceArn drives CreateDBInstance -> DescribeDBInstances ->
// ModifyDBInstance -> DeleteDBInstance through the real SDK and asserts every response
// carries a non-empty DBInstanceArn that matches DescribeDBInstances's. The internal
// DBInstance model had no ARN field at all, even though the same ARN was already being
// computed (and used as the tag-map key) elsewhere in the backend -- it was just never
// attached to the wire response.
func TestIntegration_RDS_DBInstanceArn(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name string
	}{
		{name: "arn set on every instance lifecycle op"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createRDSClient(t)

			id := "arn-inst-" + uuid.NewString()[:8]

			createOut, err := client.CreateDBInstance(ctx, &rdssdk.CreateDBInstanceInput{
				DBInstanceIdentifier: aws.String(id),
				DBInstanceClass:      aws.String("db.t3.micro"),
				Engine:               aws.String("postgres"),
				MasterUsername:       aws.String("admin"),
				MasterUserPassword:   aws.String("password123"),
				AllocatedStorage:     aws.Int32(20),
			})
			require.NoError(t, err, "CreateDBInstance should succeed")
			require.NotNil(t, createOut.DBInstance)
			createArn := aws.ToString(createOut.DBInstance.DBInstanceArn)
			assert.NotEmpty(t, createArn, "CreateDBInstance must return a DBInstanceArn")

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteDBInstance(cleanupCtx, &rdssdk.DeleteDBInstanceInput{
					DBInstanceIdentifier: aws.String(id),
					SkipFinalSnapshot:    aws.Bool(true),
				})
			})

			descOut, err := client.DescribeDBInstances(ctx, &rdssdk.DescribeDBInstancesInput{
				DBInstanceIdentifier: aws.String(id),
			})
			require.NoError(t, err, "DescribeDBInstances should succeed")
			require.Len(t, descOut.DBInstances, 1)
			describeArn := aws.ToString(descOut.DBInstances[0].DBInstanceArn)
			assert.NotEmpty(t, describeArn, "DescribeDBInstances must return a DBInstanceArn")
			assert.Equal(
				t,
				describeArn,
				createArn,
				"CreateDBInstance's DBInstanceArn must match DescribeDBInstances's",
			)

			modOut, err := client.ModifyDBInstance(ctx, &rdssdk.ModifyDBInstanceInput{
				DBInstanceIdentifier: aws.String(id),
				DBInstanceClass:      aws.String("db.r5.large"),
				ApplyImmediately:     aws.Bool(true),
			})
			require.NoError(t, err, "ModifyDBInstance should succeed")
			require.NotNil(t, modOut.DBInstance)
			modArn := aws.ToString(modOut.DBInstance.DBInstanceArn)
			assert.NotEmpty(t, modArn, "ModifyDBInstance must return a DBInstanceArn")
			assert.Equal(
				t,
				describeArn,
				modArn,
				"ModifyDBInstance's DBInstanceArn must match DescribeDBInstances's",
			)

			delOut, err := client.DeleteDBInstance(ctx, &rdssdk.DeleteDBInstanceInput{
				DBInstanceIdentifier: aws.String(id),
				SkipFinalSnapshot:    aws.Bool(true),
			})
			require.NoError(t, err, "DeleteDBInstance should succeed")
			require.NotNil(t, delOut.DBInstance)
			delArn := aws.ToString(delOut.DBInstance.DBInstanceArn)
			assert.NotEmpty(t, delArn, "DeleteDBInstance must return a DBInstanceArn")
			assert.Equal(
				t,
				describeArn,
				delArn,
				"DeleteDBInstance's DBInstanceArn must match DescribeDBInstances's",
			)
		})
	}
}

// TestIntegration_RDS_DBClusterArn drives CreateDBCluster -> DescribeDBClusters ->
// DeleteDBCluster and asserts a non-empty, consistent DBClusterArn. Same defect class
// as DBInstanceArn: DBCluster had no ARN field at all.
func TestIntegration_RDS_DBClusterArn(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name string
	}{
		{name: "arn set on every cluster lifecycle op"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createRDSClient(t)

			clusterID := "arn-cluster-" + uuid.NewString()[:8]

			createOut, err := client.CreateDBCluster(ctx, &rdssdk.CreateDBClusterInput{
				DBClusterIdentifier: aws.String(clusterID),
				Engine:              aws.String("aurora-postgresql"),
				MasterUsername:      aws.String("admin"),
				MasterUserPassword:  aws.String("password123"),
			})
			require.NoError(t, err, "CreateDBCluster should succeed")
			require.NotNil(t, createOut.DBCluster)
			createArn := aws.ToString(createOut.DBCluster.DBClusterArn)
			assert.NotEmpty(t, createArn, "CreateDBCluster must return a DBClusterArn")

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteDBCluster(cleanupCtx, &rdssdk.DeleteDBClusterInput{
					DBClusterIdentifier: aws.String(clusterID),
					SkipFinalSnapshot:   aws.Bool(true),
				})
			})

			descOut, err := client.DescribeDBClusters(ctx, &rdssdk.DescribeDBClustersInput{
				DBClusterIdentifier: aws.String(clusterID),
			})
			require.NoError(t, err, "DescribeDBClusters should succeed")
			require.Len(t, descOut.DBClusters, 1)
			describeArn := aws.ToString(descOut.DBClusters[0].DBClusterArn)
			assert.NotEmpty(t, describeArn, "DescribeDBClusters must return a DBClusterArn")
			assert.Equal(
				t,
				describeArn,
				createArn,
				"CreateDBCluster's DBClusterArn must match DescribeDBClusters's",
			)

			delOut, err := client.DeleteDBCluster(ctx, &rdssdk.DeleteDBClusterInput{
				DBClusterIdentifier: aws.String(clusterID),
				SkipFinalSnapshot:   aws.Bool(true),
			})
			require.NoError(t, err, "DeleteDBCluster should succeed")
			require.NotNil(t, delOut.DBCluster)
			delArn := aws.ToString(delOut.DBCluster.DBClusterArn)
			assert.NotEmpty(t, delArn, "DeleteDBCluster must return a DBClusterArn")
			assert.Equal(
				t,
				describeArn,
				delArn,
				"DeleteDBCluster's DBClusterArn must match DescribeDBClusters's",
			)
		})
	}
}

// TestIntegration_RDS_SnapshotArns covers DBSnapshot and DBClusterSnapshot, which had
// the same missing-ARN defect as DBInstance and DBCluster.
func TestIntegration_RDS_SnapshotArns(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name string
	}{
		{name: "arn set on instance and cluster snapshots"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createRDSClient(t)

			instID := "arn-snap-inst-" + uuid.NewString()[:8]
			_, err := client.CreateDBInstance(ctx, &rdssdk.CreateDBInstanceInput{
				DBInstanceIdentifier: aws.String(instID),
				DBInstanceClass:      aws.String("db.t3.micro"),
				Engine:               aws.String("postgres"),
				MasterUsername:       aws.String("admin"),
				MasterUserPassword:   aws.String("password123"),
				AllocatedStorage:     aws.Int32(20),
			})
			require.NoError(t, err, "CreateDBInstance should succeed")

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteDBInstance(cleanupCtx, &rdssdk.DeleteDBInstanceInput{
					DBInstanceIdentifier: aws.String(instID),
					SkipFinalSnapshot:    aws.Bool(true),
				})
			})

			snapID := "arn-snap-" + uuid.NewString()[:8]
			snapOut, err := client.CreateDBSnapshot(ctx, &rdssdk.CreateDBSnapshotInput{
				DBSnapshotIdentifier: aws.String(snapID),
				DBInstanceIdentifier: aws.String(instID),
			})
			require.NoError(t, err, "CreateDBSnapshot should succeed")
			require.NotNil(t, snapOut.DBSnapshot)
			snapCreateArn := aws.ToString(snapOut.DBSnapshot.DBSnapshotArn)
			assert.NotEmpty(t, snapCreateArn, "CreateDBSnapshot must return a DBSnapshotArn")

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteDBSnapshot(cleanupCtx, &rdssdk.DeleteDBSnapshotInput{
					DBSnapshotIdentifier: aws.String(snapID),
				})
			})

			snapDescOut, err := client.DescribeDBSnapshots(ctx, &rdssdk.DescribeDBSnapshotsInput{
				DBSnapshotIdentifier: aws.String(snapID),
			})
			require.NoError(t, err, "DescribeDBSnapshots should succeed")
			require.Len(t, snapDescOut.DBSnapshots, 1)
			snapDescArn := aws.ToString(snapDescOut.DBSnapshots[0].DBSnapshotArn)
			assert.NotEmpty(t, snapDescArn, "DescribeDBSnapshots must return a DBSnapshotArn")
			assert.Equal(t, snapDescArn, snapCreateArn,
				"CreateDBSnapshot's DBSnapshotArn must match DescribeDBSnapshots's")

			clusterID := "arn-snap-cluster-" + uuid.NewString()[:8]
			_, err = client.CreateDBCluster(ctx, &rdssdk.CreateDBClusterInput{
				DBClusterIdentifier: aws.String(clusterID),
				Engine:              aws.String("aurora-postgresql"),
				MasterUsername:      aws.String("admin"),
				MasterUserPassword:  aws.String("password123"),
			})
			require.NoError(t, err, "CreateDBCluster should succeed")

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteDBCluster(cleanupCtx, &rdssdk.DeleteDBClusterInput{
					DBClusterIdentifier: aws.String(clusterID),
					SkipFinalSnapshot:   aws.Bool(true),
				})
			})

			clusterSnapID := "arn-csnap-" + uuid.NewString()[:8]
			clusterSnapOut, err := client.CreateDBClusterSnapshot(
				ctx,
				&rdssdk.CreateDBClusterSnapshotInput{
					DBClusterSnapshotIdentifier: aws.String(clusterSnapID),
					DBClusterIdentifier:         aws.String(clusterID),
				},
			)
			require.NoError(t, err, "CreateDBClusterSnapshot should succeed")
			require.NotNil(t, clusterSnapOut.DBClusterSnapshot)
			clusterSnapCreateArn := aws.ToString(
				clusterSnapOut.DBClusterSnapshot.DBClusterSnapshotArn,
			)
			assert.NotEmpty(
				t,
				clusterSnapCreateArn,
				"CreateDBClusterSnapshot must return a DBClusterSnapshotArn",
			)

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteDBClusterSnapshot(
					cleanupCtx,
					&rdssdk.DeleteDBClusterSnapshotInput{
						DBClusterSnapshotIdentifier: aws.String(clusterSnapID),
					},
				)
			})

			clusterSnapDescOut, err := client.DescribeDBClusterSnapshots(
				ctx,
				&rdssdk.DescribeDBClusterSnapshotsInput{
					DBClusterSnapshotIdentifier: aws.String(clusterSnapID),
				},
			)
			require.NoError(t, err, "DescribeDBClusterSnapshots should succeed")
			require.Len(t, clusterSnapDescOut.DBClusterSnapshots, 1)
			clusterSnapDescArn := aws.ToString(
				clusterSnapDescOut.DBClusterSnapshots[0].DBClusterSnapshotArn,
			)
			assert.NotEmpty(
				t,
				clusterSnapDescArn,
				"DescribeDBClusterSnapshots must return a DBClusterSnapshotArn",
			)
			assert.Equal(
				t,
				clusterSnapDescArn,
				clusterSnapCreateArn,
				"CreateDBClusterSnapshot's DBClusterSnapshotArn must match DescribeDBClusterSnapshots's",
			)
		})
	}
}

// TestIntegration_RDS_ParameterGroupArns covers DBParameterGroup and
// DBClusterParameterGroup, which shared the same missing-ARN defect.
func TestIntegration_RDS_ParameterGroupArns(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name string
	}{
		{name: "arn set on parameter groups and cluster parameter groups"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createRDSClient(t)

			pgName := "arn-pg-" + uuid.NewString()[:8]
			pgOut, err := client.CreateDBParameterGroup(ctx, &rdssdk.CreateDBParameterGroupInput{
				DBParameterGroupName:   aws.String(pgName),
				DBParameterGroupFamily: aws.String("postgres15"),
				Description:            aws.String("arn test"),
			})
			require.NoError(t, err, "CreateDBParameterGroup should succeed")
			require.NotNil(t, pgOut.DBParameterGroup)
			pgCreateArn := aws.ToString(pgOut.DBParameterGroup.DBParameterGroupArn)
			assert.NotEmpty(
				t,
				pgCreateArn,
				"CreateDBParameterGroup must return a DBParameterGroupArn",
			)

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteDBParameterGroup(
					cleanupCtx,
					&rdssdk.DeleteDBParameterGroupInput{
						DBParameterGroupName: aws.String(pgName),
					},
				)
			})

			pgDescOut, err := client.DescribeDBParameterGroups(
				ctx,
				&rdssdk.DescribeDBParameterGroupsInput{
					DBParameterGroupName: aws.String(pgName),
				},
			)
			require.NoError(t, err, "DescribeDBParameterGroups should succeed")
			require.Len(t, pgDescOut.DBParameterGroups, 1)
			pgDescArn := aws.ToString(pgDescOut.DBParameterGroups[0].DBParameterGroupArn)
			assert.NotEmpty(
				t,
				pgDescArn,
				"DescribeDBParameterGroups must return a DBParameterGroupArn",
			)
			assert.Equal(
				t,
				pgDescArn,
				pgCreateArn,
				"CreateDBParameterGroup's DBParameterGroupArn must match DescribeDBParameterGroups's",
			)

			cpgName := "arn-cpg-" + uuid.NewString()[:8]
			cpgOut, err := client.CreateDBClusterParameterGroup(
				ctx,
				&rdssdk.CreateDBClusterParameterGroupInput{
					DBClusterParameterGroupName: aws.String(cpgName),
					DBParameterGroupFamily:      aws.String("aurora-postgresql15"),
					Description:                 aws.String("arn test"),
				},
			)
			require.NoError(t, err, "CreateDBClusterParameterGroup should succeed")
			require.NotNil(t, cpgOut.DBClusterParameterGroup)
			cpgCreateArn := aws.ToString(cpgOut.DBClusterParameterGroup.DBClusterParameterGroupArn)
			assert.NotEmpty(
				t,
				cpgCreateArn,
				"CreateDBClusterParameterGroup must return a DBClusterParameterGroupArn",
			)

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteDBClusterParameterGroup(
					cleanupCtx,
					&rdssdk.DeleteDBClusterParameterGroupInput{
						DBClusterParameterGroupName: aws.String(cpgName),
					},
				)
			})

			cpgDescOut, err := client.DescribeDBClusterParameterGroups(
				ctx,
				&rdssdk.DescribeDBClusterParameterGroupsInput{
					DBClusterParameterGroupName: aws.String(cpgName),
				},
			)
			require.NoError(t, err, "DescribeDBClusterParameterGroups should succeed")
			require.Len(t, cpgDescOut.DBClusterParameterGroups, 1)
			cpgDescArn := aws.ToString(
				cpgDescOut.DBClusterParameterGroups[0].DBClusterParameterGroupArn,
			)
			assert.NotEmpty(
				t,
				cpgDescArn,
				"DescribeDBClusterParameterGroups must return a DBClusterParameterGroupArn",
			)
			assert.Equal(
				t,
				cpgDescArn,
				cpgCreateArn,
				"CreateDBClusterParameterGroup's DBClusterParameterGroupArn must match DescribeDBClusterParameterGroups's",
			)

			assert.NotEqual(
				t,
				pgCreateArn,
				cpgCreateArn,
				"a DB parameter group and a cluster parameter group must not share an ARN resource type",
			)
		})
	}
}
