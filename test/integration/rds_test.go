package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSDK_RDS_FullLifecycle creates a postgres DB instance via the AWS RDS SDK,
// verifies it can be described and modified, creates a snapshot, then tears it all
// down through the SDK — exercising the full stub lifecycle.
func TestSDK_RDS_FullLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := context.Background()
	client := createRDSClient(t)

	id := "sdk-pg-" + uuid.NewString()[:8]

	// CreateDBInstance
	createOut, err := client.CreateDBInstance(ctx, &rdssdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String(id),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("postgres"),
		MasterUsername:       aws.String("admin"),
		MasterUserPassword:   aws.String("password123"),
		DBName:               aws.String("testdb"),
		AllocatedStorage:     aws.Int32(20),
	})
	require.NoError(t, err, "CreateDBInstance should succeed")
	require.NotNil(t, createOut.DBInstance)
	assert.Equal(t, id, aws.ToString(createOut.DBInstance.DBInstanceIdentifier))
	assert.Equal(t, "postgres", aws.ToString(createOut.DBInstance.Engine))
	assert.Equal(t, "available", aws.ToString(createOut.DBInstance.DBInstanceStatus))
	assert.NotEmpty(t, createOut.DBInstance.Endpoint.Address)
	assert.Equal(t, int32(5432), aws.ToInt32(createOut.DBInstance.Endpoint.Port))

	// DescribeDBInstances by ID
	descOut, err := client.DescribeDBInstances(ctx, &rdssdk.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(id),
	})
	require.NoError(t, err, "DescribeDBInstances should succeed")
	require.Len(t, descOut.DBInstances, 1)
	assert.Equal(t, id, aws.ToString(descOut.DBInstances[0].DBInstanceIdentifier))

	// ModifyDBInstance
	modOut, err := client.ModifyDBInstance(ctx, &rdssdk.ModifyDBInstanceInput{
		DBInstanceIdentifier: aws.String(id),
		DBInstanceClass:      aws.String("db.r5.large"),
		AllocatedStorage:     aws.Int32(50),
	})
	require.NoError(t, err, "ModifyDBInstance should succeed")
	assert.Equal(t, "db.r5.large", aws.ToString(modOut.DBInstance.DBInstanceClass))

	// CreateDBSnapshot
	snapID := "sdk-snap-" + uuid.NewString()[:8]
	snapOut, err := client.CreateDBSnapshot(ctx, &rdssdk.CreateDBSnapshotInput{
		DBSnapshotIdentifier: aws.String(snapID),
		DBInstanceIdentifier: aws.String(id),
	})
	require.NoError(t, err, "CreateDBSnapshot should succeed")
	assert.Equal(t, snapID, aws.ToString(snapOut.DBSnapshot.DBSnapshotIdentifier))
	assert.Equal(t, "available", aws.ToString(snapOut.DBSnapshot.Status))

	// DescribeDBSnapshots
	snapDescOut, err := client.DescribeDBSnapshots(ctx, &rdssdk.DescribeDBSnapshotsInput{
		DBSnapshotIdentifier: aws.String(snapID),
	})
	require.NoError(t, err, "DescribeDBSnapshots should succeed")
	require.Len(t, snapDescOut.DBSnapshots, 1)
	assert.Equal(t, snapID, aws.ToString(snapDescOut.DBSnapshots[0].DBSnapshotIdentifier))

	// DeleteDBSnapshot
	_, err = client.DeleteDBSnapshot(ctx, &rdssdk.DeleteDBSnapshotInput{
		DBSnapshotIdentifier: aws.String(snapID),
	})
	require.NoError(t, err, "DeleteDBSnapshot should succeed")

	// DeleteDBInstance
	delOut, err := client.DeleteDBInstance(ctx, &rdssdk.DeleteDBInstanceInput{
		DBInstanceIdentifier: aws.String(id),
		SkipFinalSnapshot:    aws.Bool(true),
	})
	require.NoError(t, err, "DeleteDBInstance should succeed")
	assert.Equal(t, id, aws.ToString(delOut.DBInstance.DBInstanceIdentifier))

	// Confirm it is gone
	_, err = client.DescribeDBInstances(ctx, &rdssdk.DescribeDBInstancesInput{
		DBInstanceIdentifier: aws.String(id),
	})
	require.Error(t, err, "DescribeDBInstances should return error for deleted instance")
}

// TestSDK_RDS_MySQL creates a MySQL DB instance via the AWS RDS SDK and verifies
// engine-specific port allocation.
func TestSDK_RDS_MySQL(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := context.Background()
	client := createRDSClient(t)

	id := "sdk-mysql-" + uuid.NewString()[:8]

	out, err := client.CreateDBInstance(ctx, &rdssdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String(id),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("mysql"),
		MasterUsername:       aws.String("root"),
		MasterUserPassword:   aws.String("password123"),
	})
	require.NoError(t, err, "CreateDBInstance (mysql) should succeed")
	require.NotNil(t, out.DBInstance)
	assert.Equal(t, "mysql", aws.ToString(out.DBInstance.Engine))
	assert.Equal(t, int32(3306), aws.ToInt32(out.DBInstance.Endpoint.Port), "MySQL should use port 3306")

	// Cleanup
	_, err = client.DeleteDBInstance(ctx, &rdssdk.DeleteDBInstanceInput{
		DBInstanceIdentifier: aws.String(id),
		SkipFinalSnapshot:    aws.Bool(true),
	})
	require.NoError(t, err)
}

// TestSDK_RDS_DBCluster_Aurora tests Aurora DB Cluster CRUD lifecycle including
// start/stop, cluster snapshots, and point-in-time restore operations.
func TestSDK_RDS_DBCluster_Aurora(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := context.Background()
	client := createRDSClient(t)

	clusterID := "sdk-aurora-" + uuid.NewString()[:8]

	// CreateDBCluster
	createOut, err := client.CreateDBCluster(ctx, &rdssdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String(clusterID),
		Engine:              aws.String("aurora-postgresql"),
		MasterUsername:      aws.String("admin"),
		MasterUserPassword:  aws.String("password123"),
		DatabaseName:        aws.String("testdb"),
	})
	require.NoError(t, err, "CreateDBCluster should succeed")
	require.NotNil(t, createOut.DBCluster)
	assert.Equal(t, clusterID, aws.ToString(createOut.DBCluster.DBClusterIdentifier))
	assert.Equal(t, "aurora-postgresql", aws.ToString(createOut.DBCluster.Engine))
	assert.Equal(t, "available", aws.ToString(createOut.DBCluster.Status))

	// DescribeDBClusters
	descOut, err := client.DescribeDBClusters(ctx, &rdssdk.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String(clusterID),
	})
	require.NoError(t, err, "DescribeDBClusters should succeed")
	require.Len(t, descOut.DBClusters, 1)
	assert.Equal(t, clusterID, aws.ToString(descOut.DBClusters[0].DBClusterIdentifier))

	// StopDBCluster
	stopOut, err := client.StopDBCluster(ctx, &rdssdk.StopDBClusterInput{
		DBClusterIdentifier: aws.String(clusterID),
	})
	require.NoError(t, err, "StopDBCluster should succeed")
	assert.Equal(t, "stopped", aws.ToString(stopOut.DBCluster.Status))

	// StartDBCluster
	startOut, err := client.StartDBCluster(ctx, &rdssdk.StartDBClusterInput{
		DBClusterIdentifier: aws.String(clusterID),
	})
	require.NoError(t, err, "StartDBCluster should succeed")
	assert.Equal(t, "available", aws.ToString(startOut.DBCluster.Status))

	// CreateDBClusterSnapshot
	snapID := "sdk-csnap-" + uuid.NewString()[:8]
	snapOut, err := client.CreateDBClusterSnapshot(ctx, &rdssdk.CreateDBClusterSnapshotInput{
		DBClusterSnapshotIdentifier: aws.String(snapID),
		DBClusterIdentifier:         aws.String(clusterID),
	})
	require.NoError(t, err, "CreateDBClusterSnapshot should succeed")
	assert.Equal(t, snapID, aws.ToString(snapOut.DBClusterSnapshot.DBClusterSnapshotIdentifier))
	assert.Equal(t, "available", aws.ToString(snapOut.DBClusterSnapshot.Status))

	// DescribeDBClusterSnapshots
	snapDescOut, err := client.DescribeDBClusterSnapshots(ctx, &rdssdk.DescribeDBClusterSnapshotsInput{
		DBClusterSnapshotIdentifier: aws.String(snapID),
	})
	require.NoError(t, err, "DescribeDBClusterSnapshots should succeed")
	require.Len(t, snapDescOut.DBClusterSnapshots, 1)
	assert.Equal(t, snapID, aws.ToString(snapDescOut.DBClusterSnapshots[0].DBClusterSnapshotIdentifier))

	// CopyDBClusterSnapshot
	snapCopyID := "sdk-csnap-copy-" + uuid.NewString()[:8]
	copyOut, err := client.CopyDBClusterSnapshot(ctx, &rdssdk.CopyDBClusterSnapshotInput{
		SourceDBClusterSnapshotIdentifier: aws.String(snapID),
		TargetDBClusterSnapshotIdentifier: aws.String(snapCopyID),
	})
	require.NoError(t, err, "CopyDBClusterSnapshot should succeed")
	assert.Equal(t, snapCopyID, aws.ToString(copyOut.DBClusterSnapshot.DBClusterSnapshotIdentifier))

	// RestoreDBClusterFromSnapshot
	restoredID := "sdk-restored-" + uuid.NewString()[:8]
	restoreOut, err := client.RestoreDBClusterFromSnapshot(ctx, &rdssdk.RestoreDBClusterFromSnapshotInput{
		DBClusterIdentifier: aws.String(restoredID),
		SnapshotIdentifier:  aws.String(snapID),
		Engine:              aws.String("aurora-postgresql"),
	})
	require.NoError(t, err, "RestoreDBClusterFromSnapshot should succeed")
	assert.Equal(t, restoredID, aws.ToString(restoreOut.DBCluster.DBClusterIdentifier))
	assert.Equal(t, "available", aws.ToString(restoreOut.DBCluster.Status))

	// RestoreDBClusterToPointInTime
	pitRestoredID := "sdk-pitr-" + uuid.NewString()[:8]
	pitrOut, err := client.RestoreDBClusterToPointInTime(ctx, &rdssdk.RestoreDBClusterToPointInTimeInput{
		DBClusterIdentifier:       aws.String(pitRestoredID),
		SourceDBClusterIdentifier: aws.String(clusterID),
	})
	require.NoError(t, err, "RestoreDBClusterToPointInTime should succeed")
	assert.Equal(t, pitRestoredID, aws.ToString(pitrOut.DBCluster.DBClusterIdentifier))

	// DeleteDBClusterSnapshot (copy)
	_, err = client.DeleteDBClusterSnapshot(ctx, &rdssdk.DeleteDBClusterSnapshotInput{
		DBClusterSnapshotIdentifier: aws.String(snapCopyID),
	})
	require.NoError(t, err, "DeleteDBClusterSnapshot (copy) should succeed")

	// DeleteDBClusterSnapshot (original)
	_, err = client.DeleteDBClusterSnapshot(ctx, &rdssdk.DeleteDBClusterSnapshotInput{
		DBClusterSnapshotIdentifier: aws.String(snapID),
	})
	require.NoError(t, err, "DeleteDBClusterSnapshot should succeed")

	// Cleanup restored clusters
	_, err = client.DeleteDBCluster(ctx, &rdssdk.DeleteDBClusterInput{
		DBClusterIdentifier: aws.String(restoredID),
		SkipFinalSnapshot:   aws.Bool(true),
	})
	require.NoError(t, err)
	_, err = client.DeleteDBCluster(ctx, &rdssdk.DeleteDBClusterInput{
		DBClusterIdentifier: aws.String(pitRestoredID),
		SkipFinalSnapshot:   aws.Bool(true),
	})
	require.NoError(t, err)
	_, err = client.DeleteDBCluster(ctx, &rdssdk.DeleteDBClusterInput{
		DBClusterIdentifier: aws.String(clusterID),
		SkipFinalSnapshot:   aws.Bool(true),
	})
	require.NoError(t, err)
}

// TestSDK_RDS_DBClusterEndpoints tests creating and managing custom DB cluster endpoints.
func TestSDK_RDS_DBClusterEndpoints(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := context.Background()
	client := createRDSClient(t)

	clusterID := "sdk-ep-cluster-" + uuid.NewString()[:8]

	// Create cluster first
	_, err := client.CreateDBCluster(ctx, &rdssdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String(clusterID),
		Engine:              aws.String("aurora-postgresql"),
		MasterUsername:      aws.String("admin"),
		MasterUserPassword:  aws.String("password123"),
	})
	require.NoError(t, err, "CreateDBCluster should succeed")

	// CreateDBClusterEndpoint
	endpointID := "sdk-ep-" + uuid.NewString()[:8]
	createEpOut, err := client.CreateDBClusterEndpoint(ctx, &rdssdk.CreateDBClusterEndpointInput{
		DBClusterEndpointIdentifier: aws.String(endpointID),
		DBClusterIdentifier:         aws.String(clusterID),
		EndpointType:                aws.String("READER"),
	})
	require.NoError(t, err, "CreateDBClusterEndpoint should succeed")
	assert.Equal(t, endpointID, aws.ToString(createEpOut.DBClusterEndpointIdentifier))
	assert.Equal(t, "READER", aws.ToString(createEpOut.EndpointType))

	// DescribeDBClusterEndpoints
	descEpOut, err := client.DescribeDBClusterEndpoints(ctx, &rdssdk.DescribeDBClusterEndpointsInput{
		DBClusterIdentifier: aws.String(clusterID),
	})
	require.NoError(t, err, "DescribeDBClusterEndpoints should succeed")
	require.NotEmpty(t, descEpOut.DBClusterEndpoints)
	assert.Equal(t, endpointID, aws.ToString(descEpOut.DBClusterEndpoints[0].DBClusterEndpointIdentifier))

	// DeleteDBClusterEndpoint
	_, err = client.DeleteDBClusterEndpoint(ctx, &rdssdk.DeleteDBClusterEndpointInput{
		DBClusterEndpointIdentifier: aws.String(endpointID),
	})
	require.NoError(t, err, "DeleteDBClusterEndpoint should succeed")

	// Cleanup
	_, err = client.DeleteDBCluster(ctx, &rdssdk.DeleteDBClusterInput{
		DBClusterIdentifier: aws.String(clusterID),
		SkipFinalSnapshot:   aws.Bool(true),
	})
	require.NoError(t, err)
}

// TestSDK_RDS_ExportTasks tests creating and describing export tasks.
func TestSDK_RDS_ExportTasks(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := context.Background()
	client := createRDSClient(t)

	taskID := "sdk-export-" + uuid.NewString()[:8]

	// StartExportTask
	startOut, err := client.StartExportTask(ctx, &rdssdk.StartExportTaskInput{
		ExportTaskIdentifier: aws.String(taskID),
		SourceArn:            aws.String("arn:aws:rds:us-east-1:000000000000:snapshot:my-snap"),
		S3BucketName:         aws.String("my-export-bucket"),
		IamRoleArn:           aws.String("arn:aws:iam::000000000000:role/ExportRole"),
		KmsKeyId:             aws.String("arn:aws:kms:us-east-1:000000000000:key/test-key"),
	})
	require.NoError(t, err, "StartExportTask should succeed")
	assert.Equal(t, taskID, aws.ToString(startOut.ExportTaskIdentifier))
	assert.Equal(t, "complete", aws.ToString(startOut.Status))

	// DescribeExportTasks
	descOut, err := client.DescribeExportTasks(ctx, &rdssdk.DescribeExportTasksInput{
		ExportTaskIdentifier: aws.String(taskID),
	})
	require.NoError(t, err, "DescribeExportTasks should succeed")
	require.Len(t, descOut.ExportTasks, 1)
	assert.Equal(t, taskID, aws.ToString(descOut.ExportTasks[0].ExportTaskIdentifier))
}

// TestSDK_RDS_ValidDBInstanceModifications tests describing valid DB instance modifications.
func TestSDK_RDS_ValidDBInstanceModifications(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := context.Background()
	client := createRDSClient(t)

	id := "sdk-valid-mod-" + uuid.NewString()[:8]

	// Create instance first
	_, err := client.CreateDBInstance(ctx, &rdssdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String(id),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("postgres"),
		MasterUsername:       aws.String("admin"),
		MasterUserPassword:   aws.String("password123"),
		AllocatedStorage:     aws.Int32(20),
	})
	require.NoError(t, err, "CreateDBInstance should succeed")

	// DescribeValidDBInstanceModifications
	descOut, err := client.DescribeValidDBInstanceModifications(ctx,
		&rdssdk.DescribeValidDBInstanceModificationsInput{
			DBInstanceIdentifier: aws.String(id),
		})
	require.NoError(t, err, "DescribeValidDBInstanceModifications should succeed")
	assert.NotNil(t, descOut.ValidDBInstanceModificationsMessage)

	// Cleanup
	_, err = client.DeleteDBInstance(ctx, &rdssdk.DeleteDBInstanceInput{
		DBInstanceIdentifier: aws.String(id),
		SkipFinalSnapshot:    aws.Bool(true),
	})
	require.NoError(t, err)
}

// TestSDK_RDS_SubnetGroup creates a DB subnet group via the AWS RDS SDK.
func TestSDK_RDS_SubnetGroup(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := context.Background()
	client := createRDSClient(t)

	name := "sdk-sg-" + uuid.NewString()[:8]

	out, err := client.CreateDBSubnetGroup(ctx, &rdssdk.CreateDBSubnetGroupInput{
		DBSubnetGroupName:        aws.String(name),
		DBSubnetGroupDescription: aws.String("SDK integration test subnet group"),
		SubnetIds:                []string{"subnet-1", "subnet-2"},
	})
	require.NoError(t, err, "CreateDBSubnetGroup should succeed")
	assert.Equal(t, name, aws.ToString(out.DBSubnetGroup.DBSubnetGroupName))

	// DescribeDBSubnetGroups
	descOut, err := client.DescribeDBSubnetGroups(ctx, &rdssdk.DescribeDBSubnetGroupsInput{
		DBSubnetGroupName: aws.String(name),
	})
	require.NoError(t, err, "DescribeDBSubnetGroups should succeed")
	require.Len(t, descOut.DBSubnetGroups, 1)
	assert.Equal(t, name, aws.ToString(descOut.DBSubnetGroups[0].DBSubnetGroupName))

	// DeleteDBSubnetGroup
	_, err = client.DeleteDBSubnetGroup(ctx, &rdssdk.DeleteDBSubnetGroupInput{
		DBSubnetGroupName: aws.String(name),
	})
	require.NoError(t, err, "DeleteDBSubnetGroup should succeed")
}
