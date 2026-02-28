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
