package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	rdstypes "github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSDK_RDS_Refinement3_DBProxy exercises the full DB proxy lifecycle:
// CreateDBProxy, DescribeDBProxies, ModifyDBProxy, RegisterDBProxyTargets,
// DescribeDBProxyTargets, DescribeDBProxyTargetGroups, ModifyDBProxyTargetGroup,
// CreateDBProxyEndpoint, DescribeDBProxyEndpoints, ModifyDBProxyEndpoint,
// DeregisterDBProxyTargets, DeleteDBProxyEndpoint, DeleteDBProxy.
func TestSDK_RDS_Refinement3_DBProxy(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createRDSClient(t)

	proxyName := "sdk-proxy-" + uuid.NewString()[:8]
	endpointName := "sdk-ep-" + uuid.NewString()[:8]

	// CreateDBProxy
	createOut, err := client.CreateDBProxy(ctx, &rdssdk.CreateDBProxyInput{
		DBProxyName:  aws.String(proxyName),
		EngineFamily: rdstypes.EngineFamilyMysql,
		RoleArn:      aws.String("arn:aws:iam::000000000000:role/proxy-role"),
		VpcSubnetIds: []string{"subnet-1", "subnet-2"},
		Auth: []rdstypes.UserAuthConfig{
			{
				AuthScheme: rdstypes.AuthSchemeSecrets,
				SecretArn:  aws.String("arn:aws:secretsmanager:us-east-1:000000000000:secret:test"),
			},
		},
	})
	require.NoError(t, err, "CreateDBProxy should succeed")
	require.NotNil(t, createOut.DBProxy)
	assert.Equal(t, proxyName, aws.ToString(createOut.DBProxy.DBProxyName))
	assert.Equal(t, rdstypes.DBProxyStatusAvailable, createOut.DBProxy.Status)

	// DescribeDBProxies
	descOut, err := client.DescribeDBProxies(ctx, &rdssdk.DescribeDBProxiesInput{
		DBProxyName: aws.String(proxyName),
	})
	require.NoError(t, err, "DescribeDBProxies should succeed")
	require.Len(t, descOut.DBProxies, 1)
	assert.Equal(t, proxyName, aws.ToString(descOut.DBProxies[0].DBProxyName))

	// ModifyDBProxy
	modOut, err := client.ModifyDBProxy(ctx, &rdssdk.ModifyDBProxyInput{
		DBProxyName: aws.String(proxyName),
		RequireTLS:  aws.Bool(true),
	})
	require.NoError(t, err, "ModifyDBProxy should succeed")
	require.NotNil(t, modOut.DBProxy)
	assert.True(t, aws.ToBool(modOut.DBProxy.RequireTLS))

	// Create a DB instance to register as a target
	instanceID := "sdk-prxtgt-" + uuid.NewString()[:8]
	_, err = client.CreateDBInstance(ctx, &rdssdk.CreateDBInstanceInput{
		DBInstanceIdentifier: aws.String(instanceID),
		DBInstanceClass:      aws.String("db.t3.micro"),
		Engine:               aws.String("mysql"),
		MasterUsername:       aws.String("admin"),
		MasterUserPassword:   aws.String("password123"),
	})
	require.NoError(t, err, "CreateDBInstance for proxy target should succeed")

	// RegisterDBProxyTargets
	regOut, err := client.RegisterDBProxyTargets(ctx, &rdssdk.RegisterDBProxyTargetsInput{
		DBProxyName:           aws.String(proxyName),
		TargetGroupName:       aws.String("default"),
		DBInstanceIdentifiers: []string{instanceID},
	})
	require.NoError(t, err, "RegisterDBProxyTargets should succeed")
	assert.Len(t, regOut.DBProxyTargets, 1)
	assert.Equal(t, instanceID, aws.ToString(regOut.DBProxyTargets[0].RdsResourceId))

	// DescribeDBProxyTargets
	targetsOut, err := client.DescribeDBProxyTargets(ctx, &rdssdk.DescribeDBProxyTargetsInput{
		DBProxyName:     aws.String(proxyName),
		TargetGroupName: aws.String("default"),
	})
	require.NoError(t, err, "DescribeDBProxyTargets should succeed")
	assert.Len(t, targetsOut.Targets, 1)

	// DescribeDBProxyTargetGroups
	tgOut, err := client.DescribeDBProxyTargetGroups(ctx, &rdssdk.DescribeDBProxyTargetGroupsInput{
		DBProxyName: aws.String(proxyName),
	})
	require.NoError(t, err, "DescribeDBProxyTargetGroups should succeed")
	require.Len(t, tgOut.TargetGroups, 1)
	assert.Equal(t, "default", aws.ToString(tgOut.TargetGroups[0].TargetGroupName))
	assert.True(t, aws.ToBool(tgOut.TargetGroups[0].IsDefault))

	// ModifyDBProxyTargetGroup
	modTGOut, err := client.ModifyDBProxyTargetGroup(ctx, &rdssdk.ModifyDBProxyTargetGroupInput{
		DBProxyName:     aws.String(proxyName),
		TargetGroupName: aws.String("default"),
		ConnectionPoolConfig: &rdstypes.ConnectionPoolConfiguration{
			MaxConnectionsPercent: aws.Int32(80),
		},
	})
	require.NoError(t, err, "ModifyDBProxyTargetGroup should succeed")
	require.NotNil(t, modTGOut.DBProxyTargetGroup)
	assert.Equal(t, int32(80), aws.ToInt32(modTGOut.DBProxyTargetGroup.ConnectionPoolConfig.MaxConnectionsPercent))

	// CreateDBProxyEndpoint
	epOut, err := client.CreateDBProxyEndpoint(ctx, &rdssdk.CreateDBProxyEndpointInput{
		DBProxyName:         aws.String(proxyName),
		DBProxyEndpointName: aws.String(endpointName),
		TargetRole:          rdstypes.DBProxyEndpointTargetRoleReadOnly,
		VpcSubnetIds:        []string{"subnet-1"},
	})
	require.NoError(t, err, "CreateDBProxyEndpoint should succeed")
	require.NotNil(t, epOut.DBProxyEndpoint)
	assert.Equal(t, endpointName, aws.ToString(epOut.DBProxyEndpoint.DBProxyEndpointName))
	assert.Equal(t, rdstypes.DBProxyEndpointTargetRoleReadOnly, epOut.DBProxyEndpoint.TargetRole)

	// DescribeDBProxyEndpoints
	descEpOut, err := client.DescribeDBProxyEndpoints(ctx, &rdssdk.DescribeDBProxyEndpointsInput{
		DBProxyName:         aws.String(proxyName),
		DBProxyEndpointName: aws.String(endpointName),
	})
	require.NoError(t, err, "DescribeDBProxyEndpoints should succeed")
	require.Len(t, descEpOut.DBProxyEndpoints, 1)
	assert.Equal(t, endpointName, aws.ToString(descEpOut.DBProxyEndpoints[0].DBProxyEndpointName))

	// ModifyDBProxyEndpoint
	modEpOut, err := client.ModifyDBProxyEndpoint(ctx, &rdssdk.ModifyDBProxyEndpointInput{
		DBProxyEndpointName: aws.String(endpointName),
		VpcSecurityGroupIds: []string{"sg-abc123"},
	})
	require.NoError(t, err, "ModifyDBProxyEndpoint should succeed")
	require.NotNil(t, modEpOut.DBProxyEndpoint)

	// DeregisterDBProxyTargets
	_, err = client.DeregisterDBProxyTargets(ctx, &rdssdk.DeregisterDBProxyTargetsInput{
		DBProxyName:           aws.String(proxyName),
		TargetGroupName:       aws.String("default"),
		DBInstanceIdentifiers: []string{instanceID},
	})
	require.NoError(t, err, "DeregisterDBProxyTargets should succeed")

	// Verify targets are deregistered
	emptyTargets, err := client.DescribeDBProxyTargets(ctx, &rdssdk.DescribeDBProxyTargetsInput{
		DBProxyName:     aws.String(proxyName),
		TargetGroupName: aws.String("default"),
	})
	require.NoError(t, err)
	assert.Empty(t, emptyTargets.Targets, "targets should be empty after deregistration")

	// DeleteDBProxyEndpoint
	_, err = client.DeleteDBProxyEndpoint(ctx, &rdssdk.DeleteDBProxyEndpointInput{
		DBProxyEndpointName: aws.String(endpointName),
	})
	require.NoError(t, err, "DeleteDBProxyEndpoint should succeed")

	// DeleteDBProxy
	delOut, err := client.DeleteDBProxy(ctx, &rdssdk.DeleteDBProxyInput{
		DBProxyName: aws.String(proxyName),
	})
	require.NoError(t, err, "DeleteDBProxy should succeed")
	require.NotNil(t, delOut.DBProxy)
	assert.Equal(t, proxyName, aws.ToString(delOut.DBProxy.DBProxyName))

	// Cleanup DB instance
	_, err = client.DeleteDBInstance(ctx, &rdssdk.DeleteDBInstanceInput{
		DBInstanceIdentifier: aws.String(instanceID),
		SkipFinalSnapshot:    aws.Bool(true),
	})
	require.NoError(t, err)
}

// TestSDK_RDS_Refinement3_ActivityStream exercises StartActivityStream,
// DescribeDBClusters (to verify stream fields), ModifyActivityStream, and
// StopActivityStream.
func TestSDK_RDS_Refinement3_ActivityStream(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	client := createRDSClient(t)

	clusterID := "sdk-as-" + uuid.NewString()[:8]

	// Create a DB cluster to use for activity streaming
	_, err := client.CreateDBCluster(ctx, &rdssdk.CreateDBClusterInput{
		DBClusterIdentifier: aws.String(clusterID),
		Engine:              aws.String("aurora-postgresql"),
		MasterUsername:      aws.String("admin"),
		MasterUserPassword:  aws.String("password123"),
	})
	require.NoError(t, err, "CreateDBCluster should succeed")

	clusterARN := "arn:aws:rds:us-east-1:000000000000:cluster:" + clusterID

	// StartActivityStream
	startOut, err := client.StartActivityStream(ctx, &rdssdk.StartActivityStreamInput{
		ResourceArn: aws.String(clusterARN),
		KmsKeyId:    aws.String("arn:aws:kms:us-east-1:000000000000:key/test-key"),
		Mode:        rdstypes.ActivityStreamModeAsync,
	})
	require.NoError(t, err, "StartActivityStream should succeed")
	assert.Equal(t, rdstypes.ActivityStreamStatusStarted, startOut.Status)
	assert.NotEmpty(t, aws.ToString(startOut.KinesisStreamName))

	// Verify fields via DescribeDBClusters
	descOut, err := client.DescribeDBClusters(ctx, &rdssdk.DescribeDBClustersInput{
		DBClusterIdentifier: aws.String(clusterID),
	})
	require.NoError(t, err)
	require.Len(t, descOut.DBClusters, 1)
	assert.Equal(t, rdstypes.ActivityStreamStatusStarted, descOut.DBClusters[0].ActivityStreamStatus)

	// ModifyActivityStream
	modOut, err := client.ModifyActivityStream(ctx, &rdssdk.ModifyActivityStreamInput{
		ResourceArn:      aws.String(clusterARN),
		AuditPolicyState: rdstypes.AuditPolicyStateLockedPolicy,
	})
	require.NoError(t, err, "ModifyActivityStream should succeed")
	assert.Equal(t, rdstypes.ActivityStreamStatusStarted, modOut.Status)

	// StopActivityStream
	stopOut, err := client.StopActivityStream(ctx, &rdssdk.StopActivityStreamInput{
		ResourceArn: aws.String(clusterARN),
	})
	require.NoError(t, err, "StopActivityStream should succeed")
	assert.Equal(t, rdstypes.ActivityStreamStatusStopped, stopOut.Status)

	// Cleanup
	_, err = client.DeleteDBCluster(ctx, &rdssdk.DeleteDBClusterInput{
		DBClusterIdentifier: aws.String(clusterID),
		SkipFinalSnapshot:   aws.Bool(true),
	})
	require.NoError(t, err)
}
