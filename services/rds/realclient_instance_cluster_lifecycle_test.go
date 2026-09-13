package rds_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/rds"
)

// waitForInstanceStatus polls the backend directly (no HTTP round trip)
// until the named instance reaches wantStatus, matching the repo convention
// of require.Eventually over unbubbled sleeps.
func waitForInstanceStatus(t *testing.T, backend *rds.InMemoryBackend, id, wantStatus string) {
	t.Helper()

	require.Eventually(t, func() bool {
		insts, err := backend.DescribeDBInstances(id)

		return err == nil && len(insts) == 1 && insts[0].DBInstanceStatus == wantStatus
	}, time.Second, 5*time.Millisecond)
}

// waitForClusterStatus is waitForInstanceStatus's DB cluster counterpart.
func waitForClusterStatus(t *testing.T, backend *rds.InMemoryBackend, id, wantStatus string) {
	t.Helper()

	require.Eventually(t, func() bool {
		clusters, err := backend.DescribeDBClusters(id)

		return err == nil && len(clusters) == 1 && clusters[0].Status == wantStatus
	}, time.Second, 5*time.Millisecond)
}

// TestRealClient_InstanceClusterLifecycle covers rds's highest-priority typed-client-
// uncovered op families (gopherstack-n3zi): instance/cluster
// lifecycle, read replicas, security groups, parameter/option/subnet
// groups, event subscriptions, snapshots, global clusters, blue/green
// deployments, DB shard groups, tenant databases, integrations, reserved
// instances, pending maintenance/reference data, and automated backups.
// Each subtest creates real state -- through the typed aws-sdk-go-v2 rds
// client, or directly via the backend for setup-only steps whose own op is
// already typed-covered -- and asserts decoded response values.
func TestRealClient_InstanceClusterLifecycle(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testInstanceLifecycleRealClient, "instance_lifecycle"},
		{testReadReplicaRealClient, "read_replica"},
		{testClusterLifecycleRealClient, "cluster_lifecycle"},
		{testSecurityGroupsRealClient, "security_groups"},
		{testParameterGroupsRealClient, "parameter_groups"},
		{testOptionGroupsRealClient, "option_groups"},
		{testSubnetGroupRealClient, "subnet_group"},
		{testEventSubscriptionsRealClient, "event_subscriptions"},
		{testSnapshotsRealClient, "snapshots"},
		{testGlobalClusterBlueGreenRealClient, "global_cluster_blue_green"},
		{testShardGroupTenantIntegrationRealClient, "shard_group_tenant_integration"},
		{testReservedInstancesReferenceDataRealClient, "reserved_instances_reference_data"},
		{testAutomatedBackupsRealClient, "automated_backups"},
		{testCustomEngineVersionRealClient, "custom_engine_version"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

func newRealClientBackendAndClient(t *testing.T) (*rds.InMemoryBackend, *rdssdk.Client) {
	t.Helper()

	backend := rds.NewInMemoryBackend("123456789012", config.DefaultRegion)
	client := newTestRDSClient(t, rds.NewHandler(backend))

	return backend, client
}

func testInstanceLifecycleRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBInstance(
		"slice8-inst", "mysql", "db.t3.micro", "mydb", "admin", "", 20, rds.DBInstanceOptions{},
	)
	require.NoError(t, err)
	waitForInstanceStatus(t, backend, "slice8-inst", "available")

	_, err = client.StopDBInstance(
		ctx,
		&rdssdk.StopDBInstanceInput{DBInstanceIdentifier: aws.String("slice8-inst")},
	)
	require.NoError(t, err)

	startOut, err := client.StartDBInstance(
		ctx, &rdssdk.StartDBInstanceInput{DBInstanceIdentifier: aws.String("slice8-inst")},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice8-inst", aws.ToString(startOut.DBInstance.DBInstanceIdentifier))

	rebootOut, err := client.RebootDBInstance(
		ctx, &rdssdk.RebootDBInstanceInput{DBInstanceIdentifier: aws.String("slice8-inst")},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice8-inst", aws.ToString(rebootOut.DBInstance.DBInstanceIdentifier))

	_, err = client.AddRoleToDBInstance(ctx, &rdssdk.AddRoleToDBInstanceInput{
		DBInstanceIdentifier: aws.String("slice8-inst"),
		RoleArn:              aws.String("arn:aws:iam::123456789012:role/slice8-role"),
		FeatureName:          aws.String("s3Import"),
	})
	require.NoError(t, err)

	_, err = client.RemoveRoleFromDBInstance(ctx, &rdssdk.RemoveRoleFromDBInstanceInput{
		DBInstanceIdentifier: aws.String("slice8-inst"),
		RoleArn:              aws.String("arn:aws:iam::123456789012:role/slice8-role"),
		FeatureName:          aws.String("s3Import"),
	})
	require.NoError(t, err)
}

func testReadReplicaRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBInstance(
		"slice8-primary", "mysql", "db.t3.micro", "mydb", "admin", "", 20, rds.DBInstanceOptions{},
	)
	require.NoError(t, err)

	_, err = backend.CreateDBInstanceReadReplica("slice8-replica", "slice8-primary", "", "", "")
	require.NoError(t, err)

	promoteOut, err := client.PromoteReadReplica(
		ctx, &rdssdk.PromoteReadReplicaInput{DBInstanceIdentifier: aws.String("slice8-replica")},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice8-replica", aws.ToString(promoteOut.DBInstance.DBInstanceIdentifier))

	_, err = backend.CreateDBInstanceReadReplica("slice8-replica2", "slice8-primary", "", "", "")
	require.NoError(t, err)

	switchoverOut, err := client.SwitchoverReadReplica(
		ctx,
		&rdssdk.SwitchoverReadReplicaInput{DBInstanceIdentifier: aws.String("slice8-replica2")},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice8-replica2", aws.ToString(switchoverOut.DBInstance.DBInstanceIdentifier))
}

func testClusterLifecycleRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBCluster(
		"slice8-cluster", "aurora-mysql", "admin", "mydb", "", 3306, nil, rds.DBClusterOptions{},
	)
	require.NoError(t, err)

	modifyOut, err := client.ModifyDBCluster(ctx, &rdssdk.ModifyDBClusterInput{
		DBClusterIdentifier:   aws.String("slice8-cluster"),
		BackupRetentionPeriod: aws.Int32(14),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(14), aws.ToInt32(modifyOut.DBCluster.BackupRetentionPeriod))

	rebootOut, err := client.RebootDBCluster(
		ctx, &rdssdk.RebootDBClusterInput{DBClusterIdentifier: aws.String("slice8-cluster")},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice8-cluster", aws.ToString(rebootOut.DBCluster.DBClusterIdentifier))
	waitForClusterStatus(t, backend, "slice8-cluster", "available")

	failoverOut, err := client.FailoverDBCluster(
		ctx, &rdssdk.FailoverDBClusterInput{DBClusterIdentifier: aws.String("slice8-cluster")},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice8-cluster", aws.ToString(failoverOut.DBCluster.DBClusterIdentifier))

	_, err = backend.CreateDBCluster(
		"slice8-cluster-replica", "aurora-mysql", "admin", "mydb", "", 3306, nil,
		rds.DBClusterOptions{ReplicationSourceIdentifier: "slice8-cluster"},
	)
	require.NoError(t, err)

	promoteOut, err := client.PromoteReadReplicaDBCluster(
		ctx,
		&rdssdk.PromoteReadReplicaDBClusterInput{
			DBClusterIdentifier: aws.String("slice8-cluster-replica"),
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		"slice8-cluster-replica",
		aws.ToString(promoteOut.DBCluster.DBClusterIdentifier),
	)
}

func testSecurityGroupsRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBSecurityGroup("slice8-secgrp", "slice8 security group")
	require.NoError(t, err)

	_, err = client.AuthorizeDBSecurityGroupIngress(
		ctx,
		&rdssdk.AuthorizeDBSecurityGroupIngressInput{
			DBSecurityGroupName: aws.String("slice8-secgrp"),
			CIDRIP:              aws.String("10.0.0.0/24"),
		},
	)
	require.NoError(t, err)

	descOut, err := client.DescribeDBSecurityGroups(
		ctx,
		&rdssdk.DescribeDBSecurityGroupsInput{DBSecurityGroupName: aws.String("slice8-secgrp")},
	)
	require.NoError(t, err)
	require.Len(t, descOut.DBSecurityGroups, 1)
	require.Len(t, descOut.DBSecurityGroups[0].IPRanges, 1)
	assert.Equal(t, "10.0.0.0/24", aws.ToString(descOut.DBSecurityGroups[0].IPRanges[0].CIDRIP))

	_, err = client.RevokeDBSecurityGroupIngress(ctx, &rdssdk.RevokeDBSecurityGroupIngressInput{
		DBSecurityGroupName: aws.String("slice8-secgrp"),
		CIDRIP:              aws.String("10.0.0.0/24"),
	})
	require.NoError(t, err)

	_, err = client.DeleteDBSecurityGroup(ctx, &rdssdk.DeleteDBSecurityGroupInput{
		DBSecurityGroupName: aws.String("slice8-secgrp"),
	})
	require.NoError(t, err)
}

func testParameterGroupsRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBParameterGroup("slice8-pg", "mysql8.0", "slice8 parameter group")
	require.NoError(t, err)

	copyOut, err := client.CopyDBParameterGroup(ctx, &rdssdk.CopyDBParameterGroupInput{
		SourceDBParameterGroupIdentifier:  aws.String("slice8-pg"),
		TargetDBParameterGroupIdentifier:  aws.String("slice8-pg-copy"),
		TargetDBParameterGroupDescription: aws.String("slice8 copy"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice8-pg-copy", aws.ToString(copyOut.DBParameterGroup.DBParameterGroupName))

	resetOut, err := client.ResetDBParameterGroup(ctx, &rdssdk.ResetDBParameterGroupInput{
		DBParameterGroupName: aws.String("slice8-pg"),
		ResetAllParameters:   aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice8-pg", aws.ToString(resetOut.DBParameterGroupName))

	_, err = backend.CreateDBClusterParameterGroup(
		"slice8-cpg",
		"aurora-mysql8.0",
		"slice8 cluster param group",
	)
	require.NoError(t, err)

	copyClusterOut, err := client.CopyDBClusterParameterGroup(
		ctx,
		&rdssdk.CopyDBClusterParameterGroupInput{
			SourceDBClusterParameterGroupIdentifier:  aws.String("slice8-cpg"),
			TargetDBClusterParameterGroupIdentifier:  aws.String("slice8-cpg-copy"),
			TargetDBClusterParameterGroupDescription: aws.String("slice8 cluster copy"),
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		"slice8-cpg-copy",
		aws.ToString(copyClusterOut.DBClusterParameterGroup.DBClusterParameterGroupName),
	)

	resetClusterOut, err := client.ResetDBClusterParameterGroup(
		ctx,
		&rdssdk.ResetDBClusterParameterGroupInput{
			DBClusterParameterGroupName: aws.String("slice8-cpg"),
			ResetAllParameters:          aws.Bool(true),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice8-cpg", aws.ToString(resetClusterOut.DBClusterParameterGroupName))

	defaultsOut, err := client.DescribeEngineDefaultClusterParameters(
		ctx,
		&rdssdk.DescribeEngineDefaultClusterParametersInput{
			DBParameterGroupFamily: aws.String("aurora-mysql8.0"),
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, defaultsOut.EngineDefaults)
}

func testOptionGroupsRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateOptionGroup("slice8-og", "mysql", "8.0", "slice8 option group")
	require.NoError(t, err)

	copyOut, err := client.CopyOptionGroup(ctx, &rdssdk.CopyOptionGroupInput{
		SourceOptionGroupIdentifier:  aws.String("slice8-og"),
		TargetOptionGroupIdentifier:  aws.String("slice8-og-copy"),
		TargetOptionGroupDescription: aws.String("slice8 copy"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice8-og-copy", aws.ToString(copyOut.OptionGroup.OptionGroupName))

	modifyOut, err := client.ModifyOptionGroup(ctx, &rdssdk.ModifyOptionGroupInput{
		OptionGroupName:  aws.String("slice8-og"),
		ApplyImmediately: aws.Bool(true),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice8-og", aws.ToString(modifyOut.OptionGroup.OptionGroupName))

	descOut, err := client.DescribeOptionGroups(
		ctx, &rdssdk.DescribeOptionGroupsInput{OptionGroupName: aws.String("slice8-og")},
	)
	require.NoError(t, err)
	require.Len(t, descOut.OptionGroupsList, 1)

	optionsOut, err := client.DescribeOptionGroupOptions(
		ctx, &rdssdk.DescribeOptionGroupOptionsInput{EngineName: aws.String("mysql")},
	)
	require.NoError(t, err)
	assert.NotNil(t, optionsOut)

	_, err = client.DeleteOptionGroup(ctx, &rdssdk.DeleteOptionGroupInput{
		OptionGroupName: aws.String("slice8-og-copy"),
	})
	require.NoError(t, err)
}

func testSubnetGroupRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBSubnetGroup(
		"slice8-subnetgroup", "slice8 subnet group", "vpc-slice8", []string{"subnet-aaaa1111"},
	)
	require.NoError(t, err)

	modifyOut, err := client.ModifyDBSubnetGroup(ctx, &rdssdk.ModifyDBSubnetGroupInput{
		DBSubnetGroupName:        aws.String("slice8-subnetgroup"),
		DBSubnetGroupDescription: aws.String("slice8 updated"),
		SubnetIds:                []string{"subnet-aaaa1111", "subnet-bbbb2222"},
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		"slice8 updated",
		aws.ToString(modifyOut.DBSubnetGroup.DBSubnetGroupDescription),
	)
	assert.Len(t, modifyOut.DBSubnetGroup.Subnets, 2)
}

func testEventSubscriptionsRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateEventSubscription(
		"slice8-sub", "arn:aws:sns:us-east-1:123456789012:slice8-topic", "db-instance", nil, nil,
	)
	require.NoError(t, err)

	_, err = client.AddSourceIdentifierToSubscription(
		ctx,
		&rdssdk.AddSourceIdentifierToSubscriptionInput{
			SubscriptionName: aws.String("slice8-sub"),
			SourceIdentifier: aws.String("slice8-inst"),
		},
	)
	require.NoError(t, err)

	_, err = client.RemoveSourceIdentifierFromSubscription(
		ctx, &rdssdk.RemoveSourceIdentifierFromSubscriptionInput{
			SubscriptionName: aws.String("slice8-sub"),
			SourceIdentifier: aws.String("slice8-inst"),
		},
	)
	require.NoError(t, err)

	modifyOut, err := client.ModifyEventSubscription(ctx, &rdssdk.ModifyEventSubscriptionInput{
		SubscriptionName: aws.String("slice8-sub"),
		Enabled:          aws.Bool(false),
	})
	require.NoError(t, err)
	assert.False(t, aws.ToBool(modifyOut.EventSubscription.Enabled))

	categoriesOut, err := client.DescribeEventCategories(
		ctx,
		&rdssdk.DescribeEventCategoriesInput{},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, categoriesOut.EventCategoriesMapList)

	_, err = client.DeleteEventSubscription(
		ctx, &rdssdk.DeleteEventSubscriptionInput{SubscriptionName: aws.String("slice8-sub")},
	)
	require.NoError(t, err)
}

func testSnapshotsRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBInstance(
		"slice8-snap-inst",
		"mysql",
		"db.t3.micro",
		"mydb",
		"admin",
		"",
		20,
		rds.DBInstanceOptions{},
	)
	require.NoError(t, err)

	_, err = backend.CreateDBSnapshot("slice8-snap", "slice8-snap-inst")
	require.NoError(t, err)

	copyOut, err := client.CopyDBSnapshot(ctx, &rdssdk.CopyDBSnapshotInput{
		SourceDBSnapshotIdentifier: aws.String("slice8-snap"),
		TargetDBSnapshotIdentifier: aws.String("slice8-snap-copy"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice8-snap-copy", aws.ToString(copyOut.DBSnapshot.DBSnapshotIdentifier))

	modifyOut, err := client.ModifyDBSnapshot(ctx, &rdssdk.ModifyDBSnapshotInput{
		DBSnapshotIdentifier: aws.String("slice8-snap-copy"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice8-snap-copy", aws.ToString(modifyOut.DBSnapshot.DBSnapshotIdentifier))
}

func testGlobalClusterBlueGreenRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateGlobalCluster(
		"slice8-globalcluster",
		"aurora-mysql",
		"8.0",
		true,
		false,
	)
	require.NoError(t, err)

	modifyOut, err := client.ModifyGlobalCluster(ctx, &rdssdk.ModifyGlobalClusterInput{
		GlobalClusterIdentifier: aws.String("slice8-globalcluster"),
		DeletionProtection:      aws.Bool(true),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(modifyOut.GlobalCluster.DeletionProtection))

	removeOut, err := client.RemoveFromGlobalCluster(ctx, &rdssdk.RemoveFromGlobalClusterInput{
		GlobalClusterIdentifier: aws.String("slice8-globalcluster"),
		DbClusterIdentifier: aws.String(
			"arn:aws:rds:us-east-1:123456789012:cluster:slice8-nonmember",
		),
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		"slice8-globalcluster",
		aws.ToString(removeOut.GlobalCluster.GlobalClusterIdentifier),
	)

	failoverOut, err := client.FailoverGlobalCluster(ctx, &rdssdk.FailoverGlobalClusterInput{
		GlobalClusterIdentifier: aws.String("slice8-globalcluster"),
		TargetDbClusterIdentifier: aws.String(
			"arn:aws:rds:us-east-1:123456789012:cluster:slice8-target",
		),
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		"slice8-globalcluster",
		aws.ToString(failoverOut.GlobalCluster.GlobalClusterIdentifier),
	)

	switchoverOut, err := client.SwitchoverGlobalCluster(ctx, &rdssdk.SwitchoverGlobalClusterInput{
		GlobalClusterIdentifier: aws.String("slice8-globalcluster"),
		TargetDbClusterIdentifier: aws.String(
			"arn:aws:rds:us-east-1:123456789012:cluster:slice8-target",
		),
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		"slice8-globalcluster",
		aws.ToString(switchoverOut.GlobalCluster.GlobalClusterIdentifier),
	)

	_, err = client.ModifyGlobalCluster(ctx, &rdssdk.ModifyGlobalClusterInput{
		GlobalClusterIdentifier: aws.String("slice8-globalcluster"),
		DeletionProtection:      aws.Bool(false),
	})
	require.NoError(t, err)

	_, err = client.DeleteGlobalCluster(ctx, &rdssdk.DeleteGlobalClusterInput{
		GlobalClusterIdentifier: aws.String("slice8-globalcluster"),
	})
	require.NoError(t, err)

	_, err = backend.CreateBlueGreenDeployment("slice8-bgd", "slice8-bgd-source")
	require.NoError(t, err)

	switchoverBGDOut, err := client.SwitchoverBlueGreenDeployment(
		ctx,
		&rdssdk.SwitchoverBlueGreenDeploymentInput{
			BlueGreenDeploymentIdentifier: aws.String("bgd-slice8-bgd"),
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		"bgd-slice8-bgd",
		aws.ToString(switchoverBGDOut.BlueGreenDeployment.BlueGreenDeploymentIdentifier),
	)

	_, err = client.DeleteBlueGreenDeployment(ctx, &rdssdk.DeleteBlueGreenDeploymentInput{
		BlueGreenDeploymentIdentifier: aws.String("bgd-slice8-bgd"),
	})
	require.NoError(t, err)
}

func testShardGroupTenantIntegrationRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateDBShardGroup(
		"slice8-shardgroup",
		"slice8-shard-cluster",
		384,
		24,
		1,
		false,
	)
	require.NoError(t, err)

	modifyOut, err := client.ModifyDBShardGroup(ctx, &rdssdk.ModifyDBShardGroupInput{
		DBShardGroupIdentifier: aws.String("slice8-shardgroup"),
		MaxACU:                 aws.Float64(768),
	})
	require.NoError(t, err)
	assert.InEpsilon(t, float64(768), aws.ToFloat64(modifyOut.MaxACU), 0.001)

	descOut, err := client.DescribeDBShardGroups(
		ctx,
		&rdssdk.DescribeDBShardGroupsInput{DBShardGroupIdentifier: aws.String("slice8-shardgroup")},
	)
	require.NoError(t, err)
	require.Len(t, descOut.DBShardGroups, 1)

	rebootOut, err := client.RebootDBShardGroup(
		ctx,
		&rdssdk.RebootDBShardGroupInput{DBShardGroupIdentifier: aws.String("slice8-shardgroup")},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice8-shardgroup", aws.ToString(rebootOut.DBShardGroupIdentifier))

	_, err = client.DeleteDBShardGroup(
		ctx,
		&rdssdk.DeleteDBShardGroupInput{DBShardGroupIdentifier: aws.String("slice8-shardgroup")},
	)
	require.NoError(t, err)

	_, err = backend.CreateTenantDatabase("slice8-cdb-inst", "slice8tenant", "admin")
	require.NoError(t, err)

	modifyTenantOut, err := client.ModifyTenantDatabase(ctx, &rdssdk.ModifyTenantDatabaseInput{
		DBInstanceIdentifier: aws.String("slice8-cdb-inst"),
		TenantDBName:         aws.String("slice8tenant"),
		NewTenantDBName:      aws.String("slice8tenant2"),
	})
	require.NoError(t, err)
	assert.Equal(t, "slice8tenant2", aws.ToString(modifyTenantOut.TenantDatabase.TenantDBName))

	_, err = client.DeleteTenantDatabase(ctx, &rdssdk.DeleteTenantDatabaseInput{
		DBInstanceIdentifier: aws.String("slice8-cdb-inst"),
		TenantDBName:         aws.String("slice8tenant2"),
	})
	require.NoError(t, err)

	_, err = backend.CreateIntegration(
		"slice8-integration",
		"arn:aws:rds:us-east-1:123456789012:cluster:slice8-src",
		"arn:aws:redshift-serverless:us-east-1:123456789012:namespace/slice8",
		"",
		"",
		"slice8 integration",
	)
	require.NoError(t, err)

	modifyIntegrationOut, err := client.ModifyIntegration(ctx, &rdssdk.ModifyIntegrationInput{
		IntegrationIdentifier: aws.String("slice8-integration"),
		IntegrationName:       aws.String("slice8-integration-renamed"),
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		"slice8-integration-renamed",
		aws.ToString(modifyIntegrationOut.IntegrationName),
	)

	descIntegrationOut, err := client.DescribeIntegrations(
		ctx,
		&rdssdk.DescribeIntegrationsInput{
			IntegrationIdentifier: aws.String("slice8-integration-renamed"),
		},
	)
	require.NoError(t, err)
	require.Len(t, descIntegrationOut.Integrations, 1)

	_, err = client.DeleteIntegration(
		ctx,
		&rdssdk.DeleteIntegrationInput{
			IntegrationIdentifier: aws.String("slice8-integration-renamed"),
		},
	)
	require.NoError(t, err)
}

func testReservedInstancesReferenceDataRealClient(t *testing.T) {
	t.Helper()

	_, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	offeringsOut, err := client.DescribeReservedDBInstancesOfferings(
		ctx, &rdssdk.DescribeReservedDBInstancesOfferingsInput{},
	)
	require.NoError(t, err)
	require.NotEmpty(t, offeringsOut.ReservedDBInstancesOfferings)
	offeringID := aws.ToString(
		offeringsOut.ReservedDBInstancesOfferings[0].ReservedDBInstancesOfferingId,
	)

	purchaseOut, err := client.PurchaseReservedDBInstancesOffering(
		ctx, &rdssdk.PurchaseReservedDBInstancesOfferingInput{
			ReservedDBInstancesOfferingId: aws.String(offeringID),
			ReservedDBInstanceId:          aws.String("slice8-reserved"),
			DBInstanceCount:               aws.Int32(1),
		},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		"slice8-reserved",
		aws.ToString(purchaseOut.ReservedDBInstance.ReservedDBInstanceId),
	)

	reservedOut, err := client.DescribeReservedDBInstances(
		ctx,
		&rdssdk.DescribeReservedDBInstancesInput{
			ReservedDBInstanceId: aws.String("slice8-reserved"),
		},
	)
	require.NoError(t, err)
	require.Len(t, reservedOut.ReservedDBInstances, 1)

	certsOut, err := client.DescribeCertificates(ctx, &rdssdk.DescribeCertificatesInput{})
	require.NoError(t, err)
	require.NotEmpty(t, certsOut.Certificates)
	certID := aws.ToString(certsOut.Certificates[0].CertificateIdentifier)

	modifyCertsOut, err := client.ModifyCertificates(ctx, &rdssdk.ModifyCertificatesInput{
		CertificateIdentifier: aws.String(certID),
	})
	require.NoError(t, err)
	assert.NotNil(t, modifyCertsOut.Certificate)

	sourceRegionsOut, err := client.DescribeSourceRegions(ctx, &rdssdk.DescribeSourceRegionsInput{})
	require.NoError(t, err)
	assert.NotNil(t, sourceRegionsOut)

	majorVersionsOut, err := client.DescribeDBMajorEngineVersions(
		ctx, &rdssdk.DescribeDBMajorEngineVersionsInput{Engine: aws.String("mysql")},
	)
	require.NoError(t, err)
	assert.NotNil(t, majorVersionsOut)

	orderableOut, err := client.DescribeOrderableDBInstanceOptions(
		ctx, &rdssdk.DescribeOrderableDBInstanceOptionsInput{Engine: aws.String("mysql")},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, orderableOut.OrderableDBInstanceOptions)

	_, err = client.CancelExportTask(
		ctx, &rdssdk.CancelExportTaskInput{ExportTaskIdentifier: aws.String("slice8-export-task")},
	)
	require.Error(
		t,
		err,
		"no such export task exists, but the op must round-trip a real NotFound error",
	)
}

func testAutomatedBackupsRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	inst, err := backend.CreateDBInstance(
		"slice8-backup-inst",
		"mysql",
		"db.t3.micro",
		"mydb",
		"admin",
		"",
		20,
		rds.DBInstanceOptions{},
	)
	require.NoError(t, err)

	startReplOut, err := client.StartDBInstanceAutomatedBackupsReplication(
		ctx, &rdssdk.StartDBInstanceAutomatedBackupsReplicationInput{
			SourceDBInstanceArn: aws.String(inst.DBInstanceArn),
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, startReplOut.DBInstanceAutomatedBackup)

	_, err = client.StopDBInstanceAutomatedBackupsReplication(
		ctx, &rdssdk.StopDBInstanceAutomatedBackupsReplicationInput{
			SourceDBInstanceArn: aws.String(inst.DBInstanceArn),
		},
	)
	require.NoError(t, err)

	cluster, err := backend.CreateDBCluster(
		"slice8-backup-cluster",
		"aurora-mysql",
		"admin",
		"mydb",
		"",
		3306,
		nil,
		rds.DBClusterOptions{},
	)
	require.NoError(t, err)

	clusterBackup := backend.CreateDBClusterAutomatedBackup(cluster.DBClusterIdentifier)
	require.NotNil(t, clusterBackup)

	_, err = client.DeleteDBClusterAutomatedBackup(ctx, &rdssdk.DeleteDBClusterAutomatedBackupInput{
		DbClusterResourceId: aws.String(clusterBackup.DBClusterResourceID),
	})
	require.NoError(t, err)
}

func testCustomEngineVersionRealClient(t *testing.T) {
	t.Helper()

	backend, client := newRealClientBackendAndClient(t)
	ctx := t.Context()

	_, err := backend.CreateCustomDBEngineVersion(
		"custom-oracle-ee-cdb", "19.slice8", "slice8 custom engine version",
	)
	require.NoError(t, err)

	modifyOut, err := client.ModifyCustomDBEngineVersion(
		ctx,
		&rdssdk.ModifyCustomDBEngineVersionInput{
			Engine:        aws.String("custom-oracle-ee-cdb"),
			EngineVersion: aws.String("19.slice8"),
			Description:   aws.String("slice8 updated"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "slice8 updated", aws.ToString(modifyOut.DBEngineVersionDescription))

	_, err = client.DeleteCustomDBEngineVersion(ctx, &rdssdk.DeleteCustomDBEngineVersionInput{
		Engine:        aws.String("custom-oracle-ee-cdb"),
		EngineVersion: aws.String("19.slice8"),
	})
	require.NoError(t, err)
}
