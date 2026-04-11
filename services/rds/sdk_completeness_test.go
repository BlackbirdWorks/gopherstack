package rds_test

import (
	"testing"

	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/rds"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// rds client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.  The test fails when the upstream
// SDK adds a new operation that gopherstack has not yet handled.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := rds.NewInMemoryBackend("000000000000", "us-east-1")
	h := rds.NewHandler(backend)
	sdkcheck.CheckCompleteness(t, &rdssdk.Client{}, h.GetSupportedOperations(), []string{
		"CreateCustomDBEngineVersion",
		"CreateDBProxy",
		"CreateDBProxyEndpoint",
		"CreateDBSecurityGroup",
		"CreateDBShardGroup",
		"CreateEventSubscription",
		"CreateIntegration",
		"CreateTenantDatabase",
		"DeleteBlueGreenDeployment",
		"DeleteCustomDBEngineVersion",
		"DeleteDBClusterAutomatedBackup",
		"DeleteDBClusterParameterGroup",
		"DeleteDBInstanceAutomatedBackup",
		"DeleteDBProxy",
		"DeleteDBProxyEndpoint",
		"DeleteDBSecurityGroup",
		"DeleteDBShardGroup",
		"DeleteEventSubscription",
		"DeleteIntegration",
		"DeleteTenantDatabase",
		"DeregisterDBProxyTargets",
		"DescribeAccountAttributes",
		"DescribeBlueGreenDeployments",
		"DescribeCertificates",
		"DescribeDBClusterAutomatedBackups",
		"DescribeDBClusterBacktracks",
		"DescribeDBClusterParameters",
		"DescribeDBClusterSnapshotAttributes",
		"DescribeDBInstanceAutomatedBackups",
		"DescribeDBMajorEngineVersions",
		"DescribeDBProxies",
		"DescribeDBProxyEndpoints",
		"DescribeDBProxyTargetGroups",
		"DescribeDBProxyTargets",
		"DescribeDBRecommendations",
		"DescribeDBSecurityGroups",
		"DescribeDBShardGroups",
		"DescribeDBSnapshotAttributes",
		"DescribeDBSnapshotTenantDatabases",
		"DescribeEngineDefaultClusterParameters",
		"DescribeEngineDefaultParameters",
		"DescribeEventCategories",
		"DescribeEventSubscriptions",
		"DescribeEvents",
		"DescribeIntegrations",
		"DescribePendingMaintenanceActions",
		"DescribeReservedDBInstances",
		"DescribeReservedDBInstancesOfferings",
		"DescribeSourceRegions",
		"DescribeTenantDatabases",
		"DisableHttpEndpoint",
		"EnableHttpEndpoint",
		"FailoverDBCluster",
		"FailoverGlobalCluster",
		"ModifyActivityStream",
		"ModifyCertificates",
		"ModifyCurrentDBClusterCapacity",
		"ModifyCustomDBEngineVersion",
		"ModifyDBClusterEndpoint",
		"ModifyDBClusterParameterGroup",
		"ModifyDBClusterSnapshotAttribute",
		"ModifyDBProxy",
		"ModifyDBProxyEndpoint",
		"ModifyDBProxyTargetGroup",
		"ModifyDBRecommendation",
		"ModifyDBShardGroup",
		"ModifyDBSnapshot",
		"ModifyDBSnapshotAttribute",
		"ModifyDBSubnetGroup",
		"ModifyEventSubscription",
		"ModifyIntegration",
		"ModifyTenantDatabase",
		"PromoteReadReplicaDBCluster",
		"PurchaseReservedDBInstancesOffering",
		"RebootDBCluster",
		"RebootDBShardGroup",
		"RegisterDBProxyTargets",
		"RemoveFromGlobalCluster",
		"RemoveRoleFromDBCluster",
		"RemoveRoleFromDBInstance",
		"RemoveSourceIdentifierFromSubscription",
		"ResetDBClusterParameterGroup",
		"RestoreDBClusterFromS3",
		"RestoreDBInstanceFromS3",
		"RevokeDBSecurityGroupIngress",
		"StartActivityStream",
		"StartDBInstanceAutomatedBackupsReplication",
		"StopActivityStream",
		"StopDBInstanceAutomatedBackupsReplication",
		"SwitchoverBlueGreenDeployment",
		"SwitchoverGlobalCluster",
		"SwitchoverReadReplica",
	})
}
