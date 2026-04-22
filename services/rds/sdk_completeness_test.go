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
		"CreateIntegration",
		"CreateTenantDatabase",
		"DeleteCustomDBEngineVersion",
		"DeleteDBClusterAutomatedBackup",
		"DeleteDBInstanceAutomatedBackup",
		"DeleteDBProxy",
		"DeleteDBProxyEndpoint",
		"DeleteDBShardGroup",
		"DeleteIntegration",
		"DeleteTenantDatabase",
		"DeregisterDBProxyTargets",
		"DescribeAccountAttributes",
		"DescribeCertificates",
		"DescribeDBClusterAutomatedBackups",
		"DescribeDBClusterBacktracks",
		"DescribeDBClusterSnapshotAttributes",
		"DescribeDBInstanceAutomatedBackups",
		"DescribeDBMajorEngineVersions",
		"DescribeDBProxies",
		"DescribeDBProxyEndpoints",
		"DescribeDBProxyTargetGroups",
		"DescribeDBProxyTargets",
		"DescribeDBRecommendations",
		"DescribeDBShardGroups",
		"DescribeDBSnapshotAttributes",
		"DescribeDBSnapshotTenantDatabases",
		"DescribeEngineDefaultClusterParameters",
		"DescribeEngineDefaultParameters",
		"DescribeIntegrations",
		"DescribePendingMaintenanceActions",
		"DescribeReservedDBInstances",
		"DescribeReservedDBInstancesOfferings",
		"DescribeSourceRegions",
		"DescribeTenantDatabases",
		"DisableHttpEndpoint",
		"EnableHttpEndpoint",
		"FailoverGlobalCluster",
		"ModifyActivityStream",
		"ModifyCertificates",
		"ModifyCurrentDBClusterCapacity",
		"ModifyCustomDBEngineVersion",
		"ModifyDBClusterSnapshotAttribute",
		"ModifyDBProxy",
		"ModifyDBProxyEndpoint",
		"ModifyDBProxyTargetGroup",
		"ModifyDBRecommendation",
		"ModifyDBShardGroup",
		"ModifyDBSnapshot",
		"ModifyDBSnapshotAttribute",
		"ModifyIntegration",
		"ModifyTenantDatabase",
		"PromoteReadReplicaDBCluster",
		"PurchaseReservedDBInstancesOffering",
		"RebootDBShardGroup",
		"RegisterDBProxyTargets",
		"RemoveFromGlobalCluster",
		"RestoreDBClusterFromS3",
		"RestoreDBInstanceFromS3",
		"StartActivityStream",
		"StartDBInstanceAutomatedBackupsReplication",
		"StopActivityStream",
		"StopDBInstanceAutomatedBackupsReplication",
		"SwitchoverGlobalCluster",
		"SwitchoverReadReplica",
	})
}
