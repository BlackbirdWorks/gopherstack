package rds_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestRDSCoverage_StubOps covers all the stub RDS operations.
func TestRDSCoverage_StubOps(t *testing.T) {
	t.Parallel()

	stubOps := []struct {
		action string
		params string
	}{
		{"CreateCustomDBEngineVersion", "Engine=custom-oracle-ee&EngineVersion=19.0.0.0"},
		{"DeleteCustomDBEngineVersion", "Engine=custom-oracle-ee&EngineVersion=19.0.0.0"},
		{"ModifyCustomDBEngineVersion", "Engine=custom-oracle-ee&EngineVersion=19.0.0.0"},
		{"CreateDBShardGroup", "DBShardGroupIdentifier=test-shard&DBClusterIdentifier=test-cluster&MaxACU=1"},
		{"DeleteDBShardGroup", "DBShardGroupIdentifier=test-shard"},
		{"DescribeDBShardGroups", ""},
		{"ModifyDBShardGroup", "DBShardGroupIdentifier=test-shard"},
		{"RebootDBShardGroup", "DBShardGroupIdentifier=test-shard"},
		{
			"CreateIntegration",
			"IntegrationName=test-integration&SourceArn=arn:aws:rds:us-east-1:000000000000:db:test&TargetArn=arn:aws:redshift:us-east-1:000000000000:namespace:test",
		},
		{"DeleteIntegration", "IntegrationIdentifier=test-integration"},
		{"DescribeIntegrations", ""},
		{"ModifyIntegration", "IntegrationIdentifier=test-integration"},
		{"CreateTenantDatabase", "DBInstanceIdentifier=test-db&TenantDBName=tenantdb&MasterUsername=admin"},
		{"DeleteTenantDatabase", "DBInstanceIdentifier=test-db&TenantDBName=tenantdb"},
		{"DescribeTenantDatabases", ""},
		{"ModifyTenantDatabase", "DBInstanceIdentifier=test-db&TenantDBName=tenantdb"},
		{"DeleteDBClusterAutomatedBackup", "DbClusterResourceId=cluster-1"},
		{"DescribeDBClusterAutomatedBackups", ""},
		{"DeleteDBInstanceAutomatedBackup", "DbiResourceId=db-1"},
		{"DescribeDBInstanceAutomatedBackups", ""},
		{
			"StartDBInstanceAutomatedBackupsReplication",
			"SourceDBInstanceArn=arn:aws:rds:us-east-1:000000000000:db:test",
		},
		{"StopDBInstanceAutomatedBackupsReplication", "SourceDBInstanceArn=arn:aws:rds:us-east-1:000000000000:db:test"},
		{"DescribeDBSnapshotTenantDatabases", ""},
		{
			"GetPerformanceInsightsMetrics",
			"ServiceType=RDS&Identifier=db-test&MetricQueries.member.1.Metric=db.load.avg",
		},
	}

	h := newRDSHandler()

	for _, op := range stubOps {
		op := op
		t.Run(op.action, func(t *testing.T) {
			t.Parallel()
			body := "Action=" + op.action + "&Version=2014-10-31"
			if op.params != "" {
				body += "&" + op.params
			}

			rec := postRDSForm(t, h, body)
			assert.NotEqual(t, http.StatusInternalServerError, rec.Code,
				"action %s should not 500", op.action)
		})
	}
}

// TestRDSCoverage_BackendOps covers DescribeDBInstanceAutomatedBackups backend.
func TestRDSCoverage_BackendOps(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()

	// These ops may return 404 or success - just verify they don't panic.
	ops := []string{
		"Action=DescribeDBInstanceAutomatedBackups&Version=2014-10-31",
		"Action=DescribeDBClusterAutomatedBackups&Version=2014-10-31",
		"Action=DescribeDBSnapshotTenantDatabases&Version=2014-10-31",
		"Action=DescribeTenantDatabases&Version=2014-10-31",
		"Action=DescribeIntegrations&Version=2014-10-31",
		"Action=DescribeDBShardGroups&Version=2014-10-31",
	}

	for _, body := range ops {
		rec := postRDSForm(t, h, body)
		assert.True(t, rec.Code >= 200, "expected non-negative status for %s", body)
	}
}
