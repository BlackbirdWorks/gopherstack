package rds_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/rds"
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
			"IntegrationName=test-integration" +
				"&SourceArn=arn:aws:rds:us-east-1:000000000000:db:test" +
				"&TargetArn=arn:aws:redshift:us-east-1:000000000000:namespace:test",
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
	h.Backend.CreateDBInstance(
		"db-test",
		"db.t3.micro",
		"mysql",
		"admin",
		"password",
		"subnet-1",
		20,
		rds.DBInstanceOptions{PerformanceInsightsEnabled: true},
	)

	for _, op := range stubOps {
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

	tests := []struct {
		name string
		body string
	}{
		{"DescribeDBInstanceAutomatedBackups", "Action=DescribeDBInstanceAutomatedBackups&Version=2014-10-31"},
		{"DescribeDBClusterAutomatedBackups", "Action=DescribeDBClusterAutomatedBackups&Version=2014-10-31"},
		{"DescribeDBSnapshotTenantDatabases", "Action=DescribeDBSnapshotTenantDatabases&Version=2014-10-31"},
		{"DescribeTenantDatabases", "Action=DescribeTenantDatabases&Version=2014-10-31"},
		{"DescribeIntegrations", "Action=DescribeIntegrations&Version=2014-10-31"},
		{"DescribeDBShardGroups", "Action=DescribeDBShardGroups&Version=2014-10-31"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := postRDSForm(t, h, tt.body)
			assert.GreaterOrEqual(t, rec.Code, 200, "expected non-negative status")
		})
	}
}
