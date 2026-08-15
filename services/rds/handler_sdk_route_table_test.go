package rds_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative Action value for every real RDS
// operation, extracted from rds@v1.124.1 serializers.go: each op's
// awsAwsquery_serializeOp<Op>.HandleSerialize sets
// body.Key("Action").String("<Op>") and always POSTs to "/" -- RDS is AWS
// Query/XML (services/_PROTOCOLS.md), so unlike a REST-family service there
// is no path template to get wrong: dispatch is entirely by this one form
// field. ExtractOperation and Handler() both read r.Form.Get("Action")
// directly, so the class of bug this table catches is a dispatch-table key
// that doesn't exactly match the real op name (typo, wrong case) -- not a
// route-template mismatch. Query protocol is case-insensitive for XML field
// names on the wire, but gopherstack's own dispatch is a Go string switch,
// which is always exact-match regardless of protocol.
//
// This table covers all 164 real RDS ops (rds@v1.124.1) -- confirmed by
// diffing the dispatch-table's 162 quoted switch-case keys (across handler_
// dispatch.go's chained dispatchExtended* functions) plus the opDescribeGlobalClusters
// constant against this exact list: zero mismatches in either direction.
// Excluded from the table: "GetPerformanceInsightsMetrics", a dispatch key
// present in handler_dispatch.go's switch that does not correspond to any
// real RDS SDK operation (RDS Performance Insights metrics are fetched via
// the separate "pi" service's GetResourceMetrics, not an RDS control-plane
// action) -- it is dead code from the real SDK's perspective and cannot be
// driven from a pinned client, so no route case exists for it here.
//
// Regenerate by grepping serializers.go for every
// `body.Key("Action").String("` and pulling the argument.
func sdkRouteCases() []string {
	return []string{
		"AddRoleToDBCluster",
		"AddRoleToDBInstance",
		"AddSourceIdentifierToSubscription",
		"AddTagsToResource",
		"ApplyPendingMaintenanceAction",
		"AuthorizeDBSecurityGroupIngress",
		"BacktrackDBCluster",
		"CancelExportTask",
		"CopyDBClusterParameterGroup",
		"CopyDBClusterSnapshot",
		"CopyDBParameterGroup",
		"CopyDBSnapshot",
		"CopyOptionGroup",
		"CreateBlueGreenDeployment",
		"CreateCustomDBEngineVersion",
		"CreateDBCluster",
		"CreateDBClusterEndpoint",
		"CreateDBClusterParameterGroup",
		"CreateDBClusterSnapshot",
		"CreateDBInstance",
		"CreateDBInstanceReadReplica",
		"CreateDBParameterGroup",
		"CreateDBProxy",
		"CreateDBProxyEndpoint",
		"CreateDBSecurityGroup",
		"CreateDBShardGroup",
		"CreateDBSnapshot",
		"CreateDBSubnetGroup",
		"CreateEventSubscription",
		"CreateGlobalCluster",
		"CreateIntegration",
		"CreateOptionGroup",
		"CreateTenantDatabase",
		"DeleteBlueGreenDeployment",
		"DeleteCustomDBEngineVersion",
		"DeleteDBCluster",
		"DeleteDBClusterAutomatedBackup",
		"DeleteDBClusterEndpoint",
		"DeleteDBClusterParameterGroup",
		"DeleteDBClusterSnapshot",
		"DeleteDBInstance",
		"DeleteDBInstanceAutomatedBackup",
		"DeleteDBParameterGroup",
		"DeleteDBProxy",
		"DeleteDBProxyEndpoint",
		"DeleteDBSecurityGroup",
		"DeleteDBShardGroup",
		"DeleteDBSnapshot",
		"DeleteDBSubnetGroup",
		"DeleteEventSubscription",
		"DeleteGlobalCluster",
		"DeleteIntegration",
		"DeleteOptionGroup",
		"DeleteTenantDatabase",
		"DeregisterDBProxyTargets",
		"DescribeAccountAttributes",
		"DescribeBlueGreenDeployments",
		"DescribeCertificates",
		"DescribeDBClusterAutomatedBackups",
		"DescribeDBClusterBacktracks",
		"DescribeDBClusterEndpoints",
		"DescribeDBClusterParameterGroups",
		"DescribeDBClusterParameters",
		"DescribeDBClusterSnapshotAttributes",
		"DescribeDBClusterSnapshots",
		"DescribeDBClusters",
		"DescribeDBEngineVersions",
		"DescribeDBInstanceAutomatedBackups",
		"DescribeDBInstances",
		"DescribeDBLogFiles",
		"DescribeDBMajorEngineVersions",
		"DescribeDBParameterGroups",
		"DescribeDBParameters",
		"DescribeDBProxies",
		"DescribeDBProxyEndpoints",
		"DescribeDBProxyTargetGroups",
		"DescribeDBProxyTargets",
		"DescribeDBRecommendations",
		"DescribeDBSecurityGroups",
		"DescribeDBShardGroups",
		"DescribeDBSnapshotAttributes",
		"DescribeDBSnapshotTenantDatabases",
		"DescribeDBSnapshots",
		"DescribeDBSubnetGroups",
		"DescribeEngineDefaultClusterParameters",
		"DescribeEngineDefaultParameters",
		"DescribeEventCategories",
		"DescribeEventSubscriptions",
		"DescribeEvents",
		"DescribeExportTasks",
		"DescribeGlobalClusters",
		"DescribeIntegrations",
		"DescribeOptionGroupOptions",
		"DescribeOptionGroups",
		"DescribeOrderableDBInstanceOptions",
		"DescribePendingMaintenanceActions",
		"DescribeReservedDBInstances",
		"DescribeReservedDBInstancesOfferings",
		"DescribeServerlessV2PlatformVersions",
		"DescribeSourceRegions",
		"DescribeTenantDatabases",
		"DescribeValidDBInstanceModifications",
		"DisableHttpEndpoint",
		"DownloadDBLogFilePortion",
		"EnableHttpEndpoint",
		"FailoverDBCluster",
		"FailoverGlobalCluster",
		"ListTagsForResource",
		"ModifyActivityStream",
		"ModifyCertificates",
		"ModifyCurrentDBClusterCapacity",
		"ModifyCustomDBEngineVersion",
		"ModifyDBCluster",
		"ModifyDBClusterEndpoint",
		"ModifyDBClusterParameterGroup",
		"ModifyDBClusterSnapshotAttribute",
		"ModifyDBInstance",
		"ModifyDBParameterGroup",
		"ModifyDBProxy",
		"ModifyDBProxyEndpoint",
		"ModifyDBProxyTargetGroup",
		"ModifyDBRecommendation",
		"ModifyDBShardGroup",
		"ModifyDBSnapshot",
		"ModifyDBSnapshotAttribute",
		"ModifyDBSubnetGroup",
		"ModifyEventSubscription",
		"ModifyGlobalCluster",
		"ModifyIntegration",
		"ModifyOptionGroup",
		"ModifyTenantDatabase",
		"PromoteReadReplica",
		"PromoteReadReplicaDBCluster",
		"PurchaseReservedDBInstancesOffering",
		"RebootDBCluster",
		"RebootDBInstance",
		"RebootDBShardGroup",
		"RegisterDBProxyTargets",
		"RemoveFromGlobalCluster",
		"RemoveRoleFromDBCluster",
		"RemoveRoleFromDBInstance",
		"RemoveSourceIdentifierFromSubscription",
		"RemoveTagsFromResource",
		"ResetDBClusterParameterGroup",
		"ResetDBParameterGroup",
		"RestoreDBClusterFromS3",
		"RestoreDBClusterFromSnapshot",
		"RestoreDBClusterToPointInTime",
		"RestoreDBInstanceFromDBSnapshot",
		"RestoreDBInstanceFromS3",
		"RestoreDBInstanceToPointInTime",
		"RevokeDBSecurityGroupIngress",
		"StartActivityStream",
		"StartDBCluster",
		"StartDBInstance",
		"StartDBInstanceAutomatedBackupsReplication",
		"StartExportTask",
		"StopActivityStream",
		"StopDBCluster",
		"StopDBInstance",
		"StopDBInstanceAutomatedBackupsReplication",
		"SwitchoverBlueGreenDeployment",
		"SwitchoverGlobalCluster",
		"SwitchoverReadReplica",
	}
}

// TestExtractOperation_SDKRouteTable drives every real RDS operation's
// authoritative Action value through ExtractOperation and Handler(),
// asserting the form field resolves to the right op name and that Handler()
// does not fall through to the "is not a valid RDS action" sentinel that a
// dispatch-table key mismatch would produce.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, op := range sdkRouteCases() {
		t.Run(strings.ToLower(op), func(t *testing.T) {
			t.Parallel()

			h := newRDSHandler()
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action="+op))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "is not a valid RDS action",
				"action=%s: dispatched to the unmatched-route handler", op)
		})
	}
}
