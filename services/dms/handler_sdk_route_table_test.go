package dms_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dms"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real AWS DMS
// operation, extracted from databasemigrationservice@v1.66.4 serializers.go:
// each op's awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("AmazonDMSv20160101.<Op>")
// and always request.Request.Method = "POST" against path "/" -- DMS is
// JSON-RPC 1.1 (services/_PROTOCOLS.md), so unlike a REST-family service there
// is no path template to get wrong: dispatch is entirely by this one header.
// ExtractOperation and Handler() both derive the action the same way
// (TrimPrefix on "AmazonDMSv20160101."), so the class of bug this table can
// catch is a dispatch-table key that doesn't exactly match the real op name
// (typo, wrong case -- DMS is case-sensitive JSON-RPC), not a route-template
// mismatch.
//
// This table covers all 119 real DMS ops, which is also gopherstack's full
// implemented set (h.GetSupportedOperations(), 119/119) as of
// databasemigrationservice@v1.66.4 -- confirmed by diffing both
// GetSupportedOperations() and the actual dispatch table (the op* consts
// plus the four family entries keyed by string literal --
// BatchStartRecommendations, CancelMetadataModelConversion,
// CancelMetadataModelCreation, CancelReplicationTaskAssessmentRun -- which a
// naive identifier-only grep of the dispatch tables misses) against this
// exact list. Zero mismatches either direction in both comparisons: no dead
// key, no gap.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("AmazonDMSv20160101.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"AddTagsToResource", "AmazonDMSv20160101.AddTagsToResource"},
		{"ApplyPendingMaintenanceAction", "AmazonDMSv20160101.ApplyPendingMaintenanceAction"},
		{"BatchStartRecommendations", "AmazonDMSv20160101.BatchStartRecommendations"},
		{"CancelMetadataModelConversion", "AmazonDMSv20160101.CancelMetadataModelConversion"},
		{"CancelMetadataModelCreation", "AmazonDMSv20160101.CancelMetadataModelCreation"},
		{"CancelReplicationTaskAssessmentRun", "AmazonDMSv20160101.CancelReplicationTaskAssessmentRun"},
		{"CreateDataMigration", "AmazonDMSv20160101.CreateDataMigration"},
		{"CreateDataProvider", "AmazonDMSv20160101.CreateDataProvider"},
		{"CreateEndpoint", "AmazonDMSv20160101.CreateEndpoint"},
		{"CreateEventSubscription", "AmazonDMSv20160101.CreateEventSubscription"},
		{"CreateFleetAdvisorCollector", "AmazonDMSv20160101.CreateFleetAdvisorCollector"},
		{"CreateInstanceProfile", "AmazonDMSv20160101.CreateInstanceProfile"},
		{"CreateMigrationProject", "AmazonDMSv20160101.CreateMigrationProject"},
		{"CreateReplicationConfig", "AmazonDMSv20160101.CreateReplicationConfig"},
		{"CreateReplicationInstance", "AmazonDMSv20160101.CreateReplicationInstance"},
		{"CreateReplicationSubnetGroup", "AmazonDMSv20160101.CreateReplicationSubnetGroup"},
		{"CreateReplicationTask", "AmazonDMSv20160101.CreateReplicationTask"},
		{"DeleteCertificate", "AmazonDMSv20160101.DeleteCertificate"},
		{"DeleteConnection", "AmazonDMSv20160101.DeleteConnection"},
		{"DeleteDataMigration", "AmazonDMSv20160101.DeleteDataMigration"},
		{"DeleteDataProvider", "AmazonDMSv20160101.DeleteDataProvider"},
		{"DeleteEndpoint", "AmazonDMSv20160101.DeleteEndpoint"},
		{"DeleteEventSubscription", "AmazonDMSv20160101.DeleteEventSubscription"},
		{"DeleteFleetAdvisorCollector", "AmazonDMSv20160101.DeleteFleetAdvisorCollector"},
		{"DeleteFleetAdvisorDatabases", "AmazonDMSv20160101.DeleteFleetAdvisorDatabases"},
		{"DeleteInstanceProfile", "AmazonDMSv20160101.DeleteInstanceProfile"},
		{"DeleteMigrationProject", "AmazonDMSv20160101.DeleteMigrationProject"},
		{"DeleteReplicationConfig", "AmazonDMSv20160101.DeleteReplicationConfig"},
		{"DeleteReplicationInstance", "AmazonDMSv20160101.DeleteReplicationInstance"},
		{"DeleteReplicationSubnetGroup", "AmazonDMSv20160101.DeleteReplicationSubnetGroup"},
		{"DeleteReplicationTask", "AmazonDMSv20160101.DeleteReplicationTask"},
		{"DeleteReplicationTaskAssessmentRun", "AmazonDMSv20160101.DeleteReplicationTaskAssessmentRun"},
		{"DescribeAccountAttributes", "AmazonDMSv20160101.DescribeAccountAttributes"},
		{"DescribeApplicableIndividualAssessments", "AmazonDMSv20160101.DescribeApplicableIndividualAssessments"},
		{"DescribeCertificates", "AmazonDMSv20160101.DescribeCertificates"},
		{"DescribeConnections", "AmazonDMSv20160101.DescribeConnections"},
		{"DescribeConversionConfiguration", "AmazonDMSv20160101.DescribeConversionConfiguration"},
		{"DescribeDataMigrations", "AmazonDMSv20160101.DescribeDataMigrations"},
		{"DescribeDataProviders", "AmazonDMSv20160101.DescribeDataProviders"},
		{"DescribeEndpoints", "AmazonDMSv20160101.DescribeEndpoints"},
		{"DescribeEndpointSettings", "AmazonDMSv20160101.DescribeEndpointSettings"},
		{"DescribeEndpointTypes", "AmazonDMSv20160101.DescribeEndpointTypes"},
		{"DescribeEngineVersions", "AmazonDMSv20160101.DescribeEngineVersions"},
		{"DescribeEventCategories", "AmazonDMSv20160101.DescribeEventCategories"},
		{"DescribeEvents", "AmazonDMSv20160101.DescribeEvents"},
		{"DescribeEventSubscriptions", "AmazonDMSv20160101.DescribeEventSubscriptions"},
		{"DescribeExtensionPackAssociations", "AmazonDMSv20160101.DescribeExtensionPackAssociations"},
		{"DescribeFleetAdvisorCollectors", "AmazonDMSv20160101.DescribeFleetAdvisorCollectors"},
		{"DescribeFleetAdvisorDatabases", "AmazonDMSv20160101.DescribeFleetAdvisorDatabases"},
		{"DescribeFleetAdvisorLsaAnalysis", "AmazonDMSv20160101.DescribeFleetAdvisorLsaAnalysis"},
		{"DescribeFleetAdvisorSchemaObjectSummary", "AmazonDMSv20160101.DescribeFleetAdvisorSchemaObjectSummary"},
		{"DescribeFleetAdvisorSchemas", "AmazonDMSv20160101.DescribeFleetAdvisorSchemas"},
		{"DescribeInstanceProfiles", "AmazonDMSv20160101.DescribeInstanceProfiles"},
		{"DescribeMetadataModel", "AmazonDMSv20160101.DescribeMetadataModel"},
		{"DescribeMetadataModelAssessments", "AmazonDMSv20160101.DescribeMetadataModelAssessments"},
		{"DescribeMetadataModelChildren", "AmazonDMSv20160101.DescribeMetadataModelChildren"},
		{"DescribeMetadataModelConversions", "AmazonDMSv20160101.DescribeMetadataModelConversions"},
		{"DescribeMetadataModelCreations", "AmazonDMSv20160101.DescribeMetadataModelCreations"},
		{"DescribeMetadataModelExportsAsScript", "AmazonDMSv20160101.DescribeMetadataModelExportsAsScript"},
		{"DescribeMetadataModelExportsToTarget", "AmazonDMSv20160101.DescribeMetadataModelExportsToTarget"},
		{"DescribeMetadataModelImports", "AmazonDMSv20160101.DescribeMetadataModelImports"},
		{"DescribeMigrationProjects", "AmazonDMSv20160101.DescribeMigrationProjects"},
		{"DescribeOrderableReplicationInstances", "AmazonDMSv20160101.DescribeOrderableReplicationInstances"},
		{"DescribePendingMaintenanceActions", "AmazonDMSv20160101.DescribePendingMaintenanceActions"},
		{"DescribeRecommendationLimitations", "AmazonDMSv20160101.DescribeRecommendationLimitations"},
		{"DescribeRecommendations", "AmazonDMSv20160101.DescribeRecommendations"},
		{"DescribeRefreshSchemasStatus", "AmazonDMSv20160101.DescribeRefreshSchemasStatus"},
		{"DescribeReplicationConfigs", "AmazonDMSv20160101.DescribeReplicationConfigs"},
		{"DescribeReplicationInstances", "AmazonDMSv20160101.DescribeReplicationInstances"},
		{"DescribeReplicationInstanceTaskLogs", "AmazonDMSv20160101.DescribeReplicationInstanceTaskLogs"},
		{"DescribeReplications", "AmazonDMSv20160101.DescribeReplications"},
		{"DescribeReplicationSubnetGroups", "AmazonDMSv20160101.DescribeReplicationSubnetGroups"},
		{"DescribeReplicationTableStatistics", "AmazonDMSv20160101.DescribeReplicationTableStatistics"},
		{"DescribeReplicationTaskAssessmentResults", "AmazonDMSv20160101.DescribeReplicationTaskAssessmentResults"},
		{"DescribeReplicationTaskAssessmentRuns", "AmazonDMSv20160101.DescribeReplicationTaskAssessmentRuns"},
		{
			"DescribeReplicationTaskIndividualAssessments",
			"AmazonDMSv20160101.DescribeReplicationTaskIndividualAssessments",
		},
		{"DescribeReplicationTasks", "AmazonDMSv20160101.DescribeReplicationTasks"},
		{"DescribeSchemas", "AmazonDMSv20160101.DescribeSchemas"},
		{"DescribeTableStatistics", "AmazonDMSv20160101.DescribeTableStatistics"},
		{"ExportMetadataModelAssessment", "AmazonDMSv20160101.ExportMetadataModelAssessment"},
		{"GetTargetSelectionRules", "AmazonDMSv20160101.GetTargetSelectionRules"},
		{"ImportCertificate", "AmazonDMSv20160101.ImportCertificate"},
		{"ListTagsForResource", "AmazonDMSv20160101.ListTagsForResource"},
		{"ModifyConversionConfiguration", "AmazonDMSv20160101.ModifyConversionConfiguration"},
		{"ModifyDataMigration", "AmazonDMSv20160101.ModifyDataMigration"},
		{"ModifyDataProvider", "AmazonDMSv20160101.ModifyDataProvider"},
		{"ModifyEndpoint", "AmazonDMSv20160101.ModifyEndpoint"},
		{"ModifyEventSubscription", "AmazonDMSv20160101.ModifyEventSubscription"},
		{"ModifyInstanceProfile", "AmazonDMSv20160101.ModifyInstanceProfile"},
		{"ModifyMigrationProject", "AmazonDMSv20160101.ModifyMigrationProject"},
		{"ModifyReplicationConfig", "AmazonDMSv20160101.ModifyReplicationConfig"},
		{"ModifyReplicationInstance", "AmazonDMSv20160101.ModifyReplicationInstance"},
		{"ModifyReplicationSubnetGroup", "AmazonDMSv20160101.ModifyReplicationSubnetGroup"},
		{"ModifyReplicationTask", "AmazonDMSv20160101.ModifyReplicationTask"},
		{"MoveReplicationTask", "AmazonDMSv20160101.MoveReplicationTask"},
		{"RebootReplicationInstance", "AmazonDMSv20160101.RebootReplicationInstance"},
		{"RefreshSchemas", "AmazonDMSv20160101.RefreshSchemas"},
		{"ReloadReplicationTables", "AmazonDMSv20160101.ReloadReplicationTables"},
		{"ReloadTables", "AmazonDMSv20160101.ReloadTables"},
		{"RemoveTagsFromResource", "AmazonDMSv20160101.RemoveTagsFromResource"},
		{"RunFleetAdvisorLsaAnalysis", "AmazonDMSv20160101.RunFleetAdvisorLsaAnalysis"},
		{"StartDataMigration", "AmazonDMSv20160101.StartDataMigration"},
		{"StartExtensionPackAssociation", "AmazonDMSv20160101.StartExtensionPackAssociation"},
		{"StartMetadataModelAssessment", "AmazonDMSv20160101.StartMetadataModelAssessment"},
		{"StartMetadataModelConversion", "AmazonDMSv20160101.StartMetadataModelConversion"},
		{"StartMetadataModelCreation", "AmazonDMSv20160101.StartMetadataModelCreation"},
		{"StartMetadataModelExportAsScript", "AmazonDMSv20160101.StartMetadataModelExportAsScript"},
		{"StartMetadataModelExportToTarget", "AmazonDMSv20160101.StartMetadataModelExportToTarget"},
		{"StartMetadataModelImport", "AmazonDMSv20160101.StartMetadataModelImport"},
		{"StartRecommendations", "AmazonDMSv20160101.StartRecommendations"},
		{"StartReplication", "AmazonDMSv20160101.StartReplication"},
		{"StartReplicationTask", "AmazonDMSv20160101.StartReplicationTask"},
		{"StartReplicationTaskAssessment", "AmazonDMSv20160101.StartReplicationTaskAssessment"},
		{"StartReplicationTaskAssessmentRun", "AmazonDMSv20160101.StartReplicationTaskAssessmentRun"},
		{"StopDataMigration", "AmazonDMSv20160101.StopDataMigration"},
		{"StopReplication", "AmazonDMSv20160101.StopReplication"},
		{"StopReplicationTask", "AmazonDMSv20160101.StopReplicationTask"},
		{"TestConnection", "AmazonDMSv20160101.TestConnection"},
		{"UpdateSubscriptionsToEventBridge", "AmazonDMSv20160101.UpdateSubscriptionsToEventBridge"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real DMS operation's
// authoritative X-Amz-Target through ExtractOperation and Handler(),
// asserting the header resolves to the right op name and that Handler() does
// not fall through to the "UnknownOperationException" sentinel that a
// dispatch-table key mismatch would produce. That sentinel (errUnknownAction
// in errors.go, whose Error() text is literally "UnknownOperationException")
// has exactly one production call site -- the dispatch() miss in the h.ops
// map lookup -- so it cannot collide with a legitimate error on this
// all-empty-body table.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			backend := dms.NewInMemoryBackend("000000000000", "us-east-1")
			h := dms.NewHandler(backend)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "UnknownOperationException",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
