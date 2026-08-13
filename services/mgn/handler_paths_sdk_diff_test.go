package mgn_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/services/mgn"
)

// sdkRouteCases is the authoritative method+path for every real mgn
// operation, extracted from mgn@v1.48.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for the {resourceArn} URI label on the tags trio -- the router only
// switches on the literal "tags" segment, not the ARN's shape, so the
// literal value doesn't matter here.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"ArchiveApplication", "POST", "/ArchiveApplication"},
		{"ArchiveWave", "POST", "/ArchiveWave"},
		{"AssociateApplications", "POST", "/AssociateApplications"},
		{"AssociateSourceServers", "POST", "/AssociateSourceServers"},
		{"ChangeServerLifeCycleState", "POST", "/ChangeServerLifeCycleState"},
		{"CreateApplication", "POST", "/CreateApplication"},
		{"CreateConnector", "POST", "/CreateConnector"},
		{"CreateLaunchConfigurationTemplate", "POST", "/CreateLaunchConfigurationTemplate"},
		{"CreateNetworkMigrationDefinition", "POST", "/network-migration/CreateNetworkMigrationDefinition"},
		{"CreateReplicationConfigurationTemplate", "POST", "/CreateReplicationConfigurationTemplate"},
		{"CreateWave", "POST", "/CreateWave"},
		{"DeleteApplication", "POST", "/DeleteApplication"},
		{"DeleteConnector", "POST", "/DeleteConnector"},
		{"DeleteJob", "POST", "/DeleteJob"},
		{"DeleteLaunchConfigurationTemplate", "POST", "/DeleteLaunchConfigurationTemplate"},
		{"DeleteNetworkMigrationDefinition", "POST", "/network-migration/DeleteNetworkMigrationDefinition"},
		{"DeleteReplicationConfigurationTemplate", "POST", "/DeleteReplicationConfigurationTemplate"},
		{"DeleteSourceServer", "POST", "/DeleteSourceServer"},
		{"DeleteVcenterClient", "POST", "/DeleteVcenterClient"},
		{"DeleteWave", "POST", "/DeleteWave"},
		{"DescribeJobLogItems", "POST", "/DescribeJobLogItems"},
		{"DescribeJobs", "POST", "/DescribeJobs"},
		{"DescribeLaunchConfigurationTemplates", "POST", "/DescribeLaunchConfigurationTemplates"},
		{"DescribeReplicationConfigurationTemplates", "POST", "/DescribeReplicationConfigurationTemplates"},
		{"DescribeSourceServers", "POST", "/DescribeSourceServers"},
		{"DescribeVcenterClients", "GET", "/DescribeVcenterClients"},
		{"DisassociateApplications", "POST", "/DisassociateApplications"},
		{"DisassociateSourceServers", "POST", "/DisassociateSourceServers"},
		{"DisconnectFromService", "POST", "/DisconnectFromService"},
		{"FinalizeCutover", "POST", "/FinalizeCutover"},
		{"GetLaunchConfiguration", "POST", "/GetLaunchConfiguration"},
		{"GetNetworkMigrationDefinition", "POST", "/network-migration/GetNetworkMigrationDefinition"},
		{
			"GetNetworkMigrationMapperSegmentConstruct", "POST",
			"/network-migration/GetNetworkMigrationMapperSegmentConstruct",
		},
		{"GetReplicationConfiguration", "POST", "/GetReplicationConfiguration"},
		{"InitializeService", "POST", "/InitializeService"},
		{"ListApplications", "POST", "/ListApplications"},
		{"ListConnectors", "POST", "/ListConnectors"},
		{"ListExportErrors", "POST", "/ListExportErrors"},
		{"ListExports", "POST", "/ListExports"},
		{"ListImportErrors", "POST", "/ListImportErrors"},
		{"ListImportFileEnrichments", "POST", "/network-migration/ListImportFileEnrichments"},
		{"ListImports", "POST", "/ListImports"},
		{"ListManagedAccounts", "POST", "/ListManagedAccounts"},
		{"ListNetworkMigrationAnalyses", "POST", "/network-migration/ListNetworkMigrationAnalyses"},
		{"ListNetworkMigrationAnalysisResults", "POST", "/network-migration/ListNetworkMigrationAnalysisResults"},
		{
			"ListNetworkMigrationCodeGenerationSegments", "POST",
			"/network-migration/ListNetworkMigrationCodeGenerationSegments",
		},
		{"ListNetworkMigrationCodeGenerations", "POST", "/network-migration/ListNetworkMigrationCodeGenerations"},
		{"ListNetworkMigrationDefinitions", "POST", "/network-migration/ListNetworkMigrationDefinitions"},
		{"ListNetworkMigrationDeployedStacks", "POST", "/network-migration/ListNetworkMigrationDeployedStacks"},
		{"ListNetworkMigrationDeployments", "POST", "/network-migration/ListNetworkMigrationDeployments"},
		{"ListNetworkMigrationExecutions", "POST", "/network-migration/ListNetworkMigrationExecutions"},
		{
			"ListNetworkMigrationMapperSegmentConstructs", "POST",
			"/network-migration/ListNetworkMigrationMapperSegmentConstructs",
		},
		{"ListNetworkMigrationMapperSegments", "POST", "/network-migration/ListNetworkMigrationMapperSegments"},
		{"ListNetworkMigrationMappingUpdates", "POST", "/network-migration/ListNetworkMigrationMappingUpdates"},
		{"ListNetworkMigrationMappings", "POST", "/network-migration/ListNetworkMigrationMappings"},
		{"ListSourceServerActions", "POST", "/ListSourceServerActions"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"ListTemplateActions", "POST", "/ListTemplateActions"},
		{"ListWaves", "POST", "/ListWaves"},
		{"MarkAsArchived", "POST", "/MarkAsArchived"},
		{"PauseReplication", "POST", "/PauseReplication"},
		{"PutSourceServerAction", "POST", "/PutSourceServerAction"},
		{"PutTemplateAction", "POST", "/PutTemplateAction"},
		{"RemoveSourceServerAction", "POST", "/RemoveSourceServerAction"},
		{"RemoveTemplateAction", "POST", "/RemoveTemplateAction"},
		{"ResumeReplication", "POST", "/ResumeReplication"},
		{"RetryDataReplication", "POST", "/RetryDataReplication"},
		{"StartCutover", "POST", "/StartCutover"},
		{"StartExport", "POST", "/StartExport"},
		{"StartImport", "POST", "/StartImport"},
		{"StartImportFileEnrichment", "POST", "/network-migration/StartImportFileEnrichment"},
		{"StartNetworkMigrationAnalysis", "POST", "/network-migration/StartNetworkMigrationAnalysis"},
		{"StartNetworkMigrationCodeGeneration", "POST", "/network-migration/StartNetworkMigrationCodeGeneration"},
		{"StartNetworkMigrationDeployment", "POST", "/network-migration/StartNetworkMigrationDeployment"},
		{"StartNetworkMigrationMapping", "POST", "/network-migration/StartNetworkMigrationMapping"},
		{"StartNetworkMigrationMappingUpdate", "POST", "/network-migration/StartNetworkMigrationMappingUpdate"},
		{"StartReplication", "POST", "/StartReplication"},
		{"StartTest", "POST", "/StartTest"},
		{"StopReplication", "POST", "/StopReplication"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"TerminateTargetInstances", "POST", "/TerminateTargetInstances"},
		{"UnarchiveApplication", "POST", "/UnarchiveApplication"},
		{"UnarchiveWave", "POST", "/UnarchiveWave"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateApplication", "POST", "/UpdateApplication"},
		{"UpdateConnector", "POST", "/UpdateConnector"},
		{"UpdateLaunchConfiguration", "POST", "/UpdateLaunchConfiguration"},
		{"UpdateLaunchConfigurationTemplate", "POST", "/UpdateLaunchConfigurationTemplate"},
		{"UpdateNetworkMigrationDefinition", "POST", "/network-migration/UpdateNetworkMigrationDefinition"},
		{"UpdateNetworkMigrationMapperSegment", "POST", "/network-migration/UpdateNetworkMigrationMapperSegment"},
		{"UpdateReplicationConfiguration", "POST", "/UpdateReplicationConfiguration"},
		{"UpdateReplicationConfigurationTemplate", "POST", "/UpdateReplicationConfigurationTemplate"},
		{"UpdateSourceServer", "POST", "/UpdateSourceServer"},
		{"UpdateSourceServerReplicationType", "POST", "/UpdateSourceServerReplicationType"},
		{"UpdateWave", "POST", "/UpdateWave"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real mgn op's authoritative
// method+path (see sdkRouteCases) through ExtractOperation and asserts the
// route table resolves it to the right op. gopherstack-l5ir: mgn's flat
// routeKey-lookup router (handler_routes.go) was previously undocumented in
// PARITY.md as routing-verified; this audit found zero mismatches across all
// 95 ops, including the tags trio (ListTagsForResource/TagResource/
// UntagResource all share /tags/{resourceArn}, correctly disambiguated by
// method) and the 25 ops namespaced under /network-migration/.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	backend := mgn.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	t.Cleanup(backend.Close)

	h := mgn.NewHandler(backend)

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())

			got := h.ExtractOperation(c)
			if got != tc.op {
				t.Errorf("method=%s path=%s: got op %q, want %q", tc.method, tc.path, got, tc.op)
			}
		})
	}
}
