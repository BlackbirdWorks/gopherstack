package glue_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// doGlueOp is a helper to POST a JSON body to the Glue handler via echo.
func doGlueOp(t *testing.T, h *glue.Handler, op string, body any) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(b))
	req.Header.Set("X-Amz-Target", "AWSGlue."+op)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	rr := httptest.NewRecorder()
	c := e.NewContext(req, rr)
	if hErr := h.Handler()(c); hErr != nil {
		t.Fatalf("handler error: %v", hErr)
	}

	return rr
}

// TestGlueStubOperations exercises all stub Glue operations through the HTTP handler.
// Each stub returns a valid empty JSON response (HTTP 200).
func TestGlueStubOperations(t *testing.T) {
	t.Parallel()

	stubOps := []string{
		"BatchGetJobs",
		"BatchGetPartition",
		"BatchGetTableOptimizer",
		"BatchGetTriggers",
		"BatchGetWorkflows",
		"BatchPutDataQualityStatisticAnnotation",
		"BatchUpdatePartition",
		"CancelDataQualityRuleRecommendationRun",
		"CancelMLTaskRun",
		"CancelStatement",
		"CheckSchemaVersionValidity",
		"CreateBlueprint",
		"CreateCatalog",
		"CreateClassifier",
		"CreateColumnStatisticsTaskSettings",
		"CreateCustomEntityType",
		"CreateDevEndpoint",
		"CreateGlueIdentityCenterConfiguration",
		"CreateIntegration",
		"CreateIntegrationResourceProperty",
		"CreateIntegrationTableProperties",
		"CreateMLTransform",
		"CreatePartition",
		"CreatePartitionIndex",
		"CreateRegistry",
		"CreateSchema",
		"CreateScript",
		"CreateSecurityConfiguration",
		"CreateSession",
		"CreateTableOptimizer",
		"CreateTrigger",
		"CreateUsageProfile",
		"CreateUserDefinedFunction",
		"CreateWorkflow",
		"DeleteBlueprint",
		"DeleteCatalog",
		"DeleteClassifier",
		"DeleteColumnStatisticsForPartition",
		"DeleteColumnStatisticsForTable",
		"DeleteColumnStatisticsTaskSettings",
		"DeleteConnectionType",
		"DeleteCustomEntityType",
		"DeleteDevEndpoint",
		"DeleteGlueIdentityCenterConfiguration",
		"DeleteIntegration",
		"DeleteIntegrationResourceProperty",
		"DeleteIntegrationTableProperties",
		"DeleteMLTransform",
		"DeletePartition",
		"DeletePartitionIndex",
		"DeleteRegistry",
		"DeleteResourcePolicy",
		"DeleteSchema",
		"DeleteSchemaVersions",
		"DeleteSecurityConfiguration",
		"DeleteSession",
		"DeleteTableOptimizer",
		"DeleteTableVersion",
		"DeleteTrigger",
		"DeleteUsageProfile",
		"DeleteUserDefinedFunction",
		"DeleteWorkflow",
		"DescribeConnectionType",
		"DescribeEntity",
		"DescribeInboundIntegrations",
		"DescribeIntegrations",
		"GetBlueprint",
		"GetBlueprintRun",
		"GetBlueprintRuns",
		"GetCatalog",
		"GetCatalogImportStatus",
		"GetCatalogs",
		"GetClassifier",
		"GetClassifiers",
		"GetColumnStatisticsForPartition",
		"GetColumnStatisticsForTable",
		"GetColumnStatisticsTaskRun",
		"GetColumnStatisticsTaskRuns",
		"GetColumnStatisticsTaskSettings",
		"GetCrawlerMetrics",
		"GetCustomEntityType",
		"GetDataCatalogEncryptionSettings",
		"GetDataQualityModel",
		"GetDataQualityModelResult",
		"GetDataQualityResult",
		"GetDataQualityRuleRecommendationRun",
		"GetDataflowGraph",
		"GetDevEndpoint",
		"GetDevEndpoints",
		"GetEntityRecords",
		"GetGlueIdentityCenterConfiguration",
		"GetIntegrationResourceProperty",
		"GetIntegrationTableProperties",
		"GetMLTaskRun",
		"GetMLTaskRuns",
		"GetMLTransform",
		"GetMLTransforms",
		"GetMapping",
		"GetMaterializedViewRefreshTaskRun",
		"GetPartition",
		"GetPartitionIndexes",
		"GetPartitions",
		"GetPlan",
		"GetRegistry",
		"GetResourcePolicies",
		"GetResourcePolicy",
		"GetSchema",
		"GetSchemaByDefinition",
		"GetSchemaVersion",
		"GetSchemaVersionsDiff",
		"GetSecurityConfiguration",
		"GetSecurityConfigurations",
		"GetSession",
		"GetStatement",
		"GetTableOptimizer",
		"GetTableVersion",
		"GetTableVersions",
		"GetTrigger",
		"GetTriggers",
		"GetUnfilteredPartitionMetadata",
		"GetUnfilteredPartitionsMetadata",
		"GetUnfilteredTableMetadata",
		"GetUsageProfile",
		"GetUserDefinedFunction",
		"GetUserDefinedFunctions",
		"GetWorkflow",
		"GetWorkflowRun",
		"GetWorkflowRunProperties",
		"GetWorkflowRuns",
		"ImportCatalogToGlue",
		"ListBlueprints",
		"ListColumnStatisticsTaskRuns",
		"ListConnectionTypes",
		"ListCrawls",
		"ListCustomEntityTypes",
		"ListDataQualityResults",
		"ListDataQualityRuleRecommendationRuns",
		"ListDataQualityRulesetEvaluationRuns",
		"ListDataQualityStatisticAnnotations",
		"ListDataQualityStatistics",
		"ListDevEndpoints",
		"ListEntities",
		"ListIntegrationResourceProperties",
		"ListJobs",
		"ListMLTransforms",
		"ListMaterializedViewRefreshTaskRuns",
		"ListRegistries",
		"ListSchemaVersions",
		"ListSchemas",
		"ListSessions",
		"ListStatements",
		"ListTableOptimizerRuns",
		"ListTriggers",
		"ListUsageProfiles",
		"ListWorkflows",
		"ModifyIntegration",
		"PutDataCatalogEncryptionSettings",
		"PutDataQualityProfileAnnotation",
		"PutResourcePolicy",
		"PutSchemaVersionMetadata",
		"PutWorkflowRunProperties",
		"QuerySchemaVersionMetadata",
		"RegisterConnectionType",
		"RegisterSchemaVersion",
		"RemoveSchemaVersionMetadata",
		"ResumeWorkflowRun",
		"RunStatement",
		"SearchTables",
		"StartBlueprintRun",
		"StartColumnStatisticsTaskRun",
		"StartColumnStatisticsTaskRunSchedule",
		"StartDataQualityRuleRecommendationRun",
		"StartExportLabelsTaskRun",
		"StartImportLabelsTaskRun",
		"StartMLEvaluationTaskRun",
		"StartMLLabelingSetGenerationTaskRun",
		"StartMaterializedViewRefreshTaskRun",
		"StartTrigger",
		"StartWorkflowRun",
		"StopColumnStatisticsTaskRun",
		"StopColumnStatisticsTaskRunSchedule",
		"StopMaterializedViewRefreshTaskRun",
		"StopSession",
		"StopTrigger",
		"StopWorkflowRun",
		"TestConnection",
		"UpdateBlueprint",
		"UpdateCatalog",
		"UpdateClassifier",
		"UpdateColumnStatisticsForPartition",
		"UpdateColumnStatisticsForTable",
		"UpdateColumnStatisticsTaskSettings",
		"UpdateConnection",
		"UpdateDevEndpoint",
		"UpdateGlueIdentityCenterConfiguration",
		"UpdateIntegrationResourceProperty",
		"UpdateIntegrationTableProperties",
		"UpdateJobFromSourceControl",
		"UpdateMLTransform",
		"UpdatePartition",
		"UpdateRegistry",
		"UpdateSchema",
		"UpdateSourceControlFromJob",
		"UpdateTableOptimizer",
		"UpdateTrigger",
		"UpdateUsageProfile",
		"UpdateUserDefinedFunction",
		"UpdateWorkflow",
	}

	h := newTestHandler(t)

	for _, action := range stubOps {
		t.Run(action, func(t *testing.T) {
			t.Parallel()
			rec := doGlueRequest(t, h, action, map[string]any{})
			assert.NotEqual(t, http.StatusInternalServerError, rec.Code, "action %s should not panic", action)
		})
	}
}
