package glue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	glueTargetPrefix = "AWSGlue."
	unknownAction    = "Unknown"
)

var errUnknownAction = errors.New("UnknownOperationException")

// Handler is the Echo HTTP handler for AWS Glue operations.
type Handler struct {
	Backend StorageBackend
	// ops is the pre-built dispatch table mapping operation names to handler
	// functions, initialized in NewHandler.
	ops map[string]service.JSONOpFunc
}

// NewHandler creates a new Glue handler backed by backend.
func NewHandler(backend StorageBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Reset clears all backend state. Used for test isolation.
func (h *Handler) Reset() { h.Backend.Reset() }

// Name returns the service name.
func (h *Handler) Name() string { return glueServiceName }

// GetSupportedOperations returns the list of supported Glue operations.
func (h *Handler) GetSupportedOperations() []string { //nolint:funlen
	return []string{
		"BatchCreatePartition",
		"BatchDeleteConnection",
		"BatchDeletePartition",
		"BatchDeleteTable",
		"BatchDeleteTableVersion",
		"BatchGetBlueprints",
		"BatchGetCrawlers",
		"BatchGetCustomEntityTypes",
		"BatchGetDataQualityResult",
		"BatchGetDevEndpoints",
		"BatchGetJobs",
		"BatchGetPartition",
		"BatchGetTableOptimizer",
		"BatchGetTriggers",
		"BatchGetWorkflows",
		"BatchPutDataQualityStatisticAnnotation",
		"BatchStopJobRun",
		"BatchUpdatePartition",
		"CancelDataQualityRuleRecommendationRun",
		"CancelDataQualityRulesetEvaluationRun",
		"CancelMLTaskRun",
		"CancelStatement",
		"CheckSchemaVersionValidity",
		"CreateBlueprint",
		"CreateCatalog",
		"CreateClassifier",
		"CreateColumnStatisticsTaskSettings",
		"CreateConnection",
		"CreateCrawler",
		"CreateCustomEntityType",
		"CreateDatabase",
		"CreateDataQualityRuleset",
		"CreateDevEndpoint",
		"CreateGlueIdentityCenterConfiguration",
		"CreateIntegration",
		"CreateIntegrationResourceProperty",
		"CreateIntegrationTableProperties",
		"CreateJob",
		"CreateMLTransform",
		"CreatePartition",
		"CreatePartitionIndex",
		"CreateRegistry",
		"CreateSchema",
		"CreateScript",
		"CreateSecurityConfiguration",
		"CreateSession",
		"CreateTable",
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
		"DeleteConnection",
		"DeleteConnectionType",
		"DeleteCrawler",
		"DeleteCustomEntityType",
		"DeleteDatabase",
		"DeleteDataQualityRuleset",
		"DeleteDevEndpoint",
		"DeleteGlueIdentityCenterConfiguration",
		"DeleteIntegration",
		"DeleteIntegrationResourceProperty",
		"DeleteIntegrationTableProperties",
		"DeleteJob",
		"DeleteMLTransform",
		"DeletePartition",
		"DeletePartitionIndex",
		"DeleteRegistry",
		"DeleteResourcePolicy",
		"DeleteSchema",
		"DeleteSchemaVersions",
		"DeleteSecurityConfiguration",
		"DeleteSession",
		"DeleteTable",
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
		"GetConnection",
		"GetConnections",
		"GetCrawler",
		"GetCrawlerMetrics",
		"GetCrawlers",
		"GetCustomEntityType",
		"GetDataCatalogEncryptionSettings",
		"GetDataQualityModel",
		"GetDataQualityModelResult",
		"GetDataQualityResult",
		"GetDataQualityRuleRecommendationRun",
		"GetDataQualityRuleset",
		"GetDataQualityRulesetEvaluationRun",
		"GetDataflowGraph",
		"GetDatabase",
		"GetDatabases",
		"GetDevEndpoint",
		"GetDevEndpoints",
		"GetEntityRecords",
		"GetGlueIdentityCenterConfiguration",
		"GetIntegrationResourceProperty",
		"GetIntegrationTableProperties",
		"GetJob",
		"GetJobBookmark",
		"GetJobRun",
		"GetJobRuns",
		"GetJobs",
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
		"GetTable",
		"GetTableOptimizer",
		"GetTableVersion",
		"GetTableVersions",
		"GetTables",
		"GetTags",
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
		"ListCrawlers",
		"ListCrawls",
		"ListCustomEntityTypes",
		"ListDataQualityResults",
		"ListDataQualityRuleRecommendationRuns",
		"ListDataQualityRulesetEvaluationRuns",
		"ListDataQualityRulesets",
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
		"ResetJobBookmark",
		"ResumeWorkflowRun",
		"RunStatement",
		"SearchTables",
		"StartBlueprintRun",
		"StartColumnStatisticsTaskRun",
		"StartColumnStatisticsTaskRunSchedule",
		"StartCrawler",
		"StartCrawlerSchedule",
		"StartDataQualityRuleRecommendationRun",
		"StartDataQualityRulesetEvaluationRun",
		"StartExportLabelsTaskRun",
		"StartImportLabelsTaskRun",
		"StartJobRun",
		"StartMLEvaluationTaskRun",
		"StartMLLabelingSetGenerationTaskRun",
		"StartMaterializedViewRefreshTaskRun",
		"StartTrigger",
		"StartWorkflowRun",
		"StopColumnStatisticsTaskRun",
		"StopColumnStatisticsTaskRunSchedule",
		"StopCrawler",
		"StopCrawlerSchedule",
		"StopMaterializedViewRefreshTaskRun",
		"StopSession",
		"StopTrigger",
		"StopWorkflowRun",
		"TagResource",
		"TestConnection",
		"UntagResource",
		"UpdateBlueprint",
		"UpdateCatalog",
		"UpdateClassifier",
		"UpdateColumnStatisticsForPartition",
		"UpdateColumnStatisticsForTable",
		"UpdateColumnStatisticsTaskSettings",
		"UpdateConnection",
		"UpdateCrawler",
		"UpdateCrawlerSchedule",
		"UpdateDatabase",
		"UpdateDataQualityRuleset",
		"UpdateDevEndpoint",
		"UpdateGlueIdentityCenterConfiguration",
		"UpdateIntegrationResourceProperty",
		"UpdateIntegrationTableProperties",
		"UpdateJob",
		"UpdateJobFromSourceControl",
		"UpdateMLTransform",
		"UpdatePartition",
		"UpdateRegistry",
		"UpdateSchema",
		"UpdateSourceControlFromJob",
		"UpdateTable",
		"UpdateTableOptimizer",
		"UpdateTrigger",
		"UpdateUsageProfile",
		"UpdateUserDefinedFunction",
		"UpdateWorkflow",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "glue" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches Glue requests via X-Amz-Target.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, glueTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation returns the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, glueTargetPrefix)

	if action == "" || action == target {
		return unknownAction
	}

	return action
}

// ExtractResource extracts a resource identifier from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req struct {
		Name           string `json:"Name"`
		DatabaseName   string `json:"DatabaseName"`
		ResourceArn    string `json:"ResourceArn"`
		CrawlerName    string `json:"CrawlerName"`
		JobName        string `json:"JobName"`
		ConnectionName string `json:"ConnectionName"`
	}

	_ = json.Unmarshal(body, &req)

	switch {
	case req.ResourceArn != "":
		return req.ResourceArn
	case req.Name != "":
		return req.Name
	case req.CrawlerName != "":
		return req.CrawlerName
	case req.JobName != "":
		return req.JobName
	case req.ConnectionName != "":
		return req.ConnectionName
	case req.DatabaseName != "":
		return req.DatabaseName
	}

	return ""
}

// Handler returns the Echo handler function for Glue requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			glueServiceName, "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc { //nolint:funlen
	return map[string]service.JSONOpFunc{
		"BatchCreatePartition":                   service.WrapOp(h.handleBatchCreatePartition),
		"BatchDeleteConnection":                  service.WrapOp(h.handleBatchDeleteConnection),
		"BatchDeletePartition":                   service.WrapOp(h.handleBatchDeletePartition),
		"BatchDeleteTable":                       service.WrapOp(h.handleBatchDeleteTable),
		"BatchDeleteTableVersion":                service.WrapOp(h.handleBatchDeleteTableVersion),
		"BatchGetBlueprints":                     service.WrapOp(h.handleBatchGetBlueprints),
		"BatchGetCrawlers":                       service.WrapOp(h.handleBatchGetCrawlers),
		"BatchGetCustomEntityTypes":              service.WrapOp(h.handleBatchGetCustomEntityTypes),
		"BatchGetDataQualityResult":              service.WrapOp(h.handleBatchGetDataQualityResult),
		"BatchGetDevEndpoints":                   service.WrapOp(h.handleBatchGetDevEndpoints),
		"BatchGetJobs":                           service.WrapOp(h.handleBatchGetJobs),
		"BatchGetPartition":                      service.WrapOp(h.handleBatchGetPartition),
		"BatchGetTableOptimizer":                 service.WrapOp(h.handleBatchGetTableOptimizer),
		"BatchGetTriggers":                       service.WrapOp(h.handleBatchGetTriggers),
		"BatchGetWorkflows":                      service.WrapOp(h.handleBatchGetWorkflows),
		"BatchPutDataQualityStatisticAnnotation": service.WrapOp(h.handleBatchPutDataQualityStatisticAnnotation),
		"BatchStopJobRun":                        service.WrapOp(h.handleBatchStopJobRun),
		"BatchUpdatePartition":                   service.WrapOp(h.handleBatchUpdatePartition),
		"CancelDataQualityRuleRecommendationRun": service.WrapOp(h.handleCancelDataQualityRuleRecommendationRun),
		"CancelDataQualityRulesetEvaluationRun":  service.WrapOp(h.handleCancelDataQualityRulesetEvaluationRun),
		"CancelMLTaskRun":                        service.WrapOp(h.handleCancelMLTaskRun),
		"CancelStatement":                        service.WrapOp(h.handleCancelStatement),
		"CheckSchemaVersionValidity":             service.WrapOp(h.handleCheckSchemaVersionValidity),
		"CreateBlueprint":                        service.WrapOp(h.handleCreateBlueprint),
		"CreateCatalog":                          service.WrapOp(h.handleCreateCatalog),
		"CreateClassifier":                       service.WrapOp(h.handleCreateClassifier),
		"CreateColumnStatisticsTaskSettings":     service.WrapOp(h.handleCreateColumnStatisticsTaskSettings),
		"CreateConnection":                       service.WrapOp(h.handleCreateConnection),
		"CreateCrawler":                          service.WrapOp(h.handleCreateCrawler),
		"CreateCustomEntityType":                 service.WrapOp(h.handleCreateCustomEntityType),
		"CreateDatabase":                         service.WrapOp(h.handleCreateDatabase),
		"CreateDataQualityRuleset":               service.WrapOp(h.handleCreateDataQualityRuleset),
		"CreateDevEndpoint":                      service.WrapOp(h.handleCreateDevEndpoint),
		"CreateGlueIdentityCenterConfiguration":  service.WrapOp(h.handleCreateGlueIdentityCenterConfiguration),
		"CreateIntegration":                      service.WrapOp(h.handleCreateIntegration),
		"CreateIntegrationResourceProperty":      service.WrapOp(h.handleCreateIntegrationResourceProperty),
		"CreateIntegrationTableProperties":       service.WrapOp(h.handleCreateIntegrationTableProperties),
		"CreateJob":                              service.WrapOp(h.handleCreateJob),
		"CreateMLTransform":                      service.WrapOp(h.handleCreateMLTransform),
		"CreatePartition":                        service.WrapOp(h.handleCreatePartition),
		"CreatePartitionIndex":                   service.WrapOp(h.handleCreatePartitionIndex),
		"CreateRegistry":                         service.WrapOp(h.handleCreateRegistry),
		"CreateSchema":                           service.WrapOp(h.handleCreateSchema),
		"CreateScript":                           service.WrapOp(h.handleCreateScript),
		"CreateSecurityConfiguration":            service.WrapOp(h.handleCreateSecurityConfiguration),
		"CreateSession":                          service.WrapOp(h.handleCreateSession),
		"CreateTable":                            service.WrapOp(h.handleCreateTable),
		"CreateTableOptimizer":                   service.WrapOp(h.handleCreateTableOptimizer),
		"CreateTrigger":                          service.WrapOp(h.handleCreateTrigger),
		"CreateUsageProfile":                     service.WrapOp(h.handleCreateUsageProfile),
		"CreateUserDefinedFunction":              service.WrapOp(h.handleCreateUserDefinedFunction),
		"CreateWorkflow":                         service.WrapOp(h.handleCreateWorkflow),
		"DeleteBlueprint":                        service.WrapOp(h.handleDeleteBlueprint),
		"DeleteCatalog":                          service.WrapOp(h.handleDeleteCatalog),
		"DeleteClassifier":                       service.WrapOp(h.handleDeleteClassifier),
		"DeleteColumnStatisticsForPartition":     service.WrapOp(h.handleDeleteColumnStatisticsForPartition),
		"DeleteColumnStatisticsForTable":         service.WrapOp(h.handleDeleteColumnStatisticsForTable),
		"DeleteColumnStatisticsTaskSettings":     service.WrapOp(h.handleDeleteColumnStatisticsTaskSettings),
		"DeleteConnection":                       service.WrapOp(h.handleDeleteConnection),
		"DeleteConnectionType":                   service.WrapOp(h.handleDeleteConnectionType),
		"DeleteCrawler":                          service.WrapOp(h.handleDeleteCrawler),
		"DeleteCustomEntityType":                 service.WrapOp(h.handleDeleteCustomEntityType),
		"DeleteDatabase":                         service.WrapOp(h.handleDeleteDatabase),
		"DeleteDataQualityRuleset":               service.WrapOp(h.handleDeleteDataQualityRuleset),
		"DeleteDevEndpoint":                      service.WrapOp(h.handleDeleteDevEndpoint),
		"DeleteGlueIdentityCenterConfiguration":  service.WrapOp(h.handleDeleteGlueIdentityCenterConfiguration),
		"DeleteIntegration":                      service.WrapOp(h.handleDeleteIntegration),
		"DeleteIntegrationResourceProperty":      service.WrapOp(h.handleDeleteIntegrationResourceProperty),
		"DeleteIntegrationTableProperties":       service.WrapOp(h.handleDeleteIntegrationTableProperties),
		"DeleteJob":                              service.WrapOp(h.handleDeleteJob),
		"DeleteMLTransform":                      service.WrapOp(h.handleDeleteMLTransform),
		"DeletePartition":                        service.WrapOp(h.handleDeletePartition),
		"DeletePartitionIndex":                   service.WrapOp(h.handleDeletePartitionIndex),
		"DeleteRegistry":                         service.WrapOp(h.handleDeleteRegistry),
		"DeleteResourcePolicy":                   service.WrapOp(h.handleDeleteResourcePolicy),
		"DeleteSchema":                           service.WrapOp(h.handleDeleteSchema),
		"DeleteSchemaVersions":                   service.WrapOp(h.handleDeleteSchemaVersions),
		"DeleteSecurityConfiguration":            service.WrapOp(h.handleDeleteSecurityConfiguration),
		"DeleteSession":                          service.WrapOp(h.handleDeleteSession),
		"DeleteTable":                            service.WrapOp(h.handleDeleteTable),
		"DeleteTableOptimizer":                   service.WrapOp(h.handleDeleteTableOptimizer),
		"DeleteTableVersion":                     service.WrapOp(h.handleDeleteTableVersion),
		"DeleteTrigger":                          service.WrapOp(h.handleDeleteTrigger),
		"DeleteUsageProfile":                     service.WrapOp(h.handleDeleteUsageProfile),
		"DeleteUserDefinedFunction":              service.WrapOp(h.handleDeleteUserDefinedFunction),
		"DeleteWorkflow":                         service.WrapOp(h.handleDeleteWorkflow),
		"DescribeConnectionType":                 service.WrapOp(h.handleDescribeConnectionType),
		"DescribeEntity":                         service.WrapOp(h.handleDescribeEntity),
		"DescribeInboundIntegrations":            service.WrapOp(h.handleDescribeInboundIntegrations),
		"DescribeIntegrations":                   service.WrapOp(h.handleDescribeIntegrations),
		"GetBlueprint":                           service.WrapOp(h.handleGetBlueprint),
		"GetBlueprintRun":                        service.WrapOp(h.handleGetBlueprintRun),
		"GetBlueprintRuns":                       service.WrapOp(h.handleGetBlueprintRuns),
		"GetCatalog":                             service.WrapOp(h.handleGetCatalog),
		"GetCatalogImportStatus":                 service.WrapOp(h.handleGetCatalogImportStatus),
		"GetCatalogs":                            service.WrapOp(h.handleGetCatalogs),
		"GetClassifier":                          service.WrapOp(h.handleGetClassifier),
		"GetClassifiers":                         service.WrapOp(h.handleGetClassifiers),
		"GetColumnStatisticsForPartition":        service.WrapOp(h.handleGetColumnStatisticsForPartition),
		"GetColumnStatisticsForTable":            service.WrapOp(h.handleGetColumnStatisticsForTable),
		"GetColumnStatisticsTaskRun":             service.WrapOp(h.handleGetColumnStatisticsTaskRun),
		"GetColumnStatisticsTaskRuns":            service.WrapOp(h.handleGetColumnStatisticsTaskRuns),
		"GetColumnStatisticsTaskSettings":        service.WrapOp(h.handleGetColumnStatisticsTaskSettings),
		"GetConnection":                          service.WrapOp(h.handleGetConnection),
		"GetConnections":                         service.WrapOp(h.handleGetConnections),
		"GetCrawler":                             service.WrapOp(h.handleGetCrawler),
		"GetCrawlerMetrics":                      service.WrapOp(h.handleGetCrawlerMetrics),
		"GetCrawlers":                            service.WrapOp(h.handleGetCrawlers),
		"GetCustomEntityType":                    service.WrapOp(h.handleGetCustomEntityType),
		"GetDataCatalogEncryptionSettings":       service.WrapOp(h.handleGetDataCatalogEncryptionSettings),
		"GetDataQualityModel":                    service.WrapOp(h.handleGetDataQualityModel),
		"GetDataQualityModelResult":              service.WrapOp(h.handleGetDataQualityModelResult),
		"GetDataQualityResult":                   service.WrapOp(h.handleGetDataQualityResult),
		"GetDataQualityRuleRecommendationRun":    service.WrapOp(h.handleGetDataQualityRuleRecommendationRun),
		"GetDataQualityRuleset":                  service.WrapOp(h.handleGetDataQualityRuleset),
		"GetDataQualityRulesetEvaluationRun":     service.WrapOp(h.handleGetDataQualityRulesetEvaluationRun),
		"GetDataflowGraph":                       service.WrapOp(h.handleGetDataflowGraph),
		"GetDatabase":                            service.WrapOp(h.handleGetDatabase),
		"GetDatabases":                           service.WrapOp(h.handleGetDatabases),
		"GetDevEndpoint":                         service.WrapOp(h.handleGetDevEndpoint),
		"GetDevEndpoints":                        service.WrapOp(h.handleGetDevEndpoints),
		"GetEntityRecords":                       service.WrapOp(h.handleGetEntityRecords),
		"GetGlueIdentityCenterConfiguration":     service.WrapOp(h.handleGetGlueIdentityCenterConfiguration),
		"GetIntegrationResourceProperty":         service.WrapOp(h.handleGetIntegrationResourceProperty),
		"GetIntegrationTableProperties":          service.WrapOp(h.handleGetIntegrationTableProperties),
		"GetJob":                                 service.WrapOp(h.handleGetJob),
		"GetJobBookmark":                         service.WrapOp(h.handleGetJobBookmark),
		"GetJobRun":                              service.WrapOp(h.handleGetJobRun),
		"GetJobRuns":                             service.WrapOp(h.handleGetJobRuns),
		"GetJobs":                                service.WrapOp(h.handleGetJobs),
		"GetMLTaskRun":                           service.WrapOp(h.handleGetMLTaskRun),
		"GetMLTaskRuns":                          service.WrapOp(h.handleGetMLTaskRuns),
		"GetMLTransform":                         service.WrapOp(h.handleGetMLTransform),
		"GetMLTransforms":                        service.WrapOp(h.handleGetMLTransforms),
		"GetMapping":                             service.WrapOp(h.handleGetMapping),
		"GetMaterializedViewRefreshTaskRun":      service.WrapOp(h.handleGetMaterializedViewRefreshTaskRun),
		"GetPartition":                           service.WrapOp(h.handleGetPartition),
		"GetPartitionIndexes":                    service.WrapOp(h.handleGetPartitionIndexes),
		"GetPartitions":                          service.WrapOp(h.handleGetPartitions),
		"GetPlan":                                service.WrapOp(h.handleGetPlan),
		"GetRegistry":                            service.WrapOp(h.handleGetRegistry),
		"GetResourcePolicies":                    service.WrapOp(h.handleGetResourcePolicies),
		"GetResourcePolicy":                      service.WrapOp(h.handleGetResourcePolicy),
		"GetSchema":                              service.WrapOp(h.handleGetSchema),
		"GetSchemaByDefinition":                  service.WrapOp(h.handleGetSchemaByDefinition),
		"GetSchemaVersion":                       service.WrapOp(h.handleGetSchemaVersion),
		"GetSchemaVersionsDiff":                  service.WrapOp(h.handleGetSchemaVersionsDiff),
		"GetSecurityConfiguration":               service.WrapOp(h.handleGetSecurityConfiguration),
		"GetSecurityConfigurations":              service.WrapOp(h.handleGetSecurityConfigurations),
		"GetSession":                             service.WrapOp(h.handleGetSession),
		"GetStatement":                           service.WrapOp(h.handleGetStatement),
		"GetTable":                               service.WrapOp(h.handleGetTable),
		"GetTableOptimizer":                      service.WrapOp(h.handleGetTableOptimizer),
		"GetTableVersion":                        service.WrapOp(h.handleGetTableVersion),
		"GetTableVersions":                       service.WrapOp(h.handleGetTableVersions),
		"GetTables":                              service.WrapOp(h.handleGetTables),
		"GetTags":                                service.WrapOp(h.handleGetTags),
		"GetTrigger":                             service.WrapOp(h.handleGetTrigger),
		"GetTriggers":                            service.WrapOp(h.handleGetTriggers),
		"GetUnfilteredPartitionMetadata":         service.WrapOp(h.handleGetUnfilteredPartitionMetadata),
		"GetUnfilteredPartitionsMetadata":        service.WrapOp(h.handleGetUnfilteredPartitionsMetadata),
		"GetUnfilteredTableMetadata":             service.WrapOp(h.handleGetUnfilteredTableMetadata),
		"GetUsageProfile":                        service.WrapOp(h.handleGetUsageProfile),
		"GetUserDefinedFunction":                 service.WrapOp(h.handleGetUserDefinedFunction),
		"GetUserDefinedFunctions":                service.WrapOp(h.handleGetUserDefinedFunctions),
		"GetWorkflow":                            service.WrapOp(h.handleGetWorkflow),
		"GetWorkflowRun":                         service.WrapOp(h.handleGetWorkflowRun),
		"GetWorkflowRunProperties":               service.WrapOp(h.handleGetWorkflowRunProperties),
		"GetWorkflowRuns":                        service.WrapOp(h.handleGetWorkflowRuns),
		"ImportCatalogToGlue":                    service.WrapOp(h.handleImportCatalogToGlue),
		"ListBlueprints":                         service.WrapOp(h.handleListBlueprints),
		"ListColumnStatisticsTaskRuns":           service.WrapOp(h.handleListColumnStatisticsTaskRuns),
		"ListConnectionTypes":                    service.WrapOp(h.handleListConnectionTypes),
		"ListCrawlers":                           service.WrapOp(h.handleListCrawlers),
		"ListCrawls":                             service.WrapOp(h.handleListCrawls),
		"ListCustomEntityTypes":                  service.WrapOp(h.handleListCustomEntityTypes),
		"ListDataQualityResults":                 service.WrapOp(h.handleListDataQualityResults),
		"ListDataQualityRuleRecommendationRuns":  service.WrapOp(h.handleListDataQualityRuleRecommendationRuns),
		"ListDataQualityRulesetEvaluationRuns":   service.WrapOp(h.handleListDataQualityRulesetEvaluationRuns),
		"ListDataQualityRulesets":                service.WrapOp(h.handleListDataQualityRulesets),
		"ListDataQualityStatisticAnnotations":    service.WrapOp(h.handleListDataQualityStatisticAnnotations),
		"ListDataQualityStatistics":              service.WrapOp(h.handleListDataQualityStatistics),
		"ListDevEndpoints":                       service.WrapOp(h.handleListDevEndpoints),
		"ListEntities":                           service.WrapOp(h.handleListEntities),
		"ListIntegrationResourceProperties":      service.WrapOp(h.handleListIntegrationResourceProperties),
		"ListJobs":                               service.WrapOp(h.handleListJobs),
		"ListMLTransforms":                       service.WrapOp(h.handleListMLTransforms),
		"ListMaterializedViewRefreshTaskRuns":    service.WrapOp(h.handleListMaterializedViewRefreshTaskRuns),
		"ListRegistries":                         service.WrapOp(h.handleListRegistries),
		"ListSchemaVersions":                     service.WrapOp(h.handleListSchemaVersions),
		"ListSchemas":                            service.WrapOp(h.handleListSchemas),
		"ListSessions":                           service.WrapOp(h.handleListSessions),
		"ListStatements":                         service.WrapOp(h.handleListStatements),
		"ListTableOptimizerRuns":                 service.WrapOp(h.handleListTableOptimizerRuns),
		"ListTriggers":                           service.WrapOp(h.handleListTriggers),
		"ListUsageProfiles":                      service.WrapOp(h.handleListUsageProfiles),
		"ListWorkflows":                          service.WrapOp(h.handleListWorkflows),
		"ModifyIntegration":                      service.WrapOp(h.handleModifyIntegration),
		"PutDataCatalogEncryptionSettings":       service.WrapOp(h.handlePutDataCatalogEncryptionSettings),
		"PutDataQualityProfileAnnotation":        service.WrapOp(h.handlePutDataQualityProfileAnnotation),
		"PutResourcePolicy":                      service.WrapOp(h.handlePutResourcePolicy),
		"PutSchemaVersionMetadata":               service.WrapOp(h.handlePutSchemaVersionMetadata),
		"PutWorkflowRunProperties":               service.WrapOp(h.handlePutWorkflowRunProperties),
		"QuerySchemaVersionMetadata":             service.WrapOp(h.handleQuerySchemaVersionMetadata),
		"RegisterConnectionType":                 service.WrapOp(h.handleRegisterConnectionType),
		"RegisterSchemaVersion":                  service.WrapOp(h.handleRegisterSchemaVersion),
		"RemoveSchemaVersionMetadata":            service.WrapOp(h.handleRemoveSchemaVersionMetadata),
		"ResetJobBookmark":                       service.WrapOp(h.handleResetJobBookmark),
		"ResumeWorkflowRun":                      service.WrapOp(h.handleResumeWorkflowRun),
		"RunStatement":                           service.WrapOp(h.handleRunStatement),
		"SearchTables":                           service.WrapOp(h.handleSearchTables),
		"StartBlueprintRun":                      service.WrapOp(h.handleStartBlueprintRun),
		"StartColumnStatisticsTaskRun":           service.WrapOp(h.handleStartColumnStatisticsTaskRun),
		"StartColumnStatisticsTaskRunSchedule":   service.WrapOp(h.handleStartColumnStatisticsTaskRunSchedule),
		"StartCrawler":                           service.WrapOp(h.handleStartCrawler),
		"StartCrawlerSchedule":                   service.WrapOp(h.handleStartCrawlerSchedule),
		"StartDataQualityRuleRecommendationRun":  service.WrapOp(h.handleStartDataQualityRuleRecommendationRun),
		"StartDataQualityRulesetEvaluationRun":   service.WrapOp(h.handleStartDataQualityRulesetEvaluationRun),
		"StartExportLabelsTaskRun":               service.WrapOp(h.handleStartExportLabelsTaskRun),
		"StartImportLabelsTaskRun":               service.WrapOp(h.handleStartImportLabelsTaskRun),
		"StartJobRun":                            service.WrapOp(h.handleStartJobRun),
		"StartMLEvaluationTaskRun":               service.WrapOp(h.handleStartMLEvaluationTaskRun),
		"StartMLLabelingSetGenerationTaskRun":    service.WrapOp(h.handleStartMLLabelingSetGenerationTaskRun),
		"StartMaterializedViewRefreshTaskRun":    service.WrapOp(h.handleStartMaterializedViewRefreshTaskRun),
		"StartTrigger":                           service.WrapOp(h.handleStartTrigger),
		"StartWorkflowRun":                       service.WrapOp(h.handleStartWorkflowRun),
		"StopColumnStatisticsTaskRun":            service.WrapOp(h.handleStopColumnStatisticsTaskRun),
		"StopColumnStatisticsTaskRunSchedule":    service.WrapOp(h.handleStopColumnStatisticsTaskRunSchedule),
		"StopCrawler":                            service.WrapOp(h.handleStopCrawler),
		"StopCrawlerSchedule":                    service.WrapOp(h.handleStopCrawlerSchedule),
		"StopMaterializedViewRefreshTaskRun":     service.WrapOp(h.handleStopMaterializedViewRefreshTaskRun),
		"StopSession":                            service.WrapOp(h.handleStopSession),
		"StopTrigger":                            service.WrapOp(h.handleStopTrigger),
		"StopWorkflowRun":                        service.WrapOp(h.handleStopWorkflowRun),
		"TagResource":                            service.WrapOp(h.handleTagResource),
		"TestConnection":                         service.WrapOp(h.handleTestConnection),
		"UntagResource":                          service.WrapOp(h.handleUntagResource),
		"UpdateBlueprint":                        service.WrapOp(h.handleUpdateBlueprint),
		"UpdateCatalog":                          service.WrapOp(h.handleUpdateCatalog),
		"UpdateClassifier":                       service.WrapOp(h.handleUpdateClassifier),
		"UpdateColumnStatisticsForPartition":     service.WrapOp(h.handleUpdateColumnStatisticsForPartition),
		"UpdateColumnStatisticsForTable":         service.WrapOp(h.handleUpdateColumnStatisticsForTable),
		"UpdateColumnStatisticsTaskSettings":     service.WrapOp(h.handleUpdateColumnStatisticsTaskSettings),
		"UpdateConnection":                       service.WrapOp(h.handleUpdateConnection),
		"UpdateCrawler":                          service.WrapOp(h.handleUpdateCrawler),
		"UpdateCrawlerSchedule":                  service.WrapOp(h.handleUpdateCrawlerSchedule),
		"UpdateDatabase":                         service.WrapOp(h.handleUpdateDatabase),
		"UpdateDataQualityRuleset":               service.WrapOp(h.handleUpdateDataQualityRuleset),
		"UpdateDevEndpoint":                      service.WrapOp(h.handleUpdateDevEndpoint),
		"UpdateGlueIdentityCenterConfiguration":  service.WrapOp(h.handleUpdateGlueIdentityCenterConfiguration),
		"UpdateIntegrationResourceProperty":      service.WrapOp(h.handleUpdateIntegrationResourceProperty),
		"UpdateIntegrationTableProperties":       service.WrapOp(h.handleUpdateIntegrationTableProperties),
		"UpdateJob":                              service.WrapOp(h.handleUpdateJob),
		"UpdateJobFromSourceControl":             service.WrapOp(h.handleUpdateJobFromSourceControl),
		"UpdateMLTransform":                      service.WrapOp(h.handleUpdateMLTransform),
		"UpdatePartition":                        service.WrapOp(h.handleUpdatePartition),
		"UpdateRegistry":                         service.WrapOp(h.handleUpdateRegistry),
		"UpdateSchema":                           service.WrapOp(h.handleUpdateSchema),
		"UpdateSourceControlFromJob":             service.WrapOp(h.handleUpdateSourceControlFromJob),
		"UpdateTable":                            service.WrapOp(h.handleUpdateTable),
		"UpdateTableOptimizer":                   service.WrapOp(h.handleUpdateTableOptimizer),
		"UpdateTrigger":                          service.WrapOp(h.handleUpdateTrigger),
		"UpdateUsageProfile":                     service.WrapOp(h.handleUpdateUsageProfile),
		"UpdateUserDefinedFunction":              service.WrapOp(h.handleUpdateUserDefinedFunction),
		"UpdateWorkflow":                         service.WrapOp(h.handleUpdateWorkflow),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	switch {
	case errors.Is(err, ErrCrawlerRunning):
		return c.JSON(http.StatusBadRequest, errorResponse("CrawlerRunningException", err.Error()))
	case errors.Is(err, ErrCrawlerNotRunning):
		return c.JSON(http.StatusBadRequest, errorResponse("CrawlerNotRunningException", err.Error()))
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusBadRequest, errorResponse("EntityNotFoundException", err.Error()))
	case errors.Is(err, awserr.ErrAlreadyExists):
		return c.JSON(http.StatusBadRequest, errorResponse("AlreadyExistsException", err.Error()))
	case errors.Is(err, awserr.ErrInvalidParameter):
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", err.Error()))
	case errors.Is(err, errUnknownAction):
		return c.JSON(http.StatusBadRequest, errorResponse("UnknownOperationException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}
}

func errorResponse(code, msg string) map[string]string {
	return map[string]string{"__type": code, "message": msg}
}

// --- Database handlers ---

type createDatabaseInput struct {
	Tags          map[string]string `json:"Tags,omitempty"`
	DatabaseInput DatabaseInput     `json:"DatabaseInput"`
}

type emptyOutput struct{}

func (h *Handler) handleCreateDatabase(_ context.Context, in *createDatabaseInput) (*emptyOutput, error) {
	if _, err := h.Backend.CreateDatabase(in.DatabaseInput, in.Tags); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type getDatabaseInput struct {
	Name string `json:"Name"`
}

type getDatabaseOutput struct {
	Database *Database `json:"Database"`
}

func (h *Handler) handleGetDatabase(_ context.Context, in *getDatabaseInput) (*getDatabaseOutput, error) {
	db, err := h.Backend.GetDatabase(in.Name)
	if err != nil {
		return nil, err
	}

	return &getDatabaseOutput{Database: db}, nil
}

type getDatabasesInput struct{}

type getDatabasesOutput struct {
	DatabaseList []*Database `json:"DatabaseList"`
}

func (h *Handler) handleGetDatabases(_ context.Context, _ *getDatabasesInput) (*getDatabasesOutput, error) {
	dbs := h.Backend.GetDatabases()

	return &getDatabasesOutput{DatabaseList: dbs}, nil
}

type updateDatabaseInput struct {
	Name          string        `json:"Name"`
	DatabaseInput DatabaseInput `json:"DatabaseInput"`
}

func (h *Handler) handleUpdateDatabase(_ context.Context, in *updateDatabaseInput) (*emptyOutput, error) {
	if err := h.Backend.UpdateDatabase(in.Name, in.DatabaseInput); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type deleteDatabaseInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteDatabase(_ context.Context, in *deleteDatabaseInput) (*emptyOutput, error) {
	if err := h.Backend.DeleteDatabase(in.Name); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- Table handlers ---

type createTableInput struct {
	DatabaseName string     `json:"DatabaseName"`
	TableInput   TableInput `json:"TableInput"`
}

func (h *Handler) handleCreateTable(_ context.Context, in *createTableInput) (*emptyOutput, error) {
	if _, err := h.Backend.CreateTable(in.DatabaseName, in.TableInput); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type getTableInput struct {
	DatabaseName string `json:"DatabaseName"`
	Name         string `json:"Name"`
}

type getTableOutput struct {
	Table *Table `json:"Table"`
}

func (h *Handler) handleGetTable(_ context.Context, in *getTableInput) (*getTableOutput, error) {
	t, err := h.Backend.GetTable(in.DatabaseName, in.Name)
	if err != nil {
		return nil, err
	}

	return &getTableOutput{Table: t}, nil
}

type getTablesInput struct {
	DatabaseName string `json:"DatabaseName"`
}

type getTablesOutput struct {
	TableList []*Table `json:"TableList"`
}

func (h *Handler) handleGetTables(_ context.Context, in *getTablesInput) (*getTablesOutput, error) {
	tables, err := h.Backend.GetTables(in.DatabaseName)
	if err != nil {
		return nil, err
	}

	return &getTablesOutput{TableList: tables}, nil
}

type updateTableInput struct {
	DatabaseName string     `json:"DatabaseName"`
	TableInput   TableInput `json:"TableInput"`
}

func (h *Handler) handleUpdateTable(_ context.Context, in *updateTableInput) (*emptyOutput, error) {
	if err := h.Backend.UpdateTable(in.DatabaseName, in.TableInput); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type deleteTableInput struct {
	DatabaseName string `json:"DatabaseName"`
	Name         string `json:"Name"`
}

func (h *Handler) handleDeleteTable(_ context.Context, in *deleteTableInput) (*emptyOutput, error) {
	if err := h.Backend.DeleteTable(in.DatabaseName, in.Name); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- Crawler handlers ---

type createCrawlerInput struct {
	Tags         map[string]string `json:"Tags,omitempty"`
	Name         string            `json:"Name"`
	Role         string            `json:"Role"`
	DatabaseName string            `json:"DatabaseName"`
	Targets      CrawlerTarget     `json:"Targets,omitzero"`
}

func (h *Handler) handleCreateCrawler(_ context.Context, in *createCrawlerInput) (*emptyOutput, error) {
	if _, err := h.Backend.CreateCrawler(in.Name, in.Role, in.DatabaseName, in.Targets, in.Tags); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type getCrawlerInput struct {
	Name string `json:"Name"`
}

type getCrawlerOutput struct {
	Crawler *Crawler `json:"Crawler"`
}

func (h *Handler) handleGetCrawler(_ context.Context, in *getCrawlerInput) (*getCrawlerOutput, error) {
	c, err := h.Backend.GetCrawler(in.Name)
	if err != nil {
		return nil, err
	}

	return &getCrawlerOutput{Crawler: c}, nil
}

type getCrawlersInput struct{}

type getCrawlersOutput struct {
	Crawlers []*Crawler `json:"Crawlers"`
}

func (h *Handler) handleGetCrawlers(_ context.Context, _ *getCrawlersInput) (*getCrawlersOutput, error) {
	crawlers := h.Backend.GetCrawlers()

	return &getCrawlersOutput{Crawlers: crawlers}, nil
}

type updateCrawlerInput struct {
	Name         string        `json:"Name"`
	Role         string        `json:"Role"`
	DatabaseName string        `json:"DatabaseName"`
	Targets      CrawlerTarget `json:"Targets,omitzero"`
}

func (h *Handler) handleUpdateCrawler(_ context.Context, in *updateCrawlerInput) (*emptyOutput, error) {
	if err := h.Backend.UpdateCrawler(in.Name, in.Role, in.DatabaseName, in.Targets); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type deleteCrawlerInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteCrawler(_ context.Context, in *deleteCrawlerInput) (*emptyOutput, error) {
	if err := h.Backend.DeleteCrawler(in.Name); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- Job handlers ---

type createJobInput struct {
	Tags              map[string]string `json:"Tags,omitempty"`
	DefaultArguments  map[string]string `json:"DefaultArguments,omitempty"`
	Command           JobCommand        `json:"Command,omitzero"`
	WorkerType        string            `json:"WorkerType,omitempty"`
	Role              string            `json:"Role,omitempty"`
	GlueVersion       string            `json:"GlueVersion,omitempty"`
	Name              string            `json:"Name"`
	Description       string            `json:"Description,omitempty"`
	Connections       ConnectionsList   `json:"Connections,omitzero"`
	NumberOfWorkers   int               `json:"NumberOfWorkers,omitempty"`
	MaxRetries        int               `json:"MaxRetries,omitempty"`
	Timeout           int               `json:"Timeout,omitempty"`
	ExecutionProperty ExecutionProperty `json:"ExecutionProperty,omitzero"`
}

type createJobOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleCreateJob(_ context.Context, in *createJobInput) (*createJobOutput, error) {
	j, err := h.Backend.CreateJob(Job{
		Name:              in.Name,
		Description:       in.Description,
		Role:              in.Role,
		Command:           in.Command,
		DefaultArguments:  in.DefaultArguments,
		GlueVersion:       in.GlueVersion,
		WorkerType:        in.WorkerType,
		NumberOfWorkers:   in.NumberOfWorkers,
		MaxRetries:        in.MaxRetries,
		Timeout:           in.Timeout,
		Tags:              in.Tags,
		ExecutionProperty: in.ExecutionProperty,
		Connections:       in.Connections,
	})
	if err != nil {
		return nil, err
	}

	return &createJobOutput{Name: j.Name}, nil
}

type getJobInput struct {
	JobName string `json:"JobName"`
}

type getJobOutput struct {
	Job *Job `json:"Job"`
}

func (h *Handler) handleGetJob(_ context.Context, in *getJobInput) (*getJobOutput, error) {
	j, err := h.Backend.GetJob(in.JobName)
	if err != nil {
		return nil, err
	}

	return &getJobOutput{Job: j}, nil
}

type getJobsInput struct{}

type getJobsOutput struct {
	Jobs []*Job `json:"Jobs"`
}

func (h *Handler) handleGetJobs(_ context.Context, _ *getJobsInput) (*getJobsOutput, error) {
	jobs := h.Backend.GetJobs()

	return &getJobsOutput{Jobs: jobs}, nil
}

// jobUpdatePayload models the allowed fields for Glue's JobUpdate shape.
// It intentionally omits create-only fields such as Name and Tags.
type jobUpdatePayload struct {
	DefaultArguments  map[string]string `json:"DefaultArguments,omitempty"`
	Command           JobCommand        `json:"Command,omitzero"`
	WorkerType        string            `json:"WorkerType,omitempty"`
	Role              string            `json:"Role,omitempty"`
	GlueVersion       string            `json:"GlueVersion,omitempty"`
	Description       string            `json:"Description,omitempty"`
	Connections       ConnectionsList   `json:"Connections,omitzero"`
	NumberOfWorkers   int               `json:"NumberOfWorkers,omitempty"`
	MaxRetries        int               `json:"MaxRetries,omitempty"`
	Timeout           int               `json:"Timeout,omitempty"`
	ExecutionProperty ExecutionProperty `json:"ExecutionProperty,omitzero"`
}

type updateJobInput struct {
	JobName   string           `json:"JobName"`
	JobUpdate jobUpdatePayload `json:"JobUpdate"`
}

type updateJobOutput struct {
	JobName string `json:"JobName"`
}

func (h *Handler) handleUpdateJob(_ context.Context, in *updateJobInput) (*updateJobOutput, error) {
	if err := h.Backend.UpdateJob(in.JobName, Job{
		Description:       in.JobUpdate.Description,
		Role:              in.JobUpdate.Role,
		Command:           in.JobUpdate.Command,
		DefaultArguments:  in.JobUpdate.DefaultArguments,
		GlueVersion:       in.JobUpdate.GlueVersion,
		WorkerType:        in.JobUpdate.WorkerType,
		NumberOfWorkers:   in.JobUpdate.NumberOfWorkers,
		MaxRetries:        in.JobUpdate.MaxRetries,
		Timeout:           in.JobUpdate.Timeout,
		ExecutionProperty: in.JobUpdate.ExecutionProperty,
		Connections:       in.JobUpdate.Connections,
	}); err != nil {
		return nil, err
	}

	return &updateJobOutput{JobName: in.JobName}, nil
}

type deleteJobInput struct {
	JobName string `json:"JobName"`
}

type deleteJobOutput struct {
	JobName string `json:"JobName"`
}

func (h *Handler) handleDeleteJob(_ context.Context, in *deleteJobInput) (*deleteJobOutput, error) {
	if err := h.Backend.DeleteJob(in.JobName); err != nil {
		return nil, err
	}

	return &deleteJobOutput{JobName: in.JobName}, nil
}

// --- Tag handlers ---

type tagResourceInput struct {
	TagsToAdd   map[string]string `json:"TagsToAdd"`
	ResourceArn string            `json:"ResourceArn"`
}

func (h *Handler) handleTagResource(_ context.Context, in *tagResourceInput) (*emptyOutput, error) {
	if err := h.Backend.TagResource(in.ResourceArn, in.TagsToAdd); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type untagResourceInput struct {
	ResourceArn  string   `json:"ResourceArn"`
	TagsToRemove []string `json:"TagsToRemove"`
}

func (h *Handler) handleUntagResource(_ context.Context, in *untagResourceInput) (*emptyOutput, error) {
	if err := h.Backend.UntagResource(in.ResourceArn, in.TagsToRemove); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type getTagsInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type getTagsOutput struct {
	Tags map[string]string `json:"Tags"`
}

func (h *Handler) handleGetTags(_ context.Context, in *getTagsInput) (*getTagsOutput, error) {
	tags, err := h.Backend.GetTags(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &getTagsOutput{Tags: tags}, nil
}

// --- Batch partition handlers ---

type batchCreatePartitionInput struct {
	DatabaseName       string           `json:"DatabaseName"`
	TableName          string           `json:"TableName"`
	PartitionInputList []PartitionInput `json:"PartitionInputList"`
}

type batchCreatePartitionOutput struct {
	Errors     []PartitionError `json:"Errors"`
	Partitions []*Partition     `json:"Partitions"`
}

func (h *Handler) handleBatchCreatePartition(
	_ context.Context,
	in *batchCreatePartitionInput,
) (*batchCreatePartitionOutput, error) {
	if len(in.PartitionInputList) > maxBatchCreatePartitions {
		return nil, fmt.Errorf("%w: too many partitions: maximum is %d", ErrValidation, maxBatchCreatePartitions)
	}

	created, errs := h.Backend.BatchCreatePartition(in.DatabaseName, in.TableName, in.PartitionInputList)

	return &batchCreatePartitionOutput{Partitions: created, Errors: errs}, nil
}

type batchDeletePartitionInput struct {
	DatabaseName       string               `json:"DatabaseName"`
	TableName          string               `json:"TableName"`
	PartitionsToDelete []PartitionValueList `json:"PartitionsToDelete"`
}

type batchDeletePartitionOutput struct {
	Errors []PartitionError `json:"Errors"`
}

func (h *Handler) handleBatchDeletePartition(
	_ context.Context,
	in *batchDeletePartitionInput,
) (*batchDeletePartitionOutput, error) {
	errs := h.Backend.BatchDeletePartition(in.DatabaseName, in.TableName, in.PartitionsToDelete)

	return &batchDeletePartitionOutput{Errors: errs}, nil
}

type batchDeleteTableInput struct {
	DatabaseName   string   `json:"DatabaseName"`
	TablesToDelete []string `json:"TablesToDelete"`
}

type batchDeleteTableOutput struct {
	Errors []TableError `json:"Errors"`
}

func (h *Handler) handleBatchDeleteTable(
	_ context.Context,
	in *batchDeleteTableInput,
) (*batchDeleteTableOutput, error) {
	errs := h.Backend.BatchDeleteTable(in.DatabaseName, in.TablesToDelete)

	return &batchDeleteTableOutput{Errors: errs}, nil
}

type batchDeleteTableVersionInput struct {
	DatabaseName string   `json:"DatabaseName"`
	TableName    string   `json:"TableName"`
	VersionIDs   []string `json:"VersionIds"`
}

type batchDeleteTableVersionOutput struct {
	Errors []TableVersionError `json:"Errors"`
}

func (h *Handler) handleBatchDeleteTableVersion(
	_ context.Context,
	in *batchDeleteTableVersionInput,
) (*batchDeleteTableVersionOutput, error) {
	errs := h.Backend.BatchDeleteTableVersion(in.DatabaseName, in.TableName, in.VersionIDs)

	return &batchDeleteTableVersionOutput{Errors: errs}, nil
}

// --- Batch connection handlers ---

type batchDeleteConnectionInput struct {
	ConnectionNameList []string `json:"ConnectionNameList"`
}

type batchDeleteConnectionOutput struct {
	Errors    []ErrorDetail `json:"Errors"`
	Succeeded []string      `json:"Succeeded"`
}

func (h *Handler) handleBatchDeleteConnection(
	_ context.Context,
	in *batchDeleteConnectionInput,
) (*batchDeleteConnectionOutput, error) {
	succeeded, errs := h.Backend.BatchDeleteConnection(in.ConnectionNameList)

	return &batchDeleteConnectionOutput{Succeeded: succeeded, Errors: errs}, nil
}

// --- Single connection handlers ---

type createConnectionInput struct {
	Tags            map[string]string `json:"Tags,omitempty"`
	ConnectionInput connectionInput   `json:"ConnectionInput"`
}

type connectionInput struct {
	ConnectionProperties map[string]string `json:"ConnectionProperties,omitempty"`
	Name                 string            `json:"Name"`
	ConnectionType       string            `json:"ConnectionType,omitempty"`
}

type createConnectionOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleCreateConnection(
	_ context.Context,
	in *createConnectionInput,
) (*createConnectionOutput, error) {
	c, err := h.Backend.CreateConnection(
		in.ConnectionInput.Name,
		in.ConnectionInput.ConnectionType,
		in.ConnectionInput.ConnectionProperties,
		in.Tags,
	)
	if err != nil {
		return nil, err
	}

	return &createConnectionOutput{Name: c.Name}, nil
}

type getConnectionInput struct {
	Name string `json:"Name"`
}

type getConnectionOutput struct {
	Connection *Connection `json:"Connection"`
}

func (h *Handler) handleGetConnection(
	_ context.Context,
	in *getConnectionInput,
) (*getConnectionOutput, error) {
	c, err := h.Backend.GetConnection(in.Name)
	if err != nil {
		return nil, err
	}

	return &getConnectionOutput{Connection: c}, nil
}

type getConnectionsInput struct{}

type getConnectionsOutput struct {
	ConnectionList []*Connection `json:"ConnectionList"`
}

func (h *Handler) handleGetConnections(
	_ context.Context,
	_ *getConnectionsInput,
) (*getConnectionsOutput, error) {
	conns := h.Backend.GetConnections()

	return &getConnectionsOutput{ConnectionList: conns}, nil
}

type deleteConnectionInput struct {
	ConnectionName string `json:"ConnectionName"`
}

func (h *Handler) handleDeleteConnection(
	_ context.Context,
	in *deleteConnectionInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteConnection(in.ConnectionName); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- Batch blueprint handlers ---

type batchGetBlueprintsInput struct {
	Names []string `json:"Names"`
}

type batchGetBlueprintsOutput struct {
	Blueprints        []*Blueprint `json:"Blueprints"`
	MissingBlueprints []string     `json:"MissingBlueprints"`
}

func (h *Handler) handleBatchGetBlueprints(
	_ context.Context,
	in *batchGetBlueprintsInput,
) (*batchGetBlueprintsOutput, error) {
	found, missing := h.Backend.BatchGetBlueprints(in.Names)

	return &batchGetBlueprintsOutput{Blueprints: found, MissingBlueprints: missing}, nil
}

// --- Batch crawler handlers ---

type batchGetCrawlersInput struct {
	CrawlerNames []string `json:"CrawlerNames"`
}

type batchGetCrawlersOutput struct {
	Crawlers         []*Crawler `json:"Crawlers"`
	CrawlersNotFound []string   `json:"CrawlersNotFound"`
}

func (h *Handler) handleBatchGetCrawlers(
	_ context.Context,
	in *batchGetCrawlersInput,
) (*batchGetCrawlersOutput, error) {
	found, missing := h.Backend.BatchGetCrawlers(in.CrawlerNames)

	return &batchGetCrawlersOutput{Crawlers: found, CrawlersNotFound: missing}, nil
}

// --- ListCrawlers handler ---

type listCrawlersInput struct{}

type listCrawlersOutput struct {
	CrawlerNames []string `json:"CrawlerNames"`
}

func (h *Handler) handleListCrawlers(
	_ context.Context,
	_ *listCrawlersInput,
) (*listCrawlersOutput, error) {
	names := h.Backend.ListCrawlers()

	return &listCrawlersOutput{CrawlerNames: names}, nil
}

// --- Batch custom entity type handlers ---

type batchGetCustomEntityTypesInput struct {
	Names []string `json:"Names"`
}

type batchGetCustomEntityTypesOutput struct {
	CustomEntityTypes         []*CustomEntityType `json:"CustomEntityTypes"`
	CustomEntityTypesNotFound []string            `json:"CustomEntityTypesNotFound"`
}

func (h *Handler) handleBatchGetCustomEntityTypes(
	_ context.Context,
	in *batchGetCustomEntityTypesInput,
) (*batchGetCustomEntityTypesOutput, error) {
	found, missing := h.Backend.BatchGetCustomEntityTypes(in.Names)

	return &batchGetCustomEntityTypesOutput{CustomEntityTypes: found, CustomEntityTypesNotFound: missing}, nil
}

// --- Batch data quality handlers ---

type batchGetDataQualityResultInput struct {
	ResultIDs []string `json:"ResultIds"`
}

type batchGetDataQualityResultOutput struct {
	Results         []DataQualityResult `json:"Results"`
	ResultsNotFound []ErrorDetail       `json:"ResultsNotFound"`
}

func (h *Handler) handleBatchGetDataQualityResult(
	_ context.Context,
	in *batchGetDataQualityResultInput,
) (*batchGetDataQualityResultOutput, error) {
	found, errs := h.Backend.BatchGetDataQualityResult(in.ResultIDs)
	results := make([]DataQualityResult, 0, len(found))

	for _, r := range found {
		results = append(results, *r)
	}

	return &batchGetDataQualityResultOutput{Results: results, ResultsNotFound: errs}, nil
}

// --- Batch dev endpoint handlers ---

type batchGetDevEndpointsInput struct {
	DevEndpointNames []string `json:"DevEndpointNames"`
}

type batchGetDevEndpointsOutput struct {
	DevEndpoints         []*DevEndpoint `json:"DevEndpoints"`
	DevEndpointsNotFound []string       `json:"DevEndpointsNotFound"`
}

func (h *Handler) handleBatchGetDevEndpoints(
	_ context.Context,
	in *batchGetDevEndpointsInput,
) (*batchGetDevEndpointsOutput, error) {
	found, missing := h.Backend.BatchGetDevEndpoints(in.DevEndpointNames)

	return &batchGetDevEndpointsOutput{DevEndpoints: found, DevEndpointsNotFound: missing}, nil
}

// --- Job run handlers ---

type startJobRunInput struct {
	Arguments map[string]string `json:"Arguments,omitempty"`
	JobName   string            `json:"JobName"`
}

type startJobRunOutput struct {
	JobRunID string `json:"JobRunId"`
}

func (h *Handler) handleStartJobRun(_ context.Context, in *startJobRunInput) (*startJobRunOutput, error) {
	run, err := h.Backend.StartJobRun(in.JobName, in.Arguments)
	if err != nil {
		return nil, err
	}

	return &startJobRunOutput{JobRunID: run.ID}, nil
}

type getJobRunInput struct {
	JobName string `json:"JobName"`
	RunID   string `json:"RunId"`
}

type getJobRunOutput struct {
	JobRun *JobRun `json:"JobRun"`
}

func (h *Handler) handleGetJobRun(_ context.Context, in *getJobRunInput) (*getJobRunOutput, error) {
	run, err := h.Backend.GetJobRun(in.JobName, in.RunID)
	if err != nil {
		return nil, err
	}

	return &getJobRunOutput{JobRun: run}, nil
}

type getJobRunsInput struct {
	JobName string `json:"JobName"`
}

type getJobRunsOutput struct {
	JobRuns []*JobRun `json:"JobRuns"`
}

func (h *Handler) handleGetJobRuns(_ context.Context, in *getJobRunsInput) (*getJobRunsOutput, error) {
	runs, err := h.Backend.GetJobRuns(in.JobName)
	if err != nil {
		return nil, err
	}

	return &getJobRunsOutput{JobRuns: runs}, nil
}

type batchStopJobRunInput struct {
	JobName   string   `json:"JobName"`
	JobRunIDs []string `json:"JobRunIds"`
}

type batchStopJobRunOutput struct {
	Errors []BatchStopJobRunError `json:"Errors"`
}

func (h *Handler) handleBatchStopJobRun(_ context.Context, in *batchStopJobRunInput) (*batchStopJobRunOutput, error) {
	errs := h.Backend.BatchStopJobRun(in.JobName, in.JobRunIDs)

	return &batchStopJobRunOutput{Errors: errs}, nil
}

type getJobBookmarkInput struct {
	JobName string `json:"JobName"`
}

type getJobBookmarkOutput struct {
	JobBookmarkEntry *JobBookmark `json:"JobBookmarkEntry"`
}

func (h *Handler) handleGetJobBookmark(_ context.Context, in *getJobBookmarkInput) (*getJobBookmarkOutput, error) {
	bm, err := h.Backend.GetJobBookmark(in.JobName)
	if err != nil {
		return nil, err
	}

	return &getJobBookmarkOutput{JobBookmarkEntry: bm}, nil
}

type resetJobBookmarkInput struct {
	JobName string `json:"JobName"`
}

type resetJobBookmarkOutput struct {
	JobBookmarkEntry *JobBookmark `json:"JobBookmarkEntry"`
}

func (h *Handler) handleResetJobBookmark(
	_ context.Context,
	in *resetJobBookmarkInput,
) (*resetJobBookmarkOutput, error) {
	bm, err := h.Backend.ResetJobBookmarkWithResult(in.JobName)
	if err != nil {
		return nil, err
	}

	return &resetJobBookmarkOutput{JobBookmarkEntry: bm}, nil
}

// --- Crawler scheduling handlers ---

type startCrawlerInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleStartCrawler(_ context.Context, in *startCrawlerInput) (*emptyOutput, error) {
	if err := h.Backend.StartCrawler(in.Name); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type stopCrawlerInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleStopCrawler(_ context.Context, in *stopCrawlerInput) (*emptyOutput, error) {
	if err := h.Backend.StopCrawler(in.Name); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type updateCrawlerScheduleInput struct {
	CrawlerName string `json:"CrawlerName"`
	Schedule    string `json:"Schedule"`
}

func (h *Handler) handleUpdateCrawlerSchedule(_ context.Context, in *updateCrawlerScheduleInput) (*emptyOutput, error) {
	if err := h.Backend.UpdateCrawlerSchedule(in.CrawlerName, in.Schedule); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type startCrawlerScheduleInput struct {
	CrawlerName string `json:"CrawlerName"`
}

func (h *Handler) handleStartCrawlerSchedule(_ context.Context, in *startCrawlerScheduleInput) (*emptyOutput, error) {
	if err := h.Backend.StartCrawlerSchedule(in.CrawlerName); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type stopCrawlerScheduleInput struct {
	CrawlerName string `json:"CrawlerName"`
}

func (h *Handler) handleStopCrawlerSchedule(_ context.Context, in *stopCrawlerScheduleInput) (*emptyOutput, error) {
	if err := h.Backend.StopCrawlerSchedule(in.CrawlerName); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- Data quality ruleset handlers ---

type createDataQualityRulesetInput struct {
	Tags    map[string]string `json:"Tags,omitempty"`
	Name    string            `json:"Name"`
	Ruleset string            `json:"Ruleset,omitempty"`
}

type createDataQualityRulesetOutput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleCreateDataQualityRuleset(
	_ context.Context,
	in *createDataQualityRulesetInput,
) (*createDataQualityRulesetOutput, error) {
	r, err := h.Backend.CreateDataQualityRuleset(in.Name, in.Ruleset, in.Tags)
	if err != nil {
		return nil, err
	}

	return &createDataQualityRulesetOutput{Name: r.Name}, nil
}

type getDataQualityRulesetInput struct {
	Name string `json:"Name"`
}

type getDataQualityRulesetOutput struct {
	Name           string  `json:"Name"`
	Ruleset        string  `json:"Ruleset,omitempty"`
	Description    string  `json:"Description,omitempty"`
	ARN            string  `json:"Arn,omitempty"`
	CreatedOn      float64 `json:"CreatedOn,omitempty"`
	LastModifiedOn float64 `json:"LastModifiedOn,omitempty"`
}

func (h *Handler) handleGetDataQualityRuleset(
	_ context.Context,
	in *getDataQualityRulesetInput,
) (*getDataQualityRulesetOutput, error) {
	r, err := h.Backend.GetDataQualityRuleset(in.Name)
	if err != nil {
		return nil, err
	}

	return &getDataQualityRulesetOutput{
		Name:           r.Name,
		Ruleset:        r.Ruleset,
		Description:    r.Description,
		ARN:            r.ARN,
		CreatedOn:      r.CreatedOn,
		LastModifiedOn: r.LastModifiedOn,
	}, nil
}

type deleteDataQualityRulesetInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteDataQualityRuleset(
	_ context.Context,
	in *deleteDataQualityRulesetInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteDataQualityRuleset(in.Name); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type updateDataQualityRulesetInput struct {
	Name    string `json:"Name"`
	Ruleset string `json:"Ruleset,omitempty"`
}

func (h *Handler) handleUpdateDataQualityRuleset(
	_ context.Context,
	in *updateDataQualityRulesetInput,
) (*emptyOutput, error) {
	if err := h.Backend.UpdateDataQualityRuleset(in.Name, in.Ruleset); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type listDataQualityRulesetsInput struct{}

type listDataQualityRulesetsOutput struct {
	Rulesets []*DataQualityRuleset `json:"Rulesets"`
}

func (h *Handler) handleListDataQualityRulesets(
	_ context.Context,
	_ *listDataQualityRulesetsInput,
) (*listDataQualityRulesetsOutput, error) {
	rulesets := h.Backend.ListDataQualityRulesets()

	return &listDataQualityRulesetsOutput{Rulesets: rulesets}, nil
}

type startDataQualityRulesetEvaluationRunInput struct {
	RulesetNames []string `json:"RulesetNames"`
}

type startDataQualityRulesetEvaluationRunOutput struct {
	RunID string `json:"RunId"`
}

func (h *Handler) handleStartDataQualityRulesetEvaluationRun(
	_ context.Context,
	in *startDataQualityRulesetEvaluationRunInput,
) (*startDataQualityRulesetEvaluationRunOutput, error) {
	run, err := h.Backend.StartDataQualityRulesetEvaluationRun(in.RulesetNames)
	if err != nil {
		return nil, err
	}

	return &startDataQualityRulesetEvaluationRunOutput{RunID: run.RunID}, nil
}

type getDataQualityRulesetEvaluationRunInput struct {
	RunID string `json:"RunId"`
}

type getDataQualityRulesetEvaluationRunOutput struct {
	DataQualityEvaluationRun *DataQualityEvaluationRun `json:"DataQualityEvaluationRun"`
}

func (h *Handler) handleGetDataQualityRulesetEvaluationRun(
	_ context.Context,
	in *getDataQualityRulesetEvaluationRunInput,
) (*getDataQualityRulesetEvaluationRunOutput, error) {
	run, err := h.Backend.GetDataQualityRulesetEvaluationRun(in.RunID)
	if err != nil {
		return nil, err
	}

	return &getDataQualityRulesetEvaluationRunOutput{DataQualityEvaluationRun: run}, nil
}

type cancelDataQualityRulesetEvaluationRunInput struct {
	RunID string `json:"RunId"`
}

func (h *Handler) handleCancelDataQualityRulesetEvaluationRun(
	_ context.Context,
	in *cancelDataQualityRulesetEvaluationRunInput,
) (*emptyOutput, error) {
	if err := h.Backend.CancelDataQualityRulesetEvaluationRun(in.RunID); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}
