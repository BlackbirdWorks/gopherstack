package glue

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// glueOpBindings is the single ordered source of truth for every supported
// Glue operation: its name (as used in GetSupportedOperations and routing) and
// how to bind it to a live Handler instance (as used in buildOps). A single
// data-driven table (rather than one entry per statement spread across two
// large function bodies) keeps both GetSupportedOperations and buildOps small
// regardless of how many operations Glue grows to.
//
//nolint:gochecknoglobals // registration table, analogous to tableRegistrations in store_setup.go
var glueOpBindings = []struct {
	bind func(*Handler) service.JSONOpFunc
	name string
}{
	{
		name: "AssociateGlossaryTerms",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleAssociateGlossaryTerms)
		},
	},
	{
		name: "BatchCreatePartition",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleBatchCreatePartition)
		},
	},
	{
		name: "BatchDeleteConnection",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleBatchDeleteConnection)
		},
	},
	{
		name: "BatchDeletePartition",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleBatchDeletePartition)
		},
	},
	{
		name: "BatchDeleteTable",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleBatchDeleteTable)
		},
	},
	{
		name: "BatchDeleteTableVersion",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleBatchDeleteTableVersion)
		},
	},
	{
		name: "BatchGetBlueprints",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleBatchGetBlueprints)
		},
	},
	{
		name: "BatchGetCrawlers",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleBatchGetCrawlers)
		},
	},
	{
		name: "BatchGetCustomEntityTypes",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleBatchGetCustomEntityTypes)
		},
	},
	{
		name: "BatchGetDataQualityResult",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleBatchGetDataQualityResult)
		},
	},
	{
		name: "BatchGetDataQualityRulesetEvaluationRun",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleBatchGetDataQualityRulesetEvaluationRun)
		},
	},
	{
		name: "BatchGetDevEndpoints",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleBatchGetDevEndpoints)
		},
	},
	{
		name: "BatchGetIterableForms",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleBatchGetIterableForms)
		},
	},
	{name: "BatchGetJobs", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleBatchGetJobs) }},
	{
		name: "BatchGetPartition",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleBatchGetPartition)
		},
	},
	{
		name: "BatchGetTableOptimizer",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleBatchGetTableOptimizer)
		},
	},
	{
		name: "BatchGetTriggers",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleBatchGetTriggers)
		},
	},
	{
		name: "BatchGetWorkflows",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleBatchGetWorkflows)
		},
	},
	{
		name: "BatchPutDataQualityStatisticAnnotation",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleBatchPutDataQualityStatisticAnnotation)
		},
	},
	{
		name: "BatchStopJobRun",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleBatchStopJobRun)
		},
	},
	{
		name: "BatchUpdatePartition",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleBatchUpdatePartition)
		},
	},
	{
		name: "CancelDataQualityRuleRecommendationRun",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCancelDataQualityRuleRecommendationRun)
		},
	},
	{
		name: "CancelDataQualityRulesetEvaluationRun",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCancelDataQualityRulesetEvaluationRun)
		},
	},
	{
		name: "CancelMLTaskRun",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCancelMLTaskRun)
		},
	},
	{
		name: "CancelStatement",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCancelStatement)
		},
	},
	{
		name: "CheckSchemaVersionValidity",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCheckSchemaVersionValidity)
		},
	},
	{
		name: "CreateBlueprint",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCreateBlueprint)
		},
	},
	{name: "CreateCatalog", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleCreateCatalog) }},
	{
		name: "CreateClassifier",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCreateClassifier)
		},
	},
	{
		name: "CreateColumnStatisticsTaskSettings",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCreateColumnStatisticsTaskSettings)
		},
	},
	{
		name: "CreateConnection",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCreateConnection)
		},
	},
	{name: "CreateCrawler", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleCreateCrawler) }},
	{
		name: "CreateCustomEntityType",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCreateCustomEntityType)
		},
	},
	{
		name: "CreateDatabase",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCreateDatabase)
		},
	},
	{
		name: "CreateDataQualityRuleset",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCreateDataQualityRuleset)
		},
	},
	{
		name: "CreateDevEndpoint",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCreateDevEndpoint)
		},
	},
	{
		name: "CreateGlossary",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCreateGlossary)
		},
	},
	{
		name: "CreateGlossaryTerm",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCreateGlossaryTerm)
		},
	},
	{
		name: "CreateGlueIdentityCenterConfiguration",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCreateGlueIdentityCenterConfiguration)
		},
	},
	{
		name: "CreateIntegration",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCreateIntegration)
		},
	},
	{
		name: "CreateIntegrationResourceProperty",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCreateIntegrationResourceProperty)
		},
	},
	{
		name: "CreateIntegrationTableProperties",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCreateIntegrationTableProperties)
		},
	},
	{name: "CreateJob", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleCreateJob) }},
	{
		name: "CreateMLTransform",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCreateMLTransform)
		},
	},
	{
		name: "CreatePartition",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCreatePartition)
		},
	},
	{
		name: "CreatePartitionIndex",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCreatePartitionIndex)
		},
	},
	{
		name: "CreateRegistry",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCreateRegistry)
		},
	},
	{name: "CreateSchema", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleCreateSchema) }},
	{name: "CreateScript", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleCreateScript) }},
	{
		name: "CreateSecurityConfiguration",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCreateSecurityConfiguration)
		},
	},
	{name: "CreateSession", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleCreateSession) }},
	{name: "CreateTable", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleCreateTable) }},
	{
		name: "CreateTableOptimizer",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCreateTableOptimizer)
		},
	},
	{name: "CreateTrigger", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleCreateTrigger) }},
	{
		name: "CreateUsageProfile",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCreateUsageProfile)
		},
	},
	{
		name: "CreateUserDefinedFunction",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCreateUserDefinedFunction)
		},
	},
	{
		name: "CreateWorkflow",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleCreateWorkflow)
		},
	},
	{
		name: "DeleteAsset",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteAsset)
		},
	},
	{
		name: "DeleteAssetType",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteAssetType)
		},
	},
	{
		name: "DeleteAttachment",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteAttachment)
		},
	},
	{
		name: "DeleteBlueprint",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteBlueprint)
		},
	},
	{name: "DeleteCatalog", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleDeleteCatalog) }},
	{
		name: "DeleteClassifier",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteClassifier)
		},
	},
	{
		name: "DeleteColumnStatisticsForPartition",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteColumnStatisticsForPartition)
		},
	},
	{
		name: "DeleteColumnStatisticsForTable",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteColumnStatisticsForTable)
		},
	},
	{
		name: "DeleteColumnStatisticsTaskSettings",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteColumnStatisticsTaskSettings)
		},
	},
	{
		name: "DeleteConnection",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteConnection)
		},
	},
	{
		name: "DeleteConnectionType",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteConnectionType)
		},
	},
	{name: "DeleteCrawler", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleDeleteCrawler) }},
	{
		name: "DeleteCustomEntityType",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteCustomEntityType)
		},
	},
	{
		name: "DeleteDatabase",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteDatabase)
		},
	},
	{
		name: "DeleteDataQualityRuleset",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteDataQualityRuleset)
		},
	},
	{
		name: "DeleteDevEndpoint",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteDevEndpoint)
		},
	},
	{
		name: "DeleteFormType",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteFormType)
		},
	},
	{
		name: "DeleteGlossary",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteGlossary)
		},
	},
	{
		name: "DeleteGlossaryTerm",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteGlossaryTerm)
		},
	},
	{
		name: "DeleteGlueIdentityCenterConfiguration",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteGlueIdentityCenterConfiguration)
		},
	},
	{
		name: "DeleteIntegration",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteIntegration)
		},
	},
	{
		name: "DeleteIntegrationResourceProperty",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteIntegrationResourceProperty)
		},
	},
	{
		name: "DeleteIntegrationTableProperties",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteIntegrationTableProperties)
		},
	},
	{name: "DeleteJob", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleDeleteJob) }},
	{
		name: "DeleteMLTransform",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteMLTransform)
		},
	},
	{
		name: "DeletePartition",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeletePartition)
		},
	},
	{
		name: "DeletePartitionIndex",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeletePartitionIndex)
		},
	},
	{
		name: "DeleteRegistry",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteRegistry)
		},
	},
	{
		name: "DeleteResourcePolicy",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteResourcePolicy)
		},
	},
	{name: "DeleteSchema", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleDeleteSchema) }},
	{
		name: "DeleteSchemaVersions",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteSchemaVersions)
		},
	},
	{
		name: "DeleteSecurityConfiguration",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteSecurityConfiguration)
		},
	},
	{name: "DeleteSession", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleDeleteSession) }},
	{name: "DeleteTable", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleDeleteTable) }},
	{
		name: "DeleteTableOptimizer",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteTableOptimizer)
		},
	},
	{
		name: "DeleteTableVersion",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteTableVersion)
		},
	},
	{name: "DeleteTrigger", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleDeleteTrigger) }},
	{
		name: "DeleteUsageProfile",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteUsageProfile)
		},
	},
	{
		name: "DeleteUserDefinedFunction",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteUserDefinedFunction)
		},
	},
	{
		name: "DeleteWorkflow",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDeleteWorkflow)
		},
	},
	{
		name: "DescribeConnectionType",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDescribeConnectionType)
		},
	},
	{
		name: "DescribeEntity",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDescribeEntity)
		},
	},
	{
		name: "DescribeInboundIntegrations",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDescribeInboundIntegrations)
		},
	},
	{
		name: "DescribeIntegrations",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDescribeIntegrations)
		},
	},
	{
		name: "DisassociateGlossaryTerms",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleDisassociateGlossaryTerms)
		},
	},
	{
		name: "GetAsset",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetAsset)
		},
	},
	{
		name: "GetAssetType",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetAssetType)
		},
	},
	{name: "GetBlueprint", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetBlueprint) }},
	{
		name: "GetBlueprintRun",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetBlueprintRun)
		},
	},
	{
		name: "GetBlueprintRuns",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetBlueprintRuns)
		},
	},
	{name: "GetCatalog", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetCatalog) }},
	{
		name: "GetCatalogImportStatus",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetCatalogImportStatus)
		},
	},
	{name: "GetCatalogs", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetCatalogs) }},
	{name: "GetClassifier", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetClassifier) }},
	{
		name: "GetClassifiers",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetClassifiers)
		},
	},
	{
		name: "GetColumnStatisticsForPartition",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetColumnStatisticsForPartition)
		},
	},
	{
		name: "GetColumnStatisticsForTable",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetColumnStatisticsForTable)
		},
	},
	{
		name: "GetColumnStatisticsTaskRun",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetColumnStatisticsTaskRun)
		},
	},
	{
		name: "GetColumnStatisticsTaskRuns",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetColumnStatisticsTaskRuns)
		},
	},
	{
		name: "GetColumnStatisticsTaskSettings",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetColumnStatisticsTaskSettings)
		},
	},
	{name: "GetConnection", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetConnection) }},
	{
		name: "GetConnections",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetConnections)
		},
	},
	{name: "GetCrawler", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetCrawler) }},
	{
		name: "GetCrawlerMetrics",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetCrawlerMetrics)
		},
	},
	{name: "GetCrawlers", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetCrawlers) }},
	{
		name: "GetCustomEntityType",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetCustomEntityType)
		},
	},
	{
		name: "GetDashboardUrl",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetDashboardURL)
		},
	},
	{
		name: "GetDataCatalogEncryptionSettings",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetDataCatalogEncryptionSettings)
		},
	},
	{
		name: "GetDataCatalogExportConfiguration",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetDataCatalogExportConfiguration)
		},
	},
	{
		name: "GetDataQualityModel",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetDataQualityModel)
		},
	},
	{
		name: "GetDataQualityModelResult",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetDataQualityModelResult)
		},
	},
	{
		name: "GetDataQualityResult",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetDataQualityResult)
		},
	},
	{
		name: "GetDataQualityRuleRecommendationRun",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetDataQualityRuleRecommendationRun)
		},
	},
	{
		name: "GetDataQualityRuleset",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetDataQualityRuleset)
		},
	},
	{
		name: "GetDataQualityRulesetEvaluationRun",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetDataQualityRulesetEvaluationRun)
		},
	},
	{
		name: "GetDataflowGraph",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetDataflowGraph)
		},
	},
	{name: "GetDatabase", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetDatabase) }},
	{name: "GetDatabases", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetDatabases) }},
	{
		name: "GetDevEndpoint",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetDevEndpoint)
		},
	},
	{
		name: "GetDevEndpoints",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetDevEndpoints)
		},
	},
	{
		name: "GetEntityRecords",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetEntityRecords)
		},
	},
	{
		name: "GetFormType",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetFormType)
		},
	},
	{
		name: "GetGlossary",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetGlossary)
		},
	},
	{
		name: "GetGlossaryTerm",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetGlossaryTerm)
		},
	},
	{
		name: "GetGlueIdentityCenterConfiguration",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetGlueIdentityCenterConfiguration)
		},
	},
	{
		name: "GetIntegrationResourceProperty",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetIntegrationResourceProperty)
		},
	},
	{
		name: "GetIntegrationTableProperties",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetIntegrationTableProperties)
		},
	},
	{name: "GetJob", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetJob) }},
	{
		name: "GetJobBookmark",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetJobBookmark)
		},
	},
	{name: "GetJobRun", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetJobRun) }},
	{name: "GetJobRuns", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetJobRuns) }},
	{name: "GetJobs", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetJobs) }},
	{name: "GetMLTaskRun", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetMLTaskRun) }},
	{name: "GetMLTaskRuns", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetMLTaskRuns) }},
	{
		name: "GetMLTransform",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetMLTransform)
		},
	},
	{
		name: "GetMLTransforms",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetMLTransforms)
		},
	},
	{name: "GetMapping", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetMapping) }},
	{
		name: "GetMaterializedViewRefreshTaskRun",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetMaterializedViewRefreshTaskRun)
		},
	},
	{name: "GetPartition", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetPartition) }},
	{
		name: "GetPartitionIndexes",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetPartitionIndexes)
		},
	},
	{name: "GetPartitions", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetPartitions) }},
	{name: "GetPlan", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetPlan) }},
	{name: "GetRegistry", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetRegistry) }},
	{
		name: "GetResourcePolicies",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetResourcePolicies)
		},
	},
	{
		name: "GetResourcePolicy",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetResourcePolicy)
		},
	},
	{name: "GetSchema", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetSchema) }},
	{
		name: "GetSchemaByDefinition",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetSchemaByDefinition)
		},
	},
	{
		name: "GetSchemaVersion",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetSchemaVersion)
		},
	},
	{
		name: "GetSchemaVersionsDiff",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetSchemaVersionsDiff)
		},
	},
	{
		name: "GetSecurityConfiguration",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetSecurityConfiguration)
		},
	},
	{
		name: "GetSecurityConfigurations",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetSecurityConfigurations)
		},
	},
	{name: "GetSession", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetSession) }},
	{
		name: "GetSessionEndpoint",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetSessionEndpoint)
		},
	},
	{name: "GetStatement", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetStatement) }},
	{name: "GetTable", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetTable) }},
	{
		name: "GetTableOptimizer",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetTableOptimizer)
		},
	},
	{
		name: "GetTableVersion",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetTableVersion)
		},
	},
	{
		name: "GetTableVersions",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetTableVersions)
		},
	},
	{name: "GetTables", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetTables) }},
	{name: "GetTags", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetTags) }},
	{name: "GetTrigger", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetTrigger) }},
	{name: "GetTriggers", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetTriggers) }},
	{
		name: "GetUnfilteredPartitionMetadata",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetUnfilteredPartitionMetadata)
		},
	},
	{
		name: "GetUnfilteredPartitionsMetadata",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetUnfilteredPartitionsMetadata)
		},
	},
	{
		name: "GetUnfilteredTableMetadata",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetUnfilteredTableMetadata)
		},
	},
	{
		name: "GetUsageProfile",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetUsageProfile)
		},
	},
	{
		name: "GetUserDefinedFunction",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetUserDefinedFunction)
		},
	},
	{
		name: "GetUserDefinedFunctions",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetUserDefinedFunctions)
		},
	},
	{name: "GetWorkflow", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleGetWorkflow) }},
	{
		name: "GetWorkflowRun",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetWorkflowRun)
		},
	},
	{
		name: "GetWorkflowRunProperties",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetWorkflowRunProperties)
		},
	},
	{
		name: "GetWorkflowRuns",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleGetWorkflowRuns)
		},
	},
	{
		name: "ImportCatalogToGlue",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleImportCatalogToGlue)
		},
	},
	{
		name: "ListAssetTypes",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleListAssetTypes)
		},
	},
	{
		name: "ListBlueprints",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleListBlueprints)
		},
	},
	{
		name: "ListColumnStatisticsTaskRuns",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleListColumnStatisticsTaskRuns)
		},
	},
	{
		name: "ListConnectionTypes",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleListConnectionTypes)
		},
	},
	{name: "ListCrawlers", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleListCrawlers) }},
	{name: "ListCrawls", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleListCrawls) }},
	{
		name: "ListCustomEntityTypes",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleListCustomEntityTypes)
		},
	},
	{
		name: "ListDataQualityResults",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleListDataQualityResults)
		},
	},
	{
		name: "ListDataQualityRuleRecommendationRuns",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleListDataQualityRuleRecommendationRuns)
		},
	},
	{
		name: "ListDataQualityRulesetEvaluationRuns",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleListDataQualityRulesetEvaluationRuns)
		},
	},
	{
		name: "ListDataQualityRulesets",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleListDataQualityRulesets)
		},
	},
	{
		name: "ListDataQualityStatisticAnnotations",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleListDataQualityStatisticAnnotations)
		},
	},
	{
		name: "ListDataQualityStatistics",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleListDataQualityStatistics)
		},
	},
	{
		name: "ListDevEndpoints",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleListDevEndpoints)
		},
	},
	{name: "ListEntities", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleListEntities) }},
	{
		name: "ListFormTypes",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleListFormTypes)
		},
	},
	{
		name: "ListGlossaries",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleListGlossaries)
		},
	},
	{
		name: "ListGlossaryTerms",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleListGlossaryTerms)
		},
	},
	{
		name: "ListIntegrationResourceProperties",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleListIntegrationResourceProperties)
		},
	},
	{
		name: "ListIterableForms",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleListIterableForms)
		},
	},
	{name: "ListJobs", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleListJobs) }},
	{
		name: "ListMLTransforms",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleListMLTransforms)
		},
	},
	{
		name: "ListMaterializedViewRefreshTaskRuns",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleListMaterializedViewRefreshTaskRuns)
		},
	},
	{
		name: "ListRegistries",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleListRegistries)
		},
	},
	{
		name: "ListSchemaVersions",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleListSchemaVersions)
		},
	},
	{name: "ListSchemas", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleListSchemas) }},
	{name: "ListSessions", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleListSessions) }},
	{
		name: "ListStatements",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleListStatements)
		},
	},
	{
		name: "ListTableOptimizerRuns",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleListTableOptimizerRuns)
		},
	},
	{name: "ListTriggers", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleListTriggers) }},
	{
		name: "ListUsageProfiles",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleListUsageProfiles)
		},
	},
	{name: "ListWorkflows", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleListWorkflows) }},
	{
		name: "ModifyIntegration",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleModifyIntegration)
		},
	},
	{
		name: "PutAsset",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handlePutAsset)
		},
	},
	{
		name: "PutAssetType",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handlePutAssetType)
		},
	},
	{
		name: "PutAttachment",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handlePutAttachment)
		},
	},
	{
		name: "PutDataCatalogEncryptionSettings",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handlePutDataCatalogEncryptionSettings)
		},
	},
	{
		name: "PutDataCatalogExportConfiguration",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handlePutDataCatalogExportConfiguration)
		},
	},
	{
		name: "PutDataQualityProfileAnnotation",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handlePutDataQualityProfileAnnotation)
		},
	},
	{
		name: "PutFormType",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handlePutFormType)
		},
	},
	{
		name: "PutResourcePolicy",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handlePutResourcePolicy)
		},
	},
	{
		name: "PutSchemaVersionMetadata",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handlePutSchemaVersionMetadata)
		},
	},
	{
		name: "PutWorkflowRunProperties",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handlePutWorkflowRunProperties)
		},
	},
	{
		name: "QuerySchemaVersionMetadata",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleQuerySchemaVersionMetadata)
		},
	},
	{
		name: "RegisterConnectionType",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleRegisterConnectionType)
		},
	},
	{
		name: "RegisterSchemaVersion",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleRegisterSchemaVersion)
		},
	},
	{
		name: "RemoveSchemaVersionMetadata",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleRemoveSchemaVersionMetadata)
		},
	},
	{
		name: "ResetJobBookmark",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleResetJobBookmark)
		},
	},
	{
		name: "ResumeWorkflowRun",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleResumeWorkflowRun)
		},
	},
	{name: "RunStatement", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleRunStatement) }},
	{
		name: "SearchAssets",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleSearchAssets)
		},
	},
	{name: "SearchTables", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleSearchTables) }},
	{
		name: "StartBlueprintRun",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleStartBlueprintRun)
		},
	},
	{
		name: "StartColumnStatisticsTaskRun",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleStartColumnStatisticsTaskRun)
		},
	},
	{
		name: "StartColumnStatisticsTaskRunSchedule",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleStartColumnStatisticsTaskRunSchedule)
		},
	},
	{name: "StartCrawler", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleStartCrawler) }},
	{
		name: "StartCrawlerSchedule",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleStartCrawlerSchedule)
		},
	},
	{
		name: "StartDataQualityRuleRecommendationRun",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleStartDataQualityRuleRecommendationRun)
		},
	},
	{
		name: "StartDataQualityRulesetEvaluationRun",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleStartDataQualityRulesetEvaluationRun)
		},
	},
	{
		name: "StartExportLabelsTaskRun",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleStartExportLabelsTaskRun)
		},
	},
	{
		name: "StartImportLabelsTaskRun",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleStartImportLabelsTaskRun)
		},
	},
	{name: "StartJobRun", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleStartJobRun) }},
	{
		name: "StartMLEvaluationTaskRun",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleStartMLEvaluationTaskRun)
		},
	},
	{
		name: "StartMLLabelingSetGenerationTaskRun",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleStartMLLabelingSetGenerationTaskRun)
		},
	},
	{
		name: "StartMaterializedViewRefreshTaskRun",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleStartMaterializedViewRefreshTaskRun)
		},
	},
	{name: "StartTrigger", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleStartTrigger) }},
	{
		name: "StartWorkflowRun",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleStartWorkflowRun)
		},
	},
	{
		name: "StopColumnStatisticsTaskRun",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleStopColumnStatisticsTaskRun)
		},
	},
	{
		name: "StopColumnStatisticsTaskRunSchedule",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleStopColumnStatisticsTaskRunSchedule)
		},
	},
	{name: "StopCrawler", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleStopCrawler) }},
	{
		name: "StopCrawlerSchedule",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleStopCrawlerSchedule)
		},
	},
	{
		name: "StopMaterializedViewRefreshTaskRun",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleStopMaterializedViewRefreshTaskRun)
		},
	},
	{name: "StopSession", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleStopSession) }},
	{name: "StopTrigger", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleStopTrigger) }},
	{
		name: "StopWorkflowRun",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleStopWorkflowRun)
		},
	},
	{name: "TagResource", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleTagResource) }},
	{
		name: "TestConnection",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleTestConnection)
		},
	},
	{name: "UntagResource", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleUntagResource) }},
	{
		name: "UpdateAsset",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleUpdateAsset)
		},
	},
	{
		name: "UpdateBlueprint",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleUpdateBlueprint)
		},
	},
	{name: "UpdateCatalog", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleUpdateCatalog) }},
	{
		name: "UpdateClassifier",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleUpdateClassifier)
		},
	},
	{
		name: "UpdateColumnStatisticsForPartition",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleUpdateColumnStatisticsForPartition)
		},
	},
	{
		name: "UpdateColumnStatisticsForTable",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleUpdateColumnStatisticsForTable)
		},
	},
	{
		name: "UpdateColumnStatisticsTaskSettings",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleUpdateColumnStatisticsTaskSettings)
		},
	},
	{
		name: "UpdateConnection",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleUpdateConnection)
		},
	},
	{name: "UpdateCrawler", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleUpdateCrawler) }},
	{
		name: "UpdateCrawlerSchedule",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleUpdateCrawlerSchedule)
		},
	},
	{
		name: "UpdateDatabase",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleUpdateDatabase)
		},
	},
	{
		name: "UpdateDataQualityRuleset",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleUpdateDataQualityRuleset)
		},
	},
	{
		name: "UpdateDevEndpoint",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleUpdateDevEndpoint)
		},
	},
	{
		name: "UpdateGlossary",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleUpdateGlossary)
		},
	},
	{
		name: "UpdateGlossaryTerm",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleUpdateGlossaryTerm)
		},
	},
	{
		name: "UpdateGlueIdentityCenterConfiguration",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleUpdateGlueIdentityCenterConfiguration)
		},
	},
	{
		name: "UpdateIntegrationResourceProperty",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleUpdateIntegrationResourceProperty)
		},
	},
	{
		name: "UpdateIntegrationTableProperties",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleUpdateIntegrationTableProperties)
		},
	},
	{name: "UpdateJob", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleUpdateJob) }},
	{
		name: "UpdateJobFromSourceControl",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleUpdateJobFromSourceControl)
		},
	},
	{
		name: "UpdateMLTransform",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleUpdateMLTransform)
		},
	},
	{
		name: "UpdatePartition",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleUpdatePartition)
		},
	},
	{
		name: "UpdateRegistry",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleUpdateRegistry)
		},
	},
	{name: "UpdateSchema", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleUpdateSchema) }},
	{
		name: "UpdateSourceControlFromJob",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleUpdateSourceControlFromJob)
		},
	},
	{name: "UpdateTable", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleUpdateTable) }},
	{
		name: "UpdateTableOptimizer",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleUpdateTableOptimizer)
		},
	},
	{name: "UpdateTrigger", bind: func(h *Handler) service.JSONOpFunc { return service.WrapOp(h.handleUpdateTrigger) }},
	{
		name: "UpdateUsageProfile",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleUpdateUsageProfile)
		},
	},
	{
		name: "UpdateUserDefinedFunction",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleUpdateUserDefinedFunction)
		},
	},
	{
		name: "UpdateWorkflow",
		bind: func(h *Handler) service.JSONOpFunc {
			return service.WrapOp(h.handleUpdateWorkflow)
		},
	},
}
