package glue

import "context"

// StorageBackend defines the interface for all Glue backend operations.
// InMemoryBackend implements this interface; alternative backends (e.g. test
// doubles) can implement it too, keeping the Handler backend-agnostic.
type StorageBackend interface {
	// Region returns the backend region.
	Region() string
	// AccountID returns the backend account ID.
	AccountID() string
	// Reset clears all backend state, returning it to the initial empty state.
	Reset()

	// Managed reconciler lifecycle. StartReconciler is invoked by the service
	// framework's BackgroundWorker hook; StopReconciler by its Shutdowner hook.
	StartReconciler(ctx context.Context)
	StopReconciler()

	// Connection-type registry operations.
	RegisterConnectionType(
		name, description string, spec RegisterConnectionTypeSpec,
	) (*ConnectionTypeInfo, error)
	DeleteConnectionType(name string) error
	ListConnectionTypes() []*ConnectionTypeInfo
	DescribeConnectionType(name string) (*ConnectionTypeInfo, error)

	// Connector entity metadata/data operations.
	DescribeEntity(connectionName, entityName string) ([]EntityField, error)
	GetEntityRecords(connectionName, entityName string, limit int, nextToken string) ([]map[string]any, string, error)
	ListEntities(connectionName string) ([]EntityDescriptor, error)

	// Database operations.
	CreateDatabase(input DatabaseInput, tags map[string]string) (*Database, error)
	GetDatabase(name string) (*Database, error)
	GetDatabases() []*Database
	UpdateDatabase(name string, input DatabaseInput) error
	DeleteDatabase(name string) error

	// Table operations.
	CreateTable(dbName string, input TableInput) (*Table, error)
	GetTable(dbName, tableName string) (*Table, error)
	GetTables(dbName string) ([]*Table, error)
	UpdateTable(dbName string, input TableInput) error
	DeleteTable(dbName, tableName string) error

	// Crawler operations.
	CreateCrawler(
		name, role, dbName string,
		targets CrawlerTarget,
		tags map[string]string,
	) (*Crawler, error)
	CreateCrawlerWithOptions(
		name, role, dbName string,
		targets CrawlerTarget,
		tags map[string]string,
		opts CrawlerOptions,
	) (*Crawler, error)
	GetCrawler(name string) (*Crawler, error)
	GetCrawlers() []*Crawler
	ListCrawlers() []string
	UpdateCrawler(name, role, dbName string, targets CrawlerTarget) error
	UpdateCrawlerWithOptions(name, role, dbName string, targets CrawlerTarget, opts CrawlerOptions) error
	DeleteCrawler(name string) error

	// Job operations.
	CreateJob(input Job) (*Job, error)
	GetJob(name string) (*Job, error)
	GetJobs() []*Job
	UpdateJob(name string, input Job) error
	DeleteJob(name string) error
	UpdateJobFromSourceControl(jobName string, details SourceControlDetails) error
	UpdateSourceControlFromJob(jobName string, details SourceControlDetails) error

	// Tag operations.
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	GetTags(resourceARN string) (map[string]string, error)

	// Partition operations.
	BatchCreatePartition(
		dbName, tableName string,
		inputs []PartitionInput,
	) ([]*Partition, []PartitionError)
	BatchDeletePartition(dbName, tableName string, values []PartitionValueList) []PartitionError
	BatchDeleteTableVersion(dbName, tableName string, versionIDs []string) []TableVersionError

	// Single partition operations.
	GetPartition(dbName, tableName string, values []string) (*Partition, error)
	GetPartitions(dbName, tableName string) ([]*Partition, error)
	UpdatePartition(dbName, tableName string, partitionValues []string, input PartitionInput) error
	CreatePartitionIndex(dbName, tableName string, input PartitionIndex) error
	DeletePartitionIndex(dbName, tableName, indexName string) error
	GetPartitionIndexes(dbName, tableName string) ([]*PartitionIndex, error)

	// Connection operations.
	CreateConnection(
		name, connType string,
		props map[string]string,
		tags map[string]string,
	) (*Connection, error)
	CreateConnectionWithOptions(
		name, connType string,
		props map[string]string,
		tags map[string]string,
		opts ConnectionOptions,
	) (*Connection, error)
	GetConnection(name string) (*Connection, error)
	GetConnections() []*Connection
	DeleteConnection(name string) error
	UpdateConnection(name string, connType string, props map[string]string) error
	UpdateConnectionWithOptions(name, connType string, props map[string]string, opts ConnectionOptions) error

	// Batch connection operations.
	BatchDeleteConnection(names []string) ([]string, []ErrorDetail)

	// Batch table operations.
	BatchDeleteTable(dbName string, tableNames []string) []TableError

	// Batch blueprint operations.
	BatchGetBlueprints(names []string) ([]*Blueprint, []string)

	// Batch crawler operations.
	BatchGetCrawlers(names []string) ([]*Crawler, []string)

	// Batch custom entity type operations.
	BatchGetCustomEntityTypes(names []string) ([]*CustomEntityType, []string)

	// Batch data quality operations.
	BatchGetDataQualityResult(resultIDs []string) ([]*DataQualityResult, []ErrorDetail)

	// Batch dev endpoint operations.
	BatchGetDevEndpoints(names []string) ([]*DevEndpoint, []string)

	// Seed helpers (internal use for tests).
	AddConnectionInternal(conn *Connection)
	AddBlueprintInternal(bp *Blueprint)
	AddCustomEntityTypeInternal(cet *CustomEntityType)
	AddDataQualityResultInternal(dqr *DataQualityResult)
	AddDevEndpointInternal(dep *DevEndpoint)
	AddTableVersionInternal(dbName, tableName string, tv *TableVersion)
	AddPartitionInternal(dbName, tableName string, p *Partition)

	// Job run operations.
	StartJobRun(jobName string, arguments map[string]string) (*JobRun, error)
	StartJobRunWithOptions(jobName string, arguments map[string]string, opts StartJobRunOptions) (*JobRun, error)
	GetJobRun(jobName, runID string) (*JobRun, error)
	GetJobRuns(jobName string) ([]*JobRun, error)
	BatchStopJobRun(jobName string, runIDs []string) []BatchStopJobRunError
	GetJobBookmark(jobName string) (*JobBookmark, error)
	ResetJobBookmark(jobName string) error
	ResetJobBookmarkWithResult(jobName string) (*JobBookmark, error)

	// Crawler scheduling operations.
	StartCrawler(name string) error
	StopCrawler(name string) error
	UpdateCrawlerSchedule(name, scheduleExpression string) error
	StartCrawlerSchedule(name string) error
	StopCrawlerSchedule(name string) error
	ListCrawls(crawlerName string) ([]*CrawlHistoryEntry, error)

	// Data quality ruleset operations.
	CreateDataQualityRuleset(
		name, ruleset string,
		tags map[string]string,
	) (*DataQualityRuleset, error)
	CreateDataQualityRulesetWithOptions(
		name, ruleset string,
		tags map[string]string,
		opts DataQualityRulesetOptions,
	) (*DataQualityRuleset, error)
	GetDataQualityRuleset(name string) (*DataQualityRuleset, error)
	DeleteDataQualityRuleset(name string) error
	UpdateDataQualityRuleset(name, ruleset, description string) error
	ListDataQualityRulesets() []*DataQualityRuleset
	StartDataQualityRulesetEvaluationRun(rulesetNames []string) (*DataQualityEvaluationRun, error)
	GetDataQualityRulesetEvaluationRun(runID string) (*DataQualityEvaluationRun, error)
	BatchGetDataQualityRulesetEvaluationRun(runIDs []string) ([]*DataQualityEvaluationRun, []string)
	CancelDataQualityRulesetEvaluationRun(runID string) error

	// Seed helpers for new types.
	AddJobRunInternal(run *JobRun)
	AddDataQualityRulesetInternal(r *DataQualityRuleset)
	AddDataQualityEvalRunInternal(run *DataQualityEvaluationRun)

	// Trigger operations.
	CreateTrigger(t Trigger, tags map[string]string) (*Trigger, error)
	GetTrigger(name string) (*Trigger, error)
	GetTriggers() []*Trigger
	BatchGetTriggers(names []string) ([]*Trigger, []string)
	UpdateTrigger(name string, update Trigger) error
	DeleteTrigger(name string) error
	StartTrigger(name string) error
	StopTrigger(name string) error

	// Workflow operations.
	CreateWorkflow(w Workflow, tags map[string]string) (*Workflow, error)
	GetWorkflow(name string, includeGraph bool) (*Workflow, error)
	GetWorkflows() []string
	BatchGetWorkflows(names []string, includeGraph bool) ([]*Workflow, []string)
	UpdateWorkflow(name string, update Workflow) error
	DeleteWorkflow(name string) error
	StartWorkflowRun(name string) (*WorkflowRun, error)
	GetWorkflowRun(workflowName, runID string) (*WorkflowRun, error)
	GetWorkflowRuns(workflowName string) ([]*WorkflowRun, error)

	// Classifier operations.
	CreateClassifier(c Classifier) error
	GetClassifier(name string) (*Classifier, error)
	GetClassifiers() []*Classifier
	UpdateClassifier(c Classifier) error
	DeleteClassifier(name string) error

	// DevEndpoint full CRUD.
	CreateDevEndpoint(name string, input DevEndpointInput, roleArn string, tags map[string]string) (*DevEndpoint, error)
	GetDevEndpoint(name string) (*DevEndpoint, error)
	GetAllDevEndpoints() []*DevEndpoint
	DeleteDevEndpoint(name string) error

	// Schema Registry operations.
	CreateRegistry(name, description string, tags map[string]string) (*Registry, error)
	DescribeRegistry(name string) (*Registry, error)
	ListRegistries() []*Registry
	UpdateRegistry(name, description string) error
	DeleteRegistry(name string) error

	// Schema operations.
	CreateSchema(
		registryName, schemaName, dataFormat, compatibility, description, schemaDefinition string,
		tags map[string]string,
	) (*Schema, *SchemaVersion, error)
	DescribeSchema(registryName, schemaName string) (*Schema, error)
	ListSchemas(registryName string) []*Schema
	UpdateSchema(registryName, schemaName, compatibility, description string) error
	DeleteSchema(registryName, schemaName string) error

	// Schema Version operations.
	RegisterSchemaVersion(registryName, schemaName, schemaDefinition string) (*SchemaVersion, error)
	GetSchemaVersion(registryName, schemaName string, versionNumber int64) (*SchemaVersion, error)
	ListSchemaVersions(registryName, schemaName string) []*SchemaVersion

	// Crawler metrics.
	GetCrawlerMetrics(crawlerNames []string) []*CrawlerMetrics

	// Table version retrieval (read-only; versions are created via CreateTable/UpdateTable).
	GetTableVersions(dbName, tableName string) []*TableVersion
	GetTableVersion(dbName, tableName, versionID string) (*TableVersion, error)

	// SearchTables returns tables matching a case-insensitive substring filter.
	SearchTables(searchText string) []*Table

	// UserDefinedFunction operations.
	CreateUserDefinedFunction(
		dbName string,
		input UserDefinedFunction,
		tags map[string]string,
	) (*UserDefinedFunction, error)
	GetUserDefinedFunction(dbName, name string) (*UserDefinedFunction, error)
	GetUserDefinedFunctions(dbName string) []*UserDefinedFunction
	UpdateUserDefinedFunction(dbName, name string, input UserDefinedFunction) error
	DeleteUserDefinedFunction(dbName, name string) error

	// SecurityConfiguration operations.
	CreateSecurityConfiguration(
		name string,
		enc EncryptionConfiguration,
	) (*SecurityConfiguration, error)
	GetSecurityConfiguration(name string) (*SecurityConfiguration, error)
	DeleteSecurityConfiguration(name string) error
	ListSecurityConfigurations() []*SecurityConfiguration

	// Session operations.
	CreateSession(id, role string, cmd SessionCommand, opts Session) (*Session, error)
	GetSession(id string) (*Session, error)
	ListSessions() []*Session
	DeleteSession(id string) error
	StopSession(id string) error

	// Statement operations.
	RunStatement(sessionID, code string) (*Statement, error)
	GetStatement(sessionID string, statementID int32) (*Statement, error)
	GetStatements(sessionID string) ([]*Statement, error)
	CancelStatement(sessionID string, statementID int32) error

	// TableOptimizer operations.
	CreateTableOptimizer(
		catalogID, dbName, tableName, optimizerType string,
		config TableOptimizerConfiguration,
	) error
	GetTableOptimizer(dbName, tableName, optimizerType string) (*TableOptimizer, error)
	UpdateTableOptimizer(
		dbName, tableName, optimizerType string,
		config TableOptimizerConfiguration,
	) error
	DeleteTableOptimizer(dbName, tableName, optimizerType string) error
	BatchGetTableOptimizer(
		entries []BatchGetTableOptimizerEntry,
	) ([]*TableOptimizer, []BatchGetTableOptimizerError)

	// Column statistics operations.
	UpdateColumnStatisticsForTable(dbName, tableName string, stats []*ColumnStatistics) error
	GetColumnStatisticsForTable(
		dbName, tableName string,
		columnNames []string,
	) ([]*ColumnStatistics, error)
	DeleteColumnStatisticsForTable(dbName, tableName, columnName string) error
	UpdateColumnStatisticsForPartition(
		dbName, tableName string,
		partitionValues []string,
		stats []*ColumnStatistics,
	) error
	GetColumnStatisticsForPartition(
		dbName, tableName string,
		partitionValues []string,
		columnNames []string,
	) ([]*ColumnStatistics, error)
	DeleteColumnStatisticsForPartition(
		dbName, tableName string,
		partitionValues []string,
		columnName string,
	) error

	// Resource policy operations.
	PutResourcePolicy(policy, resourceARN, existsCondition, hashCondition, enableHybrid string) (string, error)
	GetResourcePolicy(resourceARN string) (string, string, error)
	DeleteResourcePolicy(resourceARN, policyHash string) error
	ListResourcePolicies() []*resourcePolicyEntry

	// MLTransform operations.
	CreateMLTransform(
		name, description, role string,
		tables []GlueTable,
		params MLTransformParameter,
		tags map[string]string,
	) (*MLTransform, error)
	CreateMLTransformWithOptions(
		name, description, role string,
		tables []GlueTable,
		params MLTransformParameter,
		tags map[string]string,
		opts MLTransformOptions,
	) (*MLTransform, error)
	GetMLTransform(id string) (*MLTransform, error)
	GetMLTransforms() []*MLTransform
	UpdateMLTransform(id string, update MLTransform) error
	DeleteMLTransform(id string) error

	// Catalog operations.
	CreateCatalog(catalogID, name, description string, params map[string]string) error
	GetCatalog(catalogID string) (*CatalogEntry, error)
	GetCatalogs() []*CatalogEntry
	UpdateCatalog(catalogID, description string, params map[string]string) error
	DeleteCatalog(catalogID string) error

	// WorkflowRun extras.
	StopWorkflowRun(workflowName, runID string) error
	PutWorkflowRunProperties(workflowName, runID string, props map[string]string) error

	// Table version deletion.
	DeleteTableVersion(dbName, tableName, versionID string) error

	// DevEndpoint update.
	UpdateDevEndpoint(name string, addArgs map[string]string, deleteArgs []string, opts UpdateDevEndpointOptions) error

	// Data catalog encryption settings.
	PutDataCatalogEncryptionSettings(catalogID string, settings DataCatalogEncryptionSettings) error
	GetDataCatalogEncryptionSettings(catalogID string) (*DataCatalogEncryptionSettings, error)

	// Data catalog export configuration (S3 Tables metadata export).
	PutDataCatalogExportConfiguration(settings DataCatalogExportConfiguration) (*DataCatalogExportConfiguration, error)
	GetDataCatalogExportConfiguration() (*DataCatalogExportConfiguration, error)

	// Blueprint CRUD (batch 2).
	CreateBlueprint(name, blueprintLocation, description string, tags map[string]string) (*Blueprint, error)
	DeleteBlueprint(name string) error
	UpdateBlueprint(name, blueprintLocation, description string) (*Blueprint, error)
	ListBlueprints() []string

	// BlueprintRun operations.
	StartBlueprintRun(blueprintName string) (*BlueprintRun, error)
	GetBlueprintRun(blueprintName, runID string) (*BlueprintRun, error)
	GetBlueprintRuns(blueprintName string) []*BlueprintRun

	// UsageProfile operations.
	CreateUsageProfile(name, description string, tags map[string]string) (*UsageProfile, error)
	GetUsageProfile(name string) (*UsageProfile, error)
	DeleteUsageProfile(name string) error
	ListUsageProfiles() []*UsageProfile
	UpdateUsageProfile(name, description string) (*UsageProfile, error)

	// CustomEntityType individual CRUD.
	CreateCustomEntityType(
		name, regexString string,
		contextWords []string,
	) (*CustomEntityType, error)
	GetCustomEntityType(name string) (*CustomEntityType, error)
	DeleteCustomEntityType(name string) error
	ListCustomEntityTypes() []*CustomEntityType

	// DataQuality recommendation runs.
	StartDataQualityRuleRecommendationRun(s3Path string) (*DQRuleRecommendationRun, error)
	GetDataQualityRuleRecommendationRun(runID string) (*DQRuleRecommendationRun, error)
	CancelDataQualityRuleRecommendationRun(runID string) error
	ListDataQualityRuleRecommendationRuns() []*DQRuleRecommendationRun

	// ColumnStatisticsTask operations.
	CreateColumnStatisticsTaskSettings(
		dbName, tableName, roleArn string,
		columns []string,
	) (*ColumnStatisticsTaskSettings, error)
	GetColumnStatisticsTaskSettings(dbName, tableName string) (*ColumnStatisticsTaskSettings, error)
	UpdateColumnStatisticsTaskSettings(dbName, tableName, roleArn string) error
	DeleteColumnStatisticsTaskSettings(dbName, tableName string) error
	StartColumnStatisticsTaskRunSchedule(dbName, tableName string) error
	StopColumnStatisticsTaskRunSchedule(dbName, tableName string) error
	StartColumnStatisticsTaskRun(dbName, tableName string) (*ColumnStatisticsTaskRun, error)
	StopColumnStatisticsTaskRun(runID string) error
	GetColumnStatisticsTaskRun(runID string) (*ColumnStatisticsTaskRun, error)
	GetColumnStatisticsTaskRuns() []*ColumnStatisticsTaskRun
	ListColumnStatisticsTaskRuns() []*ColumnStatisticsTaskRun

	// MaterializedView refresh operations.
	StartMaterializedViewRefreshTaskRun(
		dbName, tableName string,
	) (*MaterializedViewRefreshRun, error)
	StopMaterializedViewRefreshTaskRun(taskRunID string) error
	GetMaterializedViewRefreshTaskRun(taskRunID string) (*MaterializedViewRefreshRun, error)
	ListMaterializedViewRefreshTaskRuns() []*MaterializedViewRefreshRun

	// Integration operations.
	CreateIntegration(name, sourceArn, targetArn string, tags map[string]string) (*Integration, error)
	DeleteIntegration(identifier string) (*Integration, error)
	ListIntegrations() []*Integration
	ModifyIntegration(identifier string) (*Integration, error)
	CreateIntegrationResourceProperty(
		resourceArn string,
		sourceProps, targetProps map[string]string,
	) (*IntegrationResourceProperty, error)
	GetIntegrationResourceProperty(resourceArn string) (*IntegrationResourceProperty, error)
	UpdateIntegrationResourceProperty(
		resourceArn string,
		sourceProps, targetProps map[string]string,
	) (*IntegrationResourceProperty, error)
	ListIntegrationResourceProperties() []*IntegrationResourceProperty
	CreateIntegrationTableProperties(
		resourceArn, tableName string,
		sourceConfig, targetConfig map[string]any,
	) error
	GetIntegrationTableProperties(
		resourceArn, tableName string,
	) (*IntegrationTableProperties, error)
	UpdateIntegrationTableProperties(
		resourceArn, tableName string,
		sourceConfig, targetConfig map[string]any,
	) error

	// GlueIdentityCenter operations.
	CreateGlueIdentityCenterConfiguration(instanceARN string) (*IdentityCenterConfig, error)
	GetGlueIdentityCenterConfiguration() (*IdentityCenterConfig, error)
	UpdateGlueIdentityCenterConfiguration(instanceARN string) error
	DeleteGlueIdentityCenterConfiguration() error

	// ML transform task run operations.
	StartMLEvaluationTaskRun(transformID string) (*MLTaskRun, error)
	StartMLLabelingSetGenerationTaskRun(transformID string) (*MLTaskRun, error)
	StartExportLabelsTaskRun(transformID, outputPath string) (*MLTaskRun, error)
	StartImportLabelsTaskRun(transformID, inputPath string) (*MLTaskRun, error)
	GetMLTaskRun(transformID, taskRunID string) (*MLTaskRun, error)
	GetMLTaskRuns(transformID string) ([]*MLTaskRun, error)
	CancelMLTaskRun(transformID, taskRunID string) error

	// DataQuality listing and model operations.
	ListDataQualityEvaluationRuns() []*DataQualityEvaluationRun
	ListDataQualityResults() []*DataQualityResult
	PutDataQualityStatisticAnnotation(profileID, statisticID, inclusion string)
	ListDataQualityStatisticAnnotations(profileID, statisticID string) []*StatisticAnnotation

	// CatalogImport operations.
	ImportCatalogToGlue(catalogID string) error
	GetCatalogImportStatus(catalogID string) *CatalogImportStatus

	// Schema version metadata operations.
	PutSchemaVersionMetadata(schemaVersionID, key, value string) error
	QuerySchemaVersionMetadata(schemaVersionID string) map[string]string
	RemoveSchemaVersionMetadata(schemaVersionID, key string) error

	// Schema lookup by definition.
	GetSchemaByDefinition(registryName, schemaName, definition string) (*SchemaVersion, error)

	// Schema version diff.
	GetSchemaVersionsDiff(registryName, schemaName string, v1, v2 int64) (string, error)

	// ETL plan generation.
	GetPlan(language string) (string, string)

	// Workflow resume.
	ResumeWorkflowRun(workflowName, runID string) (string, []string, error)

	// Schema version deletion (single version, by number).
	DeleteSchemaVersion(registryName, schemaName string, versionNumber int64) error

	// Integration resource/table property deletion.
	DeleteIntegrationResourceProperty(resourceArn string) error
	DeleteIntegrationTableProperties(resourceArn, tableName string) error

	// Business glossary operations (glossaries.go).
	CreateGlossary(name, description string) (*Glossary, error)
	GetGlossary(id string) (*Glossary, error)
	UpdateGlossary(id string, name, description *string) (*Glossary, error)
	DeleteGlossary(id string) error
	ListGlossaries() []*Glossary
	CreateGlossaryTerm(glossaryID, name, shortDesc, longDesc string) (*GlossaryTerm, error)
	GetGlossaryTerm(id string) (*GlossaryTerm, error)
	UpdateGlossaryTerm(id string, name, shortDesc, longDesc *string) (*GlossaryTerm, error)
	DeleteGlossaryTerm(id string) error
	ListGlossaryTerms(glossaryID string) ([]*GlossaryTerm, error)
	AssociateGlossaryTerms(assetID string, termIDs []string) ([]string, error)
	DisassociateGlossaryTerms(assetID string, termIDs []string) ([]string, error)

	// Asset catalog operations: asset types, assets, attachments, and
	// iterable form items (assets.go, search_assets.go).
	PutAssetType(name string, forms map[string]AssetTypeFormReference) (*AssetType, error)
	GetAssetType(id string) (*AssetType, error)
	DeleteAssetType(id string) error
	ListAssetTypes() []*AssetType
	PutAsset(id, name, description, assetTypeID string, forms map[string]AssetFormEntry) (*Asset, error)
	GetAsset(id string) (*Asset, error)
	UpdateAsset(id string, name, description *string) (*Asset, error)
	DeleteAsset(id string) error
	SearchAssets(searchText string, filter *searchFilterClause, sortAttr string, sortDesc bool) []*Asset
	PutAttachment(assetID, attachmentName, formTypeID, content, iterableFormName, itemIdentifier string) error
	DeleteAttachment(assetID, attachmentName, iterableFormName, itemIdentifier string) error
	BatchGetIterableForms(
		assetID, iterableFormName string,
		itemIDs []string,
	) ([]*IterableFormItem, []ItemErrorDetail, error)
	ListIterableForms(assetID, iterableFormName string) ([]*IterableFormListItem, error)

	// Form type operations (forms.go).
	PutFormType(name, schema string) (*FormType, error)
	GetFormType(id string) (*FormType, error)
	DeleteFormType(id string) error
	ListFormTypes() []*FormType

	// Spark monitoring dashboard / interactive-session Spark Connect endpoint
	// (dashboard.go).
	GetDashboardURL(resourceID, resourceType, requestOrigin string) (string, error)
	GetSessionEndpoint(sessionID string) (*SessionEndpoint, error)
}

// Snapshottable is an optional interface that a StorageBackend may implement
// to support persistence via Snapshot/Restore.
type Snapshottable interface {
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// compile-time assertion that InMemoryBackend implements StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
