package glue

import (
	"time"
)

// DatabaseInput is the input for creating or updating a Glue database.
type DatabaseInput struct {
	Name        string `json:"Name"`
	Description string `json:"Description,omitempty"`
}

// Database represents a Glue catalog database.
type Database struct {
	Tags        map[string]string `json:"-"`
	Name        string            `json:"Name"`
	Description string            `json:"Description,omitempty"`
	CatalogID   string            `json:"CatalogId"`
	ARN         string            `json:"Arn,omitempty"`
	CreateTime  float64           `json:"CreateTime,omitempty"`
}

// Column represents a column in a Glue table.
type Column struct {
	Parameters map[string]string `json:"Parameters,omitempty"`
	Name       string            `json:"Name"`
	Type       string            `json:"Type,omitempty"`
	Comment    string            `json:"Comment,omitempty"`
}

// SerDeInfo holds the serialization/deserialization information for a
// StorageDescriptor, mirroring aws-sdk-go-v2/service/glue/types.SerDeInfo.
type SerDeInfo struct {
	Parameters           map[string]string `json:"Parameters,omitempty"`
	Name                 string            `json:"Name,omitempty"`
	SerializationLibrary string            `json:"SerializationLibrary,omitempty"`
}

// Order specifies the sort order of a column, mirroring
// aws-sdk-go-v2/service/glue/types.Order.
type Order struct {
	Column    string `json:"Column"`
	SortOrder int    `json:"SortOrder"`
}

// StorageDescriptor describes the physical storage of a table or partition.
type StorageDescriptor struct {
	SerdeInfo              *SerDeInfo        `json:"SerdeInfo,omitempty"`
	Parameters             map[string]string `json:"Parameters,omitempty"`
	Location               string            `json:"Location,omitempty"`
	InputFormat            string            `json:"InputFormat,omitempty"`
	OutputFormat           string            `json:"OutputFormat,omitempty"`
	Columns                []Column          `json:"Columns,omitempty"`
	BucketColumns          []string          `json:"BucketColumns,omitempty"`
	SortColumns            []Order           `json:"SortColumns,omitempty"`
	NumberOfBuckets        int               `json:"NumberOfBuckets,omitempty"`
	Compressed             bool              `json:"Compressed,omitempty"`
	StoredAsSubDirectories bool              `json:"StoredAsSubDirectories,omitempty"`
}

// TableInput is the input for creating or updating a Glue table.
type TableInput struct {
	Parameters        map[string]string `json:"Parameters,omitempty"`
	Name              string            `json:"Name"`
	Description       string            `json:"Description,omitempty"`
	Owner             string            `json:"Owner,omitempty"`
	TableType         string            `json:"TableType,omitempty"`
	PartitionKeys     []Column          `json:"PartitionKeys,omitempty"`
	StorageDescriptor StorageDescriptor `json:"StorageDescriptor,omitzero"`
	Retention         int               `json:"Retention,omitempty"`
}

// Table represents a Glue catalog table.
type Table struct {
	Parameters        map[string]string `json:"Parameters,omitempty"`
	Name              string            `json:"Name"`
	DatabaseName      string            `json:"DatabaseName"`
	CatalogID         string            `json:"CatalogId"`
	Description       string            `json:"Description,omitempty"`
	Owner             string            `json:"Owner,omitempty"`
	TableType         string            `json:"TableType,omitempty"`
	PartitionKeys     []Column          `json:"PartitionKeys,omitempty"`
	StorageDescriptor StorageDescriptor `json:"StorageDescriptor,omitzero"`
	Retention         int               `json:"Retention,omitempty"`
	CreateTime        float64           `json:"CreateTime,omitempty"`
	UpdateTime        float64           `json:"UpdateTime,omitempty"`
}

// CrawlerTarget specifies the data stores a crawler scans. AWS supports many
// target store kinds (S3, JDBC, catalog, DynamoDB, Delta, Hudi, Iceberg,
// MongoDB); this backend models the three most commonly used ones. Additional
// kinds are deferred (see PARITY.md).
type CrawlerTarget struct {
	S3Targets      []S3Target      `json:"S3Targets,omitempty"`
	JdbcTargets    []JDBCTarget    `json:"JdbcTargets,omitempty"`
	CatalogTargets []CatalogTarget `json:"CatalogTargets,omitempty"`
}

// S3Target is an S3 path for a crawler.
type S3Target struct {
	Path       string   `json:"Path,omitempty"`
	Exclusions []string `json:"Exclusions,omitempty"`
}

// JDBCTarget is a JDBC connection/path pair for a crawler, mirroring
// aws-sdk-go-v2/service/glue/types.JdbcTarget.
type JDBCTarget struct {
	ConnectionName string   `json:"ConnectionName,omitempty"`
	Path           string   `json:"Path,omitempty"`
	Exclusions     []string `json:"Exclusions,omitempty"`
}

// CatalogTarget synchronizes an existing Data Catalog database/tables as a
// crawler target, mirroring aws-sdk-go-v2/service/glue/types.CatalogTarget.
type CatalogTarget struct {
	DatabaseName string   `json:"DatabaseName,omitempty"`
	Tables       []string `json:"Tables,omitempty"`
}

// Crawler represents a Glue crawler.
type Crawler struct {
	Tags          map[string]string `json:"-"`
	Schedule      CrawlerSchedule   `json:"Schedule,omitzero"`
	Name          string            `json:"Name"`
	Role          string            `json:"Role"`
	DatabaseName  string            `json:"DatabaseName"`
	State         string            `json:"State"`
	ARN           string            `json:"Arn,omitempty"`
	Description   string            `json:"Description,omitempty"`
	Configuration string            `json:"Configuration,omitempty"`
	TablePrefix   string            `json:"TablePrefix,omitempty"`
	Classifiers   []string          `json:"Classifiers,omitempty"`
	Targets       CrawlerTarget     `json:"Targets,omitzero"`
	CreationTime  float64           `json:"CreationTime,omitempty"`
	LastUpdated   float64           `json:"LastUpdated,omitempty"`
}

// CrawlHistoryEntry records a single crawl run for ListCrawls.
type CrawlHistoryEntry struct {
	CrawlID   string  `json:"CrawlId,omitempty"`
	State     string  `json:"State,omitempty"`
	Summary   string  `json:"Summary,omitempty"`
	StartTime float64 `json:"StartTime,omitempty"`
	EndTime   float64 `json:"EndTime,omitempty"`
}

// ConnectionsList holds connections for a Glue job.
type ConnectionsList struct {
	Connections []string `json:"Connections,omitempty"`
}

// ExecutionProperty holds max concurrent runs for a Glue job.
type ExecutionProperty struct {
	MaxConcurrentRuns int `json:"MaxConcurrentRuns,omitempty"`
}

// JobCommand holds the command for a Glue job.
type JobCommand struct {
	Name           string `json:"Name,omitempty"`
	ScriptLocation string `json:"ScriptLocation,omitempty"`
	PythonVersion  string `json:"PythonVersion,omitempty"`
}

// Job represents a Glue job.
type Job struct {
	SourceControlDetails *SourceControlDetails `json:"SourceControlDetails,omitempty"`
	Tags                 map[string]string     `json:"-"`
	DefaultArguments     map[string]string     `json:"DefaultArguments,omitempty"`
	Command              JobCommand            `json:"Command,omitzero"`
	WorkerType           string                `json:"WorkerType,omitempty"`
	Role                 string                `json:"Role,omitempty"`
	GlueVersion          string                `json:"GlueVersion,omitempty"`
	Name                 string                `json:"Name"`
	ARN                  string                `json:"Arn,omitempty"`
	Description          string                `json:"Description,omitempty"`
	Connections          ConnectionsList       `json:"Connections,omitzero"`
	NotificationProperty NotificationProperty  `json:"NotificationProperty,omitzero"`
	NumberOfWorkers      int                   `json:"NumberOfWorkers,omitempty"`
	MaxRetries           int                   `json:"MaxRetries,omitempty"`
	Timeout              int                   `json:"Timeout,omitempty"`
	// MaxCapacity is the DPU capacity for jobs that use it instead of
	// WorkerType+NumberOfWorkers (e.g. Python shell jobs, or Spark jobs on
	// Glue versions that predate worker-type based capacity). AWS rejects a
	// request that sets both MaxCapacity and WorkerType/NumberOfWorkers.
	MaxCapacity       float64           `json:"MaxCapacity,omitempty"`
	ExecutionProperty ExecutionProperty `json:"ExecutionProperty,omitzero"`
	CreatedOn         float64           `json:"CreatedOn,omitempty"`
	LastModifiedOn    float64           `json:"LastModifiedOn,omitempty"`
}

// NotificationProperty specifies the delay, in minutes, after which a job run
// notification is sent (JobRun.NotificationProperty / Job.NotificationProperty).
type NotificationProperty struct {
	NotifyDelayAfter int `json:"NotifyDelayAfter,omitempty"`
}

// SourceControlDetails records the remote-repository link for a job synchronized
// via UpdateJobFromSourceControl / UpdateSourceControlFromJob.
type SourceControlDetails struct {
	AuthStrategy string `json:"AuthStrategy,omitempty"`
	AuthToken    string `json:"AuthToken,omitempty"`
	Branch       string `json:"Branch,omitempty"`
	Folder       string `json:"Folder,omitempty"`
	LastCommitID string `json:"LastCommitId,omitempty"`
	Owner        string `json:"Owner,omitempty"`
	Provider     string `json:"Provider,omitempty"`
	Repository   string `json:"Repository,omitempty"`
}

// ErrorDetail holds an error code and message for batch operation failures.
type ErrorDetail struct {
	ErrorCode    string `json:"ErrorCode"`
	ErrorMessage string `json:"ErrorMessage"`
}

// PartitionValueList identifies a partition by its values.
type PartitionValueList struct {
	Values []string `json:"Values"`
}

// PartitionInput is the input for creating a partition.
type PartitionInput struct {
	Parameters        map[string]string `json:"Parameters,omitempty"`
	Values            []string          `json:"Values"`
	StorageDescriptor StorageDescriptor `json:"StorageDescriptor,omitzero"`
}

// Partition represents a Glue table partition.
type Partition struct {
	Parameters        map[string]string `json:"Parameters,omitempty"`
	DatabaseName      string            `json:"DatabaseName"`
	TableName         string            `json:"TableName"`
	CatalogID         string            `json:"CatalogId,omitempty"`
	Values            []string          `json:"Values"`
	StorageDescriptor StorageDescriptor `json:"StorageDescriptor,omitzero"`
	CreationTime      float64           `json:"CreationTime,omitempty"`
}

// PartitionError represents an error for a single partition operation.
type PartitionError struct {
	ErrorDetail     ErrorDetail `json:"ErrorDetail"`
	PartitionValues []string    `json:"PartitionValues"`
}

// TableError represents an error for a single table operation.
type TableError struct {
	TableName   string      `json:"TableName"`
	ErrorDetail ErrorDetail `json:"ErrorDetail"`
}

// TableVersion represents a version of a Glue table.
type TableVersion struct {
	Table     *Table `json:"Table,omitempty"`
	VersionID string `json:"VersionId"`
}

// TableVersionError represents an error for a table version operation.
type TableVersionError struct {
	TableName   string      `json:"TableName"`
	VersionID   string      `json:"VersionId"`
	ErrorDetail ErrorDetail `json:"ErrorDetail"`
}

// Connection represents a Glue connection.
type Connection struct {
	ConnectionProperties map[string]string `json:"ConnectionProperties,omitempty"`
	Tags                 map[string]string `json:"-"`
	Name                 string            `json:"Name"`
	ConnectionType       string            `json:"ConnectionType,omitempty"`
	ARN                  string            `json:"Arn,omitempty"`
	CreationTime         float64           `json:"CreationTime,omitempty"`
	LastUpdatedTime      float64           `json:"LastUpdatedTime,omitempty"`
}

// Blueprint represents a Glue blueprint.
type Blueprint struct {
	Name   string `json:"Name"`
	Status string `json:"Status,omitempty"`
}

// CustomEntityType represents a Glue custom entity type.
type CustomEntityType struct {
	Name         string   `json:"Name"`
	RegexString  string   `json:"RegexString,omitempty"`
	ContextWords []string `json:"ContextWords,omitempty"`
}

// DataQualityResult represents a Glue data quality result.
type DataQualityResult struct {
	ResultID string  `json:"ResultId"`
	Score    float64 `json:"Score,omitempty"`
}

// DevEndpoint represents a Glue development endpoint.
type DevEndpoint struct {
	Arguments    map[string]string `json:"Arguments,omitempty"`
	EndpointName string            `json:"EndpointName"`
	Status       string            `json:"Status,omitempty"`
}

// CrawlerSchedule represents the schedule configuration for a crawler.
type CrawlerSchedule struct {
	ScheduleExpression string `json:"ScheduleExpression,omitempty"`
	State              string `json:"State,omitempty"`
}

// JobRun represents a single execution of a Glue job.
type JobRun struct {
	Arguments       map[string]string `json:"Arguments,omitempty"`
	ID              string            `json:"Id"`
	JobName         string            `json:"JobName"`
	JobRunState     string            `json:"JobRunState"`
	ErrorMessage    string            `json:"ErrorMessage,omitempty"`
	WorkerType      string            `json:"WorkerType,omitempty"`
	GlueVersion     string            `json:"GlueVersion,omitempty"`
	StartedOn       float64           `json:"StartedOn,omitempty"`
	CompletedOn     float64           `json:"CompletedOn,omitempty"`
	MaxCapacity     float64           `json:"MaxCapacity,omitempty"`
	ExecutionTime   int               `json:"ExecutionTime,omitempty"`
	NumberOfWorkers int               `json:"NumberOfWorkers,omitempty"`
	Timeout         int               `json:"Timeout,omitempty"`
}

// JobBookmark holds the bookmark state for a job run.
type JobBookmark struct {
	JobName   string `json:"JobName"`
	Run       string `json:"Run,omitempty"`
	ActiveRun string `json:"ActiveRun,omitempty"`
	Version   int    `json:"Version"`
	Attempt   int    `json:"Attempt,omitempty"`
}

// BatchStopJobRunError holds error info for a single stop attempt.
type BatchStopJobRunError struct {
	ErrorDetail ErrorDetail `json:"ErrorDetail"`
	JobRunID    string      `json:"JobRunId"`
	JobName     string      `json:"JobName"`
}

// DataQualityRuleset represents a Glue data quality ruleset.
type DataQualityRuleset struct {
	Tags           map[string]string `json:"-"`
	Name           string            `json:"Name"`
	Ruleset        string            `json:"Ruleset,omitempty"`
	Description    string            `json:"Description,omitempty"`
	ARN            string            `json:"Arn,omitempty"`
	CreatedOn      float64           `json:"CreatedOn,omitempty"`
	LastModifiedOn float64           `json:"LastModifiedOn,omitempty"`
}

// DataQualityEvaluationRun represents a data quality ruleset evaluation run.
type DataQualityEvaluationRun struct {
	RunID        string   `json:"RunId"`
	Status       string   `json:"Status"`
	ErrorString  string   `json:"ErrorString,omitempty"`
	RulesetNames []string `json:"RulesetNames,omitempty"`
	StartedOn    float64  `json:"StartedOn,omitempty"`
	CompletedOn  float64  `json:"CompletedOn,omitempty"`
}

// CrawlerOptions holds the CreateCrawler/UpdateCrawler fields beyond the core
// name/role/database/targets/tags accepted by CreateCrawler and UpdateCrawler.
// It exists so those two methods' signatures — called from outside this
// package (services/cloudformation) — stay additive/stable while still
// letting the Glue handler pass through Schedule, Classifiers, Configuration,
// TablePrefix and Description.
type CrawlerOptions struct {
	Description   string
	Schedule      string // cron expression; empty means on-demand (no schedule)
	Configuration string
	TablePrefix   string
	Classifiers   []string
}

// UsageProfile represents a Glue usage profile.
type UsageProfile struct {
	CreatedOn      time.Time         `json:"CreatedOn"`
	LastModifiedOn time.Time         `json:"LastModifiedOn"`
	Tags           map[string]string `json:"Tags,omitempty"`
	Name           string            `json:"Name"`
	Description    string            `json:"Description,omitempty"`
}

// BlueprintRun represents a single execution of a Glue blueprint.
type BlueprintRun struct {
	StartedOn     time.Time `json:"StartedOn"`
	BlueprintName string    `json:"BlueprintName"`
	RunID         string    `json:"RunId"`
	WorkflowName  string    `json:"WorkflowName"`
	State         string    `json:"State"`
}

// DQRuleRecommendationRun represents a data quality rule recommendation run.
type DQRuleRecommendationRun struct {
	StartedOn           time.Time `json:"StartedOn"`
	RecommendationRunID string    `json:"RecommendationRunId"`
	DataSourceS3Path    string    `json:"DataSourceS3Path,omitempty"`
	Status              string    `json:"Status"`
}

// ColumnStatisticsTaskSettings represents column statistics task settings.
type ColumnStatisticsTaskSettings struct {
	Schedule       CrawlerSchedule `json:"Schedule,omitzero"`
	DatabaseName   string          `json:"DatabaseName"`
	TableName      string          `json:"TableName"`
	RoleArn        string          `json:"RoleArn,omitempty"`
	ColumnNameList []string        `json:"ColumnNameList,omitempty"`
}

// ColumnStatisticsTaskRun represents a column statistics task run.
type ColumnStatisticsTaskRun struct {
	StartedOn                 time.Time `json:"StartedOn"`
	DatabaseName              string    `json:"DatabaseName"`
	TableName                 string    `json:"TableName"`
	ColumnStatisticsTaskRunID string    `json:"ColumnStatisticsTaskRunId"`
	Status                    string    `json:"Status"`
}

// MaterializedViewRefreshRun represents a materialized view refresh task run.
type MaterializedViewRefreshRun struct {
	StartedOn    time.Time `json:"StartedOn"`
	DatabaseName string    `json:"DatabaseName"`
	TableName    string    `json:"TableName"`
	TaskRunID    string    `json:"TaskRunId"`
	Status       string    `json:"Status"`
}

// Integration represents a Glue integration.
type Integration struct {
	CreatedAt       time.Time         `json:"CreateTime"`
	Tags            map[string]string `json:"Tags,omitempty"`
	IntegrationName string            `json:"IntegrationName"`
	Status          string            `json:"Status"`
}

// IdentityCenterConfig represents the Glue Identity Center configuration.
type IdentityCenterConfig struct {
	InstanceARN string `json:"InstanceArn,omitempty"`
	Status      string `json:"Status"`
}

// IntegrationResourceProperty stores resource-level properties for a Zero-ETL integration.
type IntegrationResourceProperty struct {
	CreatedAt        time.Time         `json:"CreateTime"`
	SourceProperties map[string]string `json:"SourceProperties,omitempty"`
	TargetProperties map[string]string `json:"TargetProperties,omitempty"`
	ResourceArn      string            `json:"ResourceArn"`
}

// IntegrationTableProperties stores table-level properties for a Zero-ETL integration.
type IntegrationTableProperties struct {
	SourceTableConfig map[string]any `json:"SourceTableConfig,omitempty"`
	TargetTableConfig map[string]any `json:"TargetTableConfig,omitempty"`
	ResourceArn       string         `json:"ResourceArn"`
	TableName         string         `json:"TableName"`
}

// StatisticAnnotation records an inclusion annotation applied via
// PutDataQualityProfileAnnotation (profile-wide, StatisticID == "") or
// BatchPutDataQualityStatisticAnnotation (per-statistic).
type StatisticAnnotation struct {
	ProfileID      string  `json:"ProfileId"`
	StatisticID    string  `json:"StatisticId,omitempty"`
	Inclusion      string  `json:"Inclusion,omitempty"`
	RecordedOn     float64 `json:"RecordedOn,omitempty"`
	LastModifiedOn float64 `json:"LastModifiedOn,omitempty"`
}

// ConnectionTypeInfo describes a Glue connection type (connector). BuiltIn types are
// AWS-managed and undeletable; custom types are registered via RegisterConnectionType.
type ConnectionTypeInfo struct {
	// ConnectionType is the canonical connector name (e.g. "JDBC", "SALESFORCE").
	ConnectionType string `json:"ConnectionType"`
	// Description is a human-readable description of the connector.
	Description string `json:"Description,omitempty"`
	// Category groups connectors (e.g. "DATABASE", "SAAS", "STREAMING").
	Category string `json:"Category,omitempty"`
	// Capabilities lists supported connector capabilities.
	Capabilities []string `json:"Capabilities,omitempty"`
	// BuiltIn reports whether this is an AWS-managed (undeletable) type.
	BuiltIn bool `json:"BuiltIn"`
}

// MLTaskType is the category of an ML transform task run.
type MLTaskType string

// MLTaskRun represents a single ML transform task run.
type MLTaskRun struct {
	Properties    map[string]string `json:"Properties,omitempty"`
	TransformID   string            `json:"TransformId"`
	TaskRunID     string            `json:"TaskRunId"`
	TaskType      string            `json:"TaskType"`
	Status        string            `json:"Status"`
	ErrorString   string            `json:"ErrorString,omitempty"`
	LogGroupName  string            `json:"LogGroupName,omitempty"`
	StartedOn     float64           `json:"StartedOn,omitempty"`
	CompletedOn   float64           `json:"CompletedOn,omitempty"`
	ExecutionTime int               `json:"ExecutionTime,omitempty"`
}

// ResourceURI holds a URI for a UDF resource.
type ResourceURI struct {
	ResourceType string `json:"ResourceType,omitempty"`
	URI          string `json:"Uri,omitempty"`
}

// UserDefinedFunction represents a Glue UDF.
type UserDefinedFunction struct {
	DatabaseName string        `json:"DatabaseName"`
	FunctionName string        `json:"FunctionName"`
	ClassName    string        `json:"ClassName,omitempty"`
	OwnerName    string        `json:"OwnerName,omitempty"`
	OwnerType    string        `json:"OwnerType,omitempty"`
	FunctionARN  string        `json:"FunctionArn,omitempty"`
	ResourceURIs []ResourceURI `json:"ResourceUris,omitempty"`
	CreateTime   float64       `json:"CreateTime,omitempty"`
}

// EncryptionConfiguration holds encryption settings for a SecurityConfiguration.
type EncryptionConfiguration struct {
	CloudWatchEncryption   *CloudWatchEncryption   `json:"CloudWatchEncryption,omitempty"`
	JobBookmarksEncryption *JobBookmarksEncryption `json:"JobBookmarksEncryption,omitempty"`
	S3Encryption           []S3EncryptionEntry     `json:"S3Encryption,omitempty"`
}

// S3EncryptionEntry holds per-S3-bucket encryption config.
type S3EncryptionEntry struct {
	S3EncryptionMode string `json:"S3EncryptionMode,omitempty"`
	KMSKeyARN        string `json:"KmsKeyArn,omitempty"`
}

// CloudWatchEncryption holds CloudWatch encryption config.
type CloudWatchEncryption struct {
	CloudWatchEncryptionMode string `json:"CloudWatchEncryptionMode,omitempty"`
	KMSKeyARN                string `json:"KmsKeyArn,omitempty"`
}

// JobBookmarksEncryption holds job bookmarks encryption config.
type JobBookmarksEncryption struct {
	JobBookmarksEncryptionMode string `json:"JobBookmarksEncryptionMode,omitempty"`
	KMSKeyARN                  string `json:"KmsKeyArn,omitempty"`
}

// SecurityConfiguration represents a Glue security configuration.
type SecurityConfiguration struct {
	Name                    string                  `json:"Name"`
	EncryptionConfiguration EncryptionConfiguration `json:"EncryptionConfiguration"`
	CreatedTimeStamp        float64                 `json:"CreatedTimeStamp,omitempty"`
}

// SessionCommand holds the command info for a Glue interactive session.
type SessionCommand struct {
	Name          string `json:"Name,omitempty"`
	PythonVersion string `json:"PythonVersion,omitempty"`
}

// Session represents a Glue interactive session.
type Session struct {
	DefaultArguments map[string]string `json:"DefaultArguments,omitempty"`
	Command          SessionCommand    `json:"Command,omitzero"`
	SessionID        string            `json:"Id"`
	Role             string            `json:"Role,omitempty"`
	Status           string            `json:"Status"`
	Description      string            `json:"Description,omitempty"`
	CreatedOn        float64           `json:"CreatedOn,omitempty"`
	MaxCapacity      float64           `json:"MaxCapacity,omitempty"`
	Timeout          int32             `json:"Timeout,omitempty"`
}

// Statement represents a statement run within a Glue session.
type Statement struct {
	Output      any     `json:"Output,omitempty"`
	SessionId   string  `json:"SessionId,omitempty"` //nolint:revive,staticcheck // AWS API naming
	Code        string  `json:"Code,omitempty"`
	State       string  `json:"State"`
	Progress    float64 `json:"Progress,omitempty"`
	StartedOn   float64 `json:"StartedOn,omitempty"`
	CompletedOn float64 `json:"CompletedOn,omitempty"`
	Id          int32   `json:"Id"` //nolint:revive,staticcheck // AWS API uses Id not ID
}

// TableOptimizerConfiguration holds the config for a table optimizer.
type TableOptimizerConfiguration struct {
	RoleARN string `json:"RoleArn,omitempty"`
	Enabled bool   `json:"Enabled"`
}

// TableOptimizerRun holds a single run record for a table optimizer. The Glue
// Iceberg table-optimizer sub-API serializes this nested document with
// lowerCamelCase keys, unlike the rest of the Glue JSON protocol.
type TableOptimizerRun struct {
	Metrics   any     `json:"metrics,omitempty"`
	EventType string  `json:"eventType,omitempty"`
	Error     string  `json:"error,omitempty"`
	StartedAt float64 `json:"startTimestamp,omitempty"`
	EndedAt   float64 `json:"endTimestamp,omitempty"`
}

// TableOptimizer represents a single table optimizer resource.
type TableOptimizer struct {
	LastRun       *TableOptimizerRun          `json:"LastRun,omitempty"`
	CatalogID     string                      `json:"CatalogId,omitempty"`
	DatabaseName  string                      `json:"DatabaseName"`
	TableName     string                      `json:"TableName"`
	Type          string                      `json:"Type"`
	Configuration TableOptimizerConfiguration `json:"Configuration,omitzero"`
}

// BatchGetTableOptimizerEntry is one request entry for BatchGetTableOptimizer.
type BatchGetTableOptimizerEntry struct {
	CatalogID    string `json:"CatalogId,omitempty"`
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
	Type         string `json:"Type"`
}

// BatchGetTableOptimizerError is one error entry from BatchGetTableOptimizer.
type BatchGetTableOptimizerError struct {
	CatalogID    string      `json:"CatalogId,omitempty"`
	DatabaseName string      `json:"DatabaseName,omitempty"`
	TableName    string      `json:"TableName,omitempty"`
	Type         string      `json:"Type,omitempty"`
	Error        ErrorDetail `json:"Error"`
}

// ColumnStatisticsData holds statistics for a column.
type ColumnStatisticsData struct {
	BooleanColumnStatisticsData any    `json:"BooleanColumnStatisticsData,omitempty"`
	DateColumnStatisticsData    any    `json:"DateColumnStatisticsData,omitempty"`
	DecimalColumnStatisticsData any    `json:"DecimalColumnStatisticsData,omitempty"`
	DoubleColumnStatisticsData  any    `json:"DoubleColumnStatisticsData,omitempty"`
	LongColumnStatisticsData    any    `json:"LongColumnStatisticsData,omitempty"`
	StringColumnStatisticsData  any    `json:"StringColumnStatisticsData,omitempty"`
	BinaryColumnStatisticsData  any    `json:"BinaryColumnStatisticsData,omitempty"`
	Type                        string `json:"Type"`
}

// ColumnStatistics represents statistics for a single column.
type ColumnStatistics struct {
	StatisticsData ColumnStatisticsData `json:"StatisticsData"`
	ColumnName     string               `json:"ColumnName"`
	ColumnType     string               `json:"ColumnType,omitempty"`
	AnalyzedTime   float64              `json:"AnalyzedTime,omitempty"`
}

type resourcePolicyEntry struct {
	Policy     string  `json:"Policy"`
	Hash       string  `json:"Hash"`
	CreateTime float64 `json:"CreateTime,omitempty"`
	UpdateTime float64 `json:"UpdateTime,omitempty"`
}

// MLTransformParameter holds transform hyperparameters.
type MLTransformParameter struct {
	FindMatchesParameters any    `json:"FindMatchesParameters,omitempty"`
	TransformType         string `json:"TransformType,omitempty"`
}

// GlueTable holds a reference to a Glue catalog table used by an ML transform.
type GlueTable struct { //nolint:revive // GlueTable is distinct from Table type; renaming would cause conflicts
	CatalogID      string `json:"CatalogId,omitempty"`
	DatabaseName   string `json:"DatabaseName"`
	TableName      string `json:"TableName"`
	ConnectionName string `json:"ConnectionName,omitempty"`
}

// MLTransform represents an AWS Glue ML transform.
type MLTransform struct {
	Parameters        MLTransformParameter `json:"Parameters,omitzero"`
	TransformID       string               `json:"TransformId"`
	Name              string               `json:"Name"`
	Description       string               `json:"Description,omitempty"`
	Role              string               `json:"Role,omitempty"`
	GlueVersion       string               `json:"GlueVersion,omitempty"`
	WorkerType        string               `json:"WorkerType,omitempty"`
	Status            string               `json:"Status"`
	InputRecordTables []GlueTable          `json:"InputRecordTables,omitempty"`
	MaxCapacity       float64              `json:"MaxCapacity,omitempty"`
	CreatedOn         float64              `json:"CreatedOn,omitempty"`
	LastModifiedOn    float64              `json:"LastModifiedOn,omitempty"`
	NumberOfWorkers   int32                `json:"NumberOfWorkers,omitempty"`
}

// CatalogEntry represents a named AWS Glue catalog.
type CatalogEntry struct {
	Parameters  map[string]string `json:"Parameters,omitzero"`
	CatalogID   string            `json:"CatalogId"`
	Name        string            `json:"Name,omitempty"`
	Description string            `json:"Description,omitempty"`
	CreateTime  float64           `json:"CreateTime,omitempty"`
}

// DataCatalogEncryptionSettings holds catalog encryption settings.
type DataCatalogEncryptionSettings struct {
	EncryptionAtRest             *EncryptionAtRest             `json:"EncryptionAtRest,omitempty"`
	ConnectionPasswordEncryption *ConnectionPasswordEncryption `json:"ConnectionPasswordEncryption,omitempty"`
}

// EncryptionAtRest holds at-rest encryption config.
type EncryptionAtRest struct {
	CatalogEncryptionMode        string `json:"CatalogEncryptionMode,omitempty"`
	SseAwsKmsKeyID               string `json:"SseAwsKmsKeyId,omitempty"`
	CatalogEncryptionServiceRole string `json:"CatalogEncryptionServiceRole,omitempty"`
}

// ConnectionPasswordEncryption holds connection password encryption config.
type ConnectionPasswordEncryption struct {
	AwsKmsKeyID                       string `json:"AwsKmsKeyId,omitempty"`
	ReturnConnectionPasswordEncrypted bool   `json:"ReturnConnectionPasswordEncrypted"`
}

// CatalogImportStatus records the Hive metastore import completion state.
type CatalogImportStatus struct {
	ImportedBy      string  `json:"ImportedBy"`
	ImportTime      float64 `json:"ImportTime"`
	ImportCompleted bool    `json:"ImportCompleted"`
}

// MappingEntry describes a single source-to-target column mapping for GetPlan.
type MappingEntry struct {
	SourceType  string `json:"SourceType"`
	SourcePath  string `json:"SourcePath"`
	SourceTable string `json:"SourceTable"`
	TargetType  string `json:"TargetType"`
	TargetPath  string `json:"TargetPath"`
	TargetTable string `json:"TargetTable"`
}

// Registry represents a Glue Schema Registry.
type Registry struct {
	Tags        map[string]string `json:"Tags,omitempty"`
	Name        string            `json:"RegistryName"`
	ARN         string            `json:"RegistryArn"`
	Description string            `json:"Description,omitempty"`
	Status      string            `json:"Status"`
	CreatedTime float64           `json:"CreatedTime,omitempty"`
	UpdatedTime float64           `json:"UpdatedTime,omitempty"`
}

// Schema represents a Glue Schema Registry schema.
type Schema struct {
	Tags                map[string]string `json:"Tags,omitempty"`
	RegistryName        string            `json:"RegistryName"`
	SchemaName          string            `json:"SchemaName"`
	SchemaARN           string            `json:"SchemaArn"`
	RegistryARN         string            `json:"RegistryArn"`
	DataFormat          string            `json:"DataFormat"`
	Compatibility       string            `json:"Compatibility"`
	Description         string            `json:"Description,omitempty"`
	SchemaStatus        string            `json:"SchemaStatus"`
	CreatedTime         float64           `json:"CreatedTime,omitempty"`
	UpdatedTime         float64           `json:"UpdatedTime,omitempty"`
	LatestSchemaVersion int64             `json:"LatestSchemaVersion"`
	NextSchemaVersion   int64             `json:"NextSchemaVersion"`
	CheckpointVersion   int64             `json:"SchemaCheckpoint"`
}

// SchemaVersion represents a single version of a schema.
type SchemaVersion struct {
	SchemaVersionID  string  `json:"SchemaVersionId"`
	SchemaARN        string  `json:"SchemaArn"`
	SchemaDefinition string  `json:"SchemaDefinition,omitempty"`
	Status           string  `json:"Status"`
	DataFormat       string  `json:"DataFormat,omitempty"`
	VersionNumber    int64   `json:"VersionNumber"`
	CreatedTime      float64 `json:"CreatedTime,omitempty"`
}

// CrawlerMetrics holds runtime metrics for a crawler.
type CrawlerMetrics struct {
	CrawlerName          string  `json:"CrawlerName"`
	TimeLeftSeconds      float64 `json:"TimeLeftSeconds"`
	LastRuntimeSeconds   float64 `json:"LastRuntimeSeconds"`
	MedianRuntimeSeconds float64 `json:"MedianRuntimeSeconds"`
	TablesCreated        int     `json:"TablesCreated"`
	TablesUpdated        int     `json:"TablesUpdated"`
	TablesDeleted        int     `json:"TablesDeleted"`
	StillEstimating      bool    `json:"StillEstimating"`
}

// TriggerAction represents an action for a Glue trigger. An action fires either a
// job (JobName) or a crawler (CrawlerName) — real AWS triggers support both.
type TriggerAction struct {
	Arguments   map[string]string `json:"Arguments,omitempty"`
	JobName     string            `json:"JobName,omitempty"`
	CrawlerName string            `json:"CrawlerName,omitempty"`
}

// TriggerPredicate represents a predicate for a conditional trigger.
type TriggerPredicate struct {
	Logical    string             `json:"Logical,omitempty"`
	Conditions []TriggerCondition `json:"Conditions,omitempty"`
}

// TriggerCondition represents a condition within a trigger predicate.
type TriggerCondition struct {
	JobName         string `json:"JobName,omitempty"`
	LogicalOperator string `json:"LogicalOperator,omitempty"`
	State           string `json:"State,omitempty"`
}

// Trigger represents a Glue trigger.
type Trigger struct {
	Tags      map[string]string `json:"-"`
	Predicate *TriggerPredicate `json:"Predicate,omitempty"`
	ARN       string            `json:"Arn,omitempty"`
	Name      string            `json:"Name"`
	Type      string            `json:"Type,omitempty"`
	State     string            `json:"State,omitempty"`
	Schedule  string            `json:"Schedule,omitempty"`
	Actions   []TriggerAction   `json:"Actions,omitempty"`
	// StartOnCreation mirrors CreateTriggerInput.StartOnCreation: when true, a
	// SCHEDULED or CONDITIONAL trigger is activated immediately on creation. It is
	// not part of the Trigger wire shape itself (hence json:"-"), only of the
	// create request, matching the real CreateTriggerInput/Trigger type split.
	StartOnCreation bool `json:"-"`
}

// Workflow represents a Glue workflow.
type Workflow struct {
	Tags                 map[string]string `json:"-"`
	DefaultRunProperties map[string]string `json:"DefaultRunProperties,omitempty"`
	Name                 string            `json:"Name"`
	Description          string            `json:"Description,omitempty"`
	ARN                  string            `json:"Arn,omitempty"`
	CreatedOn            float64           `json:"CreatedOn,omitempty"`
	LastModifiedOn       float64           `json:"LastModifiedOn,omitempty"`
}

// WorkflowRun represents a single run of a Glue workflow.
type WorkflowRun struct {
	Properties   map[string]string `json:"WorkflowRunProperties,omitempty"`
	WorkflowName string            `json:"WorkflowName"`
	RunID        string            `json:"WorkflowRunId"`
	Status       string            `json:"Status"`
	StartedOn    float64           `json:"StartedOn,omitempty"`
	CompletedOn  float64           `json:"CompletedOn,omitempty"`
}

// GrokClassifier is a Grok-based classifier.
type GrokClassifier struct {
	Name           string `json:"Name"`
	Classification string `json:"Classification,omitempty"`
	GrokPattern    string `json:"GrokPattern,omitempty"`
	CustomPatterns string `json:"CustomPatterns,omitempty"`
}

// XMLClassifier is an XML-based classifier.
type XMLClassifier struct {
	Name           string `json:"Name"`
	Classification string `json:"Classification,omitempty"`
	RowTag         string `json:"RowTag,omitempty"`
}

// JSONClassifier is a JSON-based classifier.
type JSONClassifier struct {
	Name     string `json:"Name"`
	JSONPath string `json:"JsonPath,omitempty"`
}

// CsvClassifier is a CSV-based classifier.
type CsvClassifier struct {
	Name        string   `json:"Name"`
	Delimiter   string   `json:"Delimiter,omitempty"`
	QuoteSymbol string   `json:"QuoteSymbol,omitempty"`
	Header      []string `json:"Header,omitempty"`
}

// Classifier wraps the four classifier types.
type Classifier struct {
	GrokClassifier *GrokClassifier `json:"GrokClassifier,omitempty"`
	XMLClassifier  *XMLClassifier  `json:"XMLClassifier,omitempty"`
	JSONClassifier *JSONClassifier `json:"JsonClassifier,omitempty"`
	CsvClassifier  *CsvClassifier  `json:"CsvClassifier,omitempty"`
}
