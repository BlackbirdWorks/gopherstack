package glue

import (
	"time"
)

// DatabaseInput is the input for creating or updating a Glue database.
type DatabaseInput struct {
	Parameters                    map[string]string      `json:"Parameters,omitempty"`
	TargetDatabase                *DatabaseIdentifier    `json:"TargetDatabase,omitempty"`
	Name                          string                 `json:"Name"`
	Description                   string                 `json:"Description,omitempty"`
	LocationURI                   string                 `json:"LocationUri,omitempty"`
	CreateTableDefaultPermissions []PrincipalPermissions `json:"CreateTableDefaultPermissions,omitempty"`
}

// Database represents a Glue catalog database.
type Database struct {
	Tags                          map[string]string      `json:"-"`
	Parameters                    map[string]string      `json:"Parameters,omitempty"`
	TargetDatabase                *DatabaseIdentifier    `json:"TargetDatabase,omitempty"`
	Name                          string                 `json:"Name"`
	Description                   string                 `json:"Description,omitempty"`
	CatalogID                     string                 `json:"CatalogId"`
	ARN                           string                 `json:"Arn,omitempty"`
	LocationURI                   string                 `json:"LocationUri,omitempty"`
	CreateTableDefaultPermissions []PrincipalPermissions `json:"CreateTableDefaultPermissions,omitempty"`
	CreateTime                    float64                `json:"CreateTime,omitempty"`
}

// DatabaseIdentifier identifies a target database for resource-linking,
// mirroring aws-sdk-go-v2/service/glue/types.DatabaseIdentifier.
type DatabaseIdentifier struct {
	CatalogID    string `json:"CatalogId,omitempty"`
	DatabaseName string `json:"DatabaseName,omitempty"`
	Region       string `json:"Region,omitempty"`
}

// PrincipalPermissions grants a set of Lake Formation permissions to a
// principal, mirroring aws-sdk-go-v2/service/glue/types.PrincipalPermissions.
type PrincipalPermissions struct {
	Principal   *DataLakePrincipal `json:"Principal,omitempty"`
	Permissions []string           `json:"Permissions,omitempty"`
}

// DataLakePrincipal identifies a Lake Formation principal, mirroring
// aws-sdk-go-v2/service/glue/types.DataLakePrincipal.
type DataLakePrincipal struct {
	DataLakePrincipalIdentifier string `json:"DataLakePrincipalIdentifier,omitempty"`
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

// CrawlerTarget specifies the data stores a crawler scans, mirroring
// aws-sdk-go-v2/service/glue/types.CrawlerTargets (S3, JDBC, catalog,
// DynamoDB, Delta, Hudi, Iceberg, MongoDB — all eight target kinds).
type CrawlerTarget struct {
	S3Targets       []S3Target       `json:"S3Targets,omitempty"`
	JdbcTargets     []JDBCTarget     `json:"JdbcTargets,omitempty"`
	CatalogTargets  []CatalogTarget  `json:"CatalogTargets,omitempty"`
	DynamoDBTargets []DynamoDBTarget `json:"DynamoDBTargets,omitempty"`
	DeltaTargets    []DeltaTarget    `json:"DeltaTargets,omitempty"`
	HudiTargets     []HudiTarget     `json:"HudiTargets,omitempty"`
	IcebergTargets  []IcebergTarget  `json:"IcebergTargets,omitempty"`
	MongoDBTargets  []MongoDBTarget  `json:"MongoDBTargets,omitempty"`
}

// DynamoDBTarget is a DynamoDB table crawl target, mirroring
// aws-sdk-go-v2/service/glue/types.DynamoDBTarget.
type DynamoDBTarget struct {
	Path     string  `json:"Path,omitempty"`
	ScanAll  bool    `json:"scanAll,omitempty"`
	ScanRate float64 `json:"scanRate,omitempty"`
}

// DeltaTarget is a Delta Lake table crawl target, mirroring
// aws-sdk-go-v2/service/glue/types.DeltaTarget.
type DeltaTarget struct {
	ConnectionName         string   `json:"ConnectionName,omitempty"`
	DeltaTables            []string `json:"DeltaTables,omitempty"`
	WriteManifest          bool     `json:"WriteManifest,omitempty"`
	CreateNativeDeltaTable bool     `json:"CreateNativeDeltaTable,omitempty"`
}

// HudiTarget is an Apache Hudi table crawl target, mirroring
// aws-sdk-go-v2/service/glue/types.HudiTarget.
type HudiTarget struct {
	ConnectionName        string   `json:"ConnectionName,omitempty"`
	Paths                 []string `json:"Paths,omitempty"`
	Exclusions            []string `json:"Exclusions,omitempty"`
	MaximumTraversalDepth int32    `json:"MaximumTraversalDepth,omitempty"`
}

// IcebergTarget is an Apache Iceberg table crawl target, mirroring
// aws-sdk-go-v2/service/glue/types.IcebergTarget.
type IcebergTarget struct {
	ConnectionName        string   `json:"ConnectionName,omitempty"`
	Paths                 []string `json:"Paths,omitempty"`
	Exclusions            []string `json:"Exclusions,omitempty"`
	MaximumTraversalDepth int32    `json:"MaximumTraversalDepth,omitempty"`
}

// MongoDBTarget is a DocumentDB/MongoDB crawl target, mirroring
// aws-sdk-go-v2/service/glue/types.MongoDBTarget.
type MongoDBTarget struct {
	ConnectionName string `json:"ConnectionName,omitempty"`
	Path           string `json:"Path,omitempty"`
	ScanAll        bool   `json:"ScanAll,omitempty"`
}

// SchemaChangePolicy specifies a crawler's update/deletion behavior, mirroring
// aws-sdk-go-v2/service/glue/types.SchemaChangePolicy.
type SchemaChangePolicy struct {
	UpdateBehavior string `json:"UpdateBehavior,omitempty"`
	DeleteBehavior string `json:"DeleteBehavior,omitempty"`
}

// RecrawlPolicy specifies a crawler's re-crawl behavior, mirroring
// aws-sdk-go-v2/service/glue/types.RecrawlPolicy.
type RecrawlPolicy struct {
	RecrawlBehavior string `json:"RecrawlBehavior,omitempty"`
}

// LineageConfiguration specifies a crawler's data-lineage settings, mirroring
// aws-sdk-go-v2/service/glue/types.LineageConfiguration.
type LineageConfiguration struct {
	CrawlerLineageSettings string `json:"CrawlerLineageSettings,omitempty"`
}

// LakeFormationConfiguration specifies a crawler's Lake Formation settings,
// mirroring aws-sdk-go-v2/service/glue/types.LakeFormationConfiguration.
type LakeFormationConfiguration struct {
	AccountID                   string `json:"AccountId,omitempty"`
	UseLakeFormationCredentials bool   `json:"UseLakeFormationCredentials,omitempty"`
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
	Tags                         map[string]string           `json:"-"`
	LakeFormationConfiguration   *LakeFormationConfiguration `json:"LakeFormationConfiguration,omitempty"`
	LineageConfiguration         *LineageConfiguration       `json:"LineageConfiguration,omitempty"`
	RecrawlPolicy                *RecrawlPolicy              `json:"RecrawlPolicy,omitempty"`
	SchemaChangePolicy           *SchemaChangePolicy         `json:"SchemaChangePolicy,omitempty"`
	Schedule                     CrawlerSchedule             `json:"Schedule,omitzero"`
	Configuration                string                      `json:"Configuration,omitempty"`
	Description                  string                      `json:"Description,omitempty"`
	ARN                          string                      `json:"Arn,omitempty"`
	TablePrefix                  string                      `json:"TablePrefix,omitempty"`
	CrawlerSecurityConfiguration string                      `json:"CrawlerSecurityConfiguration,omitempty"`
	State                        string                      `json:"State"`
	DatabaseName                 string                      `json:"DatabaseName"`
	Role                         string                      `json:"Role"`
	Name                         string                      `json:"Name"`
	Targets                      CrawlerTarget               `json:"Targets,omitzero"`
	Classifiers                  []string                    `json:"Classifiers,omitempty"`
	CreationTime                 float64                     `json:"CreationTime,omitempty"`
	LastUpdated                  float64                     `json:"LastUpdated,omitempty"`
}

// CrawlHistoryEntry records a single crawl run for ListCrawls.
type CrawlHistoryEntry struct {
	CrawlID string `json:"CrawlId,omitempty"`
	State   string `json:"State,omitempty"`
	Summary string `json:"Summary,omitempty"`
	// WorkflowRunID links a workflow-triggered crawl to its WorkflowRun for
	// WorkflowRunStatistics. Real AWS's Crawl/CrawlerHistory shapes have no such
	// field (aws-sdk-go-v2/service/glue@v1.152.0 types.go:2815-2836,2916-2946), so
	// this is internal-only: it persists (handleListCrawls' crawlHistoryOut DTO
	// copies fields explicitly and never includes it, so it never reaches the wire).
	WorkflowRunID string  `json:"workflowRunId,omitempty"`
	StartTime     float64 `json:"StartTime,omitempty"`
	EndTime       float64 `json:"EndTime,omitempty"`
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
	ConnectionProperties           map[string]string               `json:"ConnectionProperties,omitempty"`
	Tags                           map[string]string               `json:"-"`
	PhysicalConnectionRequirements *PhysicalConnectionRequirements `json:"PhysicalConnectionRequirements,omitempty"`
	Name                           string                          `json:"Name"`
	ConnectionType                 string                          `json:"ConnectionType,omitempty"`
	ARN                            string                          `json:"Arn,omitempty"`
	Description                    string                          `json:"Description,omitempty"`
	MatchCriteria                  []string                        `json:"MatchCriteria,omitempty"`
	CreationTime                   float64                         `json:"CreationTime,omitempty"`
	LastUpdatedTime                float64                         `json:"LastUpdatedTime,omitempty"`
}

// PhysicalConnectionRequirements specifies the VPC/subnet/security-group
// requirements needed to make a Glue connection, mirroring
// aws-sdk-go-v2/service/glue/types.PhysicalConnectionRequirements.
type PhysicalConnectionRequirements struct {
	AvailabilityZone    string   `json:"AvailabilityZone,omitempty"`
	SubnetID            string   `json:"SubnetId,omitempty"`
	SecurityGroupIDList []string `json:"SecurityGroupIdList,omitempty"`
}

// Blueprint represents a Glue blueprint.
type Blueprint struct {
	Tags                     map[string]string `json:"-"`
	Name                     string            `json:"Name"`
	Status                   string            `json:"Status,omitempty"`
	BlueprintLocation        string            `json:"BlueprintLocation,omitempty"`
	BlueprintServiceLocation string            `json:"BlueprintServiceLocation,omitempty"`
	Description              string            `json:"Description,omitempty"`
	ParameterSpec            string            `json:"ParameterSpec,omitempty"`
	ErrorMessage             string            `json:"ErrorMessage,omitempty"`
	CreatedOn                float64           `json:"CreatedOn,omitempty"`
	LastModifiedOn           float64           `json:"LastModifiedOn,omitempty"`
}

// CustomEntityType represents a Glue custom entity type.
type CustomEntityType struct {
	Name         string   `json:"Name"`
	RegexString  string   `json:"RegexString"`
	ContextWords []string `json:"ContextWords,omitempty"`
}

// DataQualityResult represents a Glue data quality result.
type DataQualityResult struct {
	ResultID string  `json:"ResultId"`
	Score    float64 `json:"Score,omitempty"`
}

// DevEndpoint represents a Glue development endpoint.
type DevEndpoint struct {
	Arguments                          map[string]string `json:"Arguments,omitempty"`
	Tags                               map[string]string `json:"-"`
	ExtraJarsS3Path                    string            `json:"ExtraJarsS3Path,omitempty"`
	ARN                                string            `json:"-"`
	EndpointName                       string            `json:"EndpointName"`
	Status                             string            `json:"Status,omitempty"`
	RoleArn                            string            `json:"RoleArn,omitempty"`
	SubnetID                           string            `json:"SubnetId,omitempty"`
	PublicKey                          string            `json:"PublicKey,omitempty"`
	WorkerType                         string            `json:"WorkerType,omitempty"`
	GlueVersion                        string            `json:"GlueVersion,omitempty"`
	ExtraPythonLibsS3Path              string            `json:"ExtraPythonLibsS3Path,omitempty"`
	VpcID                              string            `json:"VpcId,omitempty"`
	SecurityConfiguration              string            `json:"SecurityConfiguration,omitempty"`
	YarnEndpointAddress                string            `json:"YarnEndpointAddress,omitempty"`
	LastUpdateStatus                   string            `json:"LastUpdateStatus,omitempty"`
	AvailabilityZone                   string            `json:"AvailabilityZone,omitempty"`
	PrivateAddress                     string            `json:"PrivateAddress,omitempty"`
	PublicAddress                      string            `json:"PublicAddress,omitempty"`
	FailureReason                      string            `json:"FailureReason,omitempty"`
	PublicKeys                         []string          `json:"PublicKeys,omitempty"`
	SecurityGroupIDs                   []string          `json:"SecurityGroupIds,omitempty"`
	NumberOfWorkers                    int               `json:"NumberOfWorkers,omitempty"`
	NumberOfNodes                      int               `json:"NumberOfNodes,omitempty"`
	ZeppelinRemoteSparkInterpreterPort int               `json:"ZeppelinRemoteSparkInterpreterPort,omitempty"`
	CreatedTimestamp                   float64           `json:"CreatedTimestamp,omitempty"`
	LastModifiedTimestamp              float64           `json:"LastModifiedTimestamp,omitempty"`
}

// DevEndpointInput carries the optional CreateDevEndpoint settings beyond
// EndpointName/RoleArn.
type DevEndpointInput struct {
	Arguments             map[string]string
	SubnetID              string
	PublicKey             string
	WorkerType            string
	GlueVersion           string
	ExtraPythonLibsS3Path string
	ExtraJarsS3Path       string
	SecurityConfiguration string
	SecurityGroupIDs      []string
	PublicKeys            []string
	NumberOfNodes         int
	NumberOfWorkers       int
}

// CrawlerSchedule represents the schedule configuration for a crawler.
type CrawlerSchedule struct {
	ScheduleExpression string `json:"ScheduleExpression,omitempty"`
	State              string `json:"State,omitempty"`
}

// JobRun represents a single execution of a Glue job.
type JobRun struct {
	Arguments             map[string]string `json:"Arguments,omitempty"`
	ID                    string            `json:"Id"`
	JobName               string            `json:"JobName"`
	JobRunState           string            `json:"JobRunState"`
	ErrorMessage          string            `json:"ErrorMessage,omitempty"`
	WorkerType            string            `json:"WorkerType,omitempty"`
	GlueVersion           string            `json:"GlueVersion,omitempty"`
	SecurityConfiguration string            `json:"SecurityConfiguration,omitempty"`
	// TriggerName is the name of the trigger that started this run (real field:
	// aws-sdk-go-v2/service/glue@v1.152.0 types.go:7350-7351).
	TriggerName string `json:"TriggerName,omitempty"`
	// WorkflowRunID links a workflow-triggered job run to its WorkflowRun for
	// WorkflowRunStatistics. Real AWS's JobRun has no such field, so this is
	// internal-only: it persists but GetJobRun/GetJobRuns strip it before
	// returning, since those embed *JobRun directly in the wire response.
	WorkflowRunID        string               `json:"workflowRunId,omitempty"`
	StartedOn            float64              `json:"StartedOn,omitempty"`
	CompletedOn          float64              `json:"CompletedOn,omitempty"`
	MaxCapacity          float64              `json:"MaxCapacity,omitempty"`
	ExecutionTime        int                  `json:"ExecutionTime,omitempty"`
	NumberOfWorkers      int                  `json:"NumberOfWorkers,omitempty"`
	Timeout              int                  `json:"Timeout,omitempty"`
	NotificationProperty NotificationProperty `json:"NotificationProperty,omitzero"`
}

// StartJobRunOptions carries the optional per-run overrides AWS's
// StartJobRunRequest supports beyond JobName/Arguments.
type StartJobRunOptions struct {
	NotificationProperty  *NotificationProperty
	WorkerType            string
	SecurityConfiguration string
	NumberOfWorkers       int
	MaxCapacity           float64
	Timeout               int
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

// BatchStopJobRunSuccessfulSubmission is one successfully-stopped entry from
// a BatchStopJobRun response.
type BatchStopJobRunSuccessfulSubmission struct {
	JobName  string `json:"JobName"`
	JobRunID string `json:"JobRunId"`
}

// DataQualityRuleset represents a Glue data quality ruleset.
// DataQualityRuleset.ARN keeps its "Arn" json tag for persistence (this
// struct is the value type of a persisted store.Table, and the tag doubles
// as the on-disk key), but no data-quality-ruleset op in the real API
// (Create/Get/Update/List, api_op_*DataQualityRuleset*.go) ever exposes an
// Arn member on the wire -- confirmed absent from every one of their output
// structs. GetDataQualityRuleset and ListDataQualityRulesets must not
// marshal this struct directly for that reason -- see their own response
// types below/in handler_data_quality_rulesets.go.
type DataQualityRuleset struct {
	Tags                             map[string]string       `json:"-"`
	TargetTable                      *DataQualityTargetTable `json:"TargetTable,omitempty"`
	Name                             string                  `json:"Name"`
	Ruleset                          string                  `json:"Ruleset,omitempty"`
	Description                      string                  `json:"Description,omitempty"`
	ARN                              string                  `json:"Arn,omitempty"`
	DataQualitySecurityConfiguration string                  `json:"DataQualitySecurityConfiguration,omitempty"`
	CreatedOn                        float64                 `json:"CreatedOn,omitempty"`
	LastModifiedOn                   float64                 `json:"LastModifiedOn,omitempty"`
}

// DataQualityTargetTable identifies the Glue table a data quality ruleset
// applies to, mirroring
// aws-sdk-go-v2/service/glue/types.DataQualityTargetTable.
type DataQualityTargetTable struct {
	TableName    string `json:"TableName"`
	DatabaseName string `json:"DatabaseName"`
	CatalogID    string `json:"CatalogId,omitempty"`
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
	SchemaChangePolicy           *SchemaChangePolicy
	RecrawlPolicy                *RecrawlPolicy
	LineageConfiguration         *LineageConfiguration
	LakeFormationConfiguration   *LakeFormationConfiguration
	Description                  string
	Schedule                     string
	Configuration                string
	TablePrefix                  string
	CrawlerSecurityConfiguration string
	Classifiers                  []string
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
	BlueprintName string  `json:"BlueprintName"`
	RunID         string  `json:"RunId"`
	WorkflowName  string  `json:"WorkflowName"`
	State         string  `json:"State"`
	RoleARN       string  `json:"RoleArn,omitempty"`
	Parameters    string  `json:"Parameters,omitempty"`
	StartedOn     float64 `json:"StartedOn,omitempty"`
}

// DQRuleRecommendationRun represents a data quality rule recommendation run.
type DQRuleRecommendationRun struct {
	RecommendationRunID string  `json:"RecommendationRunId"`
	DataSourceS3Path    string  `json:"DataSourceS3Path,omitempty"`
	Status              string  `json:"Status"`
	StartedOn           float64 `json:"StartedOn,omitempty"`
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
// ColumnStatisticsTaskRun.StartedOn deliberately carries the json tag
// "StartTime", not "StartedOn": the real member name (glue@v1.152.0
// deserializers.go: awsAwsjson11_deserializeDocumentColumnStatisticsTaskRun's
// case list has StartTime, no StartedOn key at all) -- the sibling
// MaterializedViewRefreshRun.StartedOn already carries the correct
// "StartTime" tag; this one did not.
type ColumnStatisticsTaskRun struct {
	DatabaseName              string  `json:"DatabaseName"`
	TableName                 string  `json:"TableName"`
	ColumnStatisticsTaskRunID string  `json:"ColumnStatisticsTaskRunId"`
	Status                    string  `json:"Status"`
	Role                      string  `json:"Role,omitempty"`
	StartedOn                 float64 `json:"StartTime,omitempty"`
}

// MaterializedViewRefreshRun represents a materialized view refresh task run.
//
// TaskRunID and StartedOn deliberately carry non-obvious json tags:
// MaterializedViewRefreshTaskRunId and StartTime are the real member names
// (glue@v1.152.0 types.MaterializedViewRefreshTaskRun), not TaskRunId/
// StartedOn -- found while wiring ListMaterializedViewRefreshTaskRuns
// (gopherstack-awzv) and fixed here since this struct is shared by that op's
// output.
type MaterializedViewRefreshRun struct {
	DatabaseName string  `json:"DatabaseName"`
	TableName    string  `json:"TableName"`
	TaskRunID    string  `json:"MaterializedViewRefreshTaskRunId"`
	Status       string  `json:"Status"`
	StartedOn    float64 `json:"StartTime,omitempty"`
}

// Integration represents a Glue integration.
type Integration struct {
	CreatedAt       time.Time         `json:"CreateTime"`
	Tags            map[string]string `json:"Tags,omitempty"`
	IntegrationName string            `json:"IntegrationName"`
	IntegrationArn  string            `json:"IntegrationArn,omitempty"`
	SourceArn       string            `json:"SourceArn"`
	TargetArn       string            `json:"TargetArn"`
	Status          string            `json:"Status"`
}

// IdentityCenterConfig represents the Glue Identity Center configuration.
type IdentityCenterConfig struct {
	InstanceARN                   string   `json:"InstanceArn,omitempty"`
	ApplicationARN                string   `json:"ApplicationArn,omitempty"`
	Status                        string   `json:"Status"`
	Scopes                        []string `json:"Scopes,omitempty"`
	UserBackgroundSessionsEnabled bool     `json:"UserBackgroundSessionsEnabled,omitempty"`
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
	// ConnectionTypeArn is the real RegisterConnectionTypeOutput field
	// (glue@v1.152.0 api_op_RegisterConnectionType.go:79-84); only populated
	// for custom (non-built-in) types, which are the only ones RegisterConnectionType
	// can create.
	ConnectionTypeArn string `json:"ConnectionTypeArn,omitempty"`
	// IntegrationType is required on RegisterConnectionType (only "REST" is a
	// legal value) but has no corresponding field on DescribeConnectionType's
	// real output, so it is captured for completeness but never echoed back.
	IntegrationType string `json:"IntegrationType,omitempty"`
	// ConnectionProperties/ConnectorAuthenticationConfiguration are required on
	// RegisterConnectionType but stored as opaque nested documents: neither has
	// a matching field on the real DescribeConnectionTypeOutput
	// (its ConnectionProperties is a differently-shaped map[string]Property,
	// and its AuthenticationConfiguration is *types.AuthConfiguration, a
	// distinct type from *types.ConnectorAuthenticationConfiguration) so there
	// is no real echo target -- captured, never echoed.
	ConnectionProperties                 map[string]any `json:"connectionProperties,omitempty"`
	ConnectorAuthenticationConfiguration map[string]any `json:"connectorAuthenticationConfiguration,omitempty"`
	// RestConfiguration IS the same type on both RegisterConnectionType's
	// input and DescribeConnectionTypeOutput.RestConfiguration, so it is
	// stored and echoed verbatim on describe.
	RestConfiguration map[string]any `json:"restConfiguration,omitempty"`
	// Capabilities holds this connector's supported data operations
	// ("READ"/"WRITE", matching types.DataOperation's only two enum values
	// verbatim). This is internal storage, not the wire shape: the real
	// DescribeConnectionTypeOutput/ConnectionTypeBrief.Capabilities is
	// *types.Capabilities, a struct with this list nested under
	// SupportedDataOperations alongside two more required members this
	// backend does not track -- see connectionCapabilities/
	// toConnectionCapabilities in handler_connection_types.go for the wire
	// shaping.
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
	Tags         map[string]string `json:"-"`
	DatabaseName string            `json:"DatabaseName"`
	FunctionName string            `json:"FunctionName"`
	ClassName    string            `json:"ClassName,omitempty"`
	OwnerName    string            `json:"OwnerName,omitempty"`
	OwnerType    string            `json:"OwnerType,omitempty"`
	FunctionType string            `json:"FunctionType,omitempty"`
	CatalogID    string            `json:"CatalogId,omitempty"`
	FunctionARN  string            `json:"-"`
	ResourceURIs []ResourceURI     `json:"ResourceUris,omitempty"`
	CreateTime   float64           `json:"CreateTime,omitempty"`
}

// EncryptionConfiguration holds encryption settings for a SecurityConfiguration.
type EncryptionConfiguration struct {
	CloudWatchEncryption   *CloudWatchEncryption   `json:"CloudWatchEncryption,omitempty"`
	JobBookmarksEncryption *JobBookmarksEncryption `json:"JobBookmarksEncryption,omitempty"`
	DataQualityEncryption  *DataQualityEncryption  `json:"DataQualityEncryption,omitempty"`
	S3Encryption           []S3EncryptionEntry     `json:"S3Encryption,omitempty"`
}

// DataQualityEncryption holds Glue Data Quality asset encryption settings,
// mirroring aws-sdk-go-v2/service/glue/types.DataQualityEncryption.
type DataQualityEncryption struct {
	DataQualityEncryptionMode string `json:"DataQualityEncryptionMode,omitempty"`
	KMSKeyARN                 string `json:"KmsKeyArn,omitempty"`
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
	RoleARN string `json:"roleArn,omitempty"`
	Enabled bool   `json:"enabled"`
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

// TableOptimizer is the real nested TableOptimizer document
// (deserializeDocumentTableOptimizer, glue@v1.152.0): LastRun, Type and
// Configuration only, all lowerCamelCase. The document has no
// CatalogId/DatabaseName/TableName members of its own -- those live one
// level up, on GetTableOptimizerOutput (PascalCase) or BatchTableOptimizer
// (lowerCamelCase) instead. See gopherstack-5mvf.
type TableOptimizer struct {
	LastRun       *TableOptimizerRun          `json:"lastRun,omitempty"`
	Type          string                      `json:"type"`
	Configuration TableOptimizerConfiguration `json:"configuration,omitzero"`
}

// BatchTableOptimizer is one result entry from BatchGetTableOptimizer.
// Unlike GetTableOptimizerOutput, which nests TableOptimizer directly under
// the op output, BatchTableOptimizer wraps it one level deeper under a
// "tableOptimizer" key, and CatalogId/DatabaseName/TableName here are
// lowerCamelCase rather than GetTableOptimizerOutput's PascalCase
// (deserializeDocumentBatchTableOptimizer, glue@v1.152.0).
type BatchTableOptimizer struct {
	TableOptimizer *TableOptimizer `json:"tableOptimizer"`
	CatalogID      string          `json:"catalogId,omitempty"`
	DatabaseName   string          `json:"databaseName,omitempty"`
	TableName      string          `json:"tableName,omitempty"`
}

// BatchGetTableOptimizerEntry is one request entry for BatchGetTableOptimizer.
type BatchGetTableOptimizerEntry struct {
	CatalogID    string `json:"CatalogId,omitempty"`
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
	Type         string `json:"Type"`
}

// BatchGetTableOptimizerError is one error entry from BatchGetTableOptimizer.
// Its own keys are lowerCamelCase (deserializeDocumentBatchGetTableOptimizerError,
// glue@v1.152.0); only the nested ErrorDetail document keeps PascalCase.
type BatchGetTableOptimizerError struct {
	CatalogID    string      `json:"catalogId,omitempty"`
	DatabaseName string      `json:"databaseName,omitempty"`
	TableName    string      `json:"tableName,omitempty"`
	Type         string      `json:"type,omitempty"`
	Error        ErrorDetail `json:"error"`
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
	ColumnType     string               `json:"ColumnType"`
	AnalyzedTime   float64              `json:"AnalyzedTime"`
}

type resourcePolicyEntry struct {
	Policy       string  `json:"Policy"`
	Hash         string  `json:"Hash"`
	EnableHybrid string  `json:"EnableHybrid,omitempty"`
	CreateTime   float64 `json:"CreateTime,omitempty"`
	UpdateTime   float64 `json:"UpdateTime,omitempty"`
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
	Parameters          MLTransformParameter `json:"Parameters,omitzero"`
	TransformEncryption *TransformEncryption `json:"TransformEncryption,omitempty"`
	Schema              []SchemaColumnEntry  `json:"Schema,omitempty"`
	Tags                map[string]string    `json:"-"`
	TransformID         string               `json:"TransformId"`
	Name                string               `json:"Name"`
	Description         string               `json:"Description,omitempty"`
	Role                string               `json:"Role,omitempty"`
	GlueVersion         string               `json:"GlueVersion,omitempty"`
	WorkerType          string               `json:"WorkerType,omitempty"`
	Status              string               `json:"Status"`
	InputRecordTables   []GlueTable          `json:"InputRecordTables,omitempty"`
	MaxCapacity         float64              `json:"MaxCapacity,omitempty"`
	CreatedOn           float64              `json:"CreatedOn,omitempty"`
	LastModifiedOn      float64              `json:"LastModifiedOn,omitempty"`
	NumberOfWorkers     int32                `json:"NumberOfWorkers,omitempty"`
	MaxRetries          int                  `json:"MaxRetries,omitempty"`
	Timeout             int                  `json:"Timeout,omitempty"`
	LabelCount          int                  `json:"LabelCount,omitempty"`
}

// SchemaColumnEntry is a column name/data-type pair describing an MLTransform's
// input schema, mirroring aws-sdk-go-v2/service/glue/types.SchemaColumn.
type SchemaColumnEntry struct {
	Name     string `json:"Name,omitempty"`
	DataType string `json:"DataType,omitempty"`
}

// TransformEncryption specifies the encryption settings for an MLTransform's
// task runs, mirroring aws-sdk-go-v2/service/glue/types.TransformEncryption.
type TransformEncryption struct {
	MLUserDataEncryption             *MLUserDataEncryption `json:"MlUserDataEncryption,omitempty"`
	TaskRunSecurityConfigurationName string                `json:"TaskRunSecurityConfigurationName,omitempty"`
}

// MLUserDataEncryption specifies the encryption mode applied to an
// MLTransform's user data, mirroring
// aws-sdk-go-v2/service/glue/types.MLUserDataEncryption.
type MLUserDataEncryption struct {
	MLUserDataEncryptionMode string `json:"MlUserDataEncryptionMode"`
	KMSKeyID                 string `json:"KmsKeyId,omitempty"`
}

// MLTransformOptions carries the optional CreateMLTransform settings beyond
// Name/Description/Role/InputRecordTables/Parameters/Tags.
type MLTransformOptions struct {
	TransformEncryption *TransformEncryption
	GlueVersion         string
	WorkerType          string
	Schema              []SchemaColumnEntry
	MaxCapacity         float64
	MaxRetries          int
	Timeout             int
	NumberOfWorkers     int32
}

// CatalogEntry represents a named AWS Glue catalog.
type CatalogEntry struct {
	Parameters  map[string]string `json:"Parameters,omitzero"`
	CatalogID   string            `json:"CatalogId"`
	Name        string            `json:"Name"`
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
	CatalogEncryptionMode        string `json:"CatalogEncryptionMode"`
	SseAwsKmsKeyID               string `json:"SseAwsKmsKeyId,omitempty"`
	CatalogEncryptionServiceRole string `json:"CatalogEncryptionServiceRole,omitempty"`
}

// ConnectionPasswordEncryption holds connection password encryption config.
type ConnectionPasswordEncryption struct {
	AwsKmsKeyID                       string `json:"AwsKmsKeyId,omitempty"`
	ReturnConnectionPasswordEncrypted bool   `json:"ReturnConnectionPasswordEncrypted"`
}

// DataCatalogExportConfiguration holds the Glue Data Catalog's S3 Tables
// export configuration. Unlike DataCatalogEncryptionSettings, this API's
// shapes carry no CatalogId -- a single backend-global setting; see catalogs.go.
type DataCatalogExportConfiguration struct {
	EncryptionConfiguration *ExportEncryptionConfiguration `json:"EncryptionConfiguration,omitempty"`
	ExportSetting           string                         `json:"ExportSetting,omitempty"`
	S3TableBucketArn        string                         `json:"S3TableBucketArn,omitempty"`
	Status                  string                         `json:"Status,omitempty"`
	CreatedAt               float64                        `json:"CreatedAt,omitempty"`
	UpdatedAt               float64                        `json:"UpdatedAt,omitempty"`
}

// ExportEncryptionConfiguration mirrors types.ExportEncryptionConfiguration.
type ExportEncryptionConfiguration struct {
	KmsKeyArn    string `json:"KmsKeyArn,omitempty"`
	SseAlgorithm string `json:"SseAlgorithm,omitempty"`
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
	Arguments             map[string]string     `json:"Arguments,omitempty"`
	NotificationProperty  *NotificationProperty `json:"NotificationProperty,omitempty"`
	JobName               string                `json:"JobName,omitempty"`
	CrawlerName           string                `json:"CrawlerName,omitempty"`
	SecurityConfiguration string                `json:"SecurityConfiguration,omitempty"`
	Timeout               int                   `json:"Timeout,omitempty"`
}

// TriggerPredicate represents a predicate for a conditional trigger.
type TriggerPredicate struct {
	Logical    string             `json:"Logical,omitempty"`
	Conditions []TriggerCondition `json:"Conditions,omitempty"`
}

// TriggerCondition represents a condition within a trigger predicate.
type TriggerCondition struct {
	JobName         string `json:"JobName,omitempty"`
	CrawlerName     string `json:"CrawlerName,omitempty"`
	CrawlState      string `json:"CrawlState,omitempty"`
	LogicalOperator string `json:"LogicalOperator,omitempty"`
	State           string `json:"State,omitempty"`
}

// TriggerEventBatchingCondition specifies EventBridge event-batching settings
// for an EVENT-type trigger, mirroring
// aws-sdk-go-v2/service/glue/types.EventBatchingCondition.
type TriggerEventBatchingCondition struct {
	BatchSize   int `json:"BatchSize"`
	BatchWindow int `json:"BatchWindow,omitempty"`
}

// Trigger represents a Glue trigger.
type Trigger struct {
	Tags                   map[string]string              `json:"-"`
	Predicate              *TriggerPredicate              `json:"Predicate,omitempty"`
	EventBatchingCondition *TriggerEventBatchingCondition `json:"EventBatchingCondition,omitempty"`
	ARN                    string                         `json:"Arn,omitempty"`
	Name                   string                         `json:"Name"`
	Type                   string                         `json:"Type,omitempty"`
	State                  string                         `json:"State,omitempty"`
	Schedule               string                         `json:"Schedule,omitempty"`
	Description            string                         `json:"Description,omitempty"`
	WorkflowName           string                         `json:"WorkflowName,omitempty"`
	Actions                []TriggerAction                `json:"Actions,omitempty"`
	// StartOnCreation mirrors CreateTriggerInput.StartOnCreation: when true, a
	// SCHEDULED or CONDITIONAL trigger is activated immediately on creation. It is
	// not part of the Trigger wire shape itself (hence json:"-"), only of the
	// create request, matching the real CreateTriggerInput/Trigger type split.
	StartOnCreation bool `json:"-"`
}

// Workflow represents a Glue workflow.
type Workflow struct {
	Tags                 map[string]string `json:"-"`
	Graph                *WorkflowGraph    `json:"Graph,omitempty"`
	LastRun              *WorkflowRun      `json:"LastRun,omitempty"`
	DefaultRunProperties map[string]string `json:"DefaultRunProperties,omitempty"`
	Name                 string            `json:"Name"`
	Description          string            `json:"Description,omitempty"`
	ARN                  string            `json:"Arn,omitempty"`
	CreatedOn            float64           `json:"CreatedOn,omitempty"`
	LastModifiedOn       float64           `json:"LastModifiedOn,omitempty"`
	MaxConcurrentRuns    int               `json:"MaxConcurrentRuns,omitempty"`
}

// WorkflowGraph is the DAG of triggers/jobs/crawlers belonging to a workflow,
// mirroring aws-sdk-go-v2/service/glue/types.WorkflowGraph. Derived from real
// Trigger.WorkflowName membership plus each trigger's Actions/Predicate --
// see workflowGraphLocked in workflow_graph.go.
type WorkflowGraph struct {
	Nodes []WorkflowNode `json:"Nodes,omitempty"`
	Edges []WorkflowEdge `json:"Edges,omitempty"`
}

// WorkflowNode is one Glue component (crawler/job/trigger) in a
// WorkflowGraph, mirroring aws-sdk-go-v2/service/glue/types.Node.
// TriggerDetails is populated with the real Trigger definition; JobDetails/
// CrawlerDetails (per-node run history) are not modeled -- the job/crawler-run
// to workflow-run link they'd need now exists (JobRun/CrawlHistoryEntry's
// internal WorkflowRunID, see workflows.go), but converting it into per-node
// run lists is separate work not done yet.
type WorkflowNode struct {
	TriggerDetails *WorkflowTriggerNodeDetails `json:"TriggerDetails,omitempty"`
	Name           string                      `json:"Name,omitempty"`
	Type           string                      `json:"Type,omitempty"`
	UniqueID       string                      `json:"UniqueId,omitempty"`
}

// WorkflowTriggerNodeDetails mirrors aws-sdk-go-v2/service/glue/types.TriggerNodeDetails.
type WorkflowTriggerNodeDetails struct {
	Trigger *Trigger `json:"Trigger,omitempty"`
}

// WorkflowEdge is a directed connection between two WorkflowGraph nodes,
// mirroring aws-sdk-go-v2/service/glue/types.Edge.
type WorkflowEdge struct {
	SourceID      string `json:"SourceId,omitempty"`
	DestinationID string `json:"DestinationId,omitempty"`
}

// WorkflowRun represents a single run of a Glue workflow.
type WorkflowRun struct {
	Properties   map[string]string      `json:"WorkflowRunProperties,omitempty"`
	Statistics   *WorkflowRunStatistics `json:"Statistics,omitempty"`
	WorkflowName string                 `json:"WorkflowName"`
	RunID        string                 `json:"WorkflowRunId"`
	Status       string                 `json:"Status"`
	StartedOn    float64                `json:"StartedOn,omitempty"`
	CompletedOn  float64                `json:"CompletedOn,omitempty"`
}

// WorkflowRunStatistics mirrors aws-sdk-go-v2/service/glue/types.WorkflowRunStatistics
// (types.go:13201-13228). Computed live from job runs/crawls carrying this run's
// WorkflowRunID -- see computeWorkflowRunStatisticsLocked in workflows.go -- never
// stored, so it can never go stale.
type WorkflowRunStatistics struct {
	TotalActions     int `json:"TotalActions,omitempty"`
	TimeoutActions   int `json:"TimeoutActions,omitempty"`
	FailedActions    int `json:"FailedActions,omitempty"`
	StoppedActions   int `json:"StoppedActions,omitempty"`
	SucceededActions int `json:"SucceededActions,omitempty"`
	RunningActions   int `json:"RunningActions,omitempty"`
	ErroredActions   int `json:"ErroredActions,omitempty"`
	WaitingActions   int `json:"WaitingActions,omitempty"`
}

// GrokClassifier is a Grok-based classifier.
type GrokClassifier struct {
	Name           string `json:"Name"`
	Classification string `json:"Classification"`
	GrokPattern    string `json:"GrokPattern"`
	CustomPatterns string `json:"CustomPatterns,omitempty"`
}

// XMLClassifier is an XML-based classifier.
type XMLClassifier struct {
	Name           string `json:"Name"`
	Classification string `json:"Classification"`
	RowTag         string `json:"RowTag,omitempty"`
}

// JSONClassifier is a JSON-based classifier.
type JSONClassifier struct {
	Name     string `json:"Name"`
	JSONPath string `json:"JsonPath"`
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
