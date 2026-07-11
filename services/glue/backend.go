package glue

import (
	"fmt"
	"maps"
	mrand "math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("EntityNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("AlreadyExistsException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when input validation fails.
	//
	// Glue's per-operation error models (aws-sdk-go-v2/service/glue deserializers.go)
	// list InvalidInputException — not ValidationException — as the hand-validation
	// error for the overwhelming majority of Create/Update/Delete operations (e.g.
	// CreateDatabase, CreateTable, CreateJob, CreateCrawler, CreateTrigger,
	// CreateBlueprint, CreateCustomEntityType, CreateUsageProfile, tag validation).
	// A handful of newer operations (e.g. DeleteConnectionType) do document
	// ValidationException instead, but since this sentinel is shared across every
	// hand-rolled validation check in the backend, InvalidInputException is the more
	// accurate default.
	ErrValidation = awserr.New("InvalidInputException", awserr.ErrInvalidParameter)
	// ErrCrawlerRunning is returned when an operation requires the crawler to not be running.
	ErrCrawlerRunning = awserr.New("CrawlerRunningException", awserr.ErrInvalidParameter)
	// ErrCrawlerNotRunning is returned when an operation requires the crawler to be running.
	ErrCrawlerNotRunning = awserr.New("CrawlerNotRunningException", awserr.ErrInvalidParameter)
)

// glueARNParts is the number of colon-separated parts in a Glue ARN.
// Format: arn:aws:glue:{region}:{account}:{resourceType}/{name}.
const (
	glueARNParts          = 6
	errEntityNotFoundCode = "EntityNotFoundException"
	stateRunning          = "RUNNING"
	stateStarting         = "STARTING"
	stateReady            = "READY"
	stateStopping         = "STOPPING"
	stateStopped          = "STOPPED"
	stateSucceeded        = "SUCCEEDED"

	jobTransitionDelay     = 150 * time.Millisecond // STARTING→RUNNING
	jobSucceededDelay      = 300 * time.Millisecond // RUNNING→SUCCEEDED
	crawlerTransitionDelay = 200 * time.Millisecond // RUNNING→READY
	reconcilerTickDivisor  = 5
	stateAvailable         = "AVAILABLE"
	stateDeleting          = "DELETING"
	stateProvisioning      = "PROVISIONING"
	stateActive            = "ACTIVE"
	stateScheduled         = "SCHEDULED"
	stateNotScheduled      = "NOT_SCHEDULED"

	// maxNameLen is the maximum length (in characters) for Glue resource names.
	// AWS enforces a 255-character limit for database, table, crawler, and job names.
	maxNameLen = 255

	// maxTagsPerResource is the maximum number of tags allowed per Glue resource.
	maxTagsPerResource = 50

	// maxTagKeyLen is the maximum byte length of a tag key.
	maxTagKeyLen = 128

	// maxTagValueLen is the maximum byte length of a tag value.
	maxTagValueLen = 256

	// maxJobRetries is the maximum value for MaxRetries on a Glue job.
	maxJobRetries = 10

	// maxBatchCreatePartitions is the maximum number of partitions per BatchCreatePartition call.
	maxBatchCreatePartitions = 100
)

// validateTags checks that tags conform to AWS Glue limits:
// max 50 tags, key 1-128 chars, value 0-256 chars.
func validateTags(tags map[string]string) error {
	if len(tags) > maxTagsPerResource {
		return fmt.Errorf("%w: too many tags: maximum is %d", ErrValidation, maxTagsPerResource)
	}
	for k, v := range tags {
		if len(k) == 0 || len(k) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key length must be 1-%d", ErrValidation, maxTagKeyLen)
		}
		if len(v) > maxTagValueLen {
			return fmt.Errorf("%w: tag value length must be 0-%d", ErrValidation, maxTagValueLen)
		}
	}

	return nil
}

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

// InMemoryBackend stores Glue state in memory. Most resource collections are
// *store.Table[T], registered once on b.registry via registerAllTables (see
// store_setup.go); a handful remain plain maps because their key is not a
// pure function of the stored value's own fields, or because they hold a
// one-to-many history/list rather than a single value per key -- see the
// comment above registerAllTables in store_setup.go for the full list and
// rationale.
type InMemoryBackend struct {
	databases                 *store.Table[Database]
	tables                    *store.Table[Table]
	crawlers                  *store.Table[Crawler]
	jobs                      *store.Table[Job]
	partitions                *store.Table[Partition]
	partitionIndexes          map[string]*PartitionIndex // key: "databaseName|tableName|indexName"
	tableVersions             *store.Table[TableVersion]
	connections               *store.Table[Connection]
	blueprints                *store.Table[Blueprint]
	customEntityTypes         *store.Table[CustomEntityType]
	dataQualityResult         *store.Table[DataQualityResult]
	devEndpoints              *store.Table[DevEndpoint]
	jobRuns                   map[string][]*JobRun // key: jobName
	jobBookmarks              *store.Table[JobBookmark]
	dataQualityRulesets       *store.Table[DataQualityRuleset]
	dataQualityEvalRuns       *store.Table[DataQualityEvaluationRun]
	triggers                  *store.Table[Trigger]
	workflows                 *store.Table[Workflow]
	workflowRuns              map[string][]*WorkflowRun // key: workflowName
	classifiers               *store.Table[Classifier]
	registries                *store.Table[Registry]
	schemas                   *store.Table[Schema]
	schemaVersions            map[string][]*SchemaVersion // key: schemaARN
	udfs                      *store.Table[UserDefinedFunction]
	securityConfigs           *store.Table[SecurityConfiguration]
	sessions                  *store.Table[Session]
	sessionStatements         map[string][]*Statement // key: sessionID
	tableOptimizers           *store.Table[TableOptimizer]
	tableColumnStats          map[string]*ColumnStatistics    // key: "dbName|tableName|colName"
	partitionColumnStats      map[string]*ColumnStatistics    // key: partKey+"|"+colName
	resourcePolicies          map[string]*resourcePolicyEntry // key: resourceARN or "__global__"
	mlTransforms              *store.Table[MLTransform]
	catalogs                  *store.Table[CatalogEntry]
	catalogEncryptionSettings map[string]*DataCatalogEncryptionSettings // key: catalogID or accountID
	usageProfiles             *store.Table[UsageProfile]
	blueprintRuns             *store.Table[BlueprintRun]
	dqRecommendationRuns      *store.Table[DQRuleRecommendationRun]
	columnStatTaskSettings    *store.Table[ColumnStatisticsTaskSettings]
	columnStatTaskRuns        *store.Table[ColumnStatisticsTaskRun]
	materializedViewRuns      *store.Table[MaterializedViewRefreshRun]
	integrations              *store.Table[Integration]
	integrationResourceProps  *store.Table[IntegrationResourceProperty]
	integrationTableProps     *store.Table[IntegrationTableProperties]
	mlTaskRuns                *store.Table[MLTaskRun]
	catalogImports            map[string]*CatalogImportStatus // key: catalogID or accountID
	schemaVersionMetadata     map[string]map[string]string    // key: schemaVersionID → key → value
	crawlHistory              map[string][]*CrawlHistoryEntry // key: crawlerName
	dqStatisticAnnotations    *store.Table[StatisticAnnotation]
	glueIdentityCenterConfig  *IdentityCenterConfig
	registry                  *store.Registry
	mu                        *lockmetrics.RWMutex

	// lifecycle reconciler timers
	jobRunReadyAt  map[string]map[string]time.Time // jobName → runID → readyAt for STARTING→RUNNING
	jobRunDoneAt   map[string]map[string]time.Time // jobName → runID → doneAt for RUNNING→SUCCEEDED
	crawlerReadyAt map[string]time.Time            // crawlerName → readyAt for RUNNING→READY

	// connection-type registry (custom types registered via RegisterConnectionType).
	customConnectionTypes *store.Table[ConnectionTypeInfo]

	// reconcileStop signals the managed reconciler goroutine to exit. See the
	// non-pointer reconciler bookkeeping fields at the end of the struct.
	reconcileStop chan struct{}

	accountID string
	region    string

	// Managed reconciler lifecycle bookkeeping. The reconciler is started by the
	// service framework via StartWorker (BackgroundWorker) and stopped by Shutdown
	// (Shutdowner), replacing the previous unmanaged goroutine that leaked because
	// nothing ever called Close. reconcileMu guards this bookkeeping; b.mu continues
	// to guard resource state. These pointer-free fields are placed last to keep the
	// GC pointer-scan region minimal (govet fieldalignment).
	reconcileWG       sync.WaitGroup
	reconcileMu       sync.Mutex
	reconcileInterval time.Duration
	reconcileOn       bool
}

// NewInMemoryBackend creates a new in-memory Glue backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		partitionIndexes:          make(map[string]*PartitionIndex),
		jobRuns:                   make(map[string][]*JobRun),
		workflowRuns:              make(map[string][]*WorkflowRun),
		schemaVersions:            make(map[string][]*SchemaVersion),
		sessionStatements:         make(map[string][]*Statement),
		tableColumnStats:          make(map[string]*ColumnStatistics),
		partitionColumnStats:      make(map[string]*ColumnStatistics),
		resourcePolicies:          make(map[string]*resourcePolicyEntry),
		catalogEncryptionSettings: make(map[string]*DataCatalogEncryptionSettings),
		catalogImports:            make(map[string]*CatalogImportStatus),
		schemaVersionMetadata:     make(map[string]map[string]string),
		crawlHistory:              make(map[string][]*CrawlHistoryEntry),
		registry:                  store.NewRegistry(),
		mu:                        lockmetrics.New("glue"),
		accountID:                 accountID,
		region:                    region,
		jobRunReadyAt:             make(map[string]map[string]time.Time),
		jobRunDoneAt:              make(map[string]map[string]time.Time),
		crawlerReadyAt:            make(map[string]time.Time),
	}

	registerAllTables(b)

	return b
}

// advanceJobRunState applies STARTING→RUNNING and RUNNING→SUCCEEDED transitions for a
// single run, consulting the readyMap and doneMap timing tables. Must be called with b.mu held.
func advanceJobRunState(run *JobRun, readyMap, doneMap map[string]time.Time, now time.Time) {
	if readyMap != nil {
		if t, ok := readyMap[run.ID]; ok && now.After(t) {
			if run.JobRunState == stateStarting {
				run.JobRunState = stateRunning
			}
			delete(readyMap, run.ID)
		}
	}

	if doneMap != nil {
		if t, ok := doneMap[run.ID]; ok && now.After(t) {
			if run.JobRunState == stateRunning {
				run.JobRunState = stateSucceeded
				run.CompletedOn = float64(now.Unix())
				run.ExecutionTime = int(jobSucceededDelay.Seconds())
			}
			delete(doneMap, run.ID)
		}
	}
}

// reconcileLocked applies pending lifecycle transitions as of now. Must be called
// with b.mu held. Taking now as a parameter (rather than reading the clock again)
// keeps the due-scan and the application consistent and makes advancement
// deterministically testable.
func (b *InMemoryBackend) reconcileLocked(now time.Time) {
	// Job run transitions: STARTING→RUNNING, RUNNING→SUCCEEDED.
	for jobName, runs := range b.jobRuns {
		readyMap := b.jobRunReadyAt[jobName]
		doneMap := b.jobRunDoneAt[jobName]

		for _, run := range runs {
			advanceJobRunState(run, readyMap, doneMap, now)
		}
	}

	// Crawler transitions:
	//   RUNNING→READY  — crawl completes; create catalog tables from S3 targets.
	//   STOPPING→READY — StopCrawler was issued; the crawler winds down to READY
	//                    without creating tables (the crawl was interrupted).
	for name, readyAt := range b.crawlerReadyAt {
		if now.After(readyAt) {
			c, ok := b.crawlers.Get(name)
			if ok && c.State == stateRunning {
				c.State = stateReady
				c.LastUpdated = float64(now.Unix())
				created := b.createCrawlerTablesLocked(c)
				b.finishCrawlHistoryLocked(name, "COMPLETED", created, now)
			} else if ok && c.State == stateStopping {
				c.State = stateReady
				c.LastUpdated = float64(now.Unix())
				b.finishCrawlHistoryLocked(name, "STOPPED", 0, now)
			}

			delete(b.crawlerReadyAt, name)
		}
	}

	b.pruneOrphanJobRunTimersLocked()
}

// pruneOrphanJobRunTimersLocked removes job-run timing entries whose job or run no
// longer exists. Without this, a stale due timer would make pendingDueLocked report
// work forever, so the reconciler would take the global write lock every tick — the
// exact hot-loop the performance fix is meant to avoid. Must be called with b.mu held.
func (b *InMemoryBackend) pruneOrphanJobRunTimersLocked() {
	for jobName, timers := range b.jobRunReadyAt {
		b.pruneJobTimerMapLocked(jobName, timers, b.jobRunReadyAt)
	}

	for jobName, timers := range b.jobRunDoneAt {
		b.pruneJobTimerMapLocked(jobName, timers, b.jobRunDoneAt)
	}
}

// pruneJobTimerMapLocked drops entries in a single job's timer sub-map for runs that
// no longer exist, and drops the whole sub-map when the job or all its timers are
// gone. Must be called with b.mu held.
func (b *InMemoryBackend) pruneJobTimerMapLocked(
	jobName string,
	timers map[string]time.Time,
	parent map[string]map[string]time.Time,
) {
	live := make(map[string]struct{}, len(b.jobRuns[jobName]))
	for _, run := range b.jobRuns[jobName] {
		live[run.ID] = struct{}{}
	}

	for runID := range timers {
		if _, ok := live[runID]; !ok {
			delete(timers, runID)
		}
	}

	if len(timers) == 0 {
		delete(parent, jobName)
	}
}

// createCrawlerTablesLocked creates a Glue table per S3 prefix in the crawler's
// targets and returns how many tables were newly created (as opposed to already
// existing from a prior crawl), for the crawl-history summary. Must be called
// with b.mu held.
func (b *InMemoryBackend) createCrawlerTablesLocked(c *Crawler) int {
	created := 0

	for _, s3t := range c.Targets.S3Targets {
		path := strings.TrimPrefix(s3t.Path, "s3://")
		// Extract prefix after bucket name.
		var prefix string
		if _, after, ok := strings.Cut(path, "/"); ok {
			prefix = strings.Trim(after, "/")
		}

		if prefix == "" {
			prefix = "default"
		}

		// Sanitize prefix for use as table name.
		tableName := strings.NewReplacer("/", "_", "-", "_", ".", "_").Replace(prefix)
		if tableName == "" {
			tableName = "default"
		}

		key := c.DatabaseName + "|" + tableName
		if !b.tables.Has(key) {
			b.tables.Put(&Table{
				Name:         tableName,
				DatabaseName: c.DatabaseName,
				CreateTime:   float64(time.Now().Unix()),
			})
			created++
		}
	}

	return created
}

// Reset clears all backend state, returning it to the initial empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()

	b.partitionIndexes = make(map[string]*PartitionIndex)
	b.jobRuns = make(map[string][]*JobRun)
	b.workflowRuns = make(map[string][]*WorkflowRun)
	b.schemaVersions = make(map[string][]*SchemaVersion)
	b.sessionStatements = make(map[string][]*Statement)
	b.tableColumnStats = make(map[string]*ColumnStatistics)
	b.partitionColumnStats = make(map[string]*ColumnStatistics)
	b.resourcePolicies = make(map[string]*resourcePolicyEntry)
	b.catalogEncryptionSettings = make(map[string]*DataCatalogEncryptionSettings)
	b.catalogImports = make(map[string]*CatalogImportStatus)
	b.schemaVersionMetadata = make(map[string]map[string]string)
	b.resetStubFixState()

	b.resetLifecycleStateLocked()
}

// resetStubFixState clears the raw map backing the de-stubbed crawl-history
// feature. Kept separate from Reset to stay under the funlen limit. Must be
// called with b.mu held.
func (b *InMemoryBackend) resetStubFixState() {
	b.crawlHistory = make(map[string][]*CrawlHistoryEntry)
}

// resetLifecycleStateLocked clears identity-center config and pending
// lifecycle transition timers. Clearing the timers ensures a reset never
// resurrects a crawler/job-run state change scheduled against now-deleted
// resources. Must be called with b.mu held.
func (b *InMemoryBackend) resetLifecycleStateLocked() {
	b.glueIdentityCenterConfig = nil
	b.jobRunReadyAt = make(map[string]map[string]time.Time)
	b.jobRunDoneAt = make(map[string]map[string]time.Time)
	b.crawlerReadyAt = make(map[string]time.Time)
}

// Region returns the backend region.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the backend account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// cloneDatabase returns a deep copy of a Database.
func cloneDatabase(db *Database) *Database {
	cp := *db
	cp.Tags = maps.Clone(db.Tags)

	return &cp
}

// cloneCrawler returns a deep copy of a Crawler.
func cloneCrawler(c *Crawler) *Crawler {
	cp := *c
	cp.Tags = maps.Clone(c.Tags)
	cp.Classifiers = append([]string(nil), c.Classifiers...)
	cp.Targets = cloneCrawlerTarget(c.Targets)

	return &cp
}

// cloneCrawlerTarget returns a deep copy of a CrawlerTarget, including the
// nested Exclusions/Tables slices on each individual target entry.
func cloneCrawlerTarget(t CrawlerTarget) CrawlerTarget {
	cp := CrawlerTarget{}

	if len(t.S3Targets) > 0 {
		cp.S3Targets = make([]S3Target, len(t.S3Targets))
		for i, s := range t.S3Targets {
			cp.S3Targets[i] = s
			cp.S3Targets[i].Exclusions = append([]string(nil), s.Exclusions...)
		}
	}

	if len(t.JdbcTargets) > 0 {
		cp.JdbcTargets = make([]JDBCTarget, len(t.JdbcTargets))
		for i, j := range t.JdbcTargets {
			cp.JdbcTargets[i] = j
			cp.JdbcTargets[i].Exclusions = append([]string(nil), j.Exclusions...)
		}
	}

	if len(t.CatalogTargets) > 0 {
		cp.CatalogTargets = make([]CatalogTarget, len(t.CatalogTargets))
		for i, ct := range t.CatalogTargets {
			cp.CatalogTargets[i] = ct
			cp.CatalogTargets[i].Tables = append([]string(nil), ct.Tables...)
		}
	}

	return cp
}

// cloneJob returns a deep copy of a Job.
func cloneJob(j *Job) *Job {
	cp := *j
	cp.Tags = maps.Clone(j.Tags)
	cp.DefaultArguments = maps.Clone(j.DefaultArguments)
	if len(j.Connections.Connections) > 0 {
		cp.Connections.Connections = make([]string, len(j.Connections.Connections))
		copy(cp.Connections.Connections, j.Connections.Connections)
	}
	if j.SourceControlDetails != nil {
		scd := *j.SourceControlDetails
		cp.SourceControlDetails = &scd
	}

	return &cp
}

// cloneConnection returns a deep copy of a Connection.
func cloneConnection(c *Connection) *Connection {
	cp := *c
	cp.ConnectionProperties = maps.Clone(c.ConnectionProperties)
	cp.Tags = maps.Clone(c.Tags)

	return &cp
}

// cloneColumns returns a deep copy of a Column slice, including each column's
// Parameters map (a shallow slice copy would still alias the map headers).
func cloneColumns(cols []Column) []Column {
	if len(cols) == 0 {
		return nil
	}

	out := make([]Column, len(cols))
	for i, c := range cols {
		out[i] = c
		out[i].Parameters = maps.Clone(c.Parameters)
	}

	return out
}

// cloneStorageDescriptor returns a deep copy of a StorageDescriptor, including
// its Columns, Parameters, SerdeInfo, BucketColumns and SortColumns.
func cloneStorageDescriptor(sd StorageDescriptor) StorageDescriptor {
	cp := sd
	cp.Columns = cloneColumns(sd.Columns)
	cp.Parameters = maps.Clone(sd.Parameters)

	if len(sd.BucketColumns) > 0 {
		cp.BucketColumns = append([]string(nil), sd.BucketColumns...)
	}

	if len(sd.SortColumns) > 0 {
		cp.SortColumns = append([]Order(nil), sd.SortColumns...)
	}

	if sd.SerdeInfo != nil {
		si := *sd.SerdeInfo
		si.Parameters = maps.Clone(sd.SerdeInfo.Parameters)
		cp.SerdeInfo = &si
	}

	return cp
}

// cloneTable returns a deep copy of a Table, including nested slices.
func cloneTable(t *Table) *Table {
	cp := *t
	cp.StorageDescriptor = cloneStorageDescriptor(t.StorageDescriptor)
	cp.PartitionKeys = cloneColumns(t.PartitionKeys)
	cp.Parameters = maps.Clone(t.Parameters)

	return &cp
}

// databaseARN returns the ARN for a Glue database.
func (b *InMemoryBackend) databaseARN(name string) string {
	return arn.Build("glue", b.region, b.accountID, "database/"+name)
}

// crawlerARN returns the ARN for a Glue crawler.
func (b *InMemoryBackend) crawlerARN(name string) string {
	return arn.Build("glue", b.region, b.accountID, "crawler/"+name)
}

// jobARN returns the ARN for a Glue job.
func (b *InMemoryBackend) jobARN(name string) string {
	return arn.Build("glue", b.region, b.accountID, "job/"+name)
}

// glueResourceName extracts the resource name from a Glue ARN for a given resource type.
// Glue ARNs have the format: arn:aws:glue:{region}:{account}:{resourceType}/{name}.
// This allows matching ARNs even when the account ID differs (e.g. empty vs 000000000000).
func glueResourceName(resourceARN, resourceType string) string {
	// Split into exactly glueARNParts parts: arn, aws, glue, region, account, resource
	parts := strings.SplitN(resourceARN, ":", glueARNParts)
	if len(parts) != glueARNParts {
		return ""
	}

	prefix := resourceType + "/"
	if !strings.HasPrefix(parts[5], prefix) {
		return ""
	}

	return parts[5][len(prefix):]
}

// tableKey returns a map key for a table.
func tableKey(dbName, tableName string) string {
	return fmt.Sprintf("%s|%s", dbName, tableName)
}

// partitionKey returns a map key for a partition.
func partitionKey(dbName, tableName string, values []string) string {
	return fmt.Sprintf("%s|%s|%s", dbName, tableName, strings.Join(values, "#"))
}

// tableVersionKey returns a map key for a table version.
func tableVersionKey(dbName, tableName, versionID string) string {
	return fmt.Sprintf("%s|%s|%s", dbName, tableName, versionID)
}

// --- Database operations ---

// CreateDatabase creates a new Glue database.
func (b *InMemoryBackend) CreateDatabase(
	input DatabaseInput,
	tags map[string]string,
) (*Database, error) {
	b.mu.Lock("CreateDatabase")
	defer b.mu.Unlock()

	if input.Name == "" || len(input.Name) > maxNameLen {
		return nil, ErrValidation
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	if b.databases.Has(input.Name) {
		return nil, ErrAlreadyExists
	}

	db := &Database{
		Name:        input.Name,
		Description: input.Description,
		CatalogID:   b.accountID,
		ARN:         b.databaseARN(input.Name),
		Tags:        maps.Clone(tags),
		CreateTime:  float64(time.Now().Unix()),
	}
	b.databases.Put(db)

	return db, nil
}

// GetDatabase retrieves a Glue database by name.
func (b *InMemoryBackend) GetDatabase(name string) (*Database, error) {
	b.mu.RLock("GetDatabase")
	defer b.mu.RUnlock()

	db, ok := b.databases.Get(name)
	if !ok {
		return nil, ErrNotFound
	}

	return cloneDatabase(db), nil
}

// GetDatabases returns all Glue databases sorted by name.
func (b *InMemoryBackend) GetDatabases() []*Database {
	b.mu.RLock("GetDatabases")
	defer b.mu.RUnlock()

	src := b.databases.Snapshot()
	out := make([]*Database, 0, len(src))
	for _, db := range src {
		out = append(out, cloneDatabase(db))
	}

	return out
}

// DeleteDatabase deletes a Glue database by name, also removing all its tables and partitions.
func (b *InMemoryBackend) DeleteDatabase(name string) error {
	b.mu.Lock("DeleteDatabase")
	defer b.mu.Unlock()

	if !b.databases.Has(name) {
		return ErrNotFound
	}

	b.databases.Delete(name)

	prefix := name + "|"
	for _, t := range b.tables.Snapshot() {
		if k := tableKey(t.DatabaseName, t.Name); len(k) > len(prefix) && k[:len(prefix)] == prefix {
			b.tables.Delete(k)
		}
	}

	for _, p := range b.partitions.Snapshot() {
		if k := partitionKey(p.DatabaseName, p.TableName, p.Values); len(k) > len(prefix) && k[:len(prefix)] == prefix {
			b.partitions.Delete(k)
		}
	}

	for _, tv := range b.tableVersions.Snapshot() {
		if k := tableVersionEntryKeyFn(tv); len(k) > len(prefix) && k[:len(prefix)] == prefix {
			b.tableVersions.Delete(k)
		}
	}

	return nil
}

// UpdateDatabase updates an existing Glue database.
func (b *InMemoryBackend) UpdateDatabase(name string, input DatabaseInput) error {
	b.mu.Lock("UpdateDatabase")
	defer b.mu.Unlock()

	db, ok := b.databases.Get(name)
	if !ok {
		return ErrNotFound
	}

	db.Description = input.Description

	return nil
}

// --- Table operations ---

// CreateTable creates a new Glue table in a database.
func (b *InMemoryBackend) CreateTable(dbName string, input TableInput) (*Table, error) {
	b.mu.Lock("CreateTable")
	defer b.mu.Unlock()

	if !b.databases.Has(dbName) {
		return nil, ErrNotFound
	}

	key := tableKey(dbName, input.Name)
	if b.tables.Has(key) {
		return nil, ErrAlreadyExists
	}

	now := float64(time.Now().Unix())
	t := &Table{
		Name:              input.Name,
		DatabaseName:      dbName,
		CatalogID:         b.accountID,
		Description:       input.Description,
		Owner:             input.Owner,
		Retention:         input.Retention,
		Parameters:        maps.Clone(input.Parameters),
		StorageDescriptor: input.StorageDescriptor,
		PartitionKeys:     input.PartitionKeys,
		TableType:         input.TableType,
		CreateTime:        now,
		UpdateTime:        now,
	}
	b.tables.Put(t)

	return t, nil
}

// GetTable retrieves a Glue table.
func (b *InMemoryBackend) GetTable(dbName, tableName string) (*Table, error) {
	b.mu.RLock("GetTable")
	defer b.mu.RUnlock()

	t, ok := b.tables.Get(tableKey(dbName, tableName))
	if !ok {
		return nil, ErrNotFound
	}

	return cloneTable(t), nil
}

// GetTables returns all tables in a database sorted by name.
func (b *InMemoryBackend) GetTables(dbName string) ([]*Table, error) {
	b.mu.RLock("GetTables")
	defer b.mu.RUnlock()

	if !b.databases.Has(dbName) {
		return nil, ErrNotFound
	}

	prefix := dbName + "|"
	src := b.tables.Snapshot()
	out := make([]*Table, 0, len(src))

	for _, t := range src {
		if k := tableKey(t.DatabaseName, t.Name); len(k) > len(prefix) && k[:len(prefix)] == prefix {
			// Clone before returning: GetTable already clones, but GetTables was
			// handing out the live backend pointer, letting a caller mutate
			// (or a concurrent JSON marshal race against a writer mutating)
			// catalog state without holding b.mu.
			out = append(out, cloneTable(t))
		}
	}

	return out, nil
}

// UpdateTable updates an existing Glue table.
func (b *InMemoryBackend) UpdateTable(dbName string, input TableInput) error {
	b.mu.Lock("UpdateTable")
	defer b.mu.Unlock()

	key := tableKey(dbName, input.Name)

	t, ok := b.tables.Get(key)
	if !ok {
		return ErrNotFound
	}

	t.Description = input.Description
	t.Owner = input.Owner
	t.Retention = input.Retention
	t.Parameters = maps.Clone(input.Parameters)
	t.StorageDescriptor = input.StorageDescriptor
	t.PartitionKeys = input.PartitionKeys
	t.TableType = input.TableType
	t.UpdateTime = float64(time.Now().Unix())

	return nil
}

// DeleteTable deletes a Glue table and all its partitions.
func (b *InMemoryBackend) DeleteTable(dbName, tableName string) error {
	b.mu.Lock("DeleteTable")
	defer b.mu.Unlock()

	key := tableKey(dbName, tableName)
	if !b.tables.Has(key) {
		return ErrNotFound
	}

	b.tables.Delete(key)
	b.deleteTablePartitionsLocked(dbName, tableName)

	return nil
}

// deleteTablePartitionsLocked removes all partitions and versions for a table.
// Must be called with b.mu held for writing.
func (b *InMemoryBackend) deleteTablePartitionsLocked(dbName, tableName string) {
	prefix := dbName + "|" + tableName + "|"

	for _, p := range b.partitions.Snapshot() {
		if k := partitionKey(p.DatabaseName, p.TableName, p.Values); strings.HasPrefix(k, prefix) {
			b.partitions.Delete(k)
		}
	}

	for k := range b.partitionIndexes {
		if strings.HasPrefix(k, prefix) {
			delete(b.partitionIndexes, k)
		}
	}

	for _, tv := range b.tableVersions.Snapshot() {
		if k := tableVersionEntryKeyFn(tv); strings.HasPrefix(k, prefix) {
			b.tableVersions.Delete(k)
		}
	}
}

// --- Crawler operations ---

// CreateCrawler creates a new Glue crawler.
func (b *InMemoryBackend) CreateCrawler(
	name, role, dbName string,
	targets CrawlerTarget,
	tags map[string]string,
) (*Crawler, error) {
	return b.CreateCrawlerWithOptions(name, role, dbName, targets, tags, CrawlerOptions{})
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

// CreateCrawlerWithOptions is CreateCrawler plus the optional
// creation-time settings AWS's CreateCrawlerRequest supports
// (Schedule/Classifiers/Configuration/TablePrefix/Description) that the
// original positional-argument CreateCrawler predates.
func (b *InMemoryBackend) CreateCrawlerWithOptions(
	name, role, dbName string,
	targets CrawlerTarget,
	tags map[string]string,
	opts CrawlerOptions,
) (*Crawler, error) {
	b.mu.Lock("CreateCrawler")
	defer b.mu.Unlock()

	if name == "" || len(name) > maxNameLen || role == "" {
		return nil, ErrValidation
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	if dbName != "" {
		if !b.databases.Has(dbName) {
			return nil, ErrNotFound
		}
	}

	if b.crawlers.Has(name) {
		return nil, ErrAlreadyExists
	}

	now := float64(time.Now().Unix())
	c := &Crawler{
		Name:          name,
		Role:          role,
		DatabaseName:  dbName,
		Targets:       targets,
		State:         stateReady,
		ARN:           b.crawlerARN(name),
		Tags:          maps.Clone(tags),
		Description:   opts.Description,
		Configuration: opts.Configuration,
		TablePrefix:   opts.TablePrefix,
		Classifiers:   append([]string(nil), opts.Classifiers...),
		CreationTime:  now,
		LastUpdated:   now,
	}
	if opts.Schedule != "" {
		c.Schedule = CrawlerSchedule{ScheduleExpression: opts.Schedule, State: stateScheduled}
	}

	b.crawlers.Put(c)

	return c, nil
}

// GetCrawler retrieves a Glue crawler by name.
func (b *InMemoryBackend) GetCrawler(name string) (*Crawler, error) {
	b.advanceStates(time.Now())

	b.mu.RLock("GetCrawler")
	defer b.mu.RUnlock()

	c, ok := b.crawlers.Get(name)
	if !ok {
		return nil, ErrNotFound
	}

	return cloneCrawler(c), nil
}

// GetCrawlers returns all Glue crawlers sorted by name.
func (b *InMemoryBackend) GetCrawlers() []*Crawler {
	b.advanceStates(time.Now())

	b.mu.RLock("GetCrawlers")
	defer b.mu.RUnlock()

	src := b.crawlers.Snapshot()
	out := make([]*Crawler, 0, len(src))
	for _, c := range src {
		out = append(out, cloneCrawler(c))
	}

	return out
}

// ListCrawlers returns crawler names sorted alphabetically.
func (b *InMemoryBackend) ListCrawlers() []string {
	b.mu.RLock("ListCrawlers")
	defer b.mu.RUnlock()

	src := b.crawlers.Snapshot()
	out := make([]string, len(src))
	for i, c := range src {
		out[i] = c.Name
	}

	return out
}

// UpdateCrawler updates an existing Glue crawler.
func (b *InMemoryBackend) UpdateCrawler(name, role, dbName string, targets CrawlerTarget) error {
	return b.UpdateCrawlerWithOptions(name, role, dbName, targets, CrawlerOptions{})
}

// UpdateCrawlerWithOptions is UpdateCrawler plus the optional settings AWS's
// UpdateCrawlerRequest supports (Schedule/Classifiers/Configuration/
// TablePrefix/Description). Unset (zero-value) CrawlerOptions fields leave the
// corresponding crawler field unchanged, matching AWS's partial-update
// semantics for UpdateCrawler.
func (b *InMemoryBackend) UpdateCrawlerWithOptions(
	name, role, dbName string,
	targets CrawlerTarget,
	opts CrawlerOptions,
) error {
	b.mu.Lock("UpdateCrawler")
	defer b.mu.Unlock()

	c, ok := b.crawlers.Get(name)
	if !ok {
		return ErrNotFound
	}

	// AWS rejects UpdateCrawler while the crawler is actively running, same as
	// DeleteCrawler.
	if c.State == stateRunning || c.State == stateStarting || c.State == stateStopping {
		return ErrCrawlerRunning
	}

	c.Role = role
	c.DatabaseName = dbName
	c.Targets = targets

	if opts.Description != "" {
		c.Description = opts.Description
	}

	if opts.Configuration != "" {
		c.Configuration = opts.Configuration
	}

	if opts.TablePrefix != "" {
		c.TablePrefix = opts.TablePrefix
	}

	if opts.Classifiers != nil {
		c.Classifiers = append([]string(nil), opts.Classifiers...)
	}

	if opts.Schedule != "" {
		c.Schedule = CrawlerSchedule{ScheduleExpression: opts.Schedule, State: stateScheduled}
	}

	c.LastUpdated = float64(time.Now().Unix())

	return nil
}

// DeleteCrawler deletes a Glue crawler by name.
func (b *InMemoryBackend) DeleteCrawler(name string) error {
	b.mu.Lock("DeleteCrawler")
	defer b.mu.Unlock()

	c, ok := b.crawlers.Get(name)
	if !ok {
		return ErrNotFound
	}

	if c.State == stateRunning || c.State == stateStarting || c.State == stateStopping {
		return ErrCrawlerRunning
	}

	b.crawlers.Delete(name)

	return nil
}

// --- Job operations ---

// CreateJob creates a new Glue job.
func (b *InMemoryBackend) CreateJob(input Job) (*Job, error) {
	b.mu.Lock("CreateJob")
	defer b.mu.Unlock()

	if input.Name == "" || len(input.Name) > maxNameLen || input.Role == "" {
		return nil, ErrValidation
	}

	if input.Command.Name == "" {
		return nil, fmt.Errorf("%w: Command.Name is required", ErrValidation)
	}

	if input.MaxRetries < 0 || input.MaxRetries > maxJobRetries {
		return nil, fmt.Errorf(
			"%w: MaxRetries must be between 0 and %d",
			ErrValidation,
			maxJobRetries,
		)
	}

	if err := validateJobCapacity(input); err != nil {
		return nil, err
	}

	if err := validateTags(input.Tags); err != nil {
		return nil, err
	}

	if b.jobs.Has(input.Name) {
		return nil, ErrAlreadyExists
	}

	now := float64(time.Now().Unix())
	j := &Job{
		Name:                 input.Name,
		Description:          input.Description,
		Role:                 input.Role,
		Command:              input.Command,
		DefaultArguments:     input.DefaultArguments,
		GlueVersion:          input.GlueVersion,
		WorkerType:           input.WorkerType,
		NumberOfWorkers:      input.NumberOfWorkers,
		MaxCapacity:          input.MaxCapacity,
		MaxRetries:           input.MaxRetries,
		Timeout:              input.Timeout,
		ARN:                  b.jobARN(input.Name),
		Tags:                 maps.Clone(input.Tags),
		ExecutionProperty:    input.ExecutionProperty,
		Connections:          input.Connections,
		NotificationProperty: input.NotificationProperty,
		CreatedOn:            now,
		LastModifiedOn:       now,
	}
	b.jobs.Put(j)

	return j, nil
}

// validateJobCapacity enforces AWS Glue's mutual-exclusion rule between the
// legacy MaxCapacity (DPU) knob and WorkerType+NumberOfWorkers: a job may
// specify one or the other but not both.
func validateJobCapacity(input Job) error {
	if input.MaxCapacity > 0 && (input.WorkerType != "" || input.NumberOfWorkers != 0) {
		return fmt.Errorf(
			"%w: cannot specify MaxCapacity and WorkerType/NumberOfWorkers together",
			ErrValidation,
		)
	}

	return nil
}

// GetJob retrieves a Glue job by name.
func (b *InMemoryBackend) GetJob(name string) (*Job, error) {
	b.mu.RLock("GetJob")
	defer b.mu.RUnlock()

	j, ok := b.jobs.Get(name)
	if !ok {
		return nil, ErrNotFound
	}

	return cloneJob(j), nil
}

// GetJobs returns all Glue jobs sorted by name.
func (b *InMemoryBackend) GetJobs() []*Job {
	b.mu.RLock("GetJobs")
	defer b.mu.RUnlock()

	src := b.jobs.Snapshot()
	out := make([]*Job, 0, len(src))
	for _, j := range src {
		out = append(out, cloneJob(j))
	}

	return out
}

// UpdateJob updates an existing Glue job.
func (b *InMemoryBackend) UpdateJob(name string, input Job) error {
	b.mu.Lock("UpdateJob")
	defer b.mu.Unlock()

	j, ok := b.jobs.Get(name)
	if !ok {
		return ErrNotFound
	}

	if input.MaxRetries < 0 || input.MaxRetries > maxJobRetries {
		return fmt.Errorf("%w: MaxRetries must be between 0 and %d", ErrValidation, maxJobRetries)
	}

	if err := validateJobCapacity(input); err != nil {
		return err
	}

	j.Description = input.Description
	j.Role = input.Role
	j.Command = input.Command
	j.DefaultArguments = input.DefaultArguments
	j.GlueVersion = input.GlueVersion
	j.WorkerType = input.WorkerType
	j.NumberOfWorkers = input.NumberOfWorkers
	j.MaxCapacity = input.MaxCapacity
	j.MaxRetries = input.MaxRetries
	j.Timeout = input.Timeout
	j.ExecutionProperty = input.ExecutionProperty
	j.Connections = input.Connections
	j.NotificationProperty = input.NotificationProperty
	j.LastModifiedOn = float64(time.Now().Unix())

	return nil
}

// UpdateJobFromSourceControl synchronizes a job definition from its linked
// remote repository. The emulator has no real repository to pull from, so it
// records the sync linkage against the job as real, queryable state.
func (b *InMemoryBackend) UpdateJobFromSourceControl(jobName string, details SourceControlDetails) error {
	if jobName == "" {
		return fmt.Errorf("%w: JobName is required", ErrValidation)
	}

	b.mu.Lock("UpdateJobFromSourceControl")
	defer b.mu.Unlock()

	j, ok := b.jobs.Get(jobName)
	if !ok {
		return ErrNotFound
	}

	j.SourceControlDetails = &details
	j.LastModifiedOn = float64(time.Now().Unix())

	return nil
}

// UpdateSourceControlFromJob pushes a job's current definition to its linked
// remote repository. As with UpdateJobFromSourceControl, the emulator has no
// real repository, so it records the same sync linkage against the job.
func (b *InMemoryBackend) UpdateSourceControlFromJob(jobName string, details SourceControlDetails) error {
	if jobName == "" {
		return fmt.Errorf("%w: JobName is required", ErrValidation)
	}

	b.mu.Lock("UpdateSourceControlFromJob")
	defer b.mu.Unlock()

	j, ok := b.jobs.Get(jobName)
	if !ok {
		return ErrNotFound
	}

	j.SourceControlDetails = &details
	j.LastModifiedOn = float64(time.Now().Unix())

	return nil
}

// DeleteJob deletes a Glue job by name, also removing all job runs and bookmarks.
func (b *InMemoryBackend) DeleteJob(name string) error {
	b.mu.Lock("DeleteJob")
	defer b.mu.Unlock()

	if !b.jobs.Has(name) {
		return ErrNotFound
	}

	b.jobs.Delete(name)
	delete(b.jobRuns, name)
	b.jobBookmarks.Delete(name)

	return nil
}

// --- Tag operations ---

// TagResource adds tags to a resource by ARN.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if err := validateTags(tags); err != nil {
		return err
	}

	return b.tagResource(resourceARN, tags)
}

func mergeTags(dst *map[string]string, src map[string]string) {
	if *dst == nil {
		*dst = make(map[string]string)
	}

	maps.Copy(*dst, src)
}

func (b *InMemoryBackend) tagResource(
	resourceARN string,
	tags map[string]string,
) error {
	if db := b.findDatabaseByARN(resourceARN); db != nil {
		mergeTags(&db.Tags, tags)

		return nil
	}

	if c := b.findCrawlerByARN(resourceARN); c != nil {
		mergeTags(&c.Tags, tags)

		return nil
	}

	if j := b.findJobByARN(resourceARN); j != nil {
		mergeTags(&j.Tags, tags)

		return nil
	}

	if r := b.findDataQualityRulesetByARN(resourceARN); r != nil {
		mergeTags(&r.Tags, tags)

		return nil
	}

	if conn := b.findConnectionByARN(resourceARN); conn != nil {
		mergeTags(&conn.Tags, tags)

		return nil
	}

	if trig := b.findTriggerByARN(resourceARN); trig != nil {
		mergeTags(&trig.Tags, tags)

		return nil
	}

	if w := b.findWorkflowByARN(resourceARN); w != nil {
		mergeTags(&w.Tags, tags)

		return nil
	}

	return ErrNotFound
}

func deleteTags(tags map[string]string, keys []string) {
	for _, k := range keys {
		delete(tags, k)
	}
}

// UntagResource removes tags from a resource by ARN.
func (b *InMemoryBackend) UntagResource(
	resourceARN string,
	tagKeys []string,
) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if db := b.findDatabaseByARN(resourceARN); db != nil {
		deleteTags(db.Tags, tagKeys)

		return nil
	}

	if c := b.findCrawlerByARN(resourceARN); c != nil {
		deleteTags(c.Tags, tagKeys)

		return nil
	}

	if j := b.findJobByARN(resourceARN); j != nil {
		deleteTags(j.Tags, tagKeys)

		return nil
	}

	if r := b.findDataQualityRulesetByARN(resourceARN); r != nil {
		deleteTags(r.Tags, tagKeys)

		return nil
	}

	if conn := b.findConnectionByARN(resourceARN); conn != nil {
		deleteTags(conn.Tags, tagKeys)

		return nil
	}

	if trig := b.findTriggerByARN(resourceARN); trig != nil {
		deleteTags(trig.Tags, tagKeys)

		return nil
	}

	if w := b.findWorkflowByARN(resourceARN); w != nil {
		deleteTags(w.Tags, tagKeys)

		return nil
	}

	return ErrNotFound
}

// GetTags retrieves tags for a resource by ARN.
func (b *InMemoryBackend) GetTags(resourceARN string) (map[string]string, error) {
	b.mu.RLock("GetTags")
	defer b.mu.RUnlock()

	if db := b.findDatabaseByARN(resourceARN); db != nil {
		return maps.Clone(db.Tags), nil
	}

	if c := b.findCrawlerByARN(resourceARN); c != nil {
		return maps.Clone(c.Tags), nil
	}

	if j := b.findJobByARN(resourceARN); j != nil {
		return maps.Clone(j.Tags), nil
	}

	if r := b.findDataQualityRulesetByARN(resourceARN); r != nil {
		return maps.Clone(r.Tags), nil
	}

	if conn := b.findConnectionByARN(resourceARN); conn != nil {
		return maps.Clone(conn.Tags), nil
	}

	if t := b.findTriggerByARN(resourceARN); t != nil {
		return maps.Clone(t.Tags), nil
	}

	if w := b.findWorkflowByARN(resourceARN); w != nil {
		return maps.Clone(w.Tags), nil
	}

	return nil, ErrNotFound
}

func (b *InMemoryBackend) findDatabaseByARN(resourceARN string) *Database {
	name := glueResourceName(resourceARN, "database")
	if name == "" {
		return nil
	}

	db, ok := b.databases.Get(name)
	if !ok {
		return nil
	}

	return db
}

func (b *InMemoryBackend) findCrawlerByARN(resourceARN string) *Crawler {
	name := glueResourceName(resourceARN, "crawler")
	if name == "" {
		return nil
	}

	c, ok := b.crawlers.Get(name)
	if !ok {
		return nil
	}

	return c
}

func (b *InMemoryBackend) findJobByARN(resourceARN string) *Job {
	name := glueResourceName(resourceARN, "job")
	if name == "" {
		return nil
	}

	j, ok := b.jobs.Get(name)
	if !ok {
		return nil
	}

	return j
}

func (b *InMemoryBackend) findDataQualityRulesetByARN(resourceARN string) *DataQualityRuleset {
	name := glueResourceName(resourceARN, "dataQualityRuleset")
	if name == "" {
		return nil
	}

	r, ok := b.dataQualityRulesets.Get(name)
	if !ok {
		return nil
	}

	return r
}

func (b *InMemoryBackend) findConnectionByARN(resourceARN string) *Connection {
	name := glueResourceName(resourceARN, "connection")
	if name == "" {
		return nil
	}

	c, ok := b.connections.Get(name)
	if !ok {
		return nil
	}

	return c
}

func (b *InMemoryBackend) findTriggerByARN(resourceARN string) *Trigger {
	name := glueResourceName(resourceARN, "trigger")
	if name == "" {
		return nil
	}

	t, ok := b.triggers.Get(name)
	if !ok {
		return nil
	}

	return t
}

func (b *InMemoryBackend) findWorkflowByARN(resourceARN string) *Workflow {
	name := glueResourceName(resourceARN, "workflow")
	if name == "" {
		return nil
	}

	w, ok := b.workflows.Get(name)
	if !ok {
		return nil
	}

	return w
}

// --- Batch operations ---

// BatchCreatePartition creates multiple partitions for a table.
func (b *InMemoryBackend) BatchCreatePartition(
	dbName, tableName string,
	inputs []PartitionInput,
) ([]*Partition, []PartitionError) {
	b.mu.Lock("BatchCreatePartition")
	defer b.mu.Unlock()

	created := make([]*Partition, 0, len(inputs))
	errs := make([]PartitionError, 0, len(inputs))

	// AWS's BatchCreatePartition (and the single-partition CreatePartition, which
	// is implemented in terms of this method) reject partitions for a table that
	// does not exist with EntityNotFoundException. This was previously
	// unchecked, so CreatePartition/BatchCreatePartition against a nonexistent
	// database/table silently "succeeded" and stored orphaned partitions with no
	// owning table — a disguised stub.
	if !b.tables.Has(tableKey(dbName, tableName)) {
		for _, input := range inputs {
			errs = append(errs, PartitionError{
				PartitionValues: input.Values,
				ErrorDetail: ErrorDetail{
					ErrorCode:    errEntityNotFoundCode,
					ErrorMessage: fmt.Sprintf("table %s.%s not found", dbName, tableName),
				},
			})
		}

		return created, errs
	}

	now := float64(time.Now().Unix())

	for _, input := range inputs {
		key := partitionKey(dbName, tableName, input.Values)
		if b.partitions.Has(key) {
			errs = append(errs, PartitionError{
				PartitionValues: input.Values,
				ErrorDetail: ErrorDetail{
					ErrorCode:    "AlreadyExistsException",
					ErrorMessage: "partition already exists",
				},
			})

			continue
		}

		p := &Partition{
			DatabaseName:      dbName,
			TableName:         tableName,
			CatalogID:         b.accountID,
			Values:            append([]string(nil), input.Values...),
			StorageDescriptor: input.StorageDescriptor,
			Parameters:        maps.Clone(input.Parameters),
			CreationTime:      now,
		}
		b.partitions.Put(p)
		created = append(created, p)
	}

	return created, errs
}

// BatchDeletePartition deletes multiple partitions for a table.
func (b *InMemoryBackend) BatchDeletePartition(
	dbName, tableName string,
	values []PartitionValueList,
) []PartitionError {
	b.mu.Lock("BatchDeletePartition")
	defer b.mu.Unlock()

	errs := make([]PartitionError, 0, len(values))

	for _, pvl := range values {
		key := partitionKey(dbName, tableName, pvl.Values)
		if !b.partitions.Has(key) {
			errs = append(errs, PartitionError{
				PartitionValues: pvl.Values,
				ErrorDetail: ErrorDetail{
					ErrorCode:    errEntityNotFoundCode,
					ErrorMessage: "partition not found",
				},
			})

			continue
		}

		b.partitions.Delete(key)
	}

	return errs
}

// BatchDeleteTable deletes multiple tables and cascades to partitions and versions.
func (b *InMemoryBackend) BatchDeleteTable(dbName string, tableNames []string) []TableError {
	b.mu.Lock("BatchDeleteTable")
	defer b.mu.Unlock()

	errs := make([]TableError, 0, len(tableNames))

	for _, name := range tableNames {
		key := tableKey(dbName, name)
		if !b.tables.Has(key) {
			errs = append(errs, TableError{
				TableName: name,
				ErrorDetail: ErrorDetail{
					ErrorCode:    errEntityNotFoundCode,
					ErrorMessage: "table not found",
				},
			})

			continue
		}

		b.tables.Delete(key)
		b.deleteTablePartitionsLocked(dbName, name)
	}

	return errs
}

// BatchDeleteTableVersion deletes multiple table versions.
func (b *InMemoryBackend) BatchDeleteTableVersion(
	dbName, tableName string,
	versionIDs []string,
) []TableVersionError {
	b.mu.Lock("BatchDeleteTableVersion")
	defer b.mu.Unlock()

	errs := make([]TableVersionError, 0, len(versionIDs))

	for _, vid := range versionIDs {
		key := tableVersionKey(dbName, tableName, vid)
		if !b.tableVersions.Has(key) {
			errs = append(errs, TableVersionError{
				TableName: tableName,
				VersionID: vid,
				ErrorDetail: ErrorDetail{
					ErrorCode:    errEntityNotFoundCode,
					ErrorMessage: "table version not found",
				},
			})

			continue
		}

		b.tableVersions.Delete(key)
	}

	return errs
}

// BatchDeleteConnection deletes multiple connections.
func (b *InMemoryBackend) BatchDeleteConnection(names []string) ([]string, []ErrorDetail) {
	b.mu.Lock("BatchDeleteConnection")
	defer b.mu.Unlock()

	succeeded := make([]string, 0, len(names))
	errs := make([]ErrorDetail, 0, len(names))

	for _, name := range names {
		if !b.connections.Has(name) {
			errs = append(errs, ErrorDetail{
				ErrorCode:    errEntityNotFoundCode,
				ErrorMessage: "connection not found: " + name,
			})

			continue
		}

		b.connections.Delete(name)
		succeeded = append(succeeded, name)
	}

	return succeeded, errs
}

// BatchGetBlueprints retrieves multiple blueprints by name.
func (b *InMemoryBackend) BatchGetBlueprints(names []string) ([]*Blueprint, []string) {
	b.mu.RLock("BatchGetBlueprints")
	defer b.mu.RUnlock()

	found := make([]*Blueprint, 0, len(names))
	missing := make([]string, 0, len(names))

	for _, name := range names {
		bp, ok := b.blueprints.Get(name)
		if !ok {
			missing = append(missing, name)

			continue
		}

		cp := *bp
		found = append(found, &cp)
	}

	return found, missing
}

// BatchGetCrawlers retrieves multiple crawlers by name.
func (b *InMemoryBackend) BatchGetCrawlers(names []string) ([]*Crawler, []string) {
	b.mu.RLock("BatchGetCrawlers")
	defer b.mu.RUnlock()

	found := make([]*Crawler, 0, len(names))
	missing := make([]string, 0, len(names))

	for _, name := range names {
		c, ok := b.crawlers.Get(name)
		if !ok {
			missing = append(missing, name)

			continue
		}

		found = append(found, cloneCrawler(c))
	}

	return found, missing
}

// BatchGetCustomEntityTypes retrieves multiple custom entity types by name.
func (b *InMemoryBackend) BatchGetCustomEntityTypes(
	names []string,
) ([]*CustomEntityType, []string) {
	b.mu.RLock("BatchGetCustomEntityTypes")
	defer b.mu.RUnlock()

	found := make([]*CustomEntityType, 0, len(names))
	missing := make([]string, 0, len(names))

	for _, name := range names {
		cet, ok := b.customEntityTypes.Get(name)
		if !ok {
			missing = append(missing, name)

			continue
		}

		cp := *cet
		cp.ContextWords = append([]string(nil), cet.ContextWords...)
		found = append(found, &cp)
	}

	return found, missing
}

// BatchGetDataQualityResult retrieves multiple data quality results by ID.
func (b *InMemoryBackend) BatchGetDataQualityResult(
	resultIDs []string,
) ([]*DataQualityResult, []ErrorDetail) {
	b.mu.RLock("BatchGetDataQualityResult")
	defer b.mu.RUnlock()

	found := make([]*DataQualityResult, 0, len(resultIDs))
	errs := make([]ErrorDetail, 0, len(resultIDs))

	for _, id := range resultIDs {
		dqr, ok := b.dataQualityResult.Get(id)
		if !ok {
			errs = append(errs, ErrorDetail{
				ErrorCode:    "EntityNotFoundException",
				ErrorMessage: "data quality result not found: " + id,
			})

			continue
		}

		cp := *dqr
		found = append(found, &cp)
	}

	return found, errs
}

// BatchGetDevEndpoints retrieves multiple dev endpoints by name.
func (b *InMemoryBackend) BatchGetDevEndpoints(names []string) ([]*DevEndpoint, []string) {
	b.mu.RLock("BatchGetDevEndpoints")
	defer b.mu.RUnlock()

	found := make([]*DevEndpoint, 0, len(names))
	missing := make([]string, 0, len(names))

	for _, name := range names {
		dep, ok := b.devEndpoints.Get(name)
		if !ok {
			missing = append(missing, name)

			continue
		}

		cp := *dep
		found = append(found, &cp)
	}

	return found, missing
}

// --- Seed helpers (for testing) ---

// AddConnectionInternal adds a connection directly to the backend without validation.
func (b *InMemoryBackend) AddConnectionInternal(conn *Connection) {
	b.mu.Lock("AddConnectionInternal")
	defer b.mu.Unlock()

	b.connections.Put(cloneConnection(conn))
}

// connectionARN returns the ARN for a Glue connection.
func (b *InMemoryBackend) connectionARN(name string) string {
	return arn.Build("glue", b.region, b.accountID, "connection/"+name)
}

// CreateConnection creates a new Glue connection.
func (b *InMemoryBackend) CreateConnection(
	name, connType string, props map[string]string, tags map[string]string,
) (*Connection, error) {
	b.mu.Lock("CreateConnection")
	defer b.mu.Unlock()

	if name == "" {
		return nil, ErrValidation
	}

	if b.connections.Has(name) {
		return nil, ErrAlreadyExists
	}

	now := float64(time.Now().Unix())
	c := &Connection{
		Name:                 name,
		ConnectionType:       connType,
		ConnectionProperties: maps.Clone(props),
		Tags:                 maps.Clone(tags),
		ARN:                  b.connectionARN(name),
		CreationTime:         now,
		LastUpdatedTime:      now,
	}
	b.connections.Put(c)

	return cloneConnection(c), nil
}

// GetConnection retrieves a single Glue connection by name.
func (b *InMemoryBackend) GetConnection(name string) (*Connection, error) {
	b.mu.RLock("GetConnection")
	defer b.mu.RUnlock()

	c, ok := b.connections.Get(name)
	if !ok {
		return nil, ErrNotFound
	}

	return cloneConnection(c), nil
}

// GetConnections returns all Glue connections sorted by name.
func (b *InMemoryBackend) GetConnections() []*Connection {
	b.mu.RLock("GetConnections")
	defer b.mu.RUnlock()

	src := b.connections.Snapshot()
	out := make([]*Connection, 0, len(src))
	for _, c := range src {
		out = append(out, cloneConnection(c))
	}

	return out
}

// DeleteConnection deletes a single Glue connection by name.
func (b *InMemoryBackend) DeleteConnection(name string) error {
	b.mu.Lock("DeleteConnection")
	defer b.mu.Unlock()

	if !b.connections.Has(name) {
		return ErrNotFound
	}

	b.connections.Delete(name)

	return nil
}

// AddBlueprintInternal adds a blueprint directly to the backend without validation.
func (b *InMemoryBackend) AddBlueprintInternal(bp *Blueprint) {
	b.mu.Lock("AddBlueprintInternal")
	defer b.mu.Unlock()

	cp := *bp
	b.blueprints.Put(&cp)
}

// AddCustomEntityTypeInternal adds a custom entity type directly to the backend without validation.
func (b *InMemoryBackend) AddCustomEntityTypeInternal(cet *CustomEntityType) {
	b.mu.Lock("AddCustomEntityTypeInternal")
	defer b.mu.Unlock()

	cp := *cet
	cp.ContextWords = append([]string(nil), cet.ContextWords...)
	b.customEntityTypes.Put(&cp)
}

// AddDataQualityResultInternal adds a data quality result directly to the backend without validation.
func (b *InMemoryBackend) AddDataQualityResultInternal(dqr *DataQualityResult) {
	b.mu.Lock("AddDataQualityResultInternal")
	defer b.mu.Unlock()

	cp := *dqr
	b.dataQualityResult.Put(&cp)
}

// AddDevEndpointInternal adds a dev endpoint directly to the backend without validation.
func (b *InMemoryBackend) AddDevEndpointInternal(dep *DevEndpoint) {
	b.mu.Lock("AddDevEndpointInternal")
	defer b.mu.Unlock()

	cp := *dep
	b.devEndpoints.Put(&cp)
}

// AddTableVersionInternal adds a table version directly to the backend without
// validation. dbName/tableName are stamped onto the stored copy's nested Table
// field (rather than trusted from the caller-supplied tv, which the existing
// test-seed callers often leave zero-valued) so the store.Table key -- derived
// purely from the value via tableVersionEntryKeyFn -- matches the
// dbName/tableName this entry is filed under, exactly as the previous raw map
// (keyed externally on the same two parameters) did.
func (b *InMemoryBackend) AddTableVersionInternal(dbName, tableName string, tv *TableVersion) {
	b.mu.Lock("AddTableVersionInternal")
	defer b.mu.Unlock()

	cp := *tv
	if cp.Table == nil {
		cp.Table = &Table{}
	} else {
		t := *cp.Table
		cp.Table = &t
	}

	cp.Table.DatabaseName = dbName
	cp.Table.Name = tableName

	b.tableVersions.Put(&cp)
}

// AddPartitionInternal adds a partition directly to the backend without
// validation. dbName/tableName are stamped onto the stored copy (rather than
// trusted from the caller-supplied p, which the existing test-seed callers
// often leave zero-valued) so the store.Table key -- derived purely from the
// value via partitionEntryKeyFn -- matches the dbName/tableName this entry is
// filed under, exactly as the previous raw map (keyed externally on the same
// two parameters) did.
func (b *InMemoryBackend) AddPartitionInternal(dbName, tableName string, p *Partition) {
	b.mu.Lock("AddPartitionInternal")
	defer b.mu.Unlock()

	cp := *p
	cp.DatabaseName = dbName
	cp.TableName = tableName
	cp.Values = append([]string(nil), p.Values...)
	b.partitions.Put(&cp)
}

// --- Job run operations ---

// StartJobRun creates a new job run record for the named job.
func (b *InMemoryBackend) StartJobRun(
	jobName string,
	arguments map[string]string,
) (*JobRun, error) {
	b.mu.Lock("StartJobRun")
	defer b.mu.Unlock()

	j, ok := b.jobs.Get(jobName)
	if !ok {
		return nil, ErrNotFound
	}

	if maxConcurrent := j.ExecutionProperty.MaxConcurrentRuns; maxConcurrent > 0 {
		active := 0
		for _, r := range b.jobRuns[jobName] {
			if r.JobRunState == stateRunning || r.JobRunState == stateStarting {
				active++
			}
		}
		if active >= maxConcurrent {
			return nil, ErrValidation
		}
	}

	now := time.Now()
	run := &JobRun{
		ID: fmt.Sprintf(
			"jr_%d_%04d",
			now.UnixNano(),
			mrand.IntN(10000), //nolint:gosec,mnd // non-security mock run ID
		),
		JobName:         jobName,
		JobRunState:     stateStarting,
		StartedOn:       float64(now.Unix()),
		Arguments:       maps.Clone(arguments),
		WorkerType:      j.WorkerType,
		NumberOfWorkers: j.NumberOfWorkers,
		MaxCapacity:     j.MaxCapacity,
		GlueVersion:     j.GlueVersion,
		Timeout:         j.Timeout,
	}
	b.jobRuns[jobName] = append(b.jobRuns[jobName], run)

	// Schedule STARTING→RUNNING→SUCCEEDED transitions.
	if b.jobRunReadyAt[jobName] == nil {
		b.jobRunReadyAt[jobName] = make(map[string]time.Time)
	}

	if b.jobRunDoneAt[jobName] == nil {
		b.jobRunDoneAt[jobName] = make(map[string]time.Time)
	}

	b.jobRunReadyAt[jobName][run.ID] = now.Add(jobTransitionDelay)
	b.jobRunDoneAt[jobName][run.ID] = now.Add(jobTransitionDelay + jobSucceededDelay)

	bm, ok := b.jobBookmarks.Get(jobName)
	if !ok {
		bm = &JobBookmark{JobName: jobName}
		b.jobBookmarks.Put(bm)
	}
	bm.ActiveRun = run.ID
	bm.Attempt++

	return run, nil
}

// GetJobRun retrieves a specific job run by job name and run ID.
func (b *InMemoryBackend) GetJobRun(jobName, runID string) (*JobRun, error) {
	b.advanceStates(time.Now())

	b.mu.RLock("GetJobRun")
	defer b.mu.RUnlock()

	for _, run := range b.jobRuns[jobName] {
		if run.ID == runID {
			cp := *run
			cp.Arguments = maps.Clone(run.Arguments)

			return &cp, nil
		}
	}

	return nil, ErrNotFound
}

// GetJobRuns returns all runs for a job.
func (b *InMemoryBackend) GetJobRuns(jobName string) ([]*JobRun, error) {
	b.advanceStates(time.Now())

	b.mu.RLock("GetJobRuns")
	defer b.mu.RUnlock()

	if !b.jobs.Has(jobName) {
		return nil, ErrNotFound
	}

	src := b.jobRuns[jobName]
	out := make([]*JobRun, 0, len(src))
	for _, run := range src {
		cp := *run
		cp.Arguments = maps.Clone(run.Arguments)
		out = append(out, &cp)
	}

	return out, nil
}

// BatchStopJobRun stops multiple job runs by setting their state to STOPPING.
// Only RUNNING or STARTING runs can be stopped.
func (b *InMemoryBackend) BatchStopJobRun(jobName string, runIDs []string) []BatchStopJobRunError {
	b.mu.Lock("BatchStopJobRun")
	defer b.mu.Unlock()

	errs := make([]BatchStopJobRunError, 0, len(runIDs))

	for _, id := range runIDs {
		found := false
		for _, run := range b.jobRuns[jobName] {
			if run.ID != id {
				continue
			}
			found = true
			if run.JobRunState != stateRunning && run.JobRunState != stateStarting {
				errs = append(errs, BatchStopJobRunError{
					JobName:  jobName,
					JobRunID: id,
					ErrorDetail: ErrorDetail{
						ErrorCode:    "IllegalStateException",
						ErrorMessage: "job run " + id + " is not in a stoppable state: " + run.JobRunState,
					},
				})
			} else {
				run.JobRunState = stateStopping
			}

			break
		}
		if !found {
			errs = append(errs, BatchStopJobRunError{
				JobName:  jobName,
				JobRunID: id,
				ErrorDetail: ErrorDetail{
					ErrorCode:    "EntityNotFoundException",
					ErrorMessage: "job run not found: " + id,
				},
			})
		}
	}

	return errs
}

// GetJobBookmark returns the bookmark for a job.
func (b *InMemoryBackend) GetJobBookmark(jobName string) (*JobBookmark, error) {
	b.mu.RLock("GetJobBookmark")
	defer b.mu.RUnlock()

	if !b.jobs.Has(jobName) {
		return nil, ErrNotFound
	}

	bm, ok := b.jobBookmarks.Get(jobName)
	if !ok {
		return &JobBookmark{JobName: jobName}, nil
	}

	cp := *bm

	return &cp, nil
}

// ResetJobBookmark clears the bookmark for a job and returns the post-reset bookmark.
func (b *InMemoryBackend) ResetJobBookmark(jobName string) error {
	b.mu.Lock("ResetJobBookmark")
	defer b.mu.Unlock()

	if !b.jobs.Has(jobName) {
		return ErrNotFound
	}

	b.jobBookmarks.Delete(jobName)

	return nil
}

// ResetJobBookmarkWithResult atomically clears the bookmark for a job and returns the post-reset bookmark.
func (b *InMemoryBackend) ResetJobBookmarkWithResult(jobName string) (*JobBookmark, error) {
	b.mu.Lock("ResetJobBookmarkWithResult")
	defer b.mu.Unlock()

	if !b.jobs.Has(jobName) {
		return nil, ErrNotFound
	}

	b.jobBookmarks.Delete(jobName)

	return &JobBookmark{JobName: jobName}, nil
}

// --- Crawler scheduling operations ---

// StartCrawler sets a crawler's state to RUNNING (requires READY state).
// A background reconciler transitions the crawler to READY after crawlerTransitionDelay,
// creating Glue Catalog tables for each configured S3 prefix.
func (b *InMemoryBackend) StartCrawler(name string) error {
	b.mu.Lock("StartCrawler")
	defer b.mu.Unlock()

	c, ok := b.crawlers.Get(name)
	if !ok {
		return ErrNotFound
	}

	if c.State == stateRunning || c.State == stateStopping {
		return ErrCrawlerRunning
	}

	now := time.Now()
	c.State = stateRunning
	c.LastUpdated = float64(now.Unix())

	b.crawlerReadyAt[name] = now.Add(crawlerTransitionDelay)
	b.crawlHistory[name] = append(b.crawlHistory[name], &CrawlHistoryEntry{
		CrawlID:   "cr-" + uuid.NewString()[:8],
		State:     "RUNNING",
		StartTime: float64(now.Unix()),
	})

	return nil
}

// StopCrawler sets a crawler's state to STOPPING (requires RUNNING state).
func (b *InMemoryBackend) StopCrawler(name string) error {
	b.mu.Lock("StopCrawler")
	defer b.mu.Unlock()

	c, ok := b.crawlers.Get(name)
	if !ok {
		return ErrNotFound
	}
	if c.State != stateRunning {
		return ErrCrawlerNotRunning
	}

	now := time.Now()
	c.State = stateStopping
	c.LastUpdated = float64(now.Unix())

	// Schedule the STOPPING→READY transition so the crawler does not hang in
	// STOPPING forever. AWS returns the crawler to READY once it has stopped.
	b.crawlerReadyAt[name] = now.Add(crawlerTransitionDelay)

	return nil
}

// UpdateCrawlerSchedule updates the schedule expression on a crawler.
func (b *InMemoryBackend) UpdateCrawlerSchedule(name, scheduleExpression string) error {
	b.mu.Lock("UpdateCrawlerSchedule")
	defer b.mu.Unlock()

	c, ok := b.crawlers.Get(name)
	if !ok {
		return ErrNotFound
	}
	c.Schedule.ScheduleExpression = scheduleExpression

	return nil
}

// StartCrawlerSchedule enables the crawler's schedule.
func (b *InMemoryBackend) StartCrawlerSchedule(name string) error {
	b.mu.Lock("StartCrawlerSchedule")
	defer b.mu.Unlock()

	c, ok := b.crawlers.Get(name)
	if !ok {
		return ErrNotFound
	}
	if c.Schedule.ScheduleExpression == "" {
		return ErrValidation
	}
	if c.Schedule.State == stateScheduled {
		return ErrValidation
	}
	c.Schedule.State = stateScheduled

	return nil
}

// StopCrawlerSchedule disables the crawler's schedule.
func (b *InMemoryBackend) StopCrawlerSchedule(name string) error {
	b.mu.Lock("StopCrawlerSchedule")
	defer b.mu.Unlock()

	c, ok := b.crawlers.Get(name)
	if !ok {
		return ErrNotFound
	}
	c.Schedule.State = stateNotScheduled

	return nil
}

// finishCrawlHistoryLocked marks the most recent in-flight crawl-history entry
// for name as finished with the given terminal state. A no-op if there is no
// open entry (e.g. crawl history predates this feature). Must be called with
// b.mu held.
func (b *InMemoryBackend) finishCrawlHistoryLocked(name, state string, tablesCreated int, at time.Time) {
	hist := b.crawlHistory[name]
	if len(hist) == 0 {
		return
	}

	last := hist[len(hist)-1]
	if last.EndTime != 0 {
		return
	}

	last.State = state
	last.EndTime = float64(at.Unix())

	if state == "COMPLETED" {
		last.Summary = fmt.Sprintf(
			`{"TABLES_ADDED":%d,"TABLES_UPDATED":0,"TABLES_DELETED":0,"PARTITIONS_ADDED":0}`,
			tablesCreated,
		)
	}
}

// ListCrawls returns the crawl history for a crawler, newest first.
func (b *InMemoryBackend) ListCrawls(crawlerName string) ([]*CrawlHistoryEntry, error) {
	b.mu.RLock("ListCrawls")
	defer b.mu.RUnlock()

	if !b.crawlers.Has(crawlerName) {
		return nil, ErrNotFound
	}

	hist := b.crawlHistory[crawlerName]
	out := make([]*CrawlHistoryEntry, len(hist))

	for i, e := range hist {
		cp := *e
		out[len(hist)-1-i] = &cp
	}

	return out, nil
}

// --- Data quality ruleset operations ---

// dataQualityRulesetARN returns the ARN for a data quality ruleset.
func (b *InMemoryBackend) dataQualityRulesetARN(name string) string {
	return arn.Build("glue", b.region, b.accountID, "dataQualityRuleset/"+name)
}

// CreateDataQualityRuleset creates a new data quality ruleset.
func (b *InMemoryBackend) CreateDataQualityRuleset(
	name, ruleset string,
	tags map[string]string,
) (*DataQualityRuleset, error) {
	b.mu.Lock("CreateDataQualityRuleset")
	defer b.mu.Unlock()

	if name == "" || ruleset == "" {
		return nil, ErrValidation
	}

	if b.dataQualityRulesets.Has(name) {
		return nil, ErrAlreadyExists
	}

	now := float64(time.Now().Unix())
	r := &DataQualityRuleset{
		Name:           name,
		Ruleset:        ruleset,
		Tags:           maps.Clone(tags),
		ARN:            b.dataQualityRulesetARN(name),
		CreatedOn:      now,
		LastModifiedOn: now,
	}
	b.dataQualityRulesets.Put(r)

	return r, nil
}

// GetDataQualityRuleset retrieves a data quality ruleset by name.
func (b *InMemoryBackend) GetDataQualityRuleset(name string) (*DataQualityRuleset, error) {
	b.mu.RLock("GetDataQualityRuleset")
	defer b.mu.RUnlock()

	r, ok := b.dataQualityRulesets.Get(name)
	if !ok {
		return nil, ErrNotFound
	}

	cp := *r
	cp.Tags = maps.Clone(r.Tags)

	return &cp, nil
}

// DeleteDataQualityRuleset removes a data quality ruleset by name.
func (b *InMemoryBackend) DeleteDataQualityRuleset(name string) error {
	b.mu.Lock("DeleteDataQualityRuleset")
	defer b.mu.Unlock()

	if !b.dataQualityRulesets.Has(name) {
		return ErrNotFound
	}
	b.dataQualityRulesets.Delete(name)

	return nil
}

// UpdateDataQualityRuleset updates the ruleset expression for a named ruleset.
func (b *InMemoryBackend) UpdateDataQualityRuleset(name, ruleset string) error {
	b.mu.Lock("UpdateDataQualityRuleset")
	defer b.mu.Unlock()

	r, ok := b.dataQualityRulesets.Get(name)
	if !ok {
		return ErrNotFound
	}
	r.Ruleset = ruleset
	r.LastModifiedOn = float64(time.Now().Unix())

	return nil
}

// ListDataQualityRulesets returns all rulesets sorted by name.
func (b *InMemoryBackend) ListDataQualityRulesets() []*DataQualityRuleset {
	b.mu.RLock("ListDataQualityRulesets")
	defer b.mu.RUnlock()

	src := b.dataQualityRulesets.Snapshot()
	out := make([]*DataQualityRuleset, 0, len(src))
	for _, r := range src {
		cp := *r
		cp.Tags = maps.Clone(r.Tags)
		out = append(out, &cp)
	}

	return out
}

// StartDataQualityRulesetEvaluationRun validates the rulesets exist and creates a run.
func (b *InMemoryBackend) StartDataQualityRulesetEvaluationRun(
	rulesetNames []string,
) (*DataQualityEvaluationRun, error) {
	b.mu.Lock("StartDataQualityRulesetEvaluationRun")
	defer b.mu.Unlock()

	for _, name := range rulesetNames {
		if !b.dataQualityRulesets.Has(name) {
			return nil, ErrNotFound
		}
	}

	run := &DataQualityEvaluationRun{
		RunID: fmt.Sprintf(
			"dqer_%d_%04d",
			time.Now().UnixNano(),
			mrand.IntN(10000), //nolint:gosec,mnd // non-security mock run ID
		),
		RulesetNames: append([]string(nil), rulesetNames...),
		Status:       stateRunning,
		StartedOn:    float64(time.Now().Unix()),
	}
	b.dataQualityEvalRuns.Put(run)

	return run, nil
}

// GetDataQualityRulesetEvaluationRun retrieves an evaluation run by ID.
func (b *InMemoryBackend) GetDataQualityRulesetEvaluationRun(
	runID string,
) (*DataQualityEvaluationRun, error) {
	b.mu.RLock("GetDataQualityRulesetEvaluationRun")
	defer b.mu.RUnlock()

	run, ok := b.dataQualityEvalRuns.Get(runID)
	if !ok {
		return nil, ErrNotFound
	}

	cp := *run
	cp.RulesetNames = append([]string(nil), run.RulesetNames...)

	return &cp, nil
}

// CancelDataQualityRulesetEvaluationRun cancels an active evaluation run.
func (b *InMemoryBackend) CancelDataQualityRulesetEvaluationRun(runID string) error {
	b.mu.Lock("CancelDataQualityRulesetEvaluationRun")
	defer b.mu.Unlock()

	run, ok := b.dataQualityEvalRuns.Get(runID)
	if !ok {
		return ErrNotFound
	}
	if run.Status != stateRunning {
		return ErrValidation
	}
	run.Status = "CANCELLED"

	return nil
}

// AddJobRunInternal adds a job run directly to the backend without validation.
func (b *InMemoryBackend) AddJobRunInternal(run *JobRun) {
	b.mu.Lock("AddJobRunInternal")
	defer b.mu.Unlock()

	cp := *run
	cp.Arguments = maps.Clone(run.Arguments)
	b.jobRuns[run.JobName] = append(b.jobRuns[run.JobName], &cp)
}

// AddDataQualityRulesetInternal adds a data quality ruleset without validation.
func (b *InMemoryBackend) AddDataQualityRulesetInternal(r *DataQualityRuleset) {
	b.mu.Lock("AddDataQualityRulesetInternal")
	defer b.mu.Unlock()

	cp := *r
	cp.Tags = maps.Clone(r.Tags)
	b.dataQualityRulesets.Put(&cp)
}

// AddDataQualityEvalRunInternal adds an evaluation run directly without validation.
func (b *InMemoryBackend) AddDataQualityEvalRunInternal(run *DataQualityEvaluationRun) {
	b.mu.Lock("AddDataQualityEvalRunInternal")
	defer b.mu.Unlock()

	cp := *run
	cp.RulesetNames = append([]string(nil), run.RulesetNames...)
	b.dataQualityEvalRuns.Put(&cp)
}
