package glue

import (
	"fmt"
	"maps"
	mrand "math/rand/v2"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("EntityNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource already exists.
	ErrAlreadyExists = awserr.New("AlreadyExistsException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
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
	Name    string `json:"Name"`
	Type    string `json:"Type,omitempty"`
	Comment string `json:"Comment,omitempty"`
}

// StorageDescriptor describes the physical storage of a table.
type StorageDescriptor struct {
	Location string   `json:"Location,omitempty"`
	Columns  []Column `json:"Columns,omitempty"`
}

// TableInput is the input for creating or updating a Glue table.
type TableInput struct {
	StorageDescriptor StorageDescriptor `json:"StorageDescriptor,omitzero"`
	Name              string            `json:"Name"`
	Description       string            `json:"Description,omitempty"`
	TableType         string            `json:"TableType,omitempty"`
	PartitionKeys     []Column          `json:"PartitionKeys,omitempty"`
}

// Table represents a Glue catalog table.
type Table struct {
	StorageDescriptor StorageDescriptor `json:"StorageDescriptor,omitzero"`
	Name              string            `json:"Name"`
	DatabaseName      string            `json:"DatabaseName"`
	CatalogID         string            `json:"CatalogId"`
	Description       string            `json:"Description,omitempty"`
	TableType         string            `json:"TableType,omitempty"`
	PartitionKeys     []Column          `json:"PartitionKeys,omitempty"`
	CreateTime        float64           `json:"CreateTime,omitempty"`
	UpdateTime        float64           `json:"UpdateTime,omitempty"`
}

// CrawlerTarget specifies S3 targets for a crawler.
type CrawlerTarget struct {
	S3Targets []S3Target `json:"S3Targets,omitempty"`
}

// S3Target is an S3 path for a crawler.
type S3Target struct {
	Path string `json:"Path,omitempty"`
}

// Crawler represents a Glue crawler.
type Crawler struct {
	Tags         map[string]string `json:"-"`
	Schedule     CrawlerSchedule   `json:"Schedule,omitzero"`
	Name         string            `json:"Name"`
	Role         string            `json:"Role"`
	DatabaseName string            `json:"DatabaseName"`
	State        string            `json:"State"`
	ARN          string            `json:"Arn,omitempty"`
	Targets      CrawlerTarget     `json:"Targets,omitzero"`
	CreationTime float64           `json:"CreationTime,omitempty"`
	LastUpdated  float64           `json:"LastUpdated,omitempty"`
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
	Tags              map[string]string `json:"-"`
	DefaultArguments  map[string]string `json:"DefaultArguments,omitempty"`
	Command           JobCommand        `json:"Command,omitzero"`
	WorkerType        string            `json:"WorkerType,omitempty"`
	Role              string            `json:"Role,omitempty"`
	GlueVersion       string            `json:"GlueVersion,omitempty"`
	Name              string            `json:"Name"`
	ARN               string            `json:"Arn,omitempty"`
	Description       string            `json:"Description,omitempty"`
	Connections       ConnectionsList   `json:"Connections,omitzero"`
	NumberOfWorkers   int               `json:"NumberOfWorkers,omitempty"`
	MaxRetries        int               `json:"MaxRetries,omitempty"`
	Timeout           int               `json:"Timeout,omitempty"`
	ExecutionProperty ExecutionProperty `json:"ExecutionProperty,omitzero"`
	CreatedOn         float64           `json:"CreatedOn,omitempty"`
	LastModifiedOn    float64           `json:"LastModifiedOn,omitempty"`
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
	StorageDescriptor StorageDescriptor `json:"StorageDescriptor,omitzero"`
	Values            []string          `json:"Values"`
}

// Partition represents a Glue table partition.
type Partition struct {
	StorageDescriptor StorageDescriptor `json:"StorageDescriptor,omitzero"`
	DatabaseName      string            `json:"DatabaseName"`
	TableName         string            `json:"TableName"`
	Values            []string          `json:"Values"`
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
	Arguments     map[string]string `json:"Arguments,omitempty"`
	ID            string            `json:"Id"`
	JobName       string            `json:"JobName"`
	JobRunState   string            `json:"JobRunState"`
	ErrorMessage  string            `json:"ErrorMessage,omitempty"`
	StartedOn     float64           `json:"StartedOn,omitempty"`
	CompletedOn   float64           `json:"CompletedOn,omitempty"`
	ExecutionTime int               `json:"ExecutionTime,omitempty"`
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

// InMemoryBackend stores Glue state in memory.
type InMemoryBackend struct {
	databases                 map[string]*Database                      // key: databaseName
	tables                    map[string]*Table                         // key: "databaseName|tableName"
	crawlers                  map[string]*Crawler                       // key: crawlerName
	jobs                      map[string]*Job                           // key: jobName
	partitions                map[string]*Partition                     // key: partitionKey(db, table, values)
	partitionIndexes          map[string]*PartitionIndex                // key: "databaseName|tableName|indexName"
	tableVersions             map[string]*TableVersion                  // key: tableVersionKey(db, table, versionID)
	connections               map[string]*Connection                    // key: connectionName
	blueprints                map[string]*Blueprint                     // key: blueprintName
	customEntityTypes         map[string]*CustomEntityType              // key: name
	dataQualityResult         map[string]*DataQualityResult             // key: resultID
	devEndpoints              map[string]*DevEndpoint                   // key: endpointName
	jobRuns                   map[string][]*JobRun                      // key: jobName
	jobBookmarks              map[string]*JobBookmark                   // key: jobName
	dataQualityRulesets       map[string]*DataQualityRuleset            // key: name
	dataQualityEvalRuns       map[string]*DataQualityEvaluationRun      // key: runId
	triggers                  map[string]*Trigger                       // key: triggerName
	workflows                 map[string]*Workflow                      // key: workflowName
	workflowRuns              map[string][]*WorkflowRun                 // key: workflowName
	classifiers               map[string]*Classifier                    // key: classifierName
	registries                map[string]*Registry                      // key: registryName
	schemas                   map[string]*Schema                        // key: "registryName|schemaName"
	schemaVersions            map[string][]*SchemaVersion               // key: schemaARN
	udfs                      map[string]*UserDefinedFunction           // key: "dbName|udfName"
	securityConfigs           map[string]*SecurityConfiguration         // key: name
	sessions                  map[string]*Session                       // key: sessionID
	sessionStatements         map[string][]*Statement                   // key: sessionID
	tableOptimizers           map[string]*TableOptimizer                // key: "dbName|tableName|type"
	tableColumnStats          map[string]*ColumnStatistics              // key: "dbName|tableName|colName"
	partitionColumnStats      map[string]*ColumnStatistics              // key: partKey+"|"+colName
	resourcePolicies          map[string]*resourcePolicyEntry           // key: resourceARN or "__global__"
	mlTransforms              map[string]*MLTransform                   // key: transformID
	catalogs                  map[string]*CatalogEntry                  // key: catalogID
	catalogEncryptionSettings map[string]*DataCatalogEncryptionSettings // key: catalogID or accountID
	usageProfiles             map[string]*UsageProfile                  // key: name
	blueprintRuns             map[string]*BlueprintRun                  // key: runID
	dqRecommendationRuns      map[string]*DQRuleRecommendationRun       // key: runID
	columnStatTaskSettings    map[string]*ColumnStatisticsTaskSettings  // key: "dbName|tableName"
	columnStatTaskRuns        map[string]*ColumnStatisticsTaskRun       // key: runID
	materializedViewRuns      map[string]*MaterializedViewRefreshRun    // key: taskRunID
	integrations              map[string]*Integration                   // key: integrationName
	mlTaskRuns                map[string]*MLTaskRun                     // key: "transformID|taskRunID"
	catalogImports            map[string]*CatalogImportStatus           // key: catalogID or accountID
	schemaVersionMetadata     map[string]map[string]string              // key: schemaVersionID → key → value
	glueIdentityCenterConfig  *IdentityCenterConfig
	mu                        *lockmetrics.RWMutex

	// lifecycle reconciler
	jobRunReadyAt  map[string]map[string]time.Time // jobName → runID → readyAt for STARTING→RUNNING
	jobRunDoneAt   map[string]map[string]time.Time // jobName → runID → doneAt for RUNNING→SUCCEEDED
	crawlerReadyAt map[string]time.Time            // crawlerName → readyAt for RUNNING→READY
	stopCh         chan struct{}
	accountID      string
	region         string
}

// NewInMemoryBackend creates a new in-memory Glue backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		databases:                 make(map[string]*Database),
		tables:                    make(map[string]*Table),
		crawlers:                  make(map[string]*Crawler),
		jobs:                      make(map[string]*Job),
		partitions:                make(map[string]*Partition),
		partitionIndexes:          make(map[string]*PartitionIndex),
		tableVersions:             make(map[string]*TableVersion),
		connections:               make(map[string]*Connection),
		blueprints:                make(map[string]*Blueprint),
		customEntityTypes:         make(map[string]*CustomEntityType),
		dataQualityResult:         make(map[string]*DataQualityResult),
		devEndpoints:              make(map[string]*DevEndpoint),
		jobRuns:                   make(map[string][]*JobRun),
		jobBookmarks:              make(map[string]*JobBookmark),
		dataQualityRulesets:       make(map[string]*DataQualityRuleset),
		dataQualityEvalRuns:       make(map[string]*DataQualityEvaluationRun),
		triggers:                  make(map[string]*Trigger),
		workflows:                 make(map[string]*Workflow),
		workflowRuns:              make(map[string][]*WorkflowRun),
		classifiers:               make(map[string]*Classifier),
		registries:                make(map[string]*Registry),
		schemas:                   make(map[string]*Schema),
		schemaVersions:            make(map[string][]*SchemaVersion),
		udfs:                      make(map[string]*UserDefinedFunction),
		securityConfigs:           make(map[string]*SecurityConfiguration),
		sessions:                  make(map[string]*Session),
		sessionStatements:         make(map[string][]*Statement),
		tableOptimizers:           make(map[string]*TableOptimizer),
		tableColumnStats:          make(map[string]*ColumnStatistics),
		partitionColumnStats:      make(map[string]*ColumnStatistics),
		resourcePolicies:          make(map[string]*resourcePolicyEntry),
		mlTransforms:              make(map[string]*MLTransform),
		catalogs:                  make(map[string]*CatalogEntry),
		catalogEncryptionSettings: make(map[string]*DataCatalogEncryptionSettings),
		usageProfiles:             make(map[string]*UsageProfile),
		blueprintRuns:             make(map[string]*BlueprintRun),
		dqRecommendationRuns:      make(map[string]*DQRuleRecommendationRun),
		columnStatTaskSettings:    make(map[string]*ColumnStatisticsTaskSettings),
		columnStatTaskRuns:        make(map[string]*ColumnStatisticsTaskRun),
		materializedViewRuns:      make(map[string]*MaterializedViewRefreshRun),
		integrations:              make(map[string]*Integration),
		mlTaskRuns:                make(map[string]*MLTaskRun),
		catalogImports:            make(map[string]*CatalogImportStatus),
		schemaVersionMetadata:     make(map[string]map[string]string),
		mu:                        lockmetrics.New("glue"),
		accountID:                 accountID,
		region:                    region,
		jobRunReadyAt:             make(map[string]map[string]time.Time),
		jobRunDoneAt:              make(map[string]map[string]time.Time),
		crawlerReadyAt:            make(map[string]time.Time),
		stopCh:                    make(chan struct{}),
	}

	go b.runReconciler()

	return b
}

// Close stops the background reconciler goroutine.
func (b *InMemoryBackend) Close() {
	select {
	case <-b.stopCh:
	default:
		close(b.stopCh)
	}
}

// runReconciler periodically transitions Glue job runs and crawlers.
func (b *InMemoryBackend) runReconciler() {
	ticker := time.NewTicker(jobTransitionDelay / reconcilerTickDivisor)
	defer ticker.Stop()

	for {
		select {
		case <-b.stopCh:
			return
		case <-ticker.C:
			b.mu.Lock("glueReconciler")
			b.reconcileLocked()
			b.mu.Unlock()
		}
	}
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

// reconcileLocked applies pending lifecycle transitions. Must be called with b.mu held.
func (b *InMemoryBackend) reconcileLocked() {
	now := time.Now()

	// Job run transitions: STARTING→RUNNING, RUNNING→SUCCEEDED.
	for jobName, runs := range b.jobRuns {
		readyMap := b.jobRunReadyAt[jobName]
		doneMap := b.jobRunDoneAt[jobName]

		for _, run := range runs {
			advanceJobRunState(run, readyMap, doneMap, now)
		}
	}

	// Crawler transitions: RUNNING→READY, create catalog tables from S3 targets.
	for name, readyAt := range b.crawlerReadyAt {
		if now.After(readyAt) {
			c, ok := b.crawlers[name]
			if ok && c.State == stateRunning {
				c.State = stateReady
				c.LastUpdated = float64(now.Unix())
				b.createCrawlerTablesLocked(c)
			}

			delete(b.crawlerReadyAt, name)
		}
	}
}

// createCrawlerTablesLocked creates a Glue table per S3 prefix in the crawler's targets.
// Must be called with b.mu held.
func (b *InMemoryBackend) createCrawlerTablesLocked(c *Crawler) {
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
		if _, exists := b.tables[key]; !exists {
			b.tables[key] = &Table{
				Name:         tableName,
				DatabaseName: c.DatabaseName,
				CreateTime:   float64(time.Now().Unix()),
			}
		}
	}
}

// Reset clears all backend state, returning it to the initial empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.databases = make(map[string]*Database)
	b.tables = make(map[string]*Table)
	b.crawlers = make(map[string]*Crawler)
	b.jobs = make(map[string]*Job)
	b.partitions = make(map[string]*Partition)
	b.partitionIndexes = make(map[string]*PartitionIndex)
	b.tableVersions = make(map[string]*TableVersion)
	b.connections = make(map[string]*Connection)
	b.blueprints = make(map[string]*Blueprint)
	b.customEntityTypes = make(map[string]*CustomEntityType)
	b.dataQualityResult = make(map[string]*DataQualityResult)
	b.devEndpoints = make(map[string]*DevEndpoint)
	b.jobRuns = make(map[string][]*JobRun)
	b.jobBookmarks = make(map[string]*JobBookmark)
	b.dataQualityRulesets = make(map[string]*DataQualityRuleset)
	b.dataQualityEvalRuns = make(map[string]*DataQualityEvaluationRun)
	b.triggers = make(map[string]*Trigger)
	b.workflows = make(map[string]*Workflow)
	b.workflowRuns = make(map[string][]*WorkflowRun)
	b.classifiers = make(map[string]*Classifier)
	b.registries = make(map[string]*Registry)
	b.schemas = make(map[string]*Schema)
	b.schemaVersions = make(map[string][]*SchemaVersion)
	b.udfs = make(map[string]*UserDefinedFunction)
	b.securityConfigs = make(map[string]*SecurityConfiguration)
	b.sessions = make(map[string]*Session)
	b.sessionStatements = make(map[string][]*Statement)
	b.tableOptimizers = make(map[string]*TableOptimizer)
	b.tableColumnStats = make(map[string]*ColumnStatistics)
	b.partitionColumnStats = make(map[string]*ColumnStatistics)
	b.resourcePolicies = make(map[string]*resourcePolicyEntry)
	b.mlTransforms = make(map[string]*MLTransform)
	b.catalogs = make(map[string]*CatalogEntry)
	b.catalogEncryptionSettings = make(map[string]*DataCatalogEncryptionSettings)
	b.usageProfiles = make(map[string]*UsageProfile)
	b.blueprintRuns = make(map[string]*BlueprintRun)
	b.dqRecommendationRuns = make(map[string]*DQRuleRecommendationRun)
	b.columnStatTaskSettings = make(map[string]*ColumnStatisticsTaskSettings)
	b.columnStatTaskRuns = make(map[string]*ColumnStatisticsTaskRun)
	b.materializedViewRuns = make(map[string]*MaterializedViewRefreshRun)
	b.integrations = make(map[string]*Integration)
	b.mlTaskRuns = make(map[string]*MLTaskRun)
	b.catalogImports = make(map[string]*CatalogImportStatus)
	b.schemaVersionMetadata = make(map[string]map[string]string)
	b.glueIdentityCenterConfig = nil
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
	if len(c.Targets.S3Targets) > 0 {
		cp.Targets.S3Targets = make([]S3Target, len(c.Targets.S3Targets))
		copy(cp.Targets.S3Targets, c.Targets.S3Targets)
	}

	return &cp
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

	return &cp
}

// cloneConnection returns a deep copy of a Connection.
func cloneConnection(c *Connection) *Connection {
	cp := *c
	cp.ConnectionProperties = maps.Clone(c.ConnectionProperties)
	cp.Tags = maps.Clone(c.Tags)

	return &cp
}

// cloneTable returns a deep copy of a Table, including nested slices.
func cloneTable(t *Table) *Table {
	cp := *t
	if len(t.StorageDescriptor.Columns) > 0 {
		cp.StorageDescriptor.Columns = make([]Column, len(t.StorageDescriptor.Columns))
		copy(cp.StorageDescriptor.Columns, t.StorageDescriptor.Columns)
	}

	if len(t.PartitionKeys) > 0 {
		cp.PartitionKeys = make([]Column, len(t.PartitionKeys))
		copy(cp.PartitionKeys, t.PartitionKeys)
	}

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

// sortedKeys returns the keys of a map in sorted order.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

// --- Database operations ---

// CreateDatabase creates a new Glue database.
func (b *InMemoryBackend) CreateDatabase(input DatabaseInput, tags map[string]string) (*Database, error) {
	b.mu.Lock("CreateDatabase")
	defer b.mu.Unlock()

	if input.Name == "" || len(input.Name) > maxNameLen {
		return nil, ErrValidation
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	if _, ok := b.databases[input.Name]; ok {
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
	b.databases[input.Name] = db

	return db, nil
}

// GetDatabase retrieves a Glue database by name.
func (b *InMemoryBackend) GetDatabase(name string) (*Database, error) {
	b.mu.RLock("GetDatabase")
	defer b.mu.RUnlock()

	db, ok := b.databases[name]
	if !ok {
		return nil, ErrNotFound
	}

	return cloneDatabase(db), nil
}

// GetDatabases returns all Glue databases sorted by name.
func (b *InMemoryBackend) GetDatabases() []*Database {
	b.mu.RLock("GetDatabases")
	defer b.mu.RUnlock()

	out := make([]*Database, 0, len(b.databases))
	for _, k := range sortedKeys(b.databases) {
		out = append(out, cloneDatabase(b.databases[k]))
	}

	return out
}

// DeleteDatabase deletes a Glue database by name, also removing all its tables and partitions.
func (b *InMemoryBackend) DeleteDatabase(name string) error {
	b.mu.Lock("DeleteDatabase")
	defer b.mu.Unlock()

	if _, ok := b.databases[name]; !ok {
		return ErrNotFound
	}

	delete(b.databases, name)

	prefix := name + "|"
	for k := range b.tables {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			delete(b.tables, k)
		}
	}

	for k := range b.partitions {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			delete(b.partitions, k)
		}
	}

	for k := range b.tableVersions {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			delete(b.tableVersions, k)
		}
	}

	return nil
}

// UpdateDatabase updates an existing Glue database.
func (b *InMemoryBackend) UpdateDatabase(name string, input DatabaseInput) error {
	b.mu.Lock("UpdateDatabase")
	defer b.mu.Unlock()

	db, ok := b.databases[name]
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

	if _, ok := b.databases[dbName]; !ok {
		return nil, ErrNotFound
	}

	key := tableKey(dbName, input.Name)
	if _, ok := b.tables[key]; ok {
		return nil, ErrAlreadyExists
	}

	now := float64(time.Now().Unix())
	t := &Table{
		Name:              input.Name,
		DatabaseName:      dbName,
		CatalogID:         b.accountID,
		Description:       input.Description,
		StorageDescriptor: input.StorageDescriptor,
		PartitionKeys:     input.PartitionKeys,
		TableType:         input.TableType,
		CreateTime:        now,
		UpdateTime:        now,
	}
	b.tables[key] = t

	return t, nil
}

// GetTable retrieves a Glue table.
func (b *InMemoryBackend) GetTable(dbName, tableName string) (*Table, error) {
	b.mu.RLock("GetTable")
	defer b.mu.RUnlock()

	t, ok := b.tables[tableKey(dbName, tableName)]
	if !ok {
		return nil, ErrNotFound
	}

	return cloneTable(t), nil
}

// GetTables returns all tables in a database sorted by name.
func (b *InMemoryBackend) GetTables(dbName string) ([]*Table, error) {
	b.mu.RLock("GetTables")
	defer b.mu.RUnlock()

	if _, ok := b.databases[dbName]; !ok {
		return nil, ErrNotFound
	}

	prefix := dbName + "|"
	out := make([]*Table, 0, len(b.tables))

	for _, k := range sortedKeys(b.tables) {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, b.tables[k])
		}
	}

	return out, nil
}

// UpdateTable updates an existing Glue table.
func (b *InMemoryBackend) UpdateTable(dbName string, input TableInput) error {
	b.mu.Lock("UpdateTable")
	defer b.mu.Unlock()

	key := tableKey(dbName, input.Name)

	t, ok := b.tables[key]
	if !ok {
		return ErrNotFound
	}

	t.Description = input.Description
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
	if _, ok := b.tables[key]; !ok {
		return ErrNotFound
	}

	delete(b.tables, key)
	b.deleteTablePartitionsLocked(dbName, tableName)

	return nil
}

// deleteTablePartitionsLocked removes all partitions and versions for a table.
// Must be called with b.mu held for writing.
func (b *InMemoryBackend) deleteTablePartitionsLocked(dbName, tableName string) {
	prefix := dbName + "|" + tableName + "|"
	for k := range b.partitions {
		if strings.HasPrefix(k, prefix) {
			delete(b.partitions, k)
		}
	}

	for k := range b.partitionIndexes {
		if strings.HasPrefix(k, prefix) {
			delete(b.partitionIndexes, k)
		}
	}

	for k := range b.tableVersions {
		if strings.HasPrefix(k, prefix) {
			delete(b.tableVersions, k)
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
	b.mu.Lock("CreateCrawler")
	defer b.mu.Unlock()

	if name == "" || len(name) > maxNameLen || role == "" {
		return nil, ErrValidation
	}

	if err := validateTags(tags); err != nil {
		return nil, err
	}

	if dbName != "" {
		if _, ok := b.databases[dbName]; !ok {
			return nil, ErrNotFound
		}
	}

	if _, ok := b.crawlers[name]; ok {
		return nil, ErrAlreadyExists
	}

	now := float64(time.Now().Unix())
	c := &Crawler{
		Name:         name,
		Role:         role,
		DatabaseName: dbName,
		Targets:      targets,
		State:        stateReady,
		ARN:          b.crawlerARN(name),
		Tags:         maps.Clone(tags),
		CreationTime: now,
		LastUpdated:  now,
	}
	b.crawlers[name] = c

	return c, nil
}

// GetCrawler retrieves a Glue crawler by name.
func (b *InMemoryBackend) GetCrawler(name string) (*Crawler, error) {
	b.mu.RLock("GetCrawler")
	defer b.mu.RUnlock()

	c, ok := b.crawlers[name]
	if !ok {
		return nil, ErrNotFound
	}

	return cloneCrawler(c), nil
}

// GetCrawlers returns all Glue crawlers sorted by name.
func (b *InMemoryBackend) GetCrawlers() []*Crawler {
	b.mu.RLock("GetCrawlers")
	defer b.mu.RUnlock()

	out := make([]*Crawler, 0, len(b.crawlers))
	for _, k := range sortedKeys(b.crawlers) {
		out = append(out, cloneCrawler(b.crawlers[k]))
	}

	return out
}

// ListCrawlers returns crawler names sorted alphabetically.
func (b *InMemoryBackend) ListCrawlers() []string {
	b.mu.RLock("ListCrawlers")
	defer b.mu.RUnlock()

	return sortedKeys(b.crawlers)
}

// UpdateCrawler updates an existing Glue crawler.
func (b *InMemoryBackend) UpdateCrawler(name, role, dbName string, targets CrawlerTarget) error {
	b.mu.Lock("UpdateCrawler")
	defer b.mu.Unlock()

	c, ok := b.crawlers[name]
	if !ok {
		return ErrNotFound
	}

	c.Role = role
	c.DatabaseName = dbName
	c.Targets = targets
	c.LastUpdated = float64(time.Now().Unix())

	return nil
}

// DeleteCrawler deletes a Glue crawler by name.
func (b *InMemoryBackend) DeleteCrawler(name string) error {
	b.mu.Lock("DeleteCrawler")
	defer b.mu.Unlock()

	c, ok := b.crawlers[name]
	if !ok {
		return ErrNotFound
	}

	if c.State == stateRunning || c.State == stateStarting || c.State == stateStopping {
		return ErrCrawlerRunning
	}

	delete(b.crawlers, name)

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
		return nil, fmt.Errorf("%w: MaxRetries must be between 0 and %d", ErrValidation, maxJobRetries)
	}

	if err := validateTags(input.Tags); err != nil {
		return nil, err
	}

	if _, ok := b.jobs[input.Name]; ok {
		return nil, ErrAlreadyExists
	}

	now := float64(time.Now().Unix())
	j := &Job{
		Name:              input.Name,
		Description:       input.Description,
		Role:              input.Role,
		Command:           input.Command,
		DefaultArguments:  input.DefaultArguments,
		GlueVersion:       input.GlueVersion,
		WorkerType:        input.WorkerType,
		NumberOfWorkers:   input.NumberOfWorkers,
		MaxRetries:        input.MaxRetries,
		Timeout:           input.Timeout,
		ARN:               b.jobARN(input.Name),
		Tags:              maps.Clone(input.Tags),
		ExecutionProperty: input.ExecutionProperty,
		Connections:       input.Connections,
		CreatedOn:         now,
		LastModifiedOn:    now,
	}
	b.jobs[input.Name] = j

	return j, nil
}

// GetJob retrieves a Glue job by name.
func (b *InMemoryBackend) GetJob(name string) (*Job, error) {
	b.mu.RLock("GetJob")
	defer b.mu.RUnlock()

	j, ok := b.jobs[name]
	if !ok {
		return nil, ErrNotFound
	}

	return cloneJob(j), nil
}

// GetJobs returns all Glue jobs sorted by name.
func (b *InMemoryBackend) GetJobs() []*Job {
	b.mu.RLock("GetJobs")
	defer b.mu.RUnlock()

	out := make([]*Job, 0, len(b.jobs))
	for _, k := range sortedKeys(b.jobs) {
		out = append(out, cloneJob(b.jobs[k]))
	}

	return out
}

// UpdateJob updates an existing Glue job.
func (b *InMemoryBackend) UpdateJob(name string, input Job) error {
	b.mu.Lock("UpdateJob")
	defer b.mu.Unlock()

	j, ok := b.jobs[name]
	if !ok {
		return ErrNotFound
	}

	if input.MaxRetries < 0 || input.MaxRetries > maxJobRetries {
		return fmt.Errorf("%w: MaxRetries must be between 0 and %d", ErrValidation, maxJobRetries)
	}

	j.Description = input.Description
	j.Role = input.Role
	j.Command = input.Command
	j.DefaultArguments = input.DefaultArguments
	j.GlueVersion = input.GlueVersion
	j.WorkerType = input.WorkerType
	j.NumberOfWorkers = input.NumberOfWorkers
	j.MaxRetries = input.MaxRetries
	j.Timeout = input.Timeout
	j.ExecutionProperty = input.ExecutionProperty
	j.Connections = input.Connections
	j.LastModifiedOn = float64(time.Now().Unix())

	return nil
}

// DeleteJob deletes a Glue job by name, also removing all job runs and bookmarks.
func (b *InMemoryBackend) DeleteJob(name string) error {
	b.mu.Lock("DeleteJob")
	defer b.mu.Unlock()

	if _, ok := b.jobs[name]; !ok {
		return ErrNotFound
	}

	delete(b.jobs, name)
	delete(b.jobRuns, name)
	delete(b.jobBookmarks, name)

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

func (b *InMemoryBackend) tagResource(resourceARN string, tags map[string]string) error {
	if db := b.findDatabaseByARN(resourceARN); db != nil {
		if db.Tags == nil {
			db.Tags = make(map[string]string)
		}
		maps.Copy(db.Tags, tags)

		return nil
	}

	if c := b.findCrawlerByARN(resourceARN); c != nil {
		if c.Tags == nil {
			c.Tags = make(map[string]string)
		}
		maps.Copy(c.Tags, tags)

		return nil
	}

	if j := b.findJobByARN(resourceARN); j != nil {
		if j.Tags == nil {
			j.Tags = make(map[string]string)
		}
		maps.Copy(j.Tags, tags)

		return nil
	}

	if r := b.findDataQualityRulesetByARN(resourceARN); r != nil {
		if r.Tags == nil {
			r.Tags = make(map[string]string)
		}
		maps.Copy(r.Tags, tags)

		return nil
	}

	if conn := b.findConnectionByARN(resourceARN); conn != nil {
		if conn.Tags == nil {
			conn.Tags = make(map[string]string)
		}
		maps.Copy(conn.Tags, tags)

		return nil
	}

	return ErrNotFound
}

// UntagResource removes tags from a resource by ARN.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if db := b.findDatabaseByARN(resourceARN); db != nil {
		for _, k := range tagKeys {
			delete(db.Tags, k)
		}

		return nil
	}

	if c := b.findCrawlerByARN(resourceARN); c != nil {
		for _, k := range tagKeys {
			delete(c.Tags, k)
		}

		return nil
	}

	if j := b.findJobByARN(resourceARN); j != nil {
		for _, k := range tagKeys {
			delete(j.Tags, k)
		}

		return nil
	}

	if r := b.findDataQualityRulesetByARN(resourceARN); r != nil {
		for _, k := range tagKeys {
			delete(r.Tags, k)
		}

		return nil
	}

	if conn := b.findConnectionByARN(resourceARN); conn != nil {
		for _, k := range tagKeys {
			delete(conn.Tags, k)
		}

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

	return nil, ErrNotFound
}

func (b *InMemoryBackend) findDatabaseByARN(resourceARN string) *Database {
	name := glueResourceName(resourceARN, "database")
	if name == "" {
		return nil
	}

	db, ok := b.databases[name]
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

	c, ok := b.crawlers[name]
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

	j, ok := b.jobs[name]
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

	r, ok := b.dataQualityRulesets[name]
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

	c, ok := b.connections[name]
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

	t, ok := b.triggers[name]
	if !ok {
		return nil
	}

	return t
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

	for _, input := range inputs {
		key := partitionKey(dbName, tableName, input.Values)
		if _, exists := b.partitions[key]; exists {
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
			Values:            append([]string(nil), input.Values...),
			StorageDescriptor: input.StorageDescriptor,
		}
		b.partitions[key] = p
		created = append(created, p)
	}

	return created, errs
}

// BatchDeletePartition deletes multiple partitions for a table.
func (b *InMemoryBackend) BatchDeletePartition(dbName, tableName string, values []PartitionValueList) []PartitionError {
	b.mu.Lock("BatchDeletePartition")
	defer b.mu.Unlock()

	errs := make([]PartitionError, 0, len(values))

	for _, pvl := range values {
		key := partitionKey(dbName, tableName, pvl.Values)
		if _, ok := b.partitions[key]; !ok {
			errs = append(errs, PartitionError{
				PartitionValues: pvl.Values,
				ErrorDetail:     ErrorDetail{ErrorCode: errEntityNotFoundCode, ErrorMessage: "partition not found"},
			})

			continue
		}

		delete(b.partitions, key)
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
		if _, ok := b.tables[key]; !ok {
			errs = append(errs, TableError{
				TableName:   name,
				ErrorDetail: ErrorDetail{ErrorCode: errEntityNotFoundCode, ErrorMessage: "table not found"},
			})

			continue
		}

		delete(b.tables, key)
		b.deleteTablePartitionsLocked(dbName, name)
	}

	return errs
}

// BatchDeleteTableVersion deletes multiple table versions.
func (b *InMemoryBackend) BatchDeleteTableVersion(dbName, tableName string, versionIDs []string) []TableVersionError {
	b.mu.Lock("BatchDeleteTableVersion")
	defer b.mu.Unlock()

	errs := make([]TableVersionError, 0, len(versionIDs))

	for _, vid := range versionIDs {
		key := tableVersionKey(dbName, tableName, vid)
		if _, ok := b.tableVersions[key]; !ok {
			errs = append(errs, TableVersionError{
				TableName:   tableName,
				VersionID:   vid,
				ErrorDetail: ErrorDetail{ErrorCode: errEntityNotFoundCode, ErrorMessage: "table version not found"},
			})

			continue
		}

		delete(b.tableVersions, key)
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
		if _, ok := b.connections[name]; !ok {
			errs = append(errs, ErrorDetail{
				ErrorCode:    errEntityNotFoundCode,
				ErrorMessage: "connection not found: " + name,
			})

			continue
		}

		delete(b.connections, name)
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
		bp, ok := b.blueprints[name]
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
		c, ok := b.crawlers[name]
		if !ok {
			missing = append(missing, name)

			continue
		}

		found = append(found, cloneCrawler(c))
	}

	return found, missing
}

// BatchGetCustomEntityTypes retrieves multiple custom entity types by name.
func (b *InMemoryBackend) BatchGetCustomEntityTypes(names []string) ([]*CustomEntityType, []string) {
	b.mu.RLock("BatchGetCustomEntityTypes")
	defer b.mu.RUnlock()

	found := make([]*CustomEntityType, 0, len(names))
	missing := make([]string, 0, len(names))

	for _, name := range names {
		cet, ok := b.customEntityTypes[name]
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
func (b *InMemoryBackend) BatchGetDataQualityResult(resultIDs []string) ([]*DataQualityResult, []ErrorDetail) {
	b.mu.RLock("BatchGetDataQualityResult")
	defer b.mu.RUnlock()

	found := make([]*DataQualityResult, 0, len(resultIDs))
	errs := make([]ErrorDetail, 0, len(resultIDs))

	for _, id := range resultIDs {
		dqr, ok := b.dataQualityResult[id]
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
		dep, ok := b.devEndpoints[name]
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

	b.connections[conn.Name] = cloneConnection(conn)
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

	if _, ok := b.connections[name]; ok {
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
	b.connections[name] = c

	return cloneConnection(c), nil
}

// GetConnection retrieves a single Glue connection by name.
func (b *InMemoryBackend) GetConnection(name string) (*Connection, error) {
	b.mu.RLock("GetConnection")
	defer b.mu.RUnlock()

	c, ok := b.connections[name]
	if !ok {
		return nil, ErrNotFound
	}

	return cloneConnection(c), nil
}

// GetConnections returns all Glue connections sorted by name.
func (b *InMemoryBackend) GetConnections() []*Connection {
	b.mu.RLock("GetConnections")
	defer b.mu.RUnlock()

	out := make([]*Connection, 0, len(b.connections))
	for _, k := range sortedKeys(b.connections) {
		out = append(out, cloneConnection(b.connections[k]))
	}

	return out
}

// DeleteConnection deletes a single Glue connection by name.
func (b *InMemoryBackend) DeleteConnection(name string) error {
	b.mu.Lock("DeleteConnection")
	defer b.mu.Unlock()

	if _, ok := b.connections[name]; !ok {
		return ErrNotFound
	}

	delete(b.connections, name)

	return nil
}

// AddBlueprintInternal adds a blueprint directly to the backend without validation.
func (b *InMemoryBackend) AddBlueprintInternal(bp *Blueprint) {
	b.mu.Lock("AddBlueprintInternal")
	defer b.mu.Unlock()

	cp := *bp
	b.blueprints[bp.Name] = &cp
}

// AddCustomEntityTypeInternal adds a custom entity type directly to the backend without validation.
func (b *InMemoryBackend) AddCustomEntityTypeInternal(cet *CustomEntityType) {
	b.mu.Lock("AddCustomEntityTypeInternal")
	defer b.mu.Unlock()

	cp := *cet
	cp.ContextWords = append([]string(nil), cet.ContextWords...)
	b.customEntityTypes[cet.Name] = &cp
}

// AddDataQualityResultInternal adds a data quality result directly to the backend without validation.
func (b *InMemoryBackend) AddDataQualityResultInternal(dqr *DataQualityResult) {
	b.mu.Lock("AddDataQualityResultInternal")
	defer b.mu.Unlock()

	cp := *dqr
	b.dataQualityResult[dqr.ResultID] = &cp
}

// AddDevEndpointInternal adds a dev endpoint directly to the backend without validation.
func (b *InMemoryBackend) AddDevEndpointInternal(dep *DevEndpoint) {
	b.mu.Lock("AddDevEndpointInternal")
	defer b.mu.Unlock()

	cp := *dep
	b.devEndpoints[dep.EndpointName] = &cp
}

// AddTableVersionInternal adds a table version directly to the backend without validation.
func (b *InMemoryBackend) AddTableVersionInternal(dbName, tableName string, tv *TableVersion) {
	b.mu.Lock("AddTableVersionInternal")
	defer b.mu.Unlock()

	cp := *tv
	b.tableVersions[tableVersionKey(dbName, tableName, tv.VersionID)] = &cp
}

// AddPartitionInternal adds a partition directly to the backend without validation.
func (b *InMemoryBackend) AddPartitionInternal(dbName, tableName string, p *Partition) {
	b.mu.Lock("AddPartitionInternal")
	defer b.mu.Unlock()

	cp := *p
	cp.Values = append([]string(nil), p.Values...)
	b.partitions[partitionKey(dbName, tableName, p.Values)] = &cp
}

// --- Job run operations ---

// StartJobRun creates a new job run record for the named job.
func (b *InMemoryBackend) StartJobRun(jobName string, arguments map[string]string) (*JobRun, error) {
	b.mu.Lock("StartJobRun")
	defer b.mu.Unlock()

	j, ok := b.jobs[jobName]
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
		JobName:     jobName,
		JobRunState: stateStarting,
		StartedOn:   float64(now.Unix()),
		Arguments:   maps.Clone(arguments),
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

	bm := b.jobBookmarks[jobName]
	if bm == nil {
		bm = &JobBookmark{JobName: jobName}
		b.jobBookmarks[jobName] = bm
	}
	bm.ActiveRun = run.ID
	bm.Attempt++

	return run, nil
}

// GetJobRun retrieves a specific job run by job name and run ID.
func (b *InMemoryBackend) GetJobRun(jobName, runID string) (*JobRun, error) {
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
	b.mu.RLock("GetJobRuns")
	defer b.mu.RUnlock()

	if _, ok := b.jobs[jobName]; !ok {
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

	if _, ok := b.jobs[jobName]; !ok {
		return nil, ErrNotFound
	}

	bm, ok := b.jobBookmarks[jobName]
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

	if _, ok := b.jobs[jobName]; !ok {
		return ErrNotFound
	}

	delete(b.jobBookmarks, jobName)

	return nil
}

// ResetJobBookmarkWithResult atomically clears the bookmark for a job and returns the post-reset bookmark.
func (b *InMemoryBackend) ResetJobBookmarkWithResult(jobName string) (*JobBookmark, error) {
	b.mu.Lock("ResetJobBookmarkWithResult")
	defer b.mu.Unlock()

	if _, ok := b.jobs[jobName]; !ok {
		return nil, ErrNotFound
	}

	delete(b.jobBookmarks, jobName)

	return &JobBookmark{JobName: jobName}, nil
}

// --- Crawler scheduling operations ---

// StartCrawler sets a crawler's state to RUNNING (requires READY state).
// A background reconciler transitions the crawler to READY after crawlerTransitionDelay,
// creating Glue Catalog tables for each configured S3 prefix.
func (b *InMemoryBackend) StartCrawler(name string) error {
	b.mu.Lock("StartCrawler")
	defer b.mu.Unlock()

	c, ok := b.crawlers[name]
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

	return nil
}

// StopCrawler sets a crawler's state to STOPPING (requires RUNNING state).
func (b *InMemoryBackend) StopCrawler(name string) error {
	b.mu.Lock("StopCrawler")
	defer b.mu.Unlock()

	c, ok := b.crawlers[name]
	if !ok {
		return ErrNotFound
	}
	if c.State != stateRunning {
		return ErrCrawlerNotRunning
	}
	c.State = stateStopping
	c.LastUpdated = float64(time.Now().Unix())

	return nil
}

// UpdateCrawlerSchedule updates the schedule expression on a crawler.
func (b *InMemoryBackend) UpdateCrawlerSchedule(name, scheduleExpression string) error {
	b.mu.Lock("UpdateCrawlerSchedule")
	defer b.mu.Unlock()

	c, ok := b.crawlers[name]
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

	c, ok := b.crawlers[name]
	if !ok {
		return ErrNotFound
	}
	if c.Schedule.ScheduleExpression == "" {
		return ErrValidation
	}
	if c.Schedule.State == "SCHEDULED" {
		return ErrValidation
	}
	c.Schedule.State = "SCHEDULED"

	return nil
}

// StopCrawlerSchedule disables the crawler's schedule.
func (b *InMemoryBackend) StopCrawlerSchedule(name string) error {
	b.mu.Lock("StopCrawlerSchedule")
	defer b.mu.Unlock()

	c, ok := b.crawlers[name]
	if !ok {
		return ErrNotFound
	}
	c.Schedule.State = "NOT_SCHEDULED"

	return nil
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

	if _, ok := b.dataQualityRulesets[name]; ok {
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
	b.dataQualityRulesets[name] = r

	return r, nil
}

// GetDataQualityRuleset retrieves a data quality ruleset by name.
func (b *InMemoryBackend) GetDataQualityRuleset(name string) (*DataQualityRuleset, error) {
	b.mu.RLock("GetDataQualityRuleset")
	defer b.mu.RUnlock()

	r, ok := b.dataQualityRulesets[name]
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

	if _, ok := b.dataQualityRulesets[name]; !ok {
		return ErrNotFound
	}
	delete(b.dataQualityRulesets, name)

	return nil
}

// UpdateDataQualityRuleset updates the ruleset expression for a named ruleset.
func (b *InMemoryBackend) UpdateDataQualityRuleset(name, ruleset string) error {
	b.mu.Lock("UpdateDataQualityRuleset")
	defer b.mu.Unlock()

	r, ok := b.dataQualityRulesets[name]
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

	out := make([]*DataQualityRuleset, 0, len(b.dataQualityRulesets))
	for _, k := range sortedKeys(b.dataQualityRulesets) {
		r := b.dataQualityRulesets[k]
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
		if _, ok := b.dataQualityRulesets[name]; !ok {
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
	b.dataQualityEvalRuns[run.RunID] = run

	return run, nil
}

// GetDataQualityRulesetEvaluationRun retrieves an evaluation run by ID.
func (b *InMemoryBackend) GetDataQualityRulesetEvaluationRun(runID string) (*DataQualityEvaluationRun, error) {
	b.mu.RLock("GetDataQualityRulesetEvaluationRun")
	defer b.mu.RUnlock()

	run, ok := b.dataQualityEvalRuns[runID]
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

	run, ok := b.dataQualityEvalRuns[runID]
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
	b.dataQualityRulesets[r.Name] = &cp
}

// AddDataQualityEvalRunInternal adds an evaluation run directly without validation.
func (b *InMemoryBackend) AddDataQualityEvalRunInternal(run *DataQualityEvaluationRun) {
	b.mu.Lock("AddDataQualityEvalRunInternal")
	defer b.mu.Unlock()

	cp := *run
	cp.RulesetNames = append([]string(nil), run.RulesetNames...)
	b.dataQualityEvalRuns[run.RunID] = &cp
}
