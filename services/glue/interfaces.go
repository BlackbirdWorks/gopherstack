package glue

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
	CreateCrawler(name, role, dbName string, targets CrawlerTarget, tags map[string]string) (*Crawler, error)
	GetCrawler(name string) (*Crawler, error)
	GetCrawlers() []*Crawler
	ListCrawlers() []string
	UpdateCrawler(name, role, dbName string, targets CrawlerTarget) error
	DeleteCrawler(name string) error

	// Job operations.
	CreateJob(input Job) (*Job, error)
	GetJob(name string) (*Job, error)
	GetJobs() []*Job
	UpdateJob(name string, input Job) error
	DeleteJob(name string) error

	// Tag operations.
	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, tagKeys []string) error
	GetTags(resourceARN string) (map[string]string, error)

	// Partition operations.
	BatchCreatePartition(dbName, tableName string, inputs []PartitionInput) ([]*Partition, []PartitionError)
	BatchDeletePartition(dbName, tableName string, values []PartitionValueList) []PartitionError
	BatchDeleteTableVersion(dbName, tableName string, versionIDs []string) []TableVersionError

	// Connection operations.
	CreateConnection(name, connType string, props map[string]string, tags map[string]string) (*Connection, error)
	GetConnection(name string) (*Connection, error)
	GetConnections() []*Connection
	DeleteConnection(name string) error

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

	// Data quality ruleset operations.
	CreateDataQualityRuleset(name, ruleset string, tags map[string]string) (*DataQualityRuleset, error)
	GetDataQualityRuleset(name string) (*DataQualityRuleset, error)
	DeleteDataQualityRuleset(name string) error
	UpdateDataQualityRuleset(name, ruleset string) error
	ListDataQualityRulesets() []*DataQualityRuleset
	StartDataQualityRulesetEvaluationRun(rulesetNames []string) (*DataQualityEvaluationRun, error)
	GetDataQualityRulesetEvaluationRun(runID string) (*DataQualityEvaluationRun, error)
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
	GetWorkflow(name string) (*Workflow, error)
	GetWorkflows() []string
	BatchGetWorkflows(names []string) ([]*Workflow, []string)
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
	CreateDevEndpoint(name string) (*DevEndpoint, error)
	GetDevEndpoint(name string) (*DevEndpoint, error)
	GetAllDevEndpoints() []*DevEndpoint
	DeleteDevEndpoint(name string) error
}

// Snapshottable is an optional interface that a StorageBackend may implement
// to support persistence via Snapshot/Restore.
type Snapshottable interface {
	Snapshot() []byte
	Restore(data []byte) error
}

// compile-time assertion that InMemoryBackend implements StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
