package timestreamwrite

// StorageBackend defines the interface for Timestream Write backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	// Database operations.
	CreateDatabase(name, kmsKeyID string, tags map[string]string) (*Database, error)
	DescribeDatabase(name string) (*Database, error)
	ListDatabases() []Database
	DeleteDatabase(name string) error
	UpdateDatabase(name, kmsKeyID string) (*Database, error)

	// Table operations.
	CreateTable(dbName, tblName string, tags map[string]string, inp *CreateTableInput) (*Table, error)
	DescribeTable(dbName, tblName string) (*Table, error)
	ListTables(dbName string) ([]Table, error)
	DeleteTable(dbName, tblName string) error
	UpdateTable(dbName, tblName string, inp *UpdateTableInput) (*Table, error)

	// Record operations.
	WriteRecords(dbName, tblName string, records []Record) (*WriteRecordsOutput, error)

	// Tag operations.
	TagResource(arn string, tags map[string]string) error
	UntagResource(arn string, tagKeys []string) error
	ListTagsForResource(arn string) map[string]string

	// Batch load task operations.
	CreateBatchLoadTask(
		targetDatabase, targetTable string,
		dataSourceCfg *DataSourceConfiguration,
		reportCfg *ReportConfiguration,
	) (*BatchLoadTask, error)
	DescribeBatchLoadTask(taskID string) (*BatchLoadTask, error)
	ListBatchLoadTasks(statusFilter string) []BatchLoadTask
	ResumeBatchLoadTask(taskID string) error

	// Lifecycle operations.
	Reset()
	AccountID() string
	Region() string
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
