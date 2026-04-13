package timestreamwrite

// StorageBackend defines the interface for Timestream Write backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	CreateDatabase(name string) (*Database, error)
	DescribeDatabase(name string) (*Database, error)
	ListDatabases() []Database
	DeleteDatabase(name string) error
	UpdateDatabase(name, kmsKeyID string) (*Database, error)
	CreateTable(dbName, tblName string) (*Table, error)
	DescribeTable(dbName, tblName string) (*Table, error)
	ListTables(dbName string) ([]Table, error)
	DeleteTable(dbName, tblName string) error
	UpdateTable(dbName, tblName string) (*Table, error)
	WriteRecords(dbName, tblName string, records []Record) error
	TagResource(arn string, tags map[string]string) error
	UntagResource(arn string, tagKeys []string) error
	ListTagsForResource(arn string) map[string]string
	CreateBatchLoadTask(targetDatabase, targetTable string) (*BatchLoadTask, error)
	DescribeBatchLoadTask(taskID string) (*BatchLoadTask, error)
	ListBatchLoadTasks(statusFilter string) []BatchLoadTask
	ResumeBatchLoadTask(taskID string) error
}

// Compile-time assertion: InMemoryBackend must implement StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
