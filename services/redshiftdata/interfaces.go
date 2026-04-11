package redshiftdata

// StorageBackend defines the interface for Redshift Data backend implementations.
// All methods must be safe for concurrent use.
type StorageBackend interface {
	// Statement execution
	ExecuteStatement(
		sql, clusterIdentifier, workgroupName, database, dbUser, secretARN, statementName string,
	) (*Statement, error)
	BatchExecuteStatement(
		sqls []string, clusterIdentifier, workgroupName, database, dbUser, secretARN, statementName string,
	) (*Statement, error)

	// Statement inspection
	DescribeStatement(id string) (*Statement, error)
	CancelStatement(id string) error
	ListStatements(clusterIdentifier, workgroupName string) []*Statement

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot() []byte
	Restore(data []byte) error
}

// Ensure InMemoryBackend satisfies StorageBackend at compile time.
var _ StorageBackend = (*InMemoryBackend)(nil)
