package redshiftdata

import "time"

// StorageBackend defines the interface for Redshift Data backend implementations.
// All methods must be safe for concurrent use.
type StorageBackend interface {
	// Statement execution
	ExecuteStatement(
		sql, clusterIdentifier, workgroupName, database, dbUser, secretARN, statementName string,
		withEvent bool,
	) (*Statement, error)
	BatchExecuteStatement(
		sqls []string, clusterIdentifier, workgroupName, database, dbUser, secretARN, statementName string,
		withEvent bool,
	) (*Statement, error)

	// Statement inspection
	DescribeStatement(id string) (*Statement, error)
	CancelStatement(id string) error
	// ListStatements returns a page of statements and a next-token for pagination.
	ListStatements(
		clusterIdentifier, workgroupName, statusFilter, roleLevel string,
		maxResults int,
	) ([]*Statement, string)

	// Maintenance
	// EvictExpiredStatements removes terminal statements older than cutoff.
	// Returns the number of evicted statements.
	EvictExpiredStatements(cutoff time.Time) int

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot() []byte
	Restore(data []byte) error
}

// Ensure InMemoryBackend satisfies StorageBackend at compile time.
var _ StorageBackend = (*InMemoryBackend)(nil)
