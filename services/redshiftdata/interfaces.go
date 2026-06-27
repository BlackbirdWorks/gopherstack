package redshiftdata

import (
	"context"
	"time"
)

// StorageBackend defines the interface for Redshift Data backend implementations.
// All methods must be safe for concurrent use.
type StorageBackend interface {
	// Statement execution
	ExecuteStatement(
		ctx context.Context,
		sql, clusterIdentifier, workgroupName, database, dbUser, secretARN, statementName string,
		withEvent bool, resultFormat string,
		parameters []SQLParameter,
	) (*Statement, error)
	BatchExecuteStatement(
		ctx context.Context,
		sqls []string, clusterIdentifier, workgroupName, database, dbUser, secretARN, statementName string,
		withEvent bool, resultFormat string,
	) (*Statement, error)

	// Statement inspection
	DescribeStatement(ctx context.Context, id string) (*Statement, error)
	CancelStatement(ctx context.Context, id string) error
	// ListStatements returns a page of statements and a next-token for pagination.
	ListStatements(ctx context.Context, filter ListStatementsFilter) (
		[]*Statement, string, error,
	)

	// Maintenance
	// EvictExpiredStatements removes terminal statements older than cutoff.
	// Returns the number of evicted statements.
	EvictExpiredStatements(cutoff time.Time) int

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// Ensure InMemoryBackend satisfies StorageBackend at compile time.
var _ StorageBackend = (*InMemoryBackend)(nil)
