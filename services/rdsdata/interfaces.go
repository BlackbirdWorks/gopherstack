package rdsdata

import "context"

// StorageBackend defines the interface for RDS Data backend implementations.
// All methods must be safe for concurrent use.
type StorageBackend interface {
	// Statement execution
	ExecuteStatement(
		ctx context.Context,
		resourceARN, sql, transactionID string,
	) ([][]Field, []ColumnMetadata, int64, error)
	BatchExecuteStatement(
		ctx context.Context,
		resourceARN, sql, transactionID string,
		parameterSets [][]SQLParameter,
	) ([]UpdateResult, error)
	ExecuteSQL(ctx context.Context, resourceARN, sqlStatements string) ([]SQLStatementResult, error)

	// Transaction management
	BeginTransaction(ctx context.Context, resourceARN string) (string, error)
	CommitTransaction(ctx context.Context, transactionID string) (string, error)
	RollbackTransaction(ctx context.Context, transactionID string) (string, error)

	// Introspection helpers (used by tests and dashboard)
	ListExecutedStatements(ctx context.Context) []ExecutedStatement
	ListTransactions(ctx context.Context) map[string]Transaction

	// Lifecycle
	Reset()
	Region() string
	AccountID() string
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// compile-time assertion that InMemoryBackend satisfies StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)
