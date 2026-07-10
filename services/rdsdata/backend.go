package rdsdata

import (
	"context"
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

const (
	// transactionStatusActive is the active state for a transaction.
	transactionStatusActive = "ACTIVE"
	// transactionStatusCommitted is the status returned on successful commit.
	transactionStatusCommitted = "Transaction committed"
	// transactionStatusRolledBack is the status returned on successful rollback.
	transactionStatusRolledBack = "Transaction rolled back"
	// maxExecutedStatements is the maximum number of executed statements to retain per region.
	maxExecutedStatements = 1000
)

var (
	// ErrTransactionNotFound is returned when a transaction does not exist.
	ErrTransactionNotFound = awserr.New("TransactionNotFoundException", awserr.ErrNotFound)
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("BadRequestException", awserr.ErrInvalidParameter)
)

// Field represents a single field value in an RDS Data API record.
type Field struct {
	IsNull       *bool    `json:"isNull,omitempty"`
	BooleanValue *bool    `json:"booleanValue,omitempty"`
	LongValue    *int64   `json:"longValue,omitempty"`
	DoubleValue  *float64 `json:"doubleValue,omitempty"`
	StringValue  *string  `json:"stringValue,omitempty"`
	BlobValue    []byte   `json:"blobValue,omitempty"`
}

// ColumnMetadata describes a single column returned by a SQL statement.
type ColumnMetadata struct {
	Name     string `json:"name"`
	TypeName string `json:"typeName"`
}

// Transaction represents an in-progress database transaction.
type Transaction struct {
	TransactionID string `json:"transactionId"`
	Status        string `json:"status"`
}

// ExecutedStatement represents a record of an executed SQL statement.
type ExecutedStatement struct {
	SQL           string `json:"sql"`
	ResourceARN   string `json:"resourceArn"`
	TransactionID string `json:"transactionId,omitempty"`
}

// SQLParameter represents a named parameter for a SQL statement.
type SQLParameter struct {
	Name  string `json:"name"`
	Value Field  `json:"value"`
}

// UpdateResult represents the result of a single update in a batch.
type UpdateResult struct {
	GeneratedFields []Field `json:"generatedFields"`
}

// SQLStatementResult represents the result of a single SQL statement in an ExecuteSql call.
type SQLStatementResult struct {
	NumberOfRecordsUpdated int64 `json:"numberOfRecordsUpdated"`
}

// InMemoryBackend is an in-memory RDS Data backend.
//
// All resource maps are nested by region (outer key = region) so that
// same-named resources are isolated across regions. The per-region inner maps
// / tables are created lazily via the *Store helpers. Callers must hold b.mu
// while accessing them. See store_setup.go for why transactions is a
// map[string]*store.Table[Transaction] while executedStatements and
// txCounter remain plain maps.
type InMemoryBackend struct {
	transactions       map[string]*store.Table[Transaction]
	executedStatements map[string][]ExecutedStatement
	txCounter          map[string]int
	engine             *sqlEngine
	mu                 *lockmetrics.RWMutex
	accountID          string
	defaultRegion      string
}

// NewInMemoryBackend creates a new in-memory RDS Data backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		transactions:       make(map[string]*store.Table[Transaction]),
		executedStatements: make(map[string][]ExecutedStatement),
		txCounter:          make(map[string]int),
		engine:             newSQLEngine(),
		mu:                 lockmetrics.New("rdsdata"),
		accountID:          accountID,
		defaultRegion:      region,
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.defaultRegion }

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// The *Store helpers return the per-region inner map, lazily creating it.
// Callers must hold b.mu.

func (b *InMemoryBackend) transactionsStore(region string) *store.Table[Transaction] {
	if b.transactions[region] == nil {
		b.transactions[region] = store.New(transactionKeyFn)
	}

	return b.transactions[region]
}

// transactionRegion returns the per-region transaction [store.Table] for
// region without creating it, or nil if region has no transactions yet. Safe
// to call under either b.mu.RLock or b.mu.Lock -- used by read-only paths
// (ListTransactions) so a lazy allocation never races under a shared RLock.
func (b *InMemoryBackend) transactionRegion(region string) *store.Table[Transaction] {
	return b.transactions[region]
}

func (b *InMemoryBackend) statementsStore(region string) []ExecutedStatement {
	if b.executedStatements[region] == nil {
		b.executedStatements[region] = []ExecutedStatement{}
	}

	return b.executedStatements[region]
}

// Reset clears all backend state. Useful for test isolation.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.transactions = make(map[string]*store.Table[Transaction])
	b.executedStatements = make(map[string][]ExecutedStatement)
	b.txCounter = make(map[string]int)
	b.engine.reset()
}

// appendStatementLocked records an executed statement and trims the buffer to
// maxExecutedStatements. The caller must hold b.mu (write lock).
func (b *InMemoryBackend) appendStatementLocked(region, resourceARN, sql, transactionID string) {
	stmts := b.statementsStore(region)
	stmts = append(stmts, ExecutedStatement{
		SQL:           sql,
		ResourceARN:   resourceARN,
		TransactionID: transactionID,
	})

	if len(stmts) > maxExecutedStatements {
		trimmed := make([]ExecutedStatement, maxExecutedStatements)
		copy(trimmed, stmts[len(stmts)-maxExecutedStatements:])
		stmts = trimmed
	}

	b.executedStatements[region] = stmts
}

// ExecuteStatement executes a SQL statement and returns its result set. Named
// parameters (e.g. ":id") are bound when supplied.
func (b *InMemoryBackend) ExecuteStatement(
	ctx context.Context,
	resourceARN, sql, transactionID string,
	parameters ...SQLParameter,
) ([][]Field, []ColumnMetadata, int64, error) {
	b.mu.Lock("ExecuteStatement")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	if transactionID != "" {
		if !b.transactionsStore(region).Has(transactionID) {
			return nil, nil, 0, fmt.Errorf(
				"%w: transaction %s not found",
				ErrTransactionNotFound,
				transactionID,
			)
		}
	}

	b.appendStatementLocked(region, resourceARN, sql, transactionID)

	// Execute against the real in-memory SQL engine. A genuine result set is
	// returned for well-formed statements; anything the engine rejects (for
	// example DML against a table the caller never created) degrades to the
	// historical empty-success envelope rather than surfacing an error.
	records, columns, updated, err := b.engine.execute(ctx, region, resourceARN, sql, transactionID, parameters)
	if err != nil {
		return [][]Field{}, []ColumnMetadata{}, 0, nil
	}

	return records, columns, updated, nil
}

// BatchExecuteStatement executes a batch of SQL statements and returns results for each.
func (b *InMemoryBackend) BatchExecuteStatement(
	ctx context.Context,
	resourceARN, sql, transactionID string,
	parameterSets [][]SQLParameter,
) ([]UpdateResult, error) {
	b.mu.Lock("BatchExecuteStatement")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	if transactionID != "" {
		if !b.transactionsStore(region).Has(transactionID) {
			return nil, fmt.Errorf(
				"%w: transaction %s not found",
				ErrTransactionNotFound,
				transactionID,
			)
		}
	}

	b.appendStatementLocked(region, resourceARN, sql, transactionID)

	if len(parameterSets) == 0 {
		// A parameterless batch still executes the statement once so DDL such
		// as CREATE TABLE takes effect; the engine error is ignored to keep the
		// historical lenient behaviour.
		_, _, _, _ = b.engine.execute(ctx, region, resourceARN, sql, transactionID, nil)

		return []UpdateResult{}, nil
	}

	results := make([]UpdateResult, len(parameterSets))

	for i, params := range parameterSets {
		// Run each parameter set so inserts/updates actually land in the engine;
		// generatedFields stays empty, matching the current response model.
		_, _, _, _ = b.engine.execute(ctx, region, resourceARN, sql, transactionID, params)
		results[i] = UpdateResult{GeneratedFields: []Field{}}
	}

	return results, nil
}

// BeginTransaction starts a new transaction and returns its ID.
func (b *InMemoryBackend) BeginTransaction(ctx context.Context, resourceARN string) (string, error) {
	b.mu.Lock("BeginTransaction")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)

	b.txCounter[region]++
	id := fmt.Sprintf("txn-%06d", b.txCounter[region])

	b.transactionsStore(region).Put(&Transaction{
		TransactionID: id,
		Status:        transactionStatusActive,
	})

	// Open a matching engine-side transaction so statements tagged with this ID
	// share atomic visibility. A failure here is non-fatal: such statements
	// fall back to autocommit execution.
	_ = b.engine.beginTx(ctx, region, resourceARN, id)

	return id, nil
}

// CommitTransaction commits a transaction by ID.
func (b *InMemoryBackend) CommitTransaction(
	ctx context.Context,
	transactionID string,
) (string, error) {
	b.mu.Lock("CommitTransaction")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)
	tbl := b.transactionsStore(region)

	if !tbl.Has(transactionID) {
		return "", fmt.Errorf("%w: transaction %s not found", ErrTransactionNotFound, transactionID)
	}

	tbl.Delete(transactionID)
	b.engine.finalizeTx(transactionID, true)

	return transactionStatusCommitted, nil
}

// RollbackTransaction rolls back a transaction by ID.
func (b *InMemoryBackend) RollbackTransaction(
	ctx context.Context,
	transactionID string,
) (string, error) {
	b.mu.Lock("RollbackTransaction")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)
	tbl := b.transactionsStore(region)

	if !tbl.Has(transactionID) {
		return "", fmt.Errorf("%w: transaction %s not found", ErrTransactionNotFound, transactionID)
	}

	tbl.Delete(transactionID)
	b.engine.finalizeTx(transactionID, false)

	return transactionStatusRolledBack, nil
}

// ExecuteSQL executes one or more SQL statements against the cluster.
// This is a deprecated operation; use ExecuteStatement or BatchExecuteStatement instead.
func (b *InMemoryBackend) ExecuteSQL(
	ctx context.Context,
	resourceARN, sqlStatements string,
) ([]SQLStatementResult, error) {
	b.mu.Lock("ExecuteSql")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)
	b.appendStatementLocked(region, resourceARN, sqlStatements, "")

	// Execute for real so the deprecated entry point still mutates state; the
	// reported update count reflects the engine result when available.
	_, _, updated, err := b.engine.execute(ctx, region, resourceARN, sqlStatements, "", nil)
	if err != nil {
		return []SQLStatementResult{{NumberOfRecordsUpdated: 0}}, nil
	}

	return []SQLStatementResult{{NumberOfRecordsUpdated: updated}}, nil
}

// ListExecutedStatements returns a copy of all executed statements for the request's region.
func (b *InMemoryBackend) ListExecutedStatements(ctx context.Context) []ExecutedStatement {
	b.mu.RLock("ListExecutedStatements")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	stmts := b.executedStatements[region]
	result := make([]ExecutedStatement, len(stmts))
	copy(result, stmts)

	return result
}

// ListTransactions returns a deep copy of all active transactions for the request's region.
func (b *InMemoryBackend) ListTransactions(ctx context.Context) map[string]Transaction {
	b.mu.RLock("ListTransactions")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.defaultRegion)
	tbl := b.transactionRegion(region)

	if tbl == nil {
		return map[string]Transaction{}
	}

	items := tbl.All()
	result := make(map[string]Transaction, len(items))

	for _, v := range items {
		result[v.TransactionID] = *v
	}

	return result
}

// AddTransactionInternal directly inserts a transaction into the backend's default region.
// This is intended only for seeding test data.
func (b *InMemoryBackend) AddTransactionInternal(txID string) {
	b.mu.Lock("AddTransactionInternal")
	defer b.mu.Unlock()

	b.transactionsStore(b.defaultRegion).Put(&Transaction{
		TransactionID: txID,
		Status:        transactionStatusActive,
	})
}

// errIsValidation reports whether err wraps ErrValidation.
func errIsValidation(err error) bool {
	return errors.Is(err, awserr.ErrInvalidParameter)
}
