package rdsdata

import (
	"context"

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

// resultSetOptionsContextKey is the context key under which a request's
// ExecuteStatementInput.ResultSetOptions is stashed, mirroring
// regionContextKey above. It is request-scoped, optional configuration read
// only by the engine's result-shaping path (see shapeField in engine.go), so
// threading it through context avoids adding a rarely-used parameter to the
// StorageBackend.ExecuteStatement signature that nearly every call site would
// have to pass a zero value for.
type resultSetOptionsContextKey struct{}

// resultSetOptions mirrors types.ResultSetOptions (aws-sdk-go-v2/service/rdsdata).
// Zero value ("") for either field means "use the real API's default",
// applied in shapeField.
type resultSetOptions struct {
	DecimalReturnType string
	LongReturnType    string
}

// getResultSetOptions extracts resultSetOptions from ctx, defaulting to the
// zero value (real API defaults: DecimalReturnType=STRING, LongReturnType=LONG)
// when the request didn't set one.
func getResultSetOptions(ctx context.Context) resultSetOptions {
	if o, ok := ctx.Value(resultSetOptionsContextKey{}).(resultSetOptions); ok {
		return o
	}

	return resultSetOptions{}
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
