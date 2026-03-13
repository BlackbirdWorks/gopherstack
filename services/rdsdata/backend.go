package rdsdata

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	// transactionStatusActive is the active state for a transaction.
	transactionStatusActive = "ACTIVE"
)

var (
	// ErrTransactionNotFound is returned when a transaction does not exist.
	ErrTransactionNotFound = awserr.New("TransactionNotFoundException", awserr.ErrNotFound)
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
	TransactionID string
	Status        string
}

// ExecutedStatement represents a record of an executed SQL statement.
type ExecutedStatement struct {
	SQL           string
	ResourceARN   string
	TransactionID string
}

// InMemoryBackend is an in-memory RDS Data backend.
type InMemoryBackend struct {
	transactions       map[string]*Transaction
	mu                 *lockmetrics.RWMutex
	accountID          string
	region             string
	executedStatements []ExecutedStatement
	txCounter          int
}

// NewInMemoryBackend creates a new in-memory RDS Data backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		transactions: make(map[string]*Transaction),
		mu:           lockmetrics.New("rdsdata"),
		accountID:    accountID,
		region:       region,
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// ExecuteStatement executes a SQL statement and returns an empty result set.
func (b *InMemoryBackend) ExecuteStatement(
	resourceARN, sql, transactionID string,
) ([][]Field, []ColumnMetadata, int64, error) {
	b.mu.Lock("ExecuteStatement")
	defer b.mu.Unlock()

	if transactionID != "" {
		if _, ok := b.transactions[transactionID]; !ok {
			return nil, nil, 0, fmt.Errorf("%w: transaction %s not found", ErrTransactionNotFound, transactionID)
		}
	}

	b.executedStatements = append(b.executedStatements, ExecutedStatement{
		SQL:           sql,
		ResourceARN:   resourceARN,
		TransactionID: transactionID,
	})

	return [][]Field{}, []ColumnMetadata{}, 0, nil
}

// BatchExecuteStatement executes a batch of SQL statements and returns results for each.
func (b *InMemoryBackend) BatchExecuteStatement(
	resourceARN, sql, transactionID string,
	parameterSets [][]SQLParameter,
) ([]UpdateResult, error) {
	b.mu.Lock("BatchExecuteStatement")
	defer b.mu.Unlock()

	if transactionID != "" {
		if _, ok := b.transactions[transactionID]; !ok {
			return nil, fmt.Errorf("%w: transaction %s not found", ErrTransactionNotFound, transactionID)
		}
	}

	b.executedStatements = append(b.executedStatements, ExecutedStatement{
		SQL:           sql,
		ResourceARN:   resourceARN,
		TransactionID: transactionID,
	})

	results := make([]UpdateResult, len(parameterSets))
	for i := range results {
		results[i] = UpdateResult{GeneratedFields: []Field{}}
	}

	if len(parameterSets) == 0 {
		return []UpdateResult{}, nil
	}

	return results, nil
}

// BeginTransaction starts a new transaction and returns its ID.
func (b *InMemoryBackend) BeginTransaction(_ string) (string, error) {
	b.mu.Lock("BeginTransaction")
	defer b.mu.Unlock()

	b.txCounter++
	id := fmt.Sprintf("txn-%06d", b.txCounter)

	b.transactions[id] = &Transaction{
		TransactionID: id,
		Status:        transactionStatusActive,
	}

	return id, nil
}

// CommitTransaction commits a transaction by ID.
func (b *InMemoryBackend) CommitTransaction(transactionID string) (string, error) {
	b.mu.Lock("CommitTransaction")
	defer b.mu.Unlock()

	if _, ok := b.transactions[transactionID]; !ok {
		return "", fmt.Errorf("%w: transaction %s not found", ErrTransactionNotFound, transactionID)
	}

	delete(b.transactions, transactionID)

	return "Transaction Committed", nil
}

// RollbackTransaction rolls back a transaction by ID.
func (b *InMemoryBackend) RollbackTransaction(transactionID string) (string, error) {
	b.mu.Lock("RollbackTransaction")
	defer b.mu.Unlock()

	if _, ok := b.transactions[transactionID]; !ok {
		return "", fmt.Errorf("%w: transaction %s not found", ErrTransactionNotFound, transactionID)
	}

	delete(b.transactions, transactionID)

	return "Transaction Rolled Back", nil
}

// ListExecutedStatements returns a copy of all executed statements.
func (b *InMemoryBackend) ListExecutedStatements() []ExecutedStatement {
	b.mu.RLock("ListExecutedStatements")
	defer b.mu.RUnlock()

	result := make([]ExecutedStatement, len(b.executedStatements))
	copy(result, b.executedStatements)

	return result
}

// ListTransactions returns a deep copy of all active transactions.
func (b *InMemoryBackend) ListTransactions() map[string]Transaction {
	b.mu.RLock("ListTransactions")
	defer b.mu.RUnlock()

	result := make(map[string]Transaction, len(b.transactions))
	for k, v := range b.transactions {
		result[k] = *v
	}

	return result
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
