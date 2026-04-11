package redshiftdata

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	// statusFinished is the FINISHED status for a SQL statement.
	statusFinished = "FINISHED"
	// statusFailed is the FAILED status for a SQL statement.
	statusFailed = "FAILED"
	// statusAborted is the ABORTED status for a SQL statement (cancelled).
	statusAborted = "ABORTED"
	// maxStatementHistory is the maximum number of statements to retain in memory.
	maxStatementHistory = 1000
	// resultFormatCSV is the CSV result format returned by GetStatementResultV2.
	resultFormatCSV = "CSV"
)

var (
	// ErrNotFound is returned when a statement does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrTerminalState is returned when cancelling a statement that is already in a terminal state.
	ErrTerminalState = awserr.New("ValidationException", awserr.ErrConflict)
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrNoResultSet is returned when fetching results for a statement with no result set.
	ErrNoResultSet = awserr.New("ValidationException", awserr.ErrInvalidParameter)
)

// Statement represents an AWS Redshift Data API SQL statement.
type Statement struct {
	CreatedAt         time.Time `json:"createdAt"`
	UpdatedAt         time.Time `json:"updatedAt"`
	Database          string    `json:"database"`
	ID                string    `json:"id"`
	ClusterIdentifier string    `json:"clusterIdentifier"`
	WorkgroupName     string    `json:"workgroupName"`
	QueryString       string    `json:"queryString"`
	DBUser            string    `json:"dbUser"`
	SecretARN         string    `json:"secretARN"`
	StatementName     string    `json:"statementName"`
	Status            string    `json:"status"`
	Error             string    `json:"error"`
	QueryStrings      []string  `json:"queryStrings"`
	HasResultSet      bool      `json:"hasResultSet"`
	IsBatchStatement  bool      `json:"isBatchStatement"`
}

// InMemoryBackend is an in-memory store for Redshift Data API statements.
type InMemoryBackend struct {
	statements    map[string]*Statement
	mu            *lockmetrics.RWMutex
	accountID     string
	region        string
	statementKeys []string
}

// NewInMemoryBackend creates a new in-memory Redshift Data backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		statements: make(map[string]*Statement),
		accountID:  accountID,
		region:     region,
		mu:         lockmetrics.New("redshiftdata"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Reset clears all stored statements and resets the eviction queue.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.statements = make(map[string]*Statement)
	b.statementKeys = nil
}

// addStatement inserts a statement and evicts the oldest if the cap is exceeded.
// Caller must hold the write lock.
func (b *InMemoryBackend) addStatement(stmt *Statement) {
	b.statements[stmt.ID] = stmt
	b.statementKeys = append(b.statementKeys, stmt.ID)

	if len(b.statementKeys) > maxStatementHistory {
		oldest := b.statementKeys[0]
		b.statementKeys = b.statementKeys[1:]
		delete(b.statements, oldest)
	}
}

// ExecuteStatement creates and immediately completes a SQL statement.
func (b *InMemoryBackend) ExecuteStatement(
	sql, clusterIdentifier, workgroupName, database, dbUser, secretARN, statementName string,
) (*Statement, error) {
	if sql == "" {
		return nil, fmt.Errorf("%w: Sql is required", ErrValidation)
	}

	if database == "" {
		return nil, fmt.Errorf("%w: Database is required", ErrValidation)
	}

	b.mu.Lock("ExecuteStatement")
	defer b.mu.Unlock()

	now := time.Now()
	stmt := &Statement{
		ID:                uuid.NewString(),
		QueryString:       sql,
		ClusterIdentifier: clusterIdentifier,
		WorkgroupName:     workgroupName,
		Database:          database,
		DBUser:            dbUser,
		SecretARN:         secretARN,
		StatementName:     statementName,
		Status:            statusFinished,
		HasResultSet:      true,
		IsBatchStatement:  false,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	b.addStatement(stmt)

	return cloneStatement(stmt), nil
}

// BatchExecuteStatement creates and immediately completes a batch SQL statement.
func (b *InMemoryBackend) BatchExecuteStatement(
	sqls []string, clusterIdentifier, workgroupName, database, dbUser, secretARN, statementName string,
) (*Statement, error) {
	if len(sqls) == 0 {
		return nil, fmt.Errorf("%w: Sqls is required", ErrValidation)
	}

	if database == "" {
		return nil, fmt.Errorf("%w: Database is required", ErrValidation)
	}

	b.mu.Lock("BatchExecuteStatement")
	defer b.mu.Unlock()

	now := time.Now()
	stmt := &Statement{
		ID:                uuid.NewString(),
		QueryStrings:      append([]string(nil), sqls...),
		ClusterIdentifier: clusterIdentifier,
		WorkgroupName:     workgroupName,
		Database:          database,
		DBUser:            dbUser,
		SecretARN:         secretARN,
		StatementName:     statementName,
		Status:            statusFinished,
		HasResultSet:      false,
		IsBatchStatement:  true,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	b.addStatement(stmt)

	return cloneStatement(stmt), nil
}

// DescribeStatement returns the details of a statement by ID.
func (b *InMemoryBackend) DescribeStatement(id string) (*Statement, error) {
	b.mu.RLock("DescribeStatement")
	defer b.mu.RUnlock()

	stmt, ok := b.statements[id]
	if !ok {
		return nil, fmt.Errorf("%w: statement %s not found", ErrNotFound, id)
	}

	return cloneStatement(stmt), nil
}

// CancelStatement marks a statement as aborted.
func (b *InMemoryBackend) CancelStatement(id string) error {
	b.mu.Lock("CancelStatement")
	defer b.mu.Unlock()

	stmt, ok := b.statements[id]
	if !ok {
		return fmt.Errorf("%w: statement %s not found", ErrNotFound, id)
	}

	if stmt.Status == statusFinished || stmt.Status == statusFailed || stmt.Status == statusAborted {
		return fmt.Errorf("%w: statement %s is already in terminal state %s", ErrTerminalState, id, stmt.Status)
	}

	stmt.Status = statusAborted
	stmt.UpdatedAt = time.Now()

	return nil
}

// ListStatements returns all statements sorted by creation time (newest first),
// optionally filtered by cluster or workgroup.
func (b *InMemoryBackend) ListStatements(clusterIdentifier, workgroupName string) []*Statement {
	b.mu.RLock("ListStatements")
	defer b.mu.RUnlock()

	result := make([]*Statement, 0, len(b.statements))

	for _, stmt := range b.statements {
		if clusterIdentifier != "" && stmt.ClusterIdentifier != clusterIdentifier {
			continue
		}

		if workgroupName != "" && stmt.WorkgroupName != workgroupName {
			continue
		}

		result = append(result, cloneStatement(stmt))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt.After(result[j].CreatedAt)
	})

	return result
}

// cloneStatement returns a deep copy of stmt.
func cloneStatement(stmt *Statement) *Statement {
	cp := *stmt

	if stmt.QueryStrings != nil {
		cp.QueryStrings = append([]string(nil), stmt.QueryStrings...)
	}

	return &cp
}
