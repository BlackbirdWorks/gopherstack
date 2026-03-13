package redshiftdata

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	// statusFinished is the finished status for a SQL statement.
	statusFinished = "FINISHED"
	// statusFailed is the failed status for a SQL statement.
	statusFailed = "FAILED"
	// statusAborted is the aborted status for a SQL statement (cancelled).
	statusAborted = "ABORTED"
)

var (
	// ErrNotFound is returned when a statement does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyAborted is returned when cancelling an already-finished statement.
	ErrAlreadyAborted = awserr.New("ValidationException", awserr.ErrConflict)
)

// Statement represents an AWS Redshift Data API SQL statement.
type Statement struct {
	CreatedAt         time.Time
	UpdatedAt         time.Time
	Database          string
	ID                string
	ClusterIdentifier string
	WorkgroupName     string
	QueryString       string
	DBUser            string
	SecretARN         string
	StatementName     string
	Status            string
	Error             string
	QueryStrings      []string
	HasResultSet      bool
	IsBatchStatement  bool
}

// InMemoryBackend is an in-memory store for Redshift Data API statements.
type InMemoryBackend struct {
	statements map[string]*Statement
	mu         *lockmetrics.RWMutex
	accountID  string
	region     string
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

// ExecuteStatement creates and immediately completes a SQL statement.
func (b *InMemoryBackend) ExecuteStatement(
	sql, clusterIdentifier, workgroupName, database, dbUser, secretARN, statementName string,
) (*Statement, error) {
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
	b.statements[stmt.ID] = stmt

	return cloneStatement(stmt), nil
}

// BatchExecuteStatement creates and immediately completes a batch SQL statement.
func (b *InMemoryBackend) BatchExecuteStatement(
	sqls []string, clusterIdentifier, workgroupName, database, dbUser, secretARN, statementName string,
) (*Statement, error) {
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
	b.statements[stmt.ID] = stmt

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

	if stmt.Status == statusFinished || stmt.Status == statusFailed {
		return fmt.Errorf("%w: statement %s is already in terminal state %s", ErrAlreadyAborted, id, stmt.Status)
	}

	stmt.Status = statusAborted
	stmt.UpdatedAt = time.Now()

	return nil
}

// ListStatements returns all statements, optionally filtered by cluster or workgroup.
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
