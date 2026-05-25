package redshiftdata

import (
	"fmt"
	"sort"
	"strings"
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
	// resultFormatJSON is the default result format returned by GetStatementResult.
	resultFormatJSON = "JSON"
	// maxListStatementsResults is the maximum number of statements AWS allows per ListStatements page.
	maxListStatementsResults = 100
	// defaultListStatementsResults is the default page size for ListStatements when MaxResults is 0.
	defaultListStatementsResults = 100
	// mockColumnSize is the VARCHAR column size used in demo result metadata.
	mockColumnSize = int64(256)
	// mockColumnNullable indicates the demo column allows NULL.
	mockColumnNullable = int64(1)
	// mockStatementDurationMs is the simulated execution duration for demo statements.
	mockStatementDurationMs = int64(1)
	// demoResultRows is the simulated row count returned for FINISHED single statements.
	demoResultRows = int64(1)
	// demoResultSize is the simulated result payload size in bytes for FINISHED statements.
	demoResultSize = int64(64)
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

// SubStatementData represents a single sub-statement within a batch, matching
// the SubStatementData shape returned by AWS DescribeStatement for batch runs.
type SubStatementData struct {
	ID           string    `json:"id"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
	QueryString  string    `json:"queryString"`
	Status       string    `json:"status"`
	Error        string    `json:"error"`
	HasResultSet bool      `json:"hasResultSet"`
	ResultRows   int64     `json:"resultRows"`
	ResultSize   int64     `json:"resultSize"`
	DurationMs   int64     `json:"durationMs"`
}

// Statement represents an AWS Redshift Data API SQL statement.
type Statement struct {
	CreatedAt         time.Time          `json:"createdAt"`
	UpdatedAt         time.Time          `json:"updatedAt"`
	Database          string             `json:"database"`
	ID                string             `json:"id"`
	ClusterIdentifier string             `json:"clusterIdentifier"`
	WorkgroupName     string             `json:"workgroupName"`
	QueryString       string             `json:"queryString"`
	DBUser            string             `json:"dbUser"`
	SecretARN         string             `json:"secretARN"`
	StatementName     string             `json:"statementName"`
	ResultFormat      string             `json:"resultFormat"`
	Status            string             `json:"status"`
	Error             string             `json:"error"`
	QueryStrings      []string           `json:"queryStrings"`
	SubStatements     []SubStatementData `json:"subStatements,omitempty"`
	// DurationMs is the total wall-clock execution time in milliseconds. Populated
	// when the statement reaches a terminal state (FINISHED / FAILED / ABORTED).
	DurationMs       int64 `json:"durationMs"`
	ResultRows       int64 `json:"resultRows"`
	ResultSize       int64 `json:"resultSize"`
	HasResultSet     bool  `json:"hasResultSet"`
	IsBatchStatement bool  `json:"isBatchStatement"`
	// WithEvent indicates whether an EventBridge event is generated on completion.
	WithEvent bool `json:"withEvent"`
}

// InMemoryBackend is an in-memory store for Redshift Data API statements.
type InMemoryBackend struct {
	statements map[string]*Statement
	mu         *lockmetrics.RWMutex
	accountID  string
	region     string
	// ring buffer for ordered eviction – head points to the oldest slot.
	ringBuf  [maxStatementHistory]string
	ringLen  int // number of entries currently filled
	ringHead int // index of the oldest entry when ringLen == maxStatementHistory
}

// ListStatementsFilter controls statement filtering and pagination.
type ListStatementsFilter struct {
	ClusterIdentifier string
	WorkgroupName     string
	Database          string
	StatementName     string
	Status            string
	NextToken         string
	MaxResults        int
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

// Reset clears all stored statements and resets the ring buffer.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.statements = make(map[string]*Statement)
	b.ringLen = 0
	b.ringHead = 0
	for i := range b.ringBuf {
		b.ringBuf[i] = ""
	}
}

// addStatement inserts a statement and evicts the oldest via the ring buffer if
// the cap is exceeded. O(1) rather than the former O(n) slice shift.
// Caller must hold the write lock.
func (b *InMemoryBackend) addStatement(stmt *Statement) {
	b.statements[stmt.ID] = stmt

	if b.ringLen < maxStatementHistory {
		// Buffer not yet full: place entry at tail.
		tail := (b.ringHead + b.ringLen) % maxStatementHistory
		b.ringBuf[tail] = stmt.ID
		b.ringLen++

		return
	}

	// Buffer full: evict the oldest entry (at ringHead) before writing.
	delete(b.statements, b.ringBuf[b.ringHead])
	b.ringBuf[b.ringHead] = stmt.ID
	b.ringHead = (b.ringHead + 1) % maxStatementHistory
}

// ExecuteStatement creates and immediately completes a SQL statement.
func (b *InMemoryBackend) ExecuteStatement(
	sql, clusterIdentifier, workgroupName, database, dbUser, secretARN, statementName string,
	withEvent bool, resultFormat string,
) (*Statement, error) {
	if sql == "" {
		return nil, fmt.Errorf("%w: Sql is required", ErrValidation)
	}

	if database == "" {
		return nil, fmt.Errorf("%w: Database is required", ErrValidation)
	}

	resultFormat, err := requestedResultFormat(resultFormat)
	if err != nil {
		return nil, err
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
		ResultFormat:      resultFormat,
		Status:            statusFinished,
		HasResultSet:      true,
		IsBatchStatement:  false,
		WithEvent:         withEvent,
		CreatedAt:         now,
		UpdatedAt:         now,
		// Simulated instant execution: 1 ms so the UI always displays a
		// human-readable duration rather than showing "0ms" which could be
		// mistaken for a failed or uninitialized measurement.
		DurationMs: mockStatementDurationMs,
		ResultRows: demoResultRows,
		ResultSize: demoResultSize,
	}
	b.addStatement(stmt)

	return cloneStatement(stmt), nil
}

// BatchExecuteStatement creates and immediately completes a batch SQL statement.
func (b *InMemoryBackend) BatchExecuteStatement(
	sqls []string, clusterIdentifier, workgroupName, database, dbUser, secretARN, statementName string,
	withEvent bool, resultFormat string,
) (*Statement, error) {
	if len(sqls) == 0 {
		return nil, fmt.Errorf("%w: Sqls is required", ErrValidation)
	}

	if database == "" {
		return nil, fmt.Errorf("%w: Database is required", ErrValidation)
	}

	resultFormat, err := requestedResultFormat(resultFormat)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("BatchExecuteStatement")
	defer b.mu.Unlock()

	now := time.Now()

	// Build sub-statement data for each SQL in the batch.
	subs := make([]SubStatementData, len(sqls))
	for i, sql := range sqls {
		subs[i] = SubStatementData{
			ID:           uuid.NewString(),
			CreatedAt:    now,
			UpdatedAt:    now,
			QueryString:  sql,
			Status:       statusFinished,
			HasResultSet: false,
			DurationMs:   1,
		}
	}

	stmt := &Statement{
		ID:                uuid.NewString(),
		QueryString:       sqls[0], // AWS sets QueryString to the first SQL in the batch.
		QueryStrings:      append([]string(nil), sqls...),
		SubStatements:     subs,
		ClusterIdentifier: clusterIdentifier,
		WorkgroupName:     workgroupName,
		Database:          database,
		DBUser:            dbUser,
		SecretARN:         secretARN,
		StatementName:     statementName,
		ResultFormat:      resultFormat,
		Status:            statusFinished,
		HasResultSet:      false,
		IsBatchStatement:  true,
		WithEvent:         withEvent,
		CreatedAt:         now,
		UpdatedAt:         now,
		DurationMs:        1,
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

	now := time.Now()
	stmt.Status = statusAborted
	stmt.UpdatedAt = now
	stmt.DurationMs = now.Sub(stmt.CreatedAt).Milliseconds()

	return nil
}

// ListStatements returns statements sorted by creation time (newest first).
// An omitted Status matches AWS by returning only finished statements.
// Returns the page slice and a next-token string (non-empty when more pages exist).
func (b *InMemoryBackend) ListStatements(filter ListStatementsFilter) ([]*Statement, string, error) {
	b.mu.RLock("ListStatements")
	defer b.mu.RUnlock()

	result := make([]*Statement, 0, len(b.statements))

	for _, stmt := range b.statements {
		if filter.ClusterIdentifier != "" && stmt.ClusterIdentifier != filter.ClusterIdentifier {
			continue
		}

		if filter.WorkgroupName != "" && stmt.WorkgroupName != filter.WorkgroupName {
			continue
		}

		if filter.Database != "" && stmt.Database != filter.Database {
			continue
		}

		if filter.StatementName != "" && !strings.HasPrefix(stmt.StatementName, filter.StatementName) {
			continue
		}

		if !matchesStatementStatus(stmt.Status, filter.Status) {
			continue
		}

		result = append(result, cloneStatement(stmt))
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].ID > result[j].ID
		}

		return result[i].CreatedAt.After(result[j].CreatedAt)
	})

	start, err := statementPageStart(result, filter.NextToken)
	if err != nil {
		return nil, "", err
	}

	result = result[start:]

	limit := filter.MaxResults
	if limit <= 0 {
		limit = defaultListStatementsResults
	}

	if len(result) <= limit {
		return result, "", nil
	}

	// Return the first page and a synthetic next-token (the ID of the first item
	// on the next page), matching the real AWS behaviour.
	return result[:limit], result[limit].ID, nil
}

func requestedResultFormat(format string) (string, error) {
	if format == "" {
		return resultFormatJSON, nil
	}

	switch format {
	case resultFormatJSON, resultFormatCSV:
		return format, nil
	default:
		return "", fmt.Errorf("%w: ResultFormat must be JSON or CSV", ErrValidation)
	}
}

func statementResultFormat(stmt *Statement) string {
	if stmt.ResultFormat == "" {
		return resultFormatJSON
	}

	return stmt.ResultFormat
}

func matchesStatementStatus(actual, requested string) bool {
	if requested == "" {
		return actual == statusFinished
	}

	return requested == "ALL" || actual == requested
}

func statementPageStart(statements []*Statement, nextToken string) (int, error) {
	if nextToken == "" {
		return 0, nil
	}

	for i, stmt := range statements {
		if stmt.ID == nextToken {
			return i, nil
		}
	}

	return 0, fmt.Errorf("%w: invalid NextToken", ErrValidation)
}

// EvictExpiredStatements removes terminal statements whose UpdatedAt is older
// than the given cutoff. It returns the number of evicted statements.
// Only terminal states (FINISHED, FAILED, ABORTED) are eligible for eviction;
// in-flight statements are never removed.
func (b *InMemoryBackend) EvictExpiredStatements(cutoff time.Time) int {
	b.mu.Lock("EvictExpiredStatements")
	defer b.mu.Unlock()

	var toDelete []string

	for id, stmt := range b.statements {
		terminal := stmt.Status == statusFinished ||
			stmt.Status == statusFailed ||
			stmt.Status == statusAborted
		if terminal && stmt.UpdatedAt.Before(cutoff) {
			toDelete = append(toDelete, id)
		}
	}

	for _, id := range toDelete {
		delete(b.statements, id)
	}

	// Compact the ring buffer to remove evicted IDs.
	if len(toDelete) > 0 {
		b.compactRingBuffer()
	}

	return len(toDelete)
}

// compactRingBuffer rebuilds the ring buffer from the current statements map,
// preserving insertion order. Must be called with the write lock held.
func (b *InMemoryBackend) compactRingBuffer() {
	kept := make([]string, 0, b.ringLen)

	for i := range b.ringLen {
		id := b.ringBuf[(b.ringHead+i)%maxStatementHistory]
		if _, ok := b.statements[id]; ok {
			kept = append(kept, id)
		}
	}

	b.ringHead = 0
	b.ringLen = len(kept)

	copy(b.ringBuf[:], kept)

	// Zero out unused slots.
	for i := b.ringLen; i < maxStatementHistory; i++ {
		b.ringBuf[i] = ""
	}
}

// cloneStatement returns a deep copy of stmt.
func cloneStatement(stmt *Statement) *Statement {
	cp := *stmt

	if stmt.QueryStrings != nil {
		cp.QueryStrings = append([]string(nil), stmt.QueryStrings...)
	}

	if stmt.SubStatements != nil {
		cp.SubStatements = append([]SubStatementData(nil), stmt.SubStatements...)
	}

	return &cp
}
