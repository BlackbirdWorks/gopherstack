package redshiftdata

import (
	"context"
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
	// maxStatementHistory is the maximum number of statements to retain in memory per region.
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
	// statusAll matches all statement statuses in ListStatements.
	statusAll = "ALL"

	// maxListDatabasesResults is the maximum page size for ListDatabases.
	maxListDatabasesResults = 60
	// defaultListDatabasesResults is the default page size for ListDatabases.
	defaultListDatabasesResults = 60
	// maxListSchemasResults is the maximum page size for ListSchemas.
	maxListSchemasResults = 1000
	// defaultListSchemasResults is the default page size for ListSchemas.
	defaultListSchemasResults = 1000
	// maxListTablesResults is the maximum page size for ListTables.
	maxListTablesResults = 1000
	// defaultListTablesResults is the default page size for ListTables.
	defaultListTablesResults = 1000
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

// ValidateListStatementsStatus returns ErrValidation if status is not a known value.
// An empty string is also accepted (matches FINISHED per AWS default).
func ValidateListStatementsStatus(status string) error {
	if status == "" {
		return nil
	}

	switch status {
	case statusAll, statusAborted, statusFailed, statusFinished, "PICKED", "STARTED", "SUBMITTED":
		return nil
	default:
		return fmt.Errorf(
			"%w: Status %q is invalid; valid values are ALL, ABORTED, FAILED, FINISHED, PICKED, STARTED, SUBMITTED",
			ErrValidation, status,
		)
	}
}

// ValidateConnectionTarget verifies that exactly one of clusterIdentifier or
// workgroupName is provided, matching the AWS constraint.
func ValidateConnectionTarget(clusterIdentifier, workgroupName string) error {
	hasBoth := clusterIdentifier != "" && workgroupName != ""
	hasNeither := clusterIdentifier == "" && workgroupName == ""

	if hasBoth {
		return fmt.Errorf(
			"%w: specify either ClusterIdentifier or WorkgroupName, not both",
			ErrValidation,
		)
	}

	if hasNeither {
		return fmt.Errorf(
			"%w: either ClusterIdentifier or WorkgroupName is required",
			ErrValidation,
		)
	}

	return nil
}

// regionContextKey is the context key under which the per-request AWS region is stored.
type regionContextKey struct{}

// getRegion extracts the region from ctx, falling back to defaultRegion when unset.
func getRegion(ctx context.Context, defaultRegion string) string {
	if r, ok := ctx.Value(regionContextKey{}).(string); ok && r != "" {
		return r
	}

	return defaultRegion
}

// SQLParameter is a named SQL parameter for use in parameterized queries, matching
// the SQLParameter type in the AWS Redshift Data API.
type SQLParameter struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// sqlHasResultSet reports whether a SQL statement produces a result set.
// Real AWS sets HasResultSet=true only for read-only statements (SELECT, SHOW,
// EXPLAIN, DESCRIBE, WITH, VALUES, TABLE). DML and DDL return false.
func sqlHasResultSet(sql string) bool {
	fields := strings.Fields(strings.ToUpper(sql))
	if len(fields) == 0 {
		return false
	}

	switch fields[0] {
	case "SELECT", "SHOW", "EXPLAIN", "DESCRIBE", "WITH", "VALUES", "TABLE":
		return true
	}

	return false
}

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
	Parameters        []SQLParameter     `json:"parameters,omitempty"`
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

// regionStore holds per-region statement storage and its ring buffer.
//
// Phase 3.3 datalayer audit: statements stays a raw map[string]*Statement
// rather than a [github.com/blackbirdworks/gopherstack/pkgs/store.Table] for
// two independent reasons. (1) ringBuf/ringLen/ringHead implement FIFO
// insertion-order tracking for the per-region maxStatementHistory eviction
// cap; store.Index groups explicitly do not preserve insertion order across
// deletions (swap-and-truncate removal), so replacing this hand-rolled ring
// buffer with an Index would silently corrupt eviction order -- exactly the
// "leave raw" case the rollout calls out for order-sensitive statement
// history. (2) regionStore instances are created lazily per-region by
// storeFor() rather than known at backend-construction time, so there is no
// fixed set of tables to register once with a store.Registry the way every
// other Phase 3.3 conversion does. This was already persisted before this
// rollout (see persistence.go) and remains persisted, unchanged, after it.
type regionStore struct {
	statements map[string]*Statement
	// ring buffer for ordered eviction – head points to the oldest slot.
	ringBuf  [maxStatementHistory]string
	ringLen  int
	ringHead int
}

// addStatement inserts a statement and evicts the oldest via the ring buffer if
// the cap is exceeded. Caller must hold the backend write lock.
func (s *regionStore) addStatement(stmt *Statement) {
	s.statements[stmt.ID] = stmt

	if s.ringLen < maxStatementHistory {
		tail := (s.ringHead + s.ringLen) % maxStatementHistory
		s.ringBuf[tail] = stmt.ID
		s.ringLen++

		return
	}

	delete(s.statements, s.ringBuf[s.ringHead])
	s.ringBuf[s.ringHead] = stmt.ID
	s.ringHead = (s.ringHead + 1) % maxStatementHistory
}

// compactRingBuffer rebuilds the ring buffer from the current statements map,
// preserving insertion order. Must be called with the backend write lock held.
func (s *regionStore) compactRingBuffer() {
	kept := make([]string, 0, s.ringLen)

	for i := range s.ringLen {
		id := s.ringBuf[(s.ringHead+i)%maxStatementHistory]
		if _, ok := s.statements[id]; ok {
			kept = append(kept, id)
		}
	}

	s.ringHead = 0
	s.ringLen = len(kept)

	copy(s.ringBuf[:], kept)

	for i := s.ringLen; i < maxStatementHistory; i++ {
		s.ringBuf[i] = ""
	}
}

// InMemoryBackend is an in-memory store for Redshift Data API statements.
// All regional resource maps are nested by region (outer key = region) so that
// the same-named statement in two regions are fully isolated.
type InMemoryBackend struct {
	stores        map[string]*regionStore
	mu            *lockmetrics.RWMutex
	accountID     string
	defaultRegion string
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
		stores:        make(map[string]*regionStore),
		accountID:     accountID,
		defaultRegion: region,
		mu:            lockmetrics.New("redshiftdata"),
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.defaultRegion }

// AccountID returns the AWS account ID this backend is configured for.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// storeFor returns the regionStore for the given region, creating it on first use.
// Caller must hold b.mu write lock.
func (b *InMemoryBackend) storeFor(region string) *regionStore {
	if b.stores[region] == nil {
		b.stores[region] = &regionStore{
			statements: make(map[string]*Statement),
		}
	}

	return b.stores[region]
}

// storeForRead returns the regionStore for the given region, or nil if none exists.
// Caller must hold b.mu (read or write). Does not create a store.
func (b *InMemoryBackend) storeForRead(region string) *regionStore {
	return b.stores[region]
}

// Reset clears all stored statements across all regions.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.stores = make(map[string]*regionStore)
}

// ExecuteStatement creates and immediately completes a SQL statement.
func (b *InMemoryBackend) ExecuteStatement(
	ctx context.Context,
	sql, clusterIdentifier, workgroupName, database, dbUser, secretARN, statementName string,
	withEvent bool, resultFormat string,
	parameters []SQLParameter,
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

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("ExecuteStatement")
	defer b.mu.Unlock()

	hasResultSet := sqlHasResultSet(sql)

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
		Parameters:        parameters,
		Status:            statusFinished,
		HasResultSet:      hasResultSet,
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
	b.storeFor(region).addStatement(stmt)

	return cloneStatement(stmt), nil
}

// BatchExecuteStatement creates and immediately completes a batch SQL statement.
func (b *InMemoryBackend) BatchExecuteStatement(
	ctx context.Context,
	sqls []string, clusterIdentifier, workgroupName, database, dbUser, secretARN, statementName string,
	withEvent bool, resultFormat string,
) (*Statement, error) {
	if len(sqls) == 0 {
		return nil, fmt.Errorf("%w: Sqls is required", ErrValidation)
	}

	for i, sql := range sqls {
		if sql == "" {
			return nil, fmt.Errorf("%w: Sqls[%d] must not be empty", ErrValidation, i)
		}
	}

	if database == "" {
		return nil, fmt.Errorf("%w: Database is required", ErrValidation)
	}

	resultFormat, err := requestedResultFormat(resultFormat)
	if err != nil {
		return nil, err
	}

	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("BatchExecuteStatement")
	defer b.mu.Unlock()

	now := time.Now()

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
	b.storeFor(region).addStatement(stmt)

	return cloneStatement(stmt), nil
}

// DescribeStatement returns the details of a statement by ID.
func (b *InMemoryBackend) DescribeStatement(ctx context.Context, id string) (*Statement, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("DescribeStatement")
	defer b.mu.RUnlock()

	store := b.storeForRead(region)
	if store == nil {
		return nil, fmt.Errorf("%w: statement %s not found", ErrNotFound, id)
	}

	stmt, ok := store.statements[id]

	if !ok {
		return nil, fmt.Errorf("%w: statement %s not found", ErrNotFound, id)
	}

	return cloneStatement(stmt), nil
}

// CancelStatement marks a statement as aborted.
func (b *InMemoryBackend) CancelStatement(ctx context.Context, id string) error {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("CancelStatement")
	defer b.mu.Unlock()

	store := b.storeFor(region)
	stmt, ok := store.statements[id]

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

// statementMatchesFilter reports whether stmt satisfies every set field of filter.
func statementMatchesFilter(stmt *Statement, filter ListStatementsFilter) bool {
	if filter.ClusterIdentifier != "" && stmt.ClusterIdentifier != filter.ClusterIdentifier {
		return false
	}

	if filter.WorkgroupName != "" && stmt.WorkgroupName != filter.WorkgroupName {
		return false
	}

	if filter.Database != "" && stmt.Database != filter.Database {
		return false
	}

	if filter.StatementName != "" && !strings.HasPrefix(stmt.StatementName, filter.StatementName) {
		return false
	}

	return matchesStatementStatus(stmt.Status, filter.Status)
}

// ListStatements returns statements sorted by creation time (newest first).
// An omitted Status matches AWS by returning only finished statements.
// Returns the page slice and a next-token string (non-empty when more pages exist).
func (b *InMemoryBackend) ListStatements(
	ctx context.Context,
	filter ListStatementsFilter,
) ([]*Statement, string, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("ListStatements")
	defer b.mu.RUnlock()

	store := b.storeForRead(region)
	if store == nil {
		return nil, "", nil
	}

	result := make([]*Statement, 0, len(store.statements))

	for _, stmt := range store.statements {
		if statementMatchesFilter(stmt, filter) {
			result = append(result, cloneStatement(stmt))
		}
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

	return requested == statusAll || actual == requested
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
// than the given cutoff across all regions. Returns the number of evicted statements.
// Only terminal states (FINISHED, FAILED, ABORTED) are eligible for eviction.
func (b *InMemoryBackend) EvictExpiredStatements(cutoff time.Time) int {
	b.mu.Lock("EvictExpiredStatements")
	defer b.mu.Unlock()

	total := 0

	for _, store := range b.stores {
		var toDelete []string

		for id, stmt := range store.statements {
			terminal := stmt.Status == statusFinished ||
				stmt.Status == statusFailed ||
				stmt.Status == statusAborted
			if terminal && stmt.UpdatedAt.Before(cutoff) {
				toDelete = append(toDelete, id)
			}
		}

		for _, id := range toDelete {
			delete(store.statements, id)
		}

		if len(toDelete) > 0 {
			store.compactRingBuffer()
		}

		total += len(toDelete)
	}

	return total
}

// cloneStatement returns a deep copy of stmt.
func cloneStatement(stmt *Statement) *Statement {
	cp := *stmt

	if stmt.QueryStrings != nil {
		cp.QueryStrings = append([]string(nil), stmt.QueryStrings...)
	}

	if stmt.Parameters != nil {
		cp.Parameters = append([]SQLParameter(nil), stmt.Parameters...)
	}

	if stmt.SubStatements != nil {
		cp.SubStatements = append([]SubStatementData(nil), stmt.SubStatements...)
	}

	return &cp
}
