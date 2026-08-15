package athena

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// DefaultJanitorInterval exposes the package default janitor interval for testing.
const DefaultJanitorInterval = defaultAthenaJanitorInterval

// DefaultExecutionTTL exposes the package default execution TTL for testing.
const DefaultExecutionTTL = defaultAthenaExecutionTTL

// SetQueryExecutionState overrides a query execution's state and completion time.
// If completionDelay is negative the completion time is set to now plus the delay (i.e. in the past).
// Used only in tests.
func (b *InMemoryBackend) SetQueryExecutionState(id, state string, completionDelay time.Duration) {
	b.mu.Lock("SetQueryExecutionState")
	defer b.mu.Unlock()

	qe, ok := b.queryExecutions.Get(id)
	if !ok {
		return
	}

	completionTime := time.Now().Add(completionDelay)
	qe.Status.State = state
	qe.Status.CompletionDateTime = float64(completionTime.UnixMilli()) / millisToSeconds
}

// QueryExecutionCount returns the number of query executions stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) QueryExecutionCount() int {
	b.mu.RLock("QueryExecutionCount")
	defer b.mu.RUnlock()

	return b.queryExecutions.Len()
}

// GetJanitorTaskTimeout returns the TaskTimeout configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the timeout.
func (h *Handler) GetJanitorTaskTimeout() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.TaskTimeout
}

// GetJanitorExecutionTTL returns the ExecutionTTL configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the TTL.
func (h *Handler) GetJanitorExecutionTTL() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.ExecutionTTL
}

// SetCalculationState overrides a calculation's state for tests.
func (b *InMemoryBackend) SetCalculationState(id, state string) {
	b.mu.Lock("SetCalculationState")
	defer b.mu.Unlock()

	c, ok := b.calculations.Get(id)
	if !ok {
		return
	}

	c.Status.State = state
}

// QueryResultCount returns the number of cached query result sets. Test-only.
func (b *InMemoryBackend) QueryResultCount() int {
	b.mu.RLock("QueryResultCount")
	defer b.mu.RUnlock()

	return len(b.queryResults)
}

// SessionCount returns the number of stored sessions. Test-only.
func (b *InMemoryBackend) SessionCount() int {
	b.mu.RLock("SessionCount")
	defer b.mu.RUnlock()

	return b.sessions.Len()
}

// CalculationCount returns the number of stored calculations. Test-only.
func (b *InMemoryBackend) CalculationCount() int {
	b.mu.RLock("CalculationCount")
	defer b.mu.RUnlock()

	return b.calculations.Len()
}

// SetSessionTerminated marks a session terminated with an end time offset by
// completionDelay (negative places it in the past). Test-only.
func (b *InMemoryBackend) SetSessionTerminated(id string, completionDelay time.Duration) {
	b.mu.Lock("SetSessionTerminated")
	defer b.mu.Unlock()

	s, ok := b.sessions.Get(id)
	if !ok {
		return
	}

	end := float64(time.Now().Add(completionDelay).UnixMilli()) / millisToSeconds
	s.Status.State = sessionStateTerminated
	s.Status.EndDateTime = end
	s.Status.LastModifiedDateTime = end
}

// SetCalculationCompletion overrides a calculation's state and completion time
// (completionDelay negative places it in the past). Test-only.
func (b *InMemoryBackend) SetCalculationCompletion(id, state string, completionDelay time.Duration) {
	b.mu.Lock("SetCalculationCompletion")
	defer b.mu.Unlock()

	c, ok := b.calculations.Get(id)
	if !ok {
		return
	}

	c.Status.State = state
	c.Status.CompletionDateTime = float64(time.Now().Add(completionDelay).UnixMilli()) / millisToSeconds
}

// TableRowCount returns the number of data rows stored for a table. Test-only.
func (b *InMemoryBackend) TableRowCount(catalog, database, table string) int {
	b.mu.RLock("TableRowCount")
	defer b.mu.RUnlock()

	return len(b.tableData[catalog+"/"+database+"/"+table])
}

// HasTable reports whether the named table exists in the catalog. Test-only.
func (b *InMemoryBackend) HasTable(catalog, database, table string) bool {
	b.mu.RLock("HasTable")
	defer b.mu.RUnlock()

	return b.tables.Has(tableMetadataKey(catalog, database, table))
}

// HasDatabase reports whether the named database exists in the catalog. Test-only.
func (b *InMemoryBackend) HasDatabase(catalog, database string) bool {
	b.mu.RLock("HasDatabase")
	defer b.mu.RUnlock()

	return b.databases.Has(databaseKey(catalog, database))
}

// GetJanitorInterval returns the Interval configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the interval.
func (h *Handler) GetJanitorInterval() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.Interval
}

// Fixture carries the fixed names and randomly generated IDs
// PopulateEveryTable creates, so an external test can look each resource back
// up (by name where deterministic, by ID where PopulateEveryTable's callee
// generates it) after a Snapshot/Restore round trip.
type Fixture struct {
	WorkGroup             string
	NamedQueryID          string
	DataCatalog           string
	SelectExecID          string
	PreparedStatementName string
	CapacityReservation   string
	NotebookID            string
	SessionID             string
	CalculationID         string
	Database              string
	Table                 string
	WorkGroupARN          string
}

// PopulateEveryTable exercises every store.Table-backed resource on b through
// its public API (plus a couple of DDL statements for databases/tables, which
// are only reachable via SQL DDL, and InsertRows/a SELECT for the plain-map
// tableData/queryResults fields) so a snapshot afterward has at least one row
// in every table and field persistence.go's Snapshot/Restore round-trips.
// Exported (capitalized) so services/athena's external persistence_test.go,
// which exercises the real Snapshot/Restore methods, can reuse it instead of
// hand-rolling the same fixture again.
func PopulateEveryTable(t *testing.T, b *InMemoryBackend) Fixture {
	t.Helper()

	require.NoError(t, b.CreateWorkGroup("wg1", "", "", WorkGroupConfiguration{}, nil))

	namedQueryID, err := b.CreateNamedQuery("nq1", "", "db", "SELECT 1", "wg1")
	require.NoError(t, err)

	_, err = b.CreateDataCatalog("cat1", "GLUE", "", "", nil, nil)
	require.NoError(t, err)

	require.NoError(t, b.CreatePreparedStatement("ps1", "", "wg1", "SELECT 1"))
	require.NoError(t, b.CreateCapacityReservation("cr1", 24, nil))

	notebookID, err := b.CreateNotebook("wg1", "nb1")
	require.NoError(t, err)

	_, _, err = b.StartSession("wg1", "", "", EngineConfiguration{}, SessionConfiguration{},
		MonitoringConfiguration{}, notebookID)
	require.NoError(t, err)

	sessions, err := b.ListSessions("wg1", "")
	require.NoError(t, err)
	require.Len(t, sessions, 1)

	sessionID := sessions[0].SessionID

	calcID, _, err := b.StartCalculationExecution(sessionID, "", "print(1)")
	require.NoError(t, err)

	require.NoError(t, b.PutCapacityAssignmentConfiguration("cr1", []CapacityAssignment{
		{WorkGroupNames: []string{"wg1"}},
	}))

	// databases/tables are only reachable through SQL DDL, run via query
	// executions so both "dirty" tables gain a row beyond the
	// AwsDataCatalog/default/sample_table seeded by NewInMemoryBackend.
	_, err = b.StartQueryExecution(
		"CREATE DATABASE mydb", "wg1", QueryExecutionContext{}, ResultConfiguration{}, nil, nil,
	)
	require.NoError(t, err)

	_, err = b.StartQueryExecution(
		"CREATE TABLE mydb.mytable (id int, name string)",
		"wg1", QueryExecutionContext{}, ResultConfiguration{}, nil, nil,
	)
	require.NoError(t, err)

	// InsertRows + a SELECT populate tableData and queryResults respectively --
	// the two remaining plain-map fields persistence.go's backendSnapshot
	// persists explicitly (see its file doc).
	b.InsertRows(awsDataCatalog, "mydb", "mytable", []map[string]any{{"id": 1, "name": "a"}})

	selID, err := b.StartQueryExecution(
		"SELECT * FROM mydb.mytable", "wg1",
		QueryExecutionContext{Catalog: awsDataCatalog}, ResultConfiguration{}, nil, nil,
	)
	require.NoError(t, err)
	require.NotEmpty(t, selID)

	workGroupARN := "arn:aws:athena:us-east-1:123456789012:workgroup/wg1"
	require.NoError(t, b.TagResource(workGroupARN, map[string]string{"env": "test"}))

	return Fixture{
		WorkGroup:             "wg1",
		NamedQueryID:          namedQueryID,
		DataCatalog:           "cat1",
		SelectExecID:          selID,
		PreparedStatementName: "ps1",
		CapacityReservation:   "cr1",
		NotebookID:            notebookID,
		SessionID:             sessionID,
		CalculationID:         calcID,
		Database:              "mydb",
		Table:                 "mytable",
		WorkGroupARN:          workGroupARN,
	}
}
