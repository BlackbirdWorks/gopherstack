package athena

import (
	"encoding/json"
	"maps"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
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

// --- Phase 3.3 registry round-trip test ---
//
// Athena has no persistence.go: nothing wires InMemoryBackend into
// pkgs/persistence, so no map -- raw or store.Table-backed -- was ever
// snapshotted/restored across a restart before the Phase 3.3 datalayer
// conversion (see store_setup.go's file doc for the pkgs/store conversion
// itself), and none of the three maps deliberately left raw (queryResults,
// tableData, resourceTags) suffers any persistence regression as a result:
// they are exactly as unpersisted as they always were.
//
// What that conversion DOES introduce is a store.Registry (b.registry) plus
// two "dirty" tables (databases, tables) that would need a DTO round trip if
// a future persistence layer were added. TestRegistryDatabasesTablesRoundTrip
// below is the "registry round-trip test" verifying that machinery is
// actually correct: every clean table survives a
// SnapshotAll/ResetAll/RestoreAll cycle, and the two dirty tables survive an
// equivalent DTO-based round trip without losing the Catalog/Database
// identity that their json:"-" fields would otherwise drop.

// databaseSnapshot and tableMetadataSnapshot are the DTOs a real persistence
// layer would use to round-trip the "dirty" databases/tables tables (see
// services/ses/persistence.go for the established pattern this mirrors).
// They live here, in export_test.go, rather than a non-test .go file because
// nothing in the current codebase actually persists Athena state yet.
type databaseSnapshot struct {
	Catalog string   `json:"catalog"`
	Record  Database `json:"record"`
}

func databaseSnapshotKeyFn(v *databaseSnapshot) string { return databaseKey(v.Catalog, v.Record.Name) }

type tableMetadataSnapshot struct {
	Catalog  string        `json:"catalog"`
	Database string        `json:"database"`
	Record   TableMetadata `json:"record"`
}

func tableMetadataSnapshotKeyFn(v *tableMetadataSnapshot) string {
	return tableMetadataKey(v.Catalog, v.Database, v.Record.Name)
}

// populateEveryTable exercises every store.Table-backed resource on b through
// its public API (plus a couple of DDL statements for databases/tables, which
// are only reachable via SQL DDL) so a snapshot afterward has at least one
// row in every table the Phase 3.3 conversion touched.
func populateEveryTable(t *testing.T, b *InMemoryBackend) {
	t.Helper()

	require.NoError(t, b.CreateWorkGroup("wg1", "", "", WorkGroupConfiguration{}, nil))

	_, err := b.CreateNamedQuery("nq1", "", "db", "SELECT 1", "wg1")
	require.NoError(t, err)

	require.NoError(t, b.CreateDataCatalog("cat1", "GLUE", "", "", nil, nil))

	execID, err := b.StartQueryExecution(
		"SELECT 1", "wg1", QueryExecutionContext{}, ResultConfiguration{}, nil, nil,
	)
	require.NoError(t, err)
	require.NotEmpty(t, execID)

	require.NoError(t, b.CreatePreparedStatement("ps1", "", "wg1", "SELECT 1"))
	require.NoError(t, b.CreateCapacityReservation("cr1", 24, nil))

	notebookID, err := b.CreateNotebook("wg1", "nb1", nil)
	require.NoError(t, err)

	_, _, err = b.StartSession("wg1", "", "", EngineConfiguration{}, SessionConfiguration{}, notebookID)
	require.NoError(t, err)

	sessions, err := b.ListSessions("wg1", "")
	require.NoError(t, err)
	require.Len(t, sessions, 1)

	_, _, err = b.StartCalculationExecution(sessions[0].SessionID, "", "print(1)")
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
}

// snapshotRegistry captures both b.registry (the "clean" tables) and the two
// "dirty" tables (databases, tables) via a throwaway DTO registry, mirroring
// what a real Snapshot() method would do -- see store_setup.go's file doc.
func snapshotRegistry(t *testing.T, b *InMemoryBackend) map[string]json.RawMessage {
	t.Helper()

	dtoReg := store.NewRegistry()
	dbDTOs := store.Register(dtoReg, "databases", store.New(databaseSnapshotKeyFn))
	tblDTOs := store.Register(dtoReg, "tables", store.New(tableMetadataSnapshotKeyFn))

	for _, d := range b.databases.Snapshot() {
		dbDTOs.Put(&databaseSnapshot{Catalog: d.Catalog, Record: *d})
	}

	for _, tm := range b.tables.Snapshot() {
		tblDTOs.Put(&tableMetadataSnapshot{Catalog: tm.Catalog, Database: tm.Database, Record: *tm})
	}

	dirty, err := dtoReg.SnapshotAll()
	require.NoError(t, err)

	clean, err := b.registry.SnapshotAll()
	require.NoError(t, err)

	out := make(map[string]json.RawMessage, len(clean)+len(dirty))
	maps.Copy(out, clean)
	maps.Copy(out, dirty)

	return out
}

// restoreRegistry is the inverse of snapshotRegistry: it restores b's clean
// tables via registry.RestoreAll and rebuilds the dirty databases/tables
// tables from their DTOs, restoring the Catalog/Database identity the DTOs
// (not the live types) carry as real JSON fields.
func restoreRegistry(t *testing.T, b *InMemoryBackend, data map[string]json.RawMessage) {
	t.Helper()

	require.NoError(t, b.registry.RestoreAll(data))

	dtoReg := store.NewRegistry()
	dbDTOs := store.Register(dtoReg, "databases", store.New(databaseSnapshotKeyFn))
	tblDTOs := store.Register(dtoReg, "tables", store.New(tableMetadataSnapshotKeyFn))
	require.NoError(t, dtoReg.RestoreAll(data))

	liveDatabases := make([]*Database, 0, dbDTOs.Len())

	for _, dto := range dbDTOs.All() {
		rec := dto.Record
		rec.Catalog = dto.Catalog
		liveDatabases = append(liveDatabases, &rec)
	}

	b.databases.Restore(liveDatabases)

	liveTables := make([]*TableMetadata, 0, tblDTOs.Len())

	for _, dto := range tblDTOs.All() {
		rec := dto.Record
		rec.Catalog = dto.Catalog
		rec.Database = dto.Database
		liveTables = append(liveTables, &rec)
	}

	b.tables.Restore(liveTables)
}

// TestRegistryDatabasesTablesRoundTrip is the "registry round-trip test"
// called for by the Phase 3.3 conversion plan in lieu of Athena having a
// persistence.go to rewire (it has none -- see the block comment above). It
// verifies that every store.Table the conversion introduced -- both the
// "clean" ones registered directly on b.registry and the two "dirty" ones
// (databases, tables) that need a DTO -- survive a full
// Snapshot/Reset/Restore cycle with their data AND their secondary indexes
// intact.
func TestRegistryDatabasesTablesRoundTrip(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend("", "")
	populateEveryTable(t, b)

	data := snapshotRegistry(t, b)

	// Reset every table (clean + dirty) to simulate a fresh process before
	// restoring, exactly as a real Restore() would encounter.
	b.registry.ResetAll()
	b.databases.Reset()
	b.tables.Reset()

	restoreRegistry(t, b, data)

	t.Run("clean_tables_restored", func(t *testing.T) {
		t.Parallel()

		// The default "primary" workgroup NewInMemoryBackend seeds, plus wg1.
		assert.Equal(t, 2, b.workGroups.Len())
		assert.True(t, b.workGroups.Has("wg1"))

		assert.Equal(t, 1, b.namedQueries.Len())
		// AwsDataCatalog (seeded) + cat1.
		assert.Equal(t, 2, b.dataCatalogs.Len())
		// SELECT 1, CREATE DATABASE mydb, CREATE TABLE mydb.mytable.
		assert.Equal(t, 3, b.queryExecutions.Len())
		assert.Equal(t, 1, b.preparedStatements.Len())
		assert.Equal(t, 1, b.capacityReservations.Len())
		assert.Equal(t, 1, b.notebooks.Len())
		assert.Equal(t, 1, b.sessions.Len())
		assert.Equal(t, 1, b.calculations.Len())
		assert.Equal(t, 1, b.capacityAssignments.Len())
	})

	t.Run("dirty_tables_restored_with_identity", func(t *testing.T) {
		t.Parallel()

		// AwsDataCatalog/default (seeded) + mydb.
		assert.Equal(t, 2, b.databases.Len())

		mydb, ok := b.databases.Get(databaseKey(awsDataCatalog, "mydb"))
		require.True(t, ok, "mydb must be restored under its original catalog key")
		assert.Equal(t, awsDataCatalog, mydb.Catalog, "Catalog identity must survive the DTO round trip")

		mytable, ok := b.tables.Get(tableMetadataKey(awsDataCatalog, "mydb", "mytable"))
		require.True(t, ok, "mytable must be restored under its original catalog/database key")
		assert.Equal(t, awsDataCatalog, mytable.Catalog)
		assert.Equal(t, "mydb", mytable.Database)
	})

	t.Run("secondary_indexes_rebuilt", func(t *testing.T) {
		t.Parallel()

		// namedQueriesByWorkGroup.
		nqs := b.namedQueriesByWorkGroup.Get("wg1")
		require.Len(t, nqs, 1)
		assert.Equal(t, "nq1", nqs[0].Name)

		// preparedStatementsByWorkGroup.
		pss := b.preparedStatementsByWorkGroup.Get("wg1")
		require.Len(t, pss, 1)
		assert.Equal(t, "ps1", pss[0].StatementName)

		// queryExecutionsByWorkGroup.
		qes := b.queryExecutionsByWorkGroup.Get("wg1")
		assert.Len(t, qes, 3)

		// notebooksByName uniqueness index.
		nbs := b.notebooksByName.Get(notebookNameKey("wg1", "nb1"))
		require.Len(t, nbs, 1)
		assert.Equal(t, "nb1", nbs[0].Name)

		// databasesByCatalog / tablesByDatabase secondary indexes.
		dbs := b.databasesByCatalog.Get(awsDataCatalog)
		assert.Len(t, dbs, 2, "AwsDataCatalog must contain both the seeded default db and mydb")

		tbls := b.tablesByDatabase.Get(databaseKey(awsDataCatalog, "mydb"))
		require.Len(t, tbls, 1)
		assert.Equal(t, "mytable", tbls[0].Name)
	})
}
