package timestreamwrite

// Code in this file supports Phase 3.3 of the datalayer refactor: the three
// backend-level map[string]*T (or nested map[string]map[string]*T) resource
// fields on InMemoryBackend that carry a real, JSON-visible identity field
// are replaced with *store.Table[T], following the pattern established by
// services/medialive (clean direct registration) and services/glacier
// (composite key + parent store.Index for a previously nested map). See
// pkgs/store's package doc for the underlying primitive.
//
//   - databases (map[string]*Database) keys off DatabaseName, a real field,
//     so it is registered directly.
//   - tables (previously map[dbName]map[tblName]*Table) is flattened into a
//     single store.Table keyed by the composite "databaseName|tableName"
//     string (tableKey) -- Table.DatabaseName and Table.TableName are both
//     real fields already, so no DTO wrapper or hidden json:"-" field is
//     needed, unlike services/neptune's region-nested tables. A companion
//     "byDatabase" store.Index groups entries by database for the
//     per-database scans (ListTables, DeleteDatabase, isKnownARNLocked,
//     SweepRetention) the nested map used to answer directly.
//   - batchLoadTasks (map[string]*BatchLoadTask) keys off TaskID, a real
//     field, so it is registered directly.
//
// Left as plain maps, NOT store.Table-backed, each audited against the
// pre-refactor persistence.go for its persisted status:
//   - records (map[dbName]map[tblName]*tableRecords): every persisted, but
//     tableRecords carries an embedded *lockmetrics.RWMutex (via its
//     unexported mu field) that callers explicitly Close() at specific
//     points (DeleteDatabase, DeleteTable, AddTableInternal, Restore) to
//     avoid leaking lockmetrics registry entries. store.Table has no hook
//     for that kind of side effect on Delete/Reset/Restore, and tableRecords'
//     fields (mu, recordIndex, records) are all unexported, so it would not
//     round-trip through store.Table's default JSON marshal either. Was
//     persisted before (flattened into a plain map[string][]Record shape by
//     persistence.go) -- still persisted the same way, unchanged.
//   - tags (map[string]map[string]string): values are plain strings, not
//     *T, so they do not fit store.Table's keyed-by-identity-value shape.
//     Was persisted before -- still persisted, unchanged.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

// tableKey builds the composite "<databaseName>|<tableName>" key the tables
// store.Table (and its companion byDatabase index) is keyed by, replacing
// the nested map[dbName]map[tblName]*Table lookup used before this refactor.
func tableKey(dbName, tblName string) string { return dbName + "|" + tblName }

func databaseKeyFn(v *Database) string { return v.DatabaseName }

func tableKeyFn(v *Table) string { return tableKey(v.DatabaseName, v.TableName) }

func tableDatabaseIndexKeyFn(v *Table) string { return v.DatabaseName }

func batchLoadTaskKeyFn(v *BatchLoadTask) string { return v.TaskID }

// registerAllTables registers every store.Table-backed resource field on
// b.registry exactly once. It must be called during construction only
// (NewInMemoryBackend), never on every Reset() -- store.Register panics on a
// duplicate name, so runtime resets go through b.registry.ResetAll() instead
// (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.databases = store.Register(b.registry, "databases", store.New(databaseKeyFn))
	b.tables = store.Register(b.registry, "tables", store.New(tableKeyFn))
	b.tablesByDatabase = b.tables.AddIndex("byDatabase", tableDatabaseIndexKeyFn)
	b.batchLoadTasks = store.Register(b.registry, "batchLoadTasks", store.New(batchLoadTaskKeyFn))
}
