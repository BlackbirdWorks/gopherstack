package athena

// Code in this file wires up snapshot/restore persistence for the Athena
// backend, completing Phase 3.3 of the datalayer refactor for this service:
// store_setup.go already registers every "clean" resource collection (the
// store.Table[T]s that key off a field the value type carries) on b.registry,
// but until now nothing drove that registry through
// [github.com/blackbirdworks/gopherstack/pkgs/persistence.Persistable] --
// Athena state did not survive a snapshot/restore cycle at all. This mirrors
// the services/appsync, services/ssm, and services/ses conversions.
//
// Three kinds of state need different handling here, matching the split
// store_setup.go's file doc already documents:
//
//   - "Clean" tables (workGroups, dataCatalogs, capacityReservations,
//     capacityAssignments, sessions, calculations, namedQueries,
//     queryExecutions, preparedStatements, notebooks) round-trip for free
//     through b.registry.SnapshotAll/RestoreAll.
//   - "Dirty" tables (databases, tables) key off a Catalog/Database identity
//     the value type deliberately excludes from JSON (`json:"-"`, see
//     backend_extra.go's Database.Catalog and TableMetadata.Catalog/Database
//     doc comments) and are NOT registered on b.registry. They round-trip
//     through the ephemeral DTO registry below (databaseSnapshot /
//     tableMetadataSnapshot), exactly the pattern store_setup.go's file doc
//     promised and services/ses's persistence.go established.
//   - Plain-map fields (queryResults, tableData, resourceTags) are neither --
//     see each field's doc comment on backendSnapshot below for why each
//     can't be a store.Table, and how it is instead persisted explicitly,
//     mirroring services/appsync's apiKeys field.
import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// athenaSnapshotVersion identifies the shape of backendSnapshot. It must be
// bumped whenever a change to a DTO type, backendSnapshot itself, or the set
// of tables registered in store_setup.go would make an older snapshot unsafe
// to decode as the current shape. Restore compares this against the
// persisted value and discards (rather than attempts to partially decode)
// any mismatch -- see Restore below. A pre-persistence.go snapshot (there was
// none for Athena before this file) or one from an incompatible build
// decodes with Version == 0, which is guaranteed to mismatch and is
// discarded the same way any other incompatible snapshot is.
//
// Bumped 1 -> 2 (gopherstack-hjdd) for d83f4b5d3: CalculationExecution's
// nested CalculationStatistics.Progress (the registered "calculations" table's
// value type) changed from int64 to string, matching the real deserializer's
// type switch. A Version-1 snapshot's numeric "Progress" no longer unmarshals
// into the new string field at all -- an outright decode error that takes
// down the whole restore, not silent loss -- so it must be discarded like any
// other shape-incompatible snapshot.
const athenaSnapshotVersion = 2

// databaseSnapshot is the DTO wrapping a Database with the Catalog identity
// its `json:"-"` tag deliberately excludes from a direct JSON round trip
// (see backend_extra.go). Only used inside the ephemeral DTO registry built
// by newDirtyTableRegistry, never stored on b.registry directly.
type databaseSnapshot struct {
	Catalog string   `json:"catalog"`
	Record  Database `json:"record"`
}

func databaseSnapshotKeyFn(v *databaseSnapshot) string {
	return databaseKey(v.Catalog, v.Record.Name)
}

// tableMetadataSnapshot is the DTO wrapping a TableMetadata with the
// Catalog/Database identity its `json:"-"` tags deliberately exclude from a
// direct JSON round trip (see backend_extra.go).
type tableMetadataSnapshot struct {
	Catalog  string        `json:"catalog"`
	Database string        `json:"database"`
	Record   TableMetadata `json:"record"`
}

func tableMetadataSnapshotKeyFn(v *tableMetadataSnapshot) string {
	return tableMetadataKey(v.Catalog, v.Database, v.Record.Name)
}

// newDirtyTableRegistry builds the ephemeral DTO [store.Registry] shared by
// Snapshot and Restore for the two "dirty" tables (databases, tables) that
// can't be registered on b.registry directly -- see this file's doc comment
// and store_setup.go's file doc.
func newDirtyTableRegistry() (
	*store.Registry,
	*store.Table[databaseSnapshot],
	*store.Table[tableMetadataSnapshot],
) {
	dtoReg := store.NewRegistry()
	dbDTOs := store.Register(dtoReg, "databases", store.New(databaseSnapshotKeyFn))
	tblDTOs := store.Register(dtoReg, "tables", store.New(tableMetadataSnapshotKeyFn))

	return dtoReg, dbDTOs, tblDTOs
}

// sqlColumnSnapshot is the JSON-visible DTO for sqlColumn, whose name/typ
// fields are unexported computation-cache state (not an AWS resource shape)
// and so cannot round-trip through encoding/json directly.
type sqlColumnSnapshot struct {
	Name string `json:"name"`
	Typ  string `json:"typ"`
}

// sqlResultSnapshot is the JSON-visible DTO for sqlResult -- see
// sqlColumnSnapshot's doc comment for why a DTO is needed at all. Persisted
// explicitly via backendSnapshot.QueryResults rather than as a store.Table,
// since sqlResult carries no identity field of its own (the map key, a
// QueryExecutionID, is external to the value).
type sqlResultSnapshot struct {
	Columns []sqlColumnSnapshot `json:"columns"`
	Rows    [][]string          `json:"rows"`
}

// snapshotQueryResults converts the live queryResults map into its
// JSON-visible DTO form. Returns nil (encoded as JSON null, decoded back to
// a nil map) for an empty input, matching how the other explicit map fields
// behave when empty.
func snapshotQueryResults(m map[string]*sqlResult) map[string]*sqlResultSnapshot {
	if len(m) == 0 {
		return nil
	}

	out := make(map[string]*sqlResultSnapshot, len(m))

	for id, res := range m {
		if res == nil {
			continue
		}

		cols := make([]sqlColumnSnapshot, len(res.columns))
		for i, c := range res.columns {
			cols[i] = sqlColumnSnapshot{Name: c.name, Typ: c.typ}
		}

		rows := make([][]string, len(res.rows))
		copy(rows, res.rows)

		out[id] = &sqlResultSnapshot{Columns: cols, Rows: rows}
	}

	return out
}

// restoreQueryResults is the inverse of snapshotQueryResults.
func restoreQueryResults(m map[string]*sqlResultSnapshot) map[string]*sqlResult {
	out := make(map[string]*sqlResult, len(m))

	for id, snap := range m {
		if snap == nil {
			continue
		}

		cols := make([]sqlColumn, len(snap.Columns))
		for i, c := range snap.Columns {
			cols[i] = sqlColumn{name: c.Name, typ: c.Typ}
		}

		out[id] = &sqlResult{columns: cols, rows: snap.Rows}
	}

	return out
}

// backendSnapshot is the top-level on-disk shape for the Athena backend.
//
// Tables holds one JSON-encoded, key-sorted array per "clean" *store.Table[V]
// registered on b.registry (see store_setup.go), merged with the two "dirty"
// DTO tables (databases, tables) from the ephemeral registry newDirtyTableRegistry
// builds -- both produced by [store.Registry.SnapshotAll].
//
// QueryResults, TableData, and ResourceTags are the three fields
// store_setup.go's file doc calls out as deliberately left as plain maps
// (not store.Table-backed), each persisted here explicitly rather than
// silently dropped -- mirroring services/appsync's apiKeys field:
//
//   - QueryResults (queryResults map[string]*sqlResult): sqlResult's fields
//     are unexported computation-cache state, so it round-trips through the
//     sqlResultSnapshot DTO above rather than encoding/json directly.
//   - TableData (tableData map[string][]map[string]any): values are
//     []map[string]any, not *T, so no store.Table keyFn applies; already
//     JSON-friendly as-is.
//   - ResourceTags (resourceTags map[string]map[string]string): values are
//     map[string]string, not *T; already JSON-friendly as-is.
type backendSnapshot struct {
	Tables       map[string]json.RawMessage    `json:"tables"`
	QueryResults map[string]*sqlResultSnapshot `json:"queryResults,omitempty"`
	TableData    map[string][]map[string]any   `json:"tableData,omitempty"`
	ResourceTags map[string]map[string]string  `json:"resourceTags,omitempty"`
	Version      int                           `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		// The registered tables are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the Manager).
		logger.Load(ctx).WarnContext(ctx, "athena: snapshot table marshal failed", "error", err)

		return nil
	}

	dtoReg, dbDTOs, tblDTOs := newDirtyTableRegistry()

	for _, v := range b.databases.Snapshot() {
		dbDTOs.Put(&databaseSnapshot{Catalog: v.Catalog, Record: *v})
	}

	for _, v := range b.tables.Snapshot() {
		tblDTOs.Put(&tableMetadataSnapshot{Catalog: v.Catalog, Database: v.Database, Record: *v})
	}

	dirtyTables, err := dtoReg.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "athena: snapshot dirty table marshal failed", "error", err)

		return nil
	}

	maps.Copy(tables, dirtyTables)

	snap := backendSnapshot{
		Version:      athenaSnapshotVersion,
		Tables:       tables,
		QueryResults: snapshotQueryResults(b.queryResults),
		TableData:    b.tableData,
		ResourceTags: b.resourceTags,
	}

	return persistence.MarshalSnapshot(ctx, "athena", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "athena", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != athenaSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty (then
		// reseed exactly what NewInMemoryBackend seeds a fresh backend with)
		// instead of erroring, since this is an expected, recoverable
		// condition (e.g. upgrading gopherstack across a snapshot-format
		// change), not data corruption. Mirrors the services/appsync and
		// services/ssm conversions.
		logger.Load(ctx).WarnContext(ctx,
			"athena: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", athenaSnapshotVersion)

		b.registry.ResetAll()
		b.databases.Reset()
		b.tables.Reset()
		b.queryResults = make(map[string]*sqlResult)
		b.tableData = make(map[string][]map[string]any)
		b.resourceTags = make(map[string]map[string]string)

		b.workGroups.Put(&WorkGroup{Name: defaultWorkGroup, State: workGroupStateEnabled})
		b.seedDefaultMetadata()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("athena: restore snapshot tables: %w", err)
	}

	if err := b.restoreDirtyTablesLocked(snap.Tables); err != nil {
		return err
	}

	b.queryResults = restoreQueryResults(snap.QueryResults)

	if snap.TableData == nil {
		snap.TableData = make(map[string][]map[string]any)
	}

	b.tableData = snap.TableData

	if snap.ResourceTags == nil {
		snap.ResourceTags = make(map[string]map[string]string)
	}

	b.resourceTags = snap.ResourceTags

	return nil
}

// restoreDirtyTablesLocked restores the "dirty" tables (databases, tables)
// from tables via the ephemeral DTO registry, converting each DTO back to
// its live type. The caller MUST hold b.mu for writing.
func (b *InMemoryBackend) restoreDirtyTablesLocked(tables map[string]json.RawMessage) error {
	dtoReg, dbDTOs, tblDTOs := newDirtyTableRegistry()

	if err := dtoReg.RestoreAll(tables); err != nil {
		return fmt.Errorf("athena: restore snapshot dirty tables: %w", err)
	}

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

	return nil
}

// snapshotter is the subset of Persistable Snapshot satisfies; matched via
// type assertion below so Handler need not import persistence directly.
type snapshotter interface {
	Snapshot(ctx context.Context) []byte
}

// restorer is the subset of Persistable Restore satisfies; matched via type
// assertion below so Handler need not import persistence directly.
type restorer interface {
	Restore(context.Context, []byte) error
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(ctx, data)
	}

	return nil
}
