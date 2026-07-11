package athena

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], following the pattern established by services/ec2
// (commit 12e611a4, data-driven registration slice) and services/ses
// (commit e9af4cc7, identity-less types gaining a json:"-" key field). See
// pkgs/store's package doc for the underlying primitive.
//
// Tables split into two groups:
//
//   - "Clean" tables (workGroups, namedQueries, dataCatalogs,
//     queryExecutions, preparedStatements, capacityReservations, notebooks,
//     sessions, calculations, capacityAssignments) key off a field -- or a
//     stable composite of fields, e.g. PreparedStatement's
//     WorkGroupName+StatementName -- the value type already carries, so
//     they are registered on b.registry here and their lifecycle (reset,
//     any future snapshot/restore) collapses to one Registry call.
//   - "Dirty" tables (databases, tables) key off a Catalog (and, for
//     tables, Database) identity the value type does not naturally carry --
//     Database/TableMetadata each gained a Catalog (and TableMetadata a
//     Database) field tagged `json:"-"` purely so store.Table's keyFn can
//     derive a key, mirroring services/ses's IdentityRecord.Identity. They
//     are NOT registered on b.registry: a future persistence layer would
//     need a throwaway DTO store.Registry (see services/ses/persistence.go)
//     whose DTO types carry Catalog/Database as real JSON fields so a
//     Snapshot/Restore round trip does not lose them -- see
//     store_setup_test.go for a round-trip test demonstrating exactly this.
//
// notebookNames -- a bare map[string]struct{} the pre-conversion code used
// purely to track per-workgroup notebook name uniqueness -- is eliminated
// entirely: notebooksByName is a secondary store.Index keyed the same way
// (notebookNameKey), so uniqueness is answered by
// len(b.notebooksByName.Get(key)) == 0 instead of a parallel set.
//
// namedQueriesByWorkGroup, queryExecutionsByWorkGroup, and
// preparedStatementsByWorkGroup are secondary indexes replacing the linear
// "does this row belong to workgroup X" scans ListNamedQueries,
// ListQueryExecutions, StartQueryExecution's result-reuse check, and
// ListPreparedStatements used to perform directly over the map.
//
// queryResults (map[string]*sqlResult), tableData
// (map[string][]map[string]any), and resourceTags
// (map[string]map[string]string) are deliberately left as plain maps:
//
//   - sqlResult's fields (columns, rows) are unexported computation-cache
//     state, not an AWS resource shape -- wrapping it in store.Table (whose
//     Snapshot/Restore round-trips through encoding/json) would either
//     require exporting internal fields it has no other reason to export,
//     or would silently lose data on every round trip.
//   - tableData's values are []map[string]any, not *T.
//   - resourceTags's values are map[string]string, not *T.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func workGroupKeyFn(v *WorkGroup) string { return v.Name }

func namedQueryKeyFn(v *NamedQuery) string { return v.NamedQueryID }

func dataCatalogKeyFn(v *DataCatalog) string { return v.Name }

func queryExecutionKeyFn(v *QueryExecution) string { return v.QueryExecutionID }

// preparedStatementKeyFn mirrors preparedStatementKey (backend.go), which
// every access site also calls directly to compute the same composite key.
func preparedStatementKeyFn(v *PreparedStatement) string {
	return preparedStatementKey(v.WorkGroupName, v.StatementName)
}

func capacityReservationKeyFn(v *CapacityReservation) string { return v.Name }

func notebookKeyFn(v *Notebook) string { return v.NotebookID }

func sessionKeyFn(v *Session) string { return v.SessionID }

func calculationKeyFn(v *CalculationExecution) string { return v.CalculationID }

func capacityAssignmentKeyFn(v *CapacityAssignmentConfiguration) string {
	return v.CapacityReservationName
}

// databaseKey builds the composite key shared by the databases Table's keyFn
// and access sites that look a Database up directly.
func databaseKey(catalog, name string) string { return catalog + "/" + name }

func databaseKeyFn(v *Database) string { return databaseKey(v.Catalog, v.Name) }

// tableMetadataKey builds the composite key shared by the tables Table's
// keyFn and access sites that look a TableMetadata up directly.
func tableMetadataKey(catalog, database, name string) string {
	return catalog + "/" + database + "/" + name
}

func tableMetadataKeyFn(v *TableMetadata) string {
	return tableMetadataKey(v.Catalog, v.Database, v.Name)
}

// registerAllTables constructs every store.Table-backed resource field
// exactly once, at construction time. It must be called during construction
// only, never on every reset: store.Register panics on a duplicate name.
func registerAllTables(b *InMemoryBackend) {
	b.workGroups = store.Register(b.registry, "workGroups", store.New(workGroupKeyFn))
	b.dataCatalogs = store.Register(b.registry, "dataCatalogs", store.New(dataCatalogKeyFn))
	b.capacityReservations = store.Register(
		b.registry, "capacityReservations", store.New(capacityReservationKeyFn),
	)
	b.capacityAssignments = store.Register(
		b.registry, "capacityAssignments", store.New(capacityAssignmentKeyFn),
	)
	b.sessions = store.Register(b.registry, "sessions", store.New(sessionKeyFn))
	b.calculations = store.Register(b.registry, "calculations", store.New(calculationKeyFn))

	b.namedQueries = store.Register(b.registry, "namedQueries", store.New(namedQueryKeyFn))
	b.namedQueriesByWorkGroup = b.namedQueries.AddIndex(
		"workgroup", func(v *NamedQuery) string { return v.WorkGroup },
	)

	b.queryExecutions = store.Register(b.registry, "queryExecutions", store.New(queryExecutionKeyFn))
	b.queryExecutionsByWorkGroup = b.queryExecutions.AddIndex(
		"workgroup", func(v *QueryExecution) string { return v.WorkGroup },
	)

	b.preparedStatements = store.Register(
		b.registry, "preparedStatements", store.New(preparedStatementKeyFn),
	)
	b.preparedStatementsByWorkGroup = b.preparedStatements.AddIndex(
		"workgroup", func(v *PreparedStatement) string { return v.WorkGroupName },
	)

	b.notebooks = store.Register(b.registry, "notebooks", store.New(notebookKeyFn))
	b.notebooksByName = b.notebooks.AddIndex(
		"name", func(v *Notebook) string { return notebookNameKey(v.WorkGroup, v.Name) },
	)

	// databases and tables are "dirty" -- see the file doc comment -- so they
	// are constructed but deliberately NOT registered on b.registry.
	b.databases = store.New(databaseKeyFn)
	b.databasesByCatalog = b.databases.AddIndex("catalog", func(v *Database) string { return v.Catalog })

	b.tables = store.New(tableMetadataKeyFn)
	b.tablesByDatabase = b.tables.AddIndex(
		"database", func(v *TableMetadata) string { return databaseKey(v.Catalog, v.Database) },
	)
}
