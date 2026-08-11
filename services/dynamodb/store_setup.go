package dynamodb

// Every map[string]*T backend resource field on InMemoryDB is registered exactly
// once, here, as a *store.Table[T] on db.registry. See pkgs/store's package doc.
//
// txnTokens, txnPending, and fisReplicationPaused (all map[string]time.Time) are
// deliberately left as plain maps: the value carries no identity of its own --
// store.Table's keyFn must derive the primary key FROM the value, which is
// impossible when the key is an opaque, externally-supplied token/ARN that never
// appears in the stored time.Time.

import (
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// tableKey builds the composite primary key used by db.tables and
// db.deletingTables. DynamoDB table names cannot contain "/" (AWS restricts
// them to letters, digits, underscore, hyphen, and period), so region+"/"+name
// is a safe, collision-free composite key across regions -- the same table
// name may legitimately exist in two different regions (e.g. global table
// replicas), so region alone or name alone would not be unique.
func tableKey(region, name string) string { return region + "/" + name }

// tableRegion extracts the region a Table belongs to by parsing its ARN
// (arn:{partition}:dynamodb:{region}:{account}:table/{name}). TableArn is always
// populated before a Table is inserted into db.tables/db.deletingTables and
// never mutated afterward, so this is a stable, pure derivation suitable for a
// store.Table/store.Index key function. db.regionFromARN is intentionally not
// reused here: it falls back to db.defaultRegion on parse failure, which would
// make the key function depend on backend state rather than purely on the value.
const arnRegionPartIndex = 3

func tableRegion(t *Table) string {
	parts := strings.Split(t.TableArn, ":")
	if len(parts) > arnRegionPartIndex {
		return parts[arnRegionPartIndex]
	}

	return ""
}

// tableKeyFn is the store.Table key function for db.tables and
// db.deletingTables. Both Name and (the region parsed out of) TableArn are
// fixed at construction and never mutated on an already-inserted Table (see
// tableRegion's doc), so the composite key is stable for the table's entire
// lifetime in either map.
func tableKeyFn(t *Table) string { return tableKey(tableRegion(t), t.Name) }

// streamARNKeyFn is the store.Table key function for db.streamARNIndex, a
// reverse index from a table's StreamARN back to the same *Table pointer in
// db.tables. Unlike Name/TableArn, StreamARN DOES change in place
// (EnableStream/DisableStream/UpdateTable), so callers call Delete(oldARN) then
// Put(table) explicitly rather than relying on store.Table.Put -- safe because
// streamARNIndex is its own primary-keyed store.Table, not a store.Index over
// db.tables (whose automatic remove/add pair would derive the "old" key from
// the already-mutated value, which would be wrong here).
func streamARNKeyFn(t *Table) string { return t.StreamARN }

// backupKeyFn is the store.Table key function for db.backups.
func backupKeyFn(b *Backup) string { return b.BackupArn }

// globalTableKeyFn is the store.Table key function for db.globalTables.
func globalTableKeyFn(gt *StoredGlobalTable) string { return gt.GlobalTableName }

// exportKeyFn is the store.Table key function for db.exports.
func exportKeyFn(e *storedExport) string { return e.ExportArn }

// importKeyFn is the store.Table key function for db.imports.
func importKeyFn(i *storedImport) string { return i.ImportArn }

// registerAllTables registers every converted resource map on db.registry
// exactly once. It must be called during construction only (immediately
// after db.registry is created), never on every Reset() -- store.Register
// panics on a duplicate name.
func registerAllTables(db *InMemoryDB) {
	db.tables = store.Register(db.registry, "tables", store.New(tableKeyFn))
	db.tablesByRegion = db.tables.AddIndex("region", tableRegion)
	db.deletingTables = store.Register(db.registry, "deletingTables", store.New(tableKeyFn))
	db.backups = store.Register(db.registry, "backups", store.New(backupKeyFn))
	db.globalTables = store.Register(db.registry, "globalTables", store.New(globalTableKeyFn))
	db.exports = store.Register(db.registry, "exports", store.New(exportKeyFn))
	db.imports = store.Register(db.registry, "imports", store.New(importKeyFn))
	db.streamARNIndex = store.Register(db.registry, "streamARNIndex", store.New(streamARNKeyFn))
}

// evictOldestFromTable drops oldest-by-timeOf entries from t until
// t.Len() <= keep. It mirrors the pre-store evictOldest helper's semantics
// (evict on insert so memory stays bounded even when the janitor is
// disabled) but operates on a *store.Table instead of a bare map, using
// keyOf to recover the primary key store.Table.Delete needs (store.Table
// intentionally does not expose its internal keyFn).
func evictOldestFromTable[V any](
	t *store.Table[V],
	keep int,
	keyOf func(*V) string,
	timeOf func(*V) time.Time,
) {
	all := t.All()
	if len(all) <= keep {
		return
	}

	type kv struct {
		t time.Time
		k string
	}

	entries := make([]kv, 0, len(all))
	for _, v := range all {
		entries = append(entries, kv{timeOf(v), keyOf(v)})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].t.Before(entries[j].t) })

	for i := range len(all) - keep {
		t.Delete(entries[i].k)
	}
}
