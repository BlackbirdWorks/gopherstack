package timestreamquery

// Code in this file supports Phase 3.3 of the datalayer refactor: the
// backend-level map[string]*T resource fields on InMemoryBackend are replaced
// with *store.Table[T], following the pattern established by services/sesv2,
// services/dax, and services/timestreamwrite (clean direct registration). See
// pkgs/store's package doc for the underlying primitive.
//
//   - scheduledQueries (previously map[region]map[name]*ScheduledQuery, with a
//     companion map[region]map[arn]name reverse index, arnIndex) is flattened
//     into a single store.Table[ScheduledQuery] keyed directly by Arn -- a
//     real, wire-visible field that is (unlike Name) GLOBALLY unique: the ARN
//     format embeds the region itself
//     ("arn:aws:timestream:%s:%s:scheduled-query/%s"), so two scheduled
//     queries in different regions with the same Name always produce
//     different ARNs. The create-time "name already exists in this region"
//     check is answered by computing the candidate ARN and calling Table.Has
//     on it directly -- no secondary name index is needed. A companion
//     "byRegion" store.Index, keyed by the region parsed back out of Arn via
//     regionFromARN, replaces the outer map layer for the per-region List*
//     operations. This makes the old arnIndex map entirely redundant -- it
//     existed only to resolve ARN -> name for the nested primary map, which
//     no longer exists, so it is dropped rather than converted.
//   - queries (map[string]*QueryResult) keys off QueryID, a real field, so it
//     is registered directly. It was NOT persisted before this refactor (the
//     pre-Phase-3.3 Snapshot only covered scheduledQueries/arnIndex/
//     accountSettings); registering it here means query results now
//     round-trip through Snapshot/Restore for free via
//     b.registry.SnapshotAll/RestoreAll, a persistence-coverage expansion,
//     not a behavior change to any operation.
//
// Left as a plain map, NOT store.Table-backed:
//   - accountSettings (map[string]AccountSettings): the value type is a plain
//     struct, not *T, so it does not fit store.Table's keyed-by-identity-value
//     shape. Was persisted before -- still persisted, unchanged.
//
// clientTokens (*clientTokenCache) and pageStore (*nextTokenStore) are
// self-contained caches with their own internal sync.Mutex, not backend
// resource maps -- left untouched. Neither was persisted before (both reset
// to empty on Reset/Restore); still not persisted.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func scheduledQueryKeyFn(v *ScheduledQuery) string { return v.Arn }

// scheduledQueryRegionIndexKeyFn derives the byRegion index's group key from
// the region embedded in Arn -- the same region every access site used to
// pick a shard of the old nested map, now recovered from the single flat key.
func scheduledQueryRegionIndexKeyFn(v *ScheduledQuery) string {
	return regionFromARN(v.Arn, "")
}

func queryResultKeyFn(v *QueryResult) string { return v.QueryID }

// registerAllTables registers every store.Table-backed resource field on
// b.registry exactly once. It must be called during construction only
// (NewInMemoryBackend), never on every Reset() -- store.Register panics on a
// duplicate name, so runtime resets go through b.registry.ResetAll() instead
// (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.scheduledQueries = store.Register(b.registry, "scheduledQueries", store.New(scheduledQueryKeyFn))
	b.scheduledQueriesByRegion = b.scheduledQueries.AddIndex("byRegion", scheduledQueryRegionIndexKeyFn)
	b.queries = store.Register(b.registry, "queries", store.New(queryResultKeyFn))
}
