package rdsdata

// Code in this file supports Phase 3.3 of the datalayer refactor: converting
// InMemoryBackend's transactions map to pkgs/store.
//
// transactions was nested per-region (map[string]map[string]*Transaction --
// outer key is region) because transaction IDs are only unique within a
// region: BeginTransaction allocates them from a per-region counter
// (b.txCounter[region]), so "txn-000001" in one region and "txn-000001" in
// another are different transactions. Every access is already region-scoped
// via getRegion, so it converts to map[string]*store.Table[Transaction]
// (region -> Table keyed by TransactionID) with a lazy per-region accessor
// (transactionsStore in backend.go) plus a non-creating read accessor
// (transactionRegion, used by the RLock-held ListTransactions so it never
// allocates a Table under a shared lock) -- matching services/mediastore's
// region-nested-map pattern. The set of regions is only known at runtime, so
// these per-region tables are NOT registered on a *store.Registry
// (Registry.SnapshotAll/RestoreAll require a fixed, construction-time-known
// table-name set); persistence.go instead snapshots/restores each per-region
// Table directly via Table.Snapshot()/Table.Restore().
//
// executedStatements (map[string][]ExecutedStatement, region -> ordered log)
// is left as a raw map: it is a plain value slice (no *T identity) whose
// order is significant (statements are recorded and replayed in the order
// they were executed -- see sqlEngine.replay), so it must not become a
// store.Index (which does not preserve insertion order). txCounter
// (map[string]int, region -> next transaction number) is likewise left raw:
// it holds bare ints, not *T values, so store.Table does not apply.
func transactionKeyFn(v *Transaction) string { return v.TransactionID }
