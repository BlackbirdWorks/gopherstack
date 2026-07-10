package mediastoredata

// Code in this file supports Phase 3.3 of the datalayer refactor: converting
// InMemoryBackend's per-region objects map to pkgs/store. See pkgs/store's
// package doc and the services/sqs pilot (commit 0f09d77c) / services/ec2
// rollout (commit 12e611a4) for the general pattern, and services/mediastore
// (commit d3e6c734) for the region-nested-map variant this backend follows
// most closely -- both services partition an AWS resource collection by
// region because same-keyed resources in different regions must stay fully
// isolated.
//
// states is nested per-region (map[string]map[string]*Object -- outer key is
// region) because MediaStore Data objects are isolated per region: every
// backend operation resolves the caller's region from the request context
// (getRegion in backend.go) and operates only on that region's nested store,
// and object paths carry no region component of their own. It converts to
// map[string]*store.Table[Object] (region -> Table) with a lazy per-region
// accessor ([InMemoryBackend.state]) plus a non-creating read accessor
// ([InMemoryBackend.stateRO], used by every RLock-held read path so it never
// allocates a Table under a shared lock) -- matching services/mediastore: the
// set of regions is only known at runtime, so these per-region tables are NOT
// registered on a *store.Registry (Registry.SnapshotAll/RestoreAll require a
// fixed, construction-time-known table-name set). persistence.go instead
// snapshots/restores each per-region Table directly via
// Table.Snapshot()/Table.Restore(), exactly like services/mediastore's
// persistence.go does for its containers field.
//
// Object has no natural globally-unique identity field pre-existing on the
// struct (it was previously just a map value, keyed implicitly by the
// enclosing map[string]*Object's key); a direct-registered store.Table
// requires a real, exported, serialized identity field (see the json:"-"
// hidden-ID gotcha in .claude/memories/parity-principles.md), so a Path
// field was added to Object and is populated from the same normalized path
// PutObject already used as the map key.
//
// Objects are always accessed within a single region -- ListItems and
// ListAllObjects both do prefix scans over path, never an exact-key lookup
// across objects, so no [store.Index] is needed on the per-region Table.
func objectKeyFn(v *Object) string { return v.Path }
