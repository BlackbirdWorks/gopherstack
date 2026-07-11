package resourcegroupstaggingapi

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is registered exactly once,
// here, as a *store.Table[T] on b.registry.
//
// reportStates (region -> *reportCreationState) is the only resource map on
// this backend. The live reportCreationState type does not carry its own
// region -- nothing in the AWS wire shape needs one, since a report state is
// always looked up by the request-scoped region, never serialized as a
// standalone value -- so it gained a Region field (see backend.go) purely to
// give store.Table's keyFn something to key on. That field is tagged
// json:"-", so marshaling the live type directly would silently drop it.
// Following the services/codecommit "dirty table" convention (see that
// package's store_setup.go), reportStates is therefore built with store.New
// only and deliberately NOT store.Register-ed onto b.registry, so
// b.registry.SnapshotAll/RestoreAll/ResetAll never touch it directly;
// persistence.go instead builds an ephemeral DTO registry that gives Region
// a real JSON tag for Snapshot/Restore.
//
// caches (region -> cached GetResources result with a TTL) and the
// providers/filteredProviders/taggers/untaggers callback slices are left as
// plain fields, unchanged by this conversion: caches has no persisted
// identity of its own (it was never part of backendSnapshot before this
// conversion and remains so -- an expired-or-absent entry is simply
// recomputed), and the callback slices are runtime function values that
// cannot be serialized at all.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

// reportStateKeyFn extracts the primary key (region) from a reportCreationState.
func reportStateKeyFn(v *reportCreationState) string { return v.Region }

// registerAllTables constructs every converted resource collection on b. It
// must be called during construction only (immediately after b.registry is
// created), never on every Reset() -- store.Register panics on a duplicate
// name, so runtime resets go through b.registry.ResetAll() (plus, for the
// "dirty" reportStates table, an explicit b.reportStates.Reset()) instead --
// see InMemoryBackend.Reset in backend.go.
func registerAllTables(b *InMemoryBackend) {
	// "Dirty" table: store.New only, NOT store.Register -- see the file doc
	// comment above. Reset() (backend.go) resets it explicitly; persistence.go
	// builds a separate ephemeral DTO registry for it.
	b.reportStates = store.New(reportStateKeyFn)
}
