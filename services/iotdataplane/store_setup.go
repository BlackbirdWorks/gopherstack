package iotdataplane

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T (or nested map[string]map[string]*T) resource field on
// InMemoryBackend is replaced with a *store.Table[T], following the pattern
// established by services/sesv2 / services/dax (clean, direct registration)
// and services/ses / services/codecommit (dirty, DTO-hidden identity). See
// pkgs/store's package doc for the underlying primitive.
//
// retainedMessages is a "clean" table: RetainedMessage already carries its
// own identity as a real (non-json:"-") field, Topic, so it registers
// directly on b.registry and persistence.go drives it through
// b.registry.SnapshotAll() / RestoreAll().
//
// shadows and connections are "dirty" tables: shadowEntry and connectionEntry
// carry NO exported fields at all -- neither was ever intended for direct
// JSON marshaling; shadowEntrySnap / connectionEntrySnap (persistence.go)
// are their pre-existing serialisable forms. Each type gains an unexported
// identity field purely so store.Table's keyFn can derive a key from the
// value: shadowEntry.thingName + shadowEntry.shadowName (shadows was
// previously a nested map[string]map[string]*shadowEntry, flattened here
// into one table keyed by the composite "<thingName>#<shadowName>" string --
// see shadowKey -- with a secondary store.Index, shadowsByThing, grouping by
// thingName for the "all shadows of thing X" lookups the nested map used to
// answer directly), and connectionEntry.clientID. Neither dirty table is
// registered on b.registry: persistence.go instead builds an ephemeral DTO
// store.Registry whose DTO types give the identity a real JSON field, so
// Snapshot/Restore never depends on an unexported field to round-trip
// correctly.
//
// connections (clientID -> entry) has no natural grouping key of its own, so
// it needs no secondary index, unlike shadows.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

// shadowKey builds the composite "<thingName>#<shadowName>" primary key
// shared by the flattened shadows table. '#' cannot appear in either a
// thing name ([a-zA-Z0-9:_.-]+) or a shadow name ([a-zA-Z0-9:_-]+), so the
// composite is unambiguous. Access sites use this directly so the exact
// same key is used for Get/Put/Delete.
func shadowKey(thingName, shadowName string) string { return thingName + "#" + shadowName }

func shadowEntryKeyFn(v *shadowEntry) string { return shadowKey(v.thingName, v.shadowName) }

func shadowEntryThingIndexKeyFn(v *shadowEntry) string { return v.thingName }

func connectionEntryKeyFn(v *connectionEntry) string { return v.clientID }

func retainedMessageKeyFn(v *RetainedMessage) string { return v.Topic }

// registerAllTables constructs and registers every store.Table-backed
// resource field exactly once, at construction time. "Clean" tables are
// registered on b.registry; "dirty" tables are constructed alongside them
// but deliberately NOT registered -- see the file doc comment above. It must
// be called during construction only, never on every Reset(): store.Register
// panics on a duplicate name, so runtime resets go through
// b.registry.ResetAll() plus explicit resets of the dirty tables instead
// (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.retainedMessages = store.Register(b.registry, "retainedMessages", store.New(retainedMessageKeyFn))

	// "Dirty" tables: store.New only, NOT store.Register -- see file doc.
	b.shadows = store.New(shadowEntryKeyFn)
	b.shadowsByThing = b.shadows.AddIndex("byThing", shadowEntryThingIndexKeyFn)

	b.connections = store.New(connectionEntryKeyFn)
}
