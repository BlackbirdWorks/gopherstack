package shield

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], following the pattern established by services/sesv2 /
// services/codebuild (data-driven, direct registration -- see pkgs/store's
// package doc for the underlying primitive).
//
// Tables split into two groups:
//
//   - "Clean" tables (protections, protectionGroups, attacks) key off a real,
//     non-json:"-" identity field the value type already carries (Protection.ID,
//     ProtectionGroup.ID, Attack.AttackID), so they are registered on
//     b.registry here and persistence.go drives them through
//     b.registry.SnapshotAll() / RestoreAll() directly.
//   - alarConfigs is "dirty": ALARConfig carried no identity of its own (it
//     was only ever reached via the external map[string]*ALARConfig key), so
//     it gained a ResourceARN field tagged `json:"-"` purely so store.Table's
//     keyFn can derive a key from the value -- see the doc comment on that
//     field in backend.go. It is NOT registered on b.registry: persistence.go
//     instead round-trips it through a small alarConfigDTO slice that carries
//     ResourceARN as a real JSON field, so Snapshot/Restore never depends on
//     a json:"-" field to round-trip correctly.
//
// The former one-to-one reverse-lookup maps (resourceARNIndex, nameIndex)
// are replaced by companion *store.Index values on the protections table,
// kept consistent automatically by store.Table.Put/Delete/Restore.
//
// Left as plain fields (not store.Table-backed): subscription (*Subscription,
// a single value, not a collection), drtAccess (*DRTAccess, likewise),
// emergencyContacts ([]EmergencyContact, an ordered slice, not keyed), and
// proactiveEngagementStatus (a scalar). See persistence.go.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func protectionKeyFn(v *Protection) string { return v.ID }

func protectionResourceARNKeyFn(v *Protection) string { return v.ResourceARN }

func protectionNameKeyFn(v *Protection) string { return v.Name }

func protectionGroupKeyFn(v *ProtectionGroup) string { return v.ID }

func attackKeyFn(v *Attack) string { return v.AttackID }

func alarConfigKeyFn(v *ALARConfig) string { return v.ResourceARN }

// registerAllTables registers every backend resource table (and its
// secondary indexes) exactly once. It must be called during construction
// only, immediately after b.registry is created -- store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// instead (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.protections = store.Register(b.registry, "protections", store.New(protectionKeyFn))
	b.protectionsByResourceARN = b.protections.AddIndex("byResourceARN", protectionResourceARNKeyFn)
	b.protectionsByName = b.protections.AddIndex("byName", protectionNameKeyFn)

	b.protectionGroups = store.Register(b.registry, "protectionGroups", store.New(protectionGroupKeyFn))

	b.attacks = store.Register(b.registry, "attacks", store.New(attackKeyFn))

	// alarConfigs is "dirty" -- see the file doc comment above -- so it is
	// constructed but deliberately NOT registered on b.registry.
	b.alarConfigs = store.New(alarConfigKeyFn)
}
