package mq

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// backend-level map[string]*T resource field on InMemoryBackend is replaced
// with a *store.Table[T], following the pattern established by services/ec2
// (data-driven registration slice), services/ses (DTO-registry for types
// that can't round-trip a direct JSON marshal), and services/sesv2 (clean
// direct registration + composite-key flattening). See pkgs/store's package
// doc for the underlying primitive.
//
// Both value types here already carry their own identity as a real
// (non-json:"-") field -- Broker.BrokerID and Configuration.ID were already
// set consistently at every write site (both map keys equal the identity
// field) before this refactor. So neither table needs the ses-style "dirty"
// DTO-registry treatment: both are "clean" tables registered directly on
// b.registry, and persistence.go drives both through
// b.registry.SnapshotAll() / RestoreAll().
//
// Broker.Users (map[string]*User) and Configuration.Data/Revisions are
// nested fields of a value already held in a Table, not backend-level
// collections, so store.Table does not apply to them -- they round-trip (or
// don't; both carry json:"-"/no persistence restoration, a pre-existing
// behavior left unchanged by this refactor) as part of their parent Table
// value exactly as they did as part of the parent map before.
//
// Left as a plain map (not store.Table-backed), audited against the
// pre-refactor persistence.go for its persisted status:
//   - tags (map[string]map[string]string): values are plain string maps,
//     not *T, so they do not fit store.Table's keyed-by-identity-value
//     shape. Was persisted before -- still persisted, unchanged.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func brokerKeyFn(v *Broker) string { return v.BrokerID }

func configurationKeyFn(v *Configuration) string { return v.ID }

// registerAllTables constructs and registers every store.Table-backed
// resource field exactly once, at construction time. It must be called
// during construction only, never on every Reset(): store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// (backend.go) instead.
func registerAllTables(b *InMemoryBackend) {
	b.brokers = store.Register(b.registry, "brokers", store.New(brokerKeyFn))
	b.configurations = store.Register(b.registry, "configurations", store.New(configurationKeyFn))
}
