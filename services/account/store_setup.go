package account

// Code in this file supports Phase 3.3 of the datalayer refactor: the one
// map[ContactType]*AlternateContact resource field on InMemoryBackend is
// replaced with a *store.Table[AlternateContact], registered on b.registry.
// See pkgs/store's package doc for the underlying primitive, and
// services/sesv2 / services/dax for the clean-direct-register template this
// file follows.
//
// AlternateContact already carries its own identity as a real
// (non-json:"-") field -- AlternateContactType -- set consistently at the
// one write site (PutAlternateContact) before this refactor, so it needs no
// ses-style "dirty" DTO-registry treatment: it registers directly on
// b.registry, and persistence.go drives it through
// b.registry.SnapshotAll()/RestoreAll().
//
// Every other stateful field on InMemoryBackend is either a scalar
// (accountID, region, accountName, primaryEmail, pendingEmail, pendingOTP,
// closed) or a value that isn't a map[string]*T at all -- contactInfo is a
// single struct pointer (nothing for store.Table to key on), and regions is
// a plain (non-map) slice. None of those fit store.Table's keyed-collection
// shape, so they stay as plain fields, persisted directly by persistence.go.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

// alternateContactKeyFn extracts the AlternateContact's real identity field
// -- AlternateContactType -- as the store.Table primary key.
func alternateContactKeyFn(v *AlternateContact) string { return string(v.AlternateContactType) }

// registerAllTables constructs and registers every store.Table-backed
// resource field exactly once, at construction time. It must be called
// during construction only, never on every Reset(): store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// (backend.go) instead.
func registerAllTables(b *InMemoryBackend) {
	b.alternateContacts = store.Register(b.registry, "alternateContacts", store.New(alternateContactKeyFn))
}
