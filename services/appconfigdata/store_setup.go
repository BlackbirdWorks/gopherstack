package appconfigdata

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource collection on InMemoryBackend is registered exactly
// once, here, as a *store.Table[T]. See pkgs/store's package doc for the
// underlying primitive, and services/sesv2/services/dax for the
// clean-direct-register template this file follows throughout.
//
// profiles was already a flat, top-level map[string]*ConfigurationProfile
// keyed by a composite "app|env|profile" string (profileKey in backend.go).
// ConfigurationProfile already carries all three identity fields
// (ApplicationIdentifier, EnvironmentIdentifier,
// ConfigurationProfileIdentifier) as real, wire-visible struct fields, so it
// registers directly with no DTO wrapper -- the key is simply re-derived
// from those fields via profileKey, matching the pre-existing map key
// exactly.
//
// sessions was a flat map[string]*Session keyed by the session's own Token
// field, which is already a real, exported struct field -- registers
// directly.
//
// graceTokens was a flat map[string]*graceEntry keyed by the rotated (old)
// token, but graceEntry had no field carrying that token at all (it is
// identity-less: the token is the map key, not a struct field) and every
// field was unexported (so encoding/json, which store.Table's
// snapshot/restore relies on, could not see any of them). graceEntry's
// fields were promoted to exported and a Token field was added to carry the
// key -- see the graceEntry doc comment in backend.go for why it carries no
// json:"-" tag, unlike the hidden-identity DTOs elsewhere -- so it too
// registers directly.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func profileTableKeyFn(v *ConfigurationProfile) string {
	return profileKey(v.ApplicationIdentifier, v.EnvironmentIdentifier, v.ConfigurationProfileIdentifier)
}

func sessionTableKeyFn(v *Session) string { return v.Token }

func graceEntryTableKeyFn(v *graceEntry) string { return v.Token }

// registerAllTables registers every converted resource collection on
// b.registry exactly once. It must be called during construction only
// (immediately after b.registry is created -- see NewInMemoryBackend),
// never on every reset -- store.Register panics on a duplicate name.
func registerAllTables(b *InMemoryBackend) {
	b.profiles = store.Register(b.registry, "profiles", store.New(profileTableKeyFn))
	b.sessions = store.Register(b.registry, "sessions", store.New(sessionTableKeyFn))
	b.graceTokens = store.Register(b.registry, "graceTokens", store.New(graceEntryTableKeyFn))
}
