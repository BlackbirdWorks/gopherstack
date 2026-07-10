package sagemakerruntime

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], following the pattern established by services/sesv2 /
// services/dax (data-driven, direct registration -- see pkgs/store's
// package doc for the underlying primitive).
//
// sessions and asyncInvocations each key off a real, non-json:"-" identity
// field the value type already carries (Session.ID, AsyncInvocation.
// InferenceID), so both register directly on b.registry -- no DTO
// indirection and no secondary store.Index are needed: neither table is
// ever looked up by anything other than its own primary key.
//
// invocations ([]*Invocation, a capped, order-sensitive FIFO history) is
// left as a plain field -- it does not fit store.Table's keyed-collection
// shape (a store.Index does not preserve insertion order, and Invocation
// carries no identity field at all). See persistence.go for how it
// round-trips alongside the registered tables.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func sessionKeyFn(v *Session) string { return v.ID }

func asyncInvocationKeyFn(v *AsyncInvocation) string { return v.InferenceID }

// registerAllTables constructs and registers every store.Table-backed
// resource field exactly once, at construction time. It must be called
// during construction only, never on every Reset(): store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// (persistence.go) instead.
func registerAllTables(b *InMemoryBackend) {
	b.sessions = store.Register(b.registry, "sessions", store.New(sessionKeyFn))
	b.asyncInvocations = store.Register(b.registry, "asyncInvocations", store.New(asyncInvocationKeyFn))
}
