package cloudcontrol

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], following the pattern established by services/sesv2 and
// services/dax (see pkgs/store's package doc for the underlying primitive).
//
// Both converted collections already carry their own identity as real
// (non-json:"-") fields:
//   - Resource.TypeName + Resource.Identifier form the composite key
//     previously built by resourceKey() and used as the map key
//     ("<typeName>/<identifier>"); resourceKeyFn below reuses resourceKey so
//     every access site keeps building the exact same key string it always
//     has.
//   - ProgressEvent.RequestToken is already the exact key the previous
//     map[string]*ProgressEvent used.
//
// So both tables are "clean" (services/sesv2/services/dax sense): registered
// directly on b.registry, no ephemeral DTO-registry needed.
//
// clientTokens (map[string]string, clientToken -> requestToken) is left as a
// plain map: its value is a bare string, not *T, so there is nothing for
// store.Table to key on. It is still persisted directly -- see
// persistence.go.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func resourceKeyFn(v *Resource) string { return resourceKey(v.TypeName, v.Identifier) }

func progressEventKeyFn(v *ProgressEvent) string { return v.RequestToken }

// registerAllTables constructs and registers every store.Table-backed
// resource field exactly once, at construction time. It must be called
// during construction only, never on every Reset(): store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// (backend.go) instead.
func registerAllTables(b *InMemoryBackend) {
	b.resources = store.Register(b.registry, "resources", store.New(resourceKeyFn))
	b.requests = store.Register(b.registry, "requests", store.New(progressEventKeyFn))
}
