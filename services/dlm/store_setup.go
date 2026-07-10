package dlm

// Code in this file supports Phase 3.3 of the datalayer refactor: the
// policies map[string]*storedPolicy resource field on InMemoryBackend is
// replaced with a *store.Table[storedPolicy], following the pattern
// established by services/sesv2 / services/dax (data-driven, direct
// registration -- see pkgs/store's package doc for the underlying primitive).
//
// storedPolicy already carries its own identity as a real (non-json:"-")
// field -- PolicyID -- set consistently at the single write site
// (CreateLifecyclePolicy), so it registers directly on b.registry: no DTO
// indirection is needed, unlike services/ses. DLM has no secondary index:
// TagResource/UntagResource/ListTagsForResource resolve policies by ARN via
// a linear scan (findPolicyByARNLocked in backend.go), matching the
// pre-refactor map-scan behavior byte-for-byte, so no store.Index is added
// for that lookup.
//
// counter (the monotonic PolicyID suffix generator) is a scalar int, not a
// keyed collection, so it stays a plain field on InMemoryBackend and is
// persisted alongside the table in persistence.go rather than through the
// registry.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func storedPolicyKeyFn(v *storedPolicy) string { return v.PolicyID }

// registerAllTables registers every backend resource table exactly once. It
// must be called during construction only, immediately after b.registry is
// created -- store.Register panics on a duplicate name, so runtime resets go
// through b.registry.ResetAll() instead (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.policies = store.Register(b.registry, "policies", store.New(storedPolicyKeyFn))
}
