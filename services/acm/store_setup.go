package acm

// Code in this file supports Phase 3.3 of the datalayer refactor: the
// previous map[region]map[arn]*Certificate nesting is replaced by a single
// flat *store.Table[Certificate], keyed by the composite "region|arn" string
// (see regionKey in backend.go), with a companion *store.Index grouping
// entries by region for the per-region scans (ListCertificates) the old
// outer map nesting answered directly -- the region-qualified-table pattern
// services/codeartifact and services/resourcegroups use.
//
// Certificate carries no wire-visible Region field (ACM's real Certificate
// shape has none -- only the ARN encodes it), so it gained an unexported
// region field purely for this composite key (see backend.go). That makes
// certs a "dirty" table in the persistence sense: store.New only,
// deliberately NOT store.Register-ed onto b.registry (so b.registry.
// SnapshotAll never tries to marshal it directly, which would silently drop
// the hidden field); persistence.go instead builds an ephemeral DTO registry
// for it, and InMemoryBackend.Reset (backend.go) resets it explicitly.
//
// timers is deliberately NOT converted: it holds live *time.Timer handles
// (runtime resources with no JSON-marshalable identity of their own, and
// never persisted -- see persistence.go), so there is nothing for
// store.Table to key on or snapshot. idempotencyMap, accountIdempotency, and
// accountConfig are also left unconverted: each is a non-*T value map (their
// values are plain structs, not pointers), which does not fit store.Table's
// map[string]*V shape; all three remain persisted directly as plain maps
// (see persistence.go).
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func certTableKeyFn(v *Certificate) string       { return regionKey(v.region, v.ARN) }
func certRegionIndexKeyFn(v *Certificate) string { return v.region }

// registerAllTables builds the backend's certs table and its region index
// exactly once, during construction. certs is NOT registered on b.registry
// (dirty; see the file doc above) -- runtime resets for it go through its
// own Table.Reset() call (see InMemoryBackend.Reset in backend.go) rather
// than b.registry.ResetAll().
func registerAllTables(b *InMemoryBackend) {
	b.certs = store.New(certTableKeyFn)
	b.certsByRegion = b.certs.AddIndex("byRegion", certRegionIndexKeyFn)
}
