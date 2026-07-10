package rolesanywhere

// Code in this file supports Phase 3.3 of the datalayer refactor: the
// previous map[region]map[id]*T nesting for trust anchors, profiles, CRLs,
// and subjects is replaced by four flat *store.Table[T], each keyed by the
// composite "region|id" string (see regionKey in backend.go), with a
// companion *store.Index grouping entries by region for the per-region scans
// (ListTrustAnchors/ListProfiles/ListCrls/ListSubjects) the old outer map
// nesting answered directly -- the region-qualified-table pattern
// services/acm, services/codeartifact, and services/resourcegroups use.
//
// None of TrustAnchor/Profile/Crl/Subject carries a wire-visible Region
// field (Roles Anywhere's real API shapes have none -- region is only ever
// recoverable from the request context), so each gained an unexported
// region field purely for this composite key (see backend.go). That makes
// all four "dirty" tables in the persistence sense: store.New only,
// deliberately NOT store.Register-ed onto b.registry (so b.registry.
// SnapshotAll never tries to marshal them directly, which would silently
// drop the hidden field via encoding/json); persistence.go instead builds
// an ephemeral DTO registry for them, and InMemoryBackend.Reset (backend.go)
// resets each of the four explicitly.
//
// tags, attributeMappings, and notificationSettings are deliberately NOT
// converted: each holds a value with no identity of its own ([]TagEntry,
// []AttributeMapping, and []NotificationSetting are plain slices, not *T),
// so there is nothing for store.Table to key on; all three remain persisted
// directly as plain region-nested maps (see persistence.go).
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func trustAnchorKeyFn(v *TrustAnchor) string            { return regionKey(v.region, v.TrustAnchorID) }
func trustAnchorRegionIndexKeyFn(v *TrustAnchor) string { return v.region }

func profileKeyFn(v *Profile) string            { return regionKey(v.region, v.ProfileID) }
func profileRegionIndexKeyFn(v *Profile) string { return v.region }

func crlKeyFn(v *Crl) string            { return regionKey(v.region, v.CrlID) }
func crlRegionIndexKeyFn(v *Crl) string { return v.region }

func subjectKeyFn(v *Subject) string            { return regionKey(v.region, v.SubjectID) }
func subjectRegionIndexKeyFn(v *Subject) string { return v.region }

// registerAllTables builds the backend's four dirty tables and their region
// indexes exactly once, during construction. None of the four is registered
// on b.registry (all dirty; see the file doc above) -- runtime resets for
// each go through its own Table.Reset() call (see InMemoryBackend.Reset in
// backend.go) rather than b.registry.ResetAll().
func registerAllTables(b *InMemoryBackend) {
	for _, register := range tableRegistrations {
		register(b)
	}
}

// tableRegistrations is the data-driven list registerAllTables walks: one
// closure per resource table, each binding its own store.New/AddIndex call
// to the concrete field and value type.
//
//nolint:gochecknoglobals // registration table, analogous to errCodeLookup-style lookup tables elsewhere
var tableRegistrations = []func(*InMemoryBackend){
	func(b *InMemoryBackend) {
		b.trustAnchors = store.New(trustAnchorKeyFn)
		b.trustAnchorsByRegion = b.trustAnchors.AddIndex("byRegion", trustAnchorRegionIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.profiles = store.New(profileKeyFn)
		b.profilesByRegion = b.profiles.AddIndex("byRegion", profileRegionIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.crls = store.New(crlKeyFn)
		b.crlsByRegion = b.crls.AddIndex("byRegion", crlRegionIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.subjects = store.New(subjectKeyFn)
		b.subjectsByRegion = b.subjects.AddIndex("byRegion", subjectRegionIndexKeyFn)
	},
}
