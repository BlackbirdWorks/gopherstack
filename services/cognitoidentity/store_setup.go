package cognitoidentity

// Code in this file supports Phase 3.3 of the datalayer refactor: four resource
// collections that were previously nested by region (outer key = region, e.g.
// map[string]map[string]*IdentityPool) are each replaced by a single flat
// *store.Table[T], keyed by the composite "region|id" string (see regionKey in
// store.go), with companion *store.Index values grouping entries for the
// per-region / per-pool scans the old reverse-lookup maps (poolsByName,
// poolsByARN, identitiesByPool) and prefix-scan (principal-tag cleanup on
// DeleteIdentityPool) provided -- the region-qualified-table pattern
// services/emr, services/neptune and services/mwaa use.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func poolKeyFn(v *IdentityPool) string            { return regionKey(v.region, v.IdentityPoolID) }
func poolRegionIndexKeyFn(v *IdentityPool) string { return v.region }
func poolNameIndexKeyFn(v *IdentityPool) string   { return regionKey(v.region, v.IdentityPoolName) }
func poolARNIndexKeyFn(v *IdentityPool) string    { return regionKey(v.region, v.ARN) }

func identityKeyFn(v *Identity) string { return regionKey(v.region, v.IdentityID) }
func identityPoolIndexKeyFn(v *Identity) string {
	return regionKey(v.region, v.IdentityPoolID)
}

// rolesKeyFn keys the roles table by "region|poolID" -- one IdentityRoles record per
// pool, matching the old map[region]map[poolID]*IdentityRoles shape. No index is needed:
// roles are only ever looked up directly by (region, poolID), never listed or scanned.
func rolesKeyFn(v *IdentityRoles) string { return regionKey(v.region, v.poolID) }

// principalTagsKeyFn keys the principalTags table by "region|poolID:providerName",
// matching the old map[region]map[principalTagKey(poolID,providerName)]*PrincipalTagMapping
// shape exactly.
func principalTagsKeyFn(v *PrincipalTagMapping) string {
	return regionKey(v.region, principalTagKey(v.poolID, v.providerName))
}

// principalTagsPoolIndexKeyFn groups principal-tag mappings by (region, poolID), replacing
// the old prefix-scan ("for key := range pt { if strings.HasPrefix(key, poolID+":") }")
// DeleteIdentityPool used to find every mapping belonging to a pool being deleted.
func principalTagsPoolIndexKeyFn(v *PrincipalTagMapping) string {
	return regionKey(v.region, v.poolID)
}

// registerAllTables registers every converted resource collection on b.registry exactly
// once. It must be called during construction only (immediately after b.registry is
// created -- see NewInMemoryBackend), never on every Reset() -- store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll() instead (see
// InMemoryBackend.Reset in store.go).
func registerAllTables(b *InMemoryBackend) {
	b.pools = store.Register(b.registry, "pools", store.New(poolKeyFn))
	b.poolsByRegion = b.pools.AddIndex("byRegion", poolRegionIndexKeyFn)
	b.poolsByName = b.pools.AddIndex("byName", poolNameIndexKeyFn)
	b.poolsByARN = b.pools.AddIndex("byARN", poolARNIndexKeyFn)

	b.identities = store.Register(b.registry, "identities", store.New(identityKeyFn))
	b.identitiesByPool = b.identities.AddIndex("byPool", identityPoolIndexKeyFn)

	b.roles = store.Register(b.registry, "roles", store.New(rolesKeyFn))

	b.principalTags = store.Register(b.registry, "principalTags", store.New(principalTagsKeyFn))
	b.principalTagsByPool = b.principalTags.AddIndex("byPool", principalTagsPoolIndexKeyFn)
}
