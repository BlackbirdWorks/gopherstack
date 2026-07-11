package verifiedpermissions

// Code in this file supports Phase 3.3 of the datalayer refactor: policies,
// policyTemplates, and identitySources were previously nested by policy store
// (map[string]map[string]*T); each is now a flat *store.Table[T] keyed by the
// composite "policyStoreID/id" string, with a companion *store.Index grouping
// entries by policy store for the per-store scans the nested maps used to
// answer directly. policyStores registers directly too, keyed by its own
// PolicyStoreID field. All four are "clean" tables -- registered directly on
// b.registry, no DTO wrapper needed (see persistence.go).
//
// schemas has no wire-visible identity field (one schema per policy store),
// so it gained a hidden policyStoreID field purely for this key. It is a
// "dirty" table (store.New only, deliberately NOT store.Register-ed onto
// b.registry -- so b.registry.SnapshotAll never tries to marshal it directly,
// which would silently drop the hidden field); persistence.go instead builds
// an ephemeral DTO registry for it, and InMemoryBackend.Reset resets it
// explicitly.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func policyStoreTableKeyFn(v *PolicyStore) string { return v.PolicyStoreID }

func policyTableKeyFn(v *Policy) string      { return policyKey(v.PolicyStoreID, v.PolicyID) }
func policyStoreIndexKeyFn(v *Policy) string { return v.PolicyStoreID }

func policyTemplateTableKeyFn(v *PolicyTemplate) string {
	return policyTemplateKey(v.PolicyStoreID, v.PolicyTemplateID)
}
func policyTemplateStoreIndexKeyFn(v *PolicyTemplate) string { return v.PolicyStoreID }

func identitySourceTableKeyFn(v *IdentitySource) string {
	return identitySourceKey(v.PolicyStoreID, v.IdentitySourceID)
}
func identitySourceStoreIndexKeyFn(v *IdentitySource) string { return v.PolicyStoreID }

// schemaTableKeyFn keys the "dirty" schemas table off its hidden
// policyStoreID field (see the file doc comment above).
func schemaTableKeyFn(v *PolicyStoreSchema) string { return v.policyStoreID }

// registerAllTables registers every converted resource collection on
// b.registry (the "clean" tables) or builds it standalone (the "dirty"
// schemas table -- see the file doc comment above) exactly once. It must be
// called during construction only (immediately after b.registry is created --
// see NewInMemoryBackend), never on every Reset() -- store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll() plus
// schemas' own Reset() call instead (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.policyStores = store.Register(b.registry, "policyStores", store.New(policyStoreTableKeyFn))

	b.policies = store.Register(b.registry, "policies", store.New(policyTableKeyFn))
	b.policiesByStore = b.policies.AddIndex("byPolicyStore", policyStoreIndexKeyFn)

	b.policyTemplates = store.Register(b.registry, "policyTemplates", store.New(policyTemplateTableKeyFn))
	b.policyTemplatesByStore = b.policyTemplates.AddIndex("byPolicyStore", policyTemplateStoreIndexKeyFn)

	b.identitySources = store.Register(b.registry, "identitySources", store.New(identitySourceTableKeyFn))
	b.identitySourcesByStore = b.identitySources.AddIndex("byPolicyStore", identitySourceStoreIndexKeyFn)

	b.schemas = store.New(schemaTableKeyFn)
}
