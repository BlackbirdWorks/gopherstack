package organizations

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is registered exactly once,
// here, as a *store.Table[T] on b.registry. See pkgs/store's package doc and
// the services/sqs pilot (commit 0f09d77c) and services/ses (commit
// e9af4cc7) for the pattern this follows.
//
// Tables split into two groups:
//
//   - "Clean" tables (accounts, ous, policies, createStatuses, handshakes,
//     serviceAccess, responsibilityTransfers) key off a field the value type
//     already carries in JSON (Account.ID, OrganizationalUnit.ID,
//     Policy.PolicySummary.ID, CreateAccountStatus.ID, Handshake.ID,
//     EnabledServicePrincipal.ServicePrincipal, ResponsibilityTransfer.ID), so
//     they are registered on b.registry here and persistence.go drives them
//     through b.registry.SnapshotAll() / RestoreAll() directly.
//   - "Dirty" tables (delegatedAdmins) key off a composite of two fields,
//     one of which (DelegatedAdmin.ServicePrincipal) is tagged `json:"-"`
//     purely so store.Table's keyFn can derive a key from the value -- see
//     the doc comment on that field in models.go. delegatedAdmins is NOT
//     registered on b.registry: persistence.go instead builds a throwaway
//     DTO store.Registry (mirroring the services/ses identity pattern) whose
//     DTO type carries the ServicePrincipal as a real JSON field, so
//     Snapshot/Restore never depends on a json:"-" field to round-trip
//     correctly.
//
// ous additionally carries a secondary store.Index (ousByParentIdx) grouping
// by ParentID, used by ListOrganizationalUnitsForParent for the "children of
// OU X" lookup. delegatedAdmins carries two secondary indexes
// (delegatedAdminsByService, delegatedAdminsByAccount) supporting
// ListDelegatedAdministrators' service-principal filter and the
// account-keyed cascade-delete in RemoveAccountFromOrganization /
// ListDelegatedServicesForAccount. responsibilityTransfers carries
// responsibilityTransfersByHandshake, grouping by ActiveHandshakeID so
// AcceptHandshake/CancelHandshake/DeclineHandshake/expireStaleHandshakesLocked
// can re-sync a transfer's Status when its underlying handshake transitions.
//
// A number of fields are deliberately left as plain maps -- see the
// InMemoryBackend struct doc in backend.go for the list and why (all are
// non-*T bare-value maps: []string, string, map[string]string, or
// map[string]bool values, none of which fit store.Table's keyed-by-*T
// shape).
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func accountKeyFn(v *Account) string { return v.ID }

func ouKeyFn(v *OrganizationalUnit) string { return v.ID }

func policyKeyFn(v *Policy) string { return v.PolicySummary.ID }

func createStatusKeyFn(v *CreateAccountStatus) string { return v.ID }

func handshakeKeyFn(v *Handshake) string { return v.ID }

func responsibilityTransferKeyFn(v *ResponsibilityTransfer) string { return v.ID }

func serviceAccessKeyFn(v *EnabledServicePrincipal) string { return v.ServicePrincipal }

// delegatedAdminKey builds the composite "<servicePrincipal>#<accountID>" key
// shared by every delegated-admin registration. Access sites use this
// directly so the exact same key is used for Put/Get/Has/Delete.
func delegatedAdminKey(servicePrincipal, accountID string) string {
	return servicePrincipal + "#" + accountID
}

func delegatedAdminKeyFn(v *DelegatedAdmin) string {
	return delegatedAdminKey(v.ServicePrincipal, v.AccountID)
}

// registerAllTables constructs every store.Table-backed resource field
// exactly once, at construction time. "Clean" tables are registered on
// b.registry; the "dirty" delegatedAdmins table is constructed alongside
// them but deliberately NOT registered -- see the file doc comment above. It
// must be called during construction only, never on every Reset():
// store.Register panics on a duplicate name, so runtime resets go through
// resetStateLocked (backend.go) instead.
func registerAllTables(b *InMemoryBackend) {
	b.accounts = store.Register(b.registry, "accounts", store.New(accountKeyFn))

	b.ous = store.Register(b.registry, "ous", store.New(ouKeyFn))
	b.ousByParentIdx = b.ous.AddIndex("byParent", func(v *OrganizationalUnit) string { return v.ParentID })

	b.policies = store.Register(b.registry, "policies", store.New(policyKeyFn))
	b.createStatuses = store.Register(b.registry, "createStatuses", store.New(createStatusKeyFn))
	b.handshakes = store.Register(b.registry, "handshakes", store.New(handshakeKeyFn))
	b.serviceAccess = store.Register(b.registry, "serviceAccess", store.New(serviceAccessKeyFn))

	b.responsibilityTransfers = store.Register(
		b.registry, "responsibilityTransfers", store.New(responsibilityTransferKeyFn),
	)
	b.responsibilityTransfersByHandshake = b.responsibilityTransfers.AddIndex(
		"byHandshake",
		func(v *ResponsibilityTransfer) string { return v.ActiveHandshakeID },
	)

	b.delegatedAdmins = store.New(delegatedAdminKeyFn)
	b.delegatedAdminsByService = b.delegatedAdmins.AddIndex(
		"byService",
		func(v *DelegatedAdmin) string { return v.ServicePrincipal },
	)
	b.delegatedAdminsByAccount = b.delegatedAdmins.AddIndex(
		"byAccount",
		func(v *DelegatedAdmin) string { return v.AccountID },
	)
}
