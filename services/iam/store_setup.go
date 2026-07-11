package iam

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]T resource field on InMemoryBackend whose key is a pure function
// of the stored value's own field is registered exactly once, here, as a
// *store.Table[T] on b.registry. See pkgs/store's package doc and the
// services/ec2 (commit 12e611a4) and services/sqs (commit 0f09d77c)
// conversions this follows.
//
// The following resource fields are deliberately NOT registered here and
// remain plain maps because their key is not a pure function of the stored
// value's own fields, or they are secondary indexes/relations rather than
// primary resource collections:
//   - policyByARN, roleByARN: ARN -> primary-key secondary indexes over
//     policies/roles, not resource collections of their own.
//   - userAccessKeys: username -> []AccessKeyID secondary index, rebuilt from
//     accessKeys on Restore (see persistence.go).
//   - groupMembers, groupPolicies, userPolicies, rolePolicies: entity name ->
//     []string relation maps (group membership / attached policy ARNs), no
//     single-struct identity to key a Table by.
//   - userInlinePolicies, roleInlinePolicies, groupInlinePolicies: entity name
//     -> (policy name -> document) relation maps.
//   - policyAttachments: policy ARN -> set-of-entity-name refs, a relation not
//     a resource.
//   - policyVersions: policy ARN -> []StoredPolicyVersion, a per-policy
//     history list rather than a single keyed resource.
//   - policyVersionCounters, deletedV1Policies: policy ARN -> scalar
//     bookkeeping, no resource identity.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func usersKeyFn(v *User) string                       { return v.UserName }
func rolesKeyFn(v *Role) string                       { return v.RoleName }
func policiesKeyFn(v *Policy) string                  { return v.PolicyName }
func groupsKeyFn(v *Group) string                     { return v.GroupName }
func accessKeysKeyFn(v *AccessKey) string             { return v.AccessKeyID }
func instanceProfilesKeyFn(v *InstanceProfile) string { return v.InstanceProfileName }
func samlProvidersKeyFn(v *SAMLProvider) string       { return v.Arn }
func oidcProvidersKeyFn(v *OIDCProvider) string       { return v.Arn }
func loginProfilesKeyFn(v *LoginProfile) string       { return v.UserName }
func serviceSpecificCredsKeyFn(v *ServiceSpecificCredential) string {
	return v.ServiceSpecificCredentialID
}
func virtualMFADevicesKeyFn(v *VirtualMFADevice) string     { return v.SerialNumber }
func signingCertificatesKeyFn(v *SigningCertificate) string { return v.CertificateID }
func serverCertificatesKeyFn(v *ServerCertificate) string {
	return v.ServerCertificateName
}
func delegationRequestsKeyFn(v *DelegationRequest) string { return v.DelegationID }

// registerAllTables registers every converted resource map on b.registry
// exactly once. It must be called during construction only (immediately after
// b.registry is created), never on every Reset() -- store.Register panics on
// a duplicate name, so runtime resets go through registry.ResetAll() instead
// (see InMemoryBackend.Reset).
func registerAllTables(b *InMemoryBackend) {
	for _, register := range tableRegistrations {
		register(b)
	}
}

// tableRegistrations is the data-driven list registerAllTables walks: one
// closure per resource table, each binding its own store.New/store.Register
// call to the concrete field and value type. A closure list (rather than one
// statement per field in a single function body) mirrors the services/ec2
// pattern (store_setup.go) and keeps registerAllTables small regardless of how
// many resource tables the backend grows to.
//
//nolint:gochecknoglobals // registration table, analogous to errCodeLookup-style lookup tables elsewhere
var tableRegistrations = []func(*InMemoryBackend){
	func(b *InMemoryBackend) {
		b.users = store.Register(b.registry, "users", store.New(usersKeyFn))
	},
	func(b *InMemoryBackend) {
		b.roles = store.Register(b.registry, "roles", store.New(rolesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.policies = store.Register(b.registry, "policies", store.New(policiesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.groups = store.Register(b.registry, "groups", store.New(groupsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.accessKeys = store.Register(b.registry, "accessKeys", store.New(accessKeysKeyFn))
	},
	func(b *InMemoryBackend) {
		b.instanceProfiles = store.Register(b.registry, "instanceProfiles", store.New(instanceProfilesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.samlProviders = store.Register(b.registry, "samlProviders", store.New(samlProvidersKeyFn))
	},
	func(b *InMemoryBackend) {
		b.oidcProviders = store.Register(b.registry, "oidcProviders", store.New(oidcProvidersKeyFn))
	},
	func(b *InMemoryBackend) {
		b.loginProfiles = store.Register(b.registry, "loginProfiles", store.New(loginProfilesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.serviceSpecificCreds = store.Register(
			b.registry,
			"serviceSpecificCreds",
			store.New(serviceSpecificCredsKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.virtualMFADevices = store.Register(b.registry, "virtualMFADevices", store.New(virtualMFADevicesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.signingCertificates = store.Register(b.registry, "signingCertificates", store.New(signingCertificatesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.serverCertificates = store.Register(b.registry, "serverCertificates", store.New(serverCertificatesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.delegationRequests = store.Register(b.registry, "delegationRequests", store.New(delegationRequestsKeyFn))
	},
}

// sortedTableNames returns every key in t, ascending, by delegating to
// [store.Table.Snapshot]'s already-deterministic key-sorted order and mapping
// each item through keyFn. It replaces the old rebuildSortedNames helper
// (which sorted map keys directly) now that these fields are Tables rather
// than plain maps.
func sortedTableNames[V any](t *store.Table[V], keyFn func(*V) string) []string {
	items := t.Snapshot()
	names := make([]string, len(items))

	for i, v := range items {
		names[i] = keyFn(v)
	}

	return names
}
