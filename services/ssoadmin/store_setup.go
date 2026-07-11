package ssoadmin

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend that keys off a field the
// value type already carries is replaced with a *store.Table[T], following
// the pattern established by services/ec2 (commit 12e611a4, data-driven
// registration slice), services/sqs (commit 0f09d77c, DTO-registry for
// types that can't round-trip through a direct JSON marshal), and
// services/ses (commit e9af4cc7, json:"-" identity field + DTO for
// identity-less value types). See pkgs/store's package doc for the
// underlying primitive.
//
// Tables split into two groups:
//
//   - "Clean" tables (instances, permissionSets, creationStatuses,
//     deletionStatuses, provisioningStatuses, applications,
//     trustedTokenIssuers) key off a field the value type already carries
//     (InstanceArn / PermissionSetArn / RequestID / ApplicationArn /
//     TrustedTokenIssuerArn), so they are registered on b.registry here and
//     persistence.go drives them through b.registry.SnapshotAll() /
//     RestoreAll() directly.
//   - "Dirty" tables (instanceACAs, permissionBoundaries) key off a field
//     with no natural home on the value type: ABACConfig and
//     PermissionsBoundary carried no identity of their own. Each gained an
//     identity-only field tagged `json:"-"` purely so store.Table's keyFn
//     can derive a key from the value -- see the doc comments on those
//     fields in backend.go. They are NOT registered on b.registry;
//     persistence.go instead builds a throwaway DTO store.Registry
//     (mirroring the services/ses pilot) whose DTO types carry the identity
//     as a real JSON field, so Snapshot/Restore never depends on a
//     json:"-" field to round-trip correctly.
//
// permissionSets, applications, and trustedTokenIssuers additionally carry a
// secondary store.Index grouping by InstanceArn, replacing the linear scans
// those lookups (ListPermissionSets, cascadeDeleteInstance, per-instance
// duplicate-name checks, ...) previously performed over the raw map.
//
// The following resource fields are deliberately left as plain maps (not
// converted at all) because their values are not *T (slice-valued or
// non-pointer scalar/map-valued), which does not fit store.Table's
// keyed-by-*T-identity-value shape:
//   - assignments: map[string][]*AccountAssignment (slice-valued)
//   - assignmentCreationIDs: map[string]string (scalar-valued)
//   - instanceRegions: map[string][]RegionMetadata (slice-valued)
//   - customerManagedPolicies: map[string][]CustomerManagedPolicyReference (slice-valued)
//   - applicationAssignments: map[string][]*ApplicationAssignment (slice-valued)
//   - applicationScopes: map[string][]string (slice-valued)
//   - applicationAuthMethods: map[string]map[string]json.RawMessage (nested-map-valued)
//   - applicationGrants: map[string]map[string]json.RawMessage (nested-map-valued)
//   - applicationAssignConfig: map[string]bool (scalar-valued)
//   - applicationSessions: map[string]string (scalar-valued)
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func instanceKeyFn(v *Instance) string { return v.InstanceArn }

func permissionSetKeyFn(v *PermissionSet) string { return v.PermissionSetArn }

func creationStatusKeyFn(v *ProvisioningStatus) string { return v.RequestID }

func deletionStatusKeyFn(v *ProvisioningStatus) string { return v.RequestID }

func provisioningStatusKeyFn(v *ProvisioningStatus) string { return v.RequestID }

func applicationKeyFn(v *Application) string { return v.ApplicationArn }

func trustedTokenIssuerKeyFn(v *TrustedTokenIssuer) string { return v.TrustedTokenIssuerArn }

func instanceACAKeyFn(v *ABACConfig) string { return v.InstanceArn }

func permissionsBoundaryKeyFn(v *PermissionsBoundary) string { return v.PermissionSetArn }

// registerAllTables registers every converted resource map on b.registry
// exactly once. It must be called during construction only (immediately
// after b.registry is created), never on every Reset() -- store.Register
// panics on a duplicate name, so runtime resets go through
// b.registry.ResetAll() instead (see InMemoryBackend.Reset in backend.go).
// The "dirty" tables (instanceACAs, permissionBoundaries) are constructed
// here too but deliberately NOT registered -- see the file doc comment
// above; their lifecycle (including reset) is driven directly since they
// are plain *store.Table[V] fields on the backend.
func registerAllTables(b *InMemoryBackend) {
	b.instances = store.Register(b.registry, "instances", store.New(instanceKeyFn))

	b.permissionSets = store.Register(b.registry, "permissionSets", store.New(permissionSetKeyFn))
	b.permissionSetsByInstance = b.permissionSets.AddIndex(
		"byInstance",
		func(v *PermissionSet) string { return v.InstanceArn },
	)

	b.creationStatuses = store.Register(b.registry, "creationStatuses", store.New(creationStatusKeyFn))
	b.deletionStatuses = store.Register(b.registry, "deletionStatuses", store.New(deletionStatusKeyFn))
	b.provisioningStatuses = store.Register(b.registry, "provisioningStatuses", store.New(provisioningStatusKeyFn))

	b.applications = store.Register(b.registry, "applications", store.New(applicationKeyFn))
	b.applicationsByInstance = b.applications.AddIndex(
		"byInstance",
		func(v *Application) string { return v.InstanceArn },
	)

	b.trustedTokenIssuers = store.Register(b.registry, "trustedTokenIssuers", store.New(trustedTokenIssuerKeyFn))
	b.trustedTokenIssuersByInstance = b.trustedTokenIssuers.AddIndex(
		"byInstance",
		func(v *TrustedTokenIssuer) string { return v.InstanceArn },
	)

	b.instanceACAs = store.New(instanceACAKeyFn)
	b.permissionBoundaries = store.New(permissionsBoundaryKeyFn)
}
