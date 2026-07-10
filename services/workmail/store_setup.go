package workmail

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is registered exactly
// once, here, as a *store.Table[T]. See pkgs/store's package doc for the
// underlying primitive, and services/sesv2/services/codebuild/
// services/workspaces for the clean-direct-register template, and
// services/emr/services/efs/services/batch for the region(here: org)-nested
// composite-key + Index template this file mostly follows.
//
// organizations is the WorkMail organization itself: Organization already
// carries its own real identity field (OrgID), so it registers directly on
// b.registry with no composite key.
//
// users, groups, resources, impersonation, mailDomains, accessRules,
// availabilityConfigs, mobileDeviceRules, mobileDeviceOverrides, exportJobs,
// and personalTokens were previously nested by organization
// (map[orgID]map[childID]*T). Each becomes a flat *store.Table[T] keyed by
// the composite "orgID|id" string (see orgKey in backend.go), with a
// companion *store.Index grouping entries by orgID for the per-org scans the
// nested maps used to answer directly. Every value type already carried its
// own real identity field (UserID, GroupID, ResourceID, RoleID, DomainName,
// Name, RuleID, JobID, TokenID) except MobileDeviceAccessOverride, whose
// composite-key component is instead mobileOverrideKey(UserID, DeviceID)
// (the same derivation the pre-refactor map key used). None of these had a
// wire-visible identity field to preserve, so orgID was added as a new
// unexported field on each (see interfaces.go) rather than the json:"-"
// hidden-field treatment services/codecommit uses for wire-visible APIs --
// WorkMail's domain types were never JSON-tagged to begin with (handler.go
// uses separate request/response DTOs for the wire shape).
//
// emailMonitoring, retentionPolicies, and idpConfig are one-record-per-org
// configs (map[orgID]*T with no further nesting), so each is a *store.Table
// keyed directly by orgID -- no composite key or secondary index needed,
// since orgID itself is already the unique lookup key (mirrors EMR's
// BlockPublicAccessConfiguration treatment).
//
// permissions was nested two levels deep (map[orgID]map[entityID]map[granteeID]*Permission).
// It flattens to one *store.Table[Permission] keyed by the composite
// "orgID|entityID|granteeID" string (permissionKey in backend.go), with a
// "byOrgEntity" *store.Index grouping by "orgID|entityID" -- the pair every
// hot-path lookup (Put/Get/List/DeleteMailboxPermissions) already knows up
// front. DeleteOrganization's org-wide cleanup uses a Table.Range scan
// instead of a secondary index (see deletePermissionsForOrg in backend.go),
// since that is the only org-wide access pattern and doesn't warrant a
// second index.
//
// globalAliases and issuedTokens are unexported (package-private) types
// (trackedAlias, issuedImpersonationToken) that were already flat
// map[string]*T collections keyed by their own natural identity (the alias
// string, the issued token string). Since neither type is part of the
// package's public API, their identity fields were simply exported (see
// backend.go) rather than hidden behind a DTO wrapper, so both register
// directly with no composite key.
//
// None of the tables in this file are on b.registry: b.registry holds ONLY
// organizations, globalAliases, and issuedTokens (see the InMemoryBackend
// field comments in backend.go) -- every table below hides at least one
// field from plain JSON marshaling (orgID, and for permissions also
// entityID), so registry.SnapshotAll/RestoreAll would silently drop it.
// InMemoryBackend.resetLocked resets each of these individually instead, and
// persistence.go builds a separate ephemeral DTO registry for Snapshot/
// Restore.
//
// A handful of fields are deliberately left as plain (non-store.Table) maps
// -- see the field comments on InMemoryBackend in backend.go for the full
// audit of which stayed raw and why (each holds a value with no identity of
// its own: a set, a bare scalar, a slice, or a plain reverse-lookup string).
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func organizationKeyFn(v *Organization) string { return v.OrgID }

func userKeyFn(v *User) string         { return orgKey(v.orgID, v.UserID) }
func userOrgIndexKeyFn(v *User) string { return v.orgID }

func groupKeyFn(v *Group) string         { return orgKey(v.orgID, v.GroupID) }
func groupOrgIndexKeyFn(v *Group) string { return v.orgID }

func resourceKeyFn(v *Resource) string         { return orgKey(v.orgID, v.ResourceID) }
func resourceOrgIndexKeyFn(v *Resource) string { return v.orgID }

func impersonationKeyFn(v *ImpersonationRole) string         { return orgKey(v.orgID, v.RoleID) }
func impersonationOrgIndexKeyFn(v *ImpersonationRole) string { return v.orgID }

func mailDomainKeyFn(v *MailDomain) string         { return orgKey(v.orgID, v.DomainName) }
func mailDomainOrgIndexKeyFn(v *MailDomain) string { return v.orgID }

func accessRuleKeyFn(v *AccessControlRule) string         { return orgKey(v.orgID, v.Name) }
func accessRuleOrgIndexKeyFn(v *AccessControlRule) string { return v.orgID }

func availabilityConfigKeyFn(v *AvailabilityConfiguration) string {
	return orgKey(v.orgID, v.DomainName)
}
func availabilityConfigOrgIndexKeyFn(v *AvailabilityConfiguration) string { return v.orgID }

func mobileDeviceRuleKeyFn(v *MobileDeviceAccessRule) string         { return orgKey(v.orgID, v.RuleID) }
func mobileDeviceRuleOrgIndexKeyFn(v *MobileDeviceAccessRule) string { return v.orgID }

func mobileDeviceOverrideKeyFn(v *MobileDeviceAccessOverride) string {
	return orgKey(v.orgID, mobileOverrideKey(v.UserID, v.DeviceID))
}
func mobileDeviceOverrideOrgIndexKeyFn(v *MobileDeviceAccessOverride) string { return v.orgID }

func exportJobKeyFn(v *MailboxExportJob) string         { return orgKey(v.orgID, v.JobID) }
func exportJobOrgIndexKeyFn(v *MailboxExportJob) string { return v.orgID }

func personalTokenKeyFn(v *PersonalAccessToken) string         { return orgKey(v.orgID, v.TokenID) }
func personalTokenOrgIndexKeyFn(v *PersonalAccessToken) string { return v.orgID }

func emailMonitoringKeyFn(v *EmailMonitoringConfiguration) string { return v.orgID }

func retentionPolicyKeyFn(v *RetentionPolicy) string { return v.orgID }

func idpConfigKeyFn(v *IdentityProviderConfiguration) string { return v.orgID }

func globalAliasKeyFn(v *trackedAlias) string { return v.Alias }

func issuedTokenKeyFn(v *issuedImpersonationToken) string { return v.Token }

func permissionKeyFn(v *Permission) string { return permissionKey(v.orgID, v.entityID, v.GranteeID) }
func permissionOrgEntityIndexKeyFn(v *Permission) string {
	return permissionOrgEntityKey(v.orgID, v.entityID)
}

// registerAllTables registers every converted resource collection exactly
// once. It must be called during construction only (immediately after
// b.registry is created -- see NewInMemoryBackend), never on every Reset()
// -- store.Register panics on a duplicate name, so runtime resets go through
// b.registry.ResetAll() plus per-table Reset() calls instead (see
// InMemoryBackend.resetLocked in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.organizations = store.Register(b.registry, "organizations", store.New(organizationKeyFn))
	b.globalAliases = store.Register(b.registry, "globalAliases", store.New(globalAliasKeyFn))
	b.issuedTokens = store.Register(b.registry, "issuedTokens", store.New(issuedTokenKeyFn))

	// The org-nested / composite-keyed tables below are deliberately built
	// with store.New only, NOT store.Register -- see this file's doc comment.
	b.users = store.New(userKeyFn)
	b.usersByOrg = b.users.AddIndex("byOrg", userOrgIndexKeyFn)

	b.groups = store.New(groupKeyFn)
	b.groupsByOrg = b.groups.AddIndex("byOrg", groupOrgIndexKeyFn)

	b.resources = store.New(resourceKeyFn)
	b.resourcesByOrg = b.resources.AddIndex("byOrg", resourceOrgIndexKeyFn)

	b.impersonation = store.New(impersonationKeyFn)
	b.impersonationByOrg = b.impersonation.AddIndex("byOrg", impersonationOrgIndexKeyFn)

	b.mailDomains = store.New(mailDomainKeyFn)
	b.mailDomainsByOrg = b.mailDomains.AddIndex("byOrg", mailDomainOrgIndexKeyFn)

	b.accessRules = store.New(accessRuleKeyFn)
	b.accessRulesByOrg = b.accessRules.AddIndex("byOrg", accessRuleOrgIndexKeyFn)

	b.availabilityConfigs = store.New(availabilityConfigKeyFn)
	b.availabilityConfigsByOrg = b.availabilityConfigs.AddIndex("byOrg", availabilityConfigOrgIndexKeyFn)

	b.mobileDeviceRules = store.New(mobileDeviceRuleKeyFn)
	b.mobileDeviceRulesByOrg = b.mobileDeviceRules.AddIndex("byOrg", mobileDeviceRuleOrgIndexKeyFn)

	b.mobileDeviceOverrides = store.New(mobileDeviceOverrideKeyFn)
	b.mobileDeviceOverridesByOrg = b.mobileDeviceOverrides.AddIndex("byOrg", mobileDeviceOverrideOrgIndexKeyFn)

	b.exportJobs = store.New(exportJobKeyFn)
	b.exportJobsByOrg = b.exportJobs.AddIndex("byOrg", exportJobOrgIndexKeyFn)

	b.personalTokens = store.New(personalTokenKeyFn)
	b.personalTokensByOrg = b.personalTokens.AddIndex("byOrg", personalTokenOrgIndexKeyFn)

	b.emailMonitoring = store.New(emailMonitoringKeyFn)
	b.retentionPolicies = store.New(retentionPolicyKeyFn)
	b.idpConfig = store.New(idpConfigKeyFn)

	b.permissions = store.New(permissionKeyFn)
	b.permissionsByOrgEntity = b.permissions.AddIndex("byOrgEntity", permissionOrgEntityIndexKeyFn)
}
