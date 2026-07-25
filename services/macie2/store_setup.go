package macie2

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], following the pattern established by services/sesv2
// (clean direct-register tables) and services/codebuild. See pkgs/store's
// package doc for the underlying primitive.
//
// Every convertible value type here already carries its own identity as a
// real (non-json:"-") field -- storedAllowList/storedCustomDataID/
// storedFindingsFilter/storedFinding embed their live *Detail type's ID
// field, S3BucketMetadata.BucketArn, ClassificationJob.JobID, Member.
// AccountID, Invitation.InvitationID, OrgAdminAccount.AccountID,
// AutoDiscoveryAccount.AccountID, ClassificationScope.ID, ResourceProfile.
// ResourceArn, and SensitivityInspectionTemplate.ID were all already set
// consistently at every write site before this refactor. So none of the 13
// tables below need a "dirty" DTO-registry treatment: all are "clean"
// tables registered directly on b.registry, and persistence.go drives every
// one of them through b.registry.SnapshotAll() / RestoreAll().
//
// CustomDataIdentifier.Deleted (embedded into storedCustomDataID) is a plain
// soft-delete flag on an otherwise directly-registered value -- it rides
// along inside the table entry itself (no hidden identity, so no DTO is
// needed for it either).
//
// Left as plain maps (not store.Table-backed), and persistence-audited in
// persistence.go's backendSnapshot:
//   - tags (map[string]map[string]string): values are plain string maps,
//     not *T, so they do not fit store.Table's keyed-by-identity-value
//     shape.
//   - resourceDetections (map[string][]ResourceProfileDetection):
//     slice-valued, not *T, and ResourceProfileDetection has no identity of
//     its own within a bucket's detection list.
//
// session, administrator, orgConfig, autoDiscoveryConfig, classExportConfig,
// findingsPubConfig, and revealConfig are single pointers, not collections,
// so they are untouched by this refactor.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func storedAllowListKeyFn(v *storedAllowList) string { return v.ID }

func storedCustomDataIDKeyFn(v *storedCustomDataID) string { return v.ID }

func storedFindingsFilterKeyFn(v *storedFindingsFilter) string { return v.ID }

func storedFindingKeyFn(v *storedFinding) string { return v.ID }

func s3BucketMetadataKeyFn(v *S3BucketMetadata) string { return v.BucketArn }

func classificationJobKeyFn(v *ClassificationJob) string { return v.JobID }

func memberKeyFn(v *Member) string { return v.AccountID }

func invitationKeyFn(v *Invitation) string { return v.InvitationID }

func orgAdminAccountKeyFn(v *OrgAdminAccount) string { return v.AccountID }

func autoDiscoveryAccountKeyFn(v *AutoDiscoveryAccount) string { return v.AccountID }

func classificationScopeKeyFn(v *ClassificationScope) string { return v.ID }

func resourceProfileKeyFn(v *ResourceProfile) string { return v.ResourceArn }

func sensitivityInspectionTemplateKeyFn(v *SensitivityInspectionTemplate) string { return v.ID }

// registerAllTables constructs and registers every store.Table-backed
// resource field exactly once, at construction time. It must be called
// during construction only, never on every Reset(): store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// (backend.go) instead.
func registerAllTables(b *InMemoryBackend) {
	b.allowLists = store.Register(b.registry, "allowLists", store.New(storedAllowListKeyFn))
	b.customDataIDs = store.Register(b.registry, "customDataIDs", store.New(storedCustomDataIDKeyFn))
	b.findingsFilters = store.Register(b.registry, "findingsFilters", store.New(storedFindingsFilterKeyFn))
	b.findings = store.Register(b.registry, "findings", store.New(storedFindingKeyFn))
	b.s3Buckets = store.Register(b.registry, "s3Buckets", store.New(s3BucketMetadataKeyFn))
	b.classificationJobs = store.Register(b.registry, "classificationJobs", store.New(classificationJobKeyFn))
	b.members = store.Register(b.registry, "members", store.New(memberKeyFn))
	b.invitations = store.Register(b.registry, "invitations", store.New(invitationKeyFn))
	b.orgAdminAccounts = store.Register(b.registry, "orgAdminAccounts", store.New(orgAdminAccountKeyFn))
	b.autoDiscoveryAccounts = store.Register(
		b.registry, "autoDiscoveryAccounts", store.New(autoDiscoveryAccountKeyFn),
	)
	b.classScopes = store.Register(b.registry, "classScopes", store.New(classificationScopeKeyFn))
	b.resourceProfiles = store.Register(b.registry, "resourceProfiles", store.New(resourceProfileKeyFn))
	b.sensitivityTemplates = store.Register(
		b.registry, "sensitivityTemplates", store.New(sensitivityInspectionTemplateKeyFn),
	)
}
