package serverlessrepo

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T (or nested map[string]map[string]*T) resource field on
// InMemoryBackend is replaced with a *store.Table[T], following the pattern
// established by services/ec2 (commit 12e611a4, data-driven registration
// slice), services/ses/services/sesv2 (composite "parent#child" keys with a
// byParent secondary index for previously nested maps), and
// services/codecommit (hidden json:"-" identity fields added purely to give
// a value type's own key function something to key on). See pkgs/store's
// package doc for the underlying primitive.
//
//   - applications is "clean": Application already carried its own identity
//     (Name), so it is registered directly on b.registry and persistence.go
//     drives it through b.registry.SnapshotAll()/RestoreAll().
//
//   - appVersions, cfTemplates, and cfChangeSets were previously nested maps
//     (appName -> childID -> *T). None of the three value types carried
//     their parent's appName (only ApplicationID, the application's ARN,
//     which is not the same string as appName and is not safe to
//     reverse-parse), so each gained a new AppName field purely for this
//     refactor -- tagged json:"-" since it is not part of the Serverless
//     Application Repository wire API. All three are "dirty" tables for
//     that reason: store.New only, deliberately NOT store.Register-ed onto
//     b.registry, so persistence.go instead round-trips them through an
//     ephemeral DTO registry that gives AppName a real JSON field.
//
//   - appVersions and cfChangeSets are additionally flattened onto a
//     composite "appName#childID" primary key: SemanticVersion is unique
//     only within an application (two applications may each have version
//     "1.0.0"), and ChangeSetID is derived from a caller-supplied
//     changeSetName/stackName that is likewise unique only within an
//     application, not globally. cfTemplates keeps TemplateID (already
//     globally unique -- it is generated as "<appName>-<unixNano>") as its
//     primary key and adds AppName only to drive the byApp index.
//
//   - all three gain a "byApp" secondary store.Index grouping by AppName,
//     replacing the direct lookup/iteration a nested map used to offer for
//     "all children of application X" (ListApplicationVersions,
//     DeleteApplication's cascade delete).
//
// appPolicies (map[string][]*ApplicationPolicyStatement) and
// appDependencies (map[string]map[string][]*ApplicationDependency) are left
// as plain maps: their values are slices, not *T, so they do not fit
// store.Table's keyed-by-identity-value shape. appPolicies is additionally
// order-sensitive (PutApplicationPolicy replaces the slice wholesale and
// GetApplicationPolicy returns it in that same order) and appDependencies'
// traversal (collectDependencies) already re-sorts its own output, so
// neither would benefit from a store.Index even if the shape fit.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func applicationKeyFn(v *Application) string { return v.Name }

// versionKey builds the composite "<appName>#<semanticVersion>" key shared by
// every version nested under an application. Access sites use this directly
// so the exact same key is used for Put/Get/Has/Delete.
func versionKey(appName, semanticVersion string) string { return appName + "#" + semanticVersion }

func versionKeyFn(v *ApplicationVersion) string { return versionKey(v.AppName, v.SemanticVersion) }

func versionAppIndexKeyFn(v *ApplicationVersion) string { return v.AppName }

// cfTemplateKeyFn keys directly off TemplateID, which is already globally
// unique by construction (see CreateCloudFormationTemplate).
func cfTemplateKeyFn(v *CloudFormationTemplate) string { return v.TemplateID }

func cfTemplateAppIndexKeyFn(v *CloudFormationTemplate) string { return v.AppName }

// changeSetKey builds the composite "<appName>#<changeSetID>" key shared by
// every change set nested under an application. Access sites use this
// directly so the exact same key is used for Put/Get/Has/Delete.
func changeSetKey(appName, changeSetID string) string { return appName + "#" + changeSetID }

func changeSetKeyFn(v *CloudFormationChangeSet) string {
	return changeSetKey(v.AppName, v.ChangeSetID)
}

func changeSetAppIndexKeyFn(v *CloudFormationChangeSet) string { return v.AppName }

// registerAllTables constructs every store.Table-backed resource field
// exactly once, at construction time. The "clean" applications table is
// registered on b.registry; the three "dirty" tables are constructed
// alongside it but deliberately NOT registered -- see the file doc comment
// above. It must be called during construction only, never on every Reset():
// store.Register panics on a duplicate name, so runtime resets go through
// resetTablesLocked (backend.go) instead.
func registerAllTables(b *InMemoryBackend) {
	b.applications = store.Register(b.registry, "applications", store.New(applicationKeyFn))

	b.appVersions = store.New(versionKeyFn)
	b.appVersionsByApp = b.appVersions.AddIndex("byApp", versionAppIndexKeyFn)

	b.cfTemplates = store.New(cfTemplateKeyFn)
	b.cfTemplatesByApp = b.cfTemplates.AddIndex("byApp", cfTemplateAppIndexKeyFn)

	b.cfChangeSets = store.New(changeSetKeyFn)
	b.cfChangeSetsByApp = b.cfChangeSets.AddIndex("byApp", changeSetAppIndexKeyFn)
}
