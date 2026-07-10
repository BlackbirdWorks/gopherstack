package rekognition

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T (or nested map[string]map[string]*T) resource field on
// InMemoryBackend is replaced with a *store.Table[T], following the pattern
// established by services/sesv2 and services/codebuild (data-driven, direct
// registration) and services/emr (composite key + store.Index for former
// parent-nested maps). See pkgs/store's package doc for the underlying
// primitive.
//
// collections, streamProcessors, projects, projectVersions, datasets,
// livenessSessions, asyncJobs, and mediaAnalysisJobs already carried their
// own identity as a real (non-json:"-") field before this refactor
// (CollectionID, Name, ProjectARN, ProjectVersionARN, DatasetARN, SessionID,
// JobID), so all eight are "clean" tables registered directly on b.registry.
//
// faces was a map[string][]*storedFace (collectionID -> faces); storedFace
// already carried a real FaceID field, so it becomes a clean table too, with
// a secondary store.Index grouping by CollectionID for the per-collection
// listing/search operations that used to range over the map's value slice
// directly.
//
// projectPolicies was a nested map[string]map[string]*storedProjectPolicy
// (projectArn -> policyName -> policy); storedProjectPolicy already carried
// both ProjectARN and PolicyName as real fields, so it flattens into a single
// table keyed by the composite "projectArn|policyName", with a secondary
// store.Index grouping by ProjectARN for ListProjectPolicies.
//
// users was a nested map[string]map[string]*storedUser (collectionID ->
// userID -> user); storedUser did NOT carry a CollectionID field (it was
// implied only by the outer map key), so a CollectionID field was added to
// storedUser (storedUser is an internal persistence struct, never marshaled
// to an AWS-facing response directly -- see toUser -- so no json:"-" hiding
// is needed). users flattens into a single table keyed by the composite
// "collectionId|userId", with a secondary store.Index grouping by
// CollectionID for ListUsers/AssociateFaces/DisassociateFaces/SearchUsers*.
//
// Left as plain maps (not store.Table-backed):
//   - tags (map[string]map[string]string): values are plain string maps, not
//     *T, so they do not fit store.Table's keyed-by-identity-value shape.
//   - datasetEntries (map[string][]string): slice-valued, not *T.
//
// See persistence.go for how tags and datasetEntries are carried through
// Snapshot/Restore directly, alongside the store.Registry-driven tables.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func collectionKeyFn(v *storedCollection) string { return v.CollectionID }

func faceKeyFn(v *storedFace) string           { return v.FaceID }
func faceCollectionKeyFn(v *storedFace) string { return v.CollectionID }

func streamProcessorKeyFn(v *storedStreamProcessor) string { return v.Name }

func projectKeyFn(v *storedProject) string { return v.ProjectARN }

func projectVersionKeyFn(v *storedProjectVersion) string { return v.ProjectVersionARN }

// projectPolicyKey builds the composite "<projectArn>|<policyName>" key
// shared by every project policy nested under a project. Access sites use
// this directly so the exact same key is used for Get/Has/Delete.
func projectPolicyKey(projectARN, policyName string) string {
	return projectARN + "|" + policyName
}

func projectPolicyKeyFn(v *storedProjectPolicy) string {
	return projectPolicyKey(v.ProjectARN, v.PolicyName)
}

func projectPolicyProjectKeyFn(v *storedProjectPolicy) string { return v.ProjectARN }

func datasetKeyFn(v *storedDataset) string { return v.DatasetARN }

// userKey builds the composite "<collectionId>|<userId>" key shared by every
// user nested under a collection. Access sites use this directly so the
// exact same key is used for Get/Has/Delete.
func userKey(collectionID, userID string) string {
	return collectionID + "|" + userID
}

func userKeyFn(v *storedUser) string           { return userKey(v.CollectionID, v.UserID) }
func userCollectionKeyFn(v *storedUser) string { return v.CollectionID }

func livenessSessionKeyFn(v *storedLivenessSession) string { return v.SessionID }

func asyncJobKeyFn(v *storedAsyncJob) string { return v.JobID }

func mediaAnalysisJobKeyFn(v *storedMediaAnalysisJob) string { return v.JobID }

// registerAllTables constructs and registers every store.Table-backed
// resource field exactly once, at construction time. It must be called
// during construction only, never on every Reset(): store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// (backend.go) instead.
func registerAllTables(b *InMemoryBackend) {
	b.collections = store.Register(b.registry, "collections", store.New(collectionKeyFn))

	b.faces = store.Register(b.registry, "faces", store.New(faceKeyFn))
	b.facesByCollection = b.faces.AddIndex("byCollection", faceCollectionKeyFn)

	b.streamProcessors = store.Register(b.registry, "streamProcessors", store.New(streamProcessorKeyFn))

	b.projects = store.Register(b.registry, "projects", store.New(projectKeyFn))
	b.projectVersions = store.Register(b.registry, "projectVersions", store.New(projectVersionKeyFn))

	b.projectPolicies = store.Register(b.registry, "projectPolicies", store.New(projectPolicyKeyFn))
	b.projectPoliciesByProject = b.projectPolicies.AddIndex("byProject", projectPolicyProjectKeyFn)

	b.datasets = store.Register(b.registry, "datasets", store.New(datasetKeyFn))

	b.users = store.Register(b.registry, "users", store.New(userKeyFn))
	b.usersByCollection = b.users.AddIndex("byCollection", userCollectionKeyFn)

	b.livenessSessions = store.Register(b.registry, "livenessSessions", store.New(livenessSessionKeyFn))
	b.asyncJobs = store.Register(b.registry, "asyncJobs", store.New(asyncJobKeyFn))
	b.mediaAnalysisJobs = store.Register(b.registry, "mediaAnalysisJobs", store.New(mediaAnalysisJobKeyFn))
}
