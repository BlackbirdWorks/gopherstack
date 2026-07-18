package codecommit

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is registered exactly once,
// here, as a *store.Table[T] on b.registry.
//
// Two families of collections were previously nested per-parent maps
// (map[repositoryName]map[childID]*T or map[pullRequestID]map[ruleName]*T):
// branches, commits, and files nest under repositoryName; prApprovalRules
// nests under pullRequestID. store.Table has no notion of nesting, so each
// becomes a single flat table keyed by a composite "<parentID>|<childID>"
// string (see branchKey/commitKey/fileKey/prApprovalRuleKey below), with a
// secondary [store.Index] grouping by parent ID for the "all children of
// parent X" lookups the nested maps used to answer directly. This mirrors
// the services/apigateway conversion (map[string]*apiData holding its own
// map[string]*T flattened to "<restAPIID>#<childID>" + a "byAPI" index).
//
// Branch and Commit already carried their own RepositoryName field (a real,
// wire-visible identity field), so no change to those types was needed.
// PullRequestApprovalRule and File did not carry their parent's ID at all
// (nothing in the AWS wire shape needs it), so each gained a new field
// purely for this composite key -- PRID and RepoName respectively -- tagged
// json:"-" since it is never part of the CodeCommit API response.
//
// repositories, approvalRuleTemplates, pullRequests, and comments were
// already flat (not nested), so each is a direct *store.Table[T] keyed by
// its own real identity field (RepositoryName / ApprovalRuleTemplateName /
// PullRequestID / CommentID). Comment's PRid/RepoName/AfterCommitID fields
// are also tagged json:"-" (pre-existing, not new), which only matters for
// persistence.go's DTO (see that file's doc comment) -- the live table
// itself only needs CommentID, which is not hidden.
//
// A handful of fields are deliberately NOT registered here and remain plain
// maps -- see the field comments on InMemoryBackend in store.go for the
// full audit of which field went which way and why (repositoriesByARN is a
// rebuildable reverse index; repoTemplateAssoc/prApprovals/prOverrides/
// prOverriders/prEvents/commentReactions/fileHistory/triggers all have a
// value type with no identity of its own -- a set, a bare scalar, or a
// slice -- so there is nothing for store.Table to key on).
//
// comments, files, and prApprovalRules are "dirty" tables (per the
// services/apigateway convention): each holds a json:"-" identity field
// (Comment.PRid/RepoName/AfterCommitID; File.RepoName; PullRequestApprovalRule.PRID)
// that marshaling the live type directly would silently drop. They are
// built with store.New only, deliberately NOT store.Register-ed onto
// b.registry, so b.registry.SnapshotAll/RestoreAll/ResetAll never touch
// them directly; persistence.go instead builds an ephemeral DTO registry
// (mirroring services/neptune's regionalDTO / services/apigateway's
// resourceSnapshot pattern) that gives each hidden field a real JSON tag,
// and Reset (store.go) resets each of the three explicitly.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func repositoryKeyFn(v *Repository) string { return v.RepositoryName }

func approvalRuleTemplateKeyFn(v *ApprovalRuleTemplate) string { return v.ApprovalRuleTemplateName }

// branchKey/commitKey/fileKey/prApprovalRuleKey build the composite
// store.Table primary key ("parent|child") shared by every flattened
// nested-map collection. Access sites use these directly so the exact same
// key is used for Put/Get/Delete/Has.
func branchKey(repositoryName, branchName string) string { return repositoryName + "|" + branchName }
func branchKeyFn(v *Branch) string                       { return branchKey(v.RepositoryName, v.BranchName) }
func branchRepoIndexKeyFn(v *Branch) string              { return v.RepositoryName }

func commitKey(repositoryName, commitID string) string { return repositoryName + "|" + commitID }
func commitKeyFn(v *Commit) string                     { return commitKey(v.RepositoryName, v.CommitID) }
func commitRepoIndexKeyFn(v *Commit) string            { return v.RepositoryName }

func pullRequestKeyFn(v *PullRequest) string { return v.PullRequestID }

func prApprovalRuleKey(prID, ruleName string) string { return prID + "|" + ruleName }
func prApprovalRuleKeyFn(v *PullRequestApprovalRule) string {
	return prApprovalRuleKey(v.PRID, v.RuleName)
}
func prApprovalRulePRIndexKeyFn(v *PullRequestApprovalRule) string { return v.PRID }

func commentKeyFn(v *Comment) string { return v.CommentID }

func fileKey(repoName, filePath string) string { return repoName + "|" + filePath }
func fileKeyFn(v *File) string                 { return fileKey(v.RepoName, v.FilePath) }
func fileRepoIndexKeyFn(v *File) string        { return v.RepoName }

// registerAllTables registers every converted resource collection on
// b.registry exactly once. It must be called during construction only
// (immediately after b.registry is created), never on every Reset() --
// store.Register panics on a duplicate name, so runtime resets go through
// b.registry.ResetAll() instead (see InMemoryBackend.Reset in store.go).
func registerAllTables(b *InMemoryBackend) {
	for _, register := range tableRegistrations {
		register(b)
	}
}

// tableRegistrations is the data-driven list registerAllTables walks: one
// closure per resource table, each binding its own store.New/store.Register
// call (and, for the composite-keyed collections, its companion
// store.Table.AddIndex call) to the concrete field and value type.
//
//nolint:gochecknoglobals // registration table, analogous to errCodeLookup-style lookup tables elsewhere
var tableRegistrations = []func(*InMemoryBackend){
	func(b *InMemoryBackend) {
		b.repositories = store.Register(b.registry, "repositories", store.New(repositoryKeyFn))
	},
	func(b *InMemoryBackend) {
		b.approvalRuleTemplates = store.Register(
			b.registry, "approvalRuleTemplates", store.New(approvalRuleTemplateKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.branches = store.Register(b.registry, "branches", store.New(branchKeyFn))
		b.branchesByRepo = b.branches.AddIndex("byRepo", branchRepoIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.commits = store.Register(b.registry, "commits", store.New(commitKeyFn))
		b.commitsByRepo = b.commits.AddIndex("byRepo", commitRepoIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.pullRequests = store.Register(b.registry, "pullRequests", store.New(pullRequestKeyFn))
	},
	// "Dirty" tables: store.New only, NOT store.Register -- see the file doc
	// comment above. Reset() (store.go) resets each of these explicitly;
	// persistence.go builds a separate ephemeral DTO registry for them.
	func(b *InMemoryBackend) {
		b.prApprovalRules = store.New(prApprovalRuleKeyFn)
		b.prApprovalRulesByPR = b.prApprovalRules.AddIndex("byPR", prApprovalRulePRIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.comments = store.New(commentKeyFn)
	},
	func(b *InMemoryBackend) {
		b.files = store.New(fileKeyFn)
		b.filesByRepo = b.files.AddIndex("byRepo", fileRepoIndexKeyFn)
	},
}
