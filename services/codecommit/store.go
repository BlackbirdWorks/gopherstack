package codecommit

import (
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// InMemoryBackend is the in-memory store for CodeCommit resources.
//
// Phase 3.3 datalayer refactor: every map[string]*T resource field is
// registered exactly once (see store_setup.go's registerAllTables) as a
// *store.Table[T] on registry. Maps whose value is not a *T of its own (sets,
// plain scalars/strings, slices, or a pure reverse/derived index) are left as
// plain maps below -- see store_setup.go's file doc for the full audit of
// which field went which way and why.
type InMemoryBackend struct {
	registry *store.Registry

	repositories *store.Table[Repository] // key: repositoryName
	// repositoriesByARN is a pure reverse-lookup cache (ARN -> repositoryName)
	// rebuildable in full from repositories; it is never persisted and is
	// rebuilt by rebuildRepositoriesByARN after Restore (see persistence.go).
	repositoriesByARN map[string]string

	approvalRuleTemplates *store.Table[ApprovalRuleTemplate] // key: templateName

	// repoTemplateAssoc maps repositoryName -> set of templateNames. Its
	// value (map[string]struct{}) has no identity of its own, so it is not a
	// store.Table candidate; it remains a plain persisted map.
	repoTemplateAssoc map[string]map[string]struct{}

	// branches was previously nested (repositoryName -> branchName ->
	// *Branch); it is now one flat table keyed by the composite
	// "repositoryName|branchName" string (see branchKey in store_setup.go),
	// with branchesByRepo grouping entries by repository for the "all
	// branches in repo X" lookups the nested map used to answer directly.
	branches       *store.Table[Branch]
	branchesByRepo *store.Index[Branch]

	// commits was previously nested (repositoryName -> commitId -> *Commit);
	// same composite-key + index treatment as branches.
	commits       *store.Table[Commit]
	commitsByRepo *store.Index[Commit]

	pullRequests *store.Table[PullRequest] // key: pullRequestId

	// prApprovals maps prID -> userARN -> approvalState; a bare string value
	// with no identity of its own, so it remains a plain persisted map.
	prApprovals map[string]map[string]string

	// prApprovalRules was previously nested (prID -> ruleName -> *rule); flat
	// table keyed by the composite "prID|ruleName" string (see
	// prApprovalRuleKey), with prApprovalRulesByPR grouping by pull request.
	// "Dirty" table (see store_setup.go's registerAllTables doc): NOT on
	// b.registry, persisted via a DTO in persistence.go.
	prApprovalRules     *store.Table[PullRequestApprovalRule]
	prApprovalRulesByPR *store.Index[PullRequestApprovalRule]

	// prOverrides maps prID -> overridden; plain persisted map (bare bool value).
	prOverrides map[string]bool
	// prOverriders maps prID -> overrider ARN; plain persisted map (bare string value).
	prOverriders map[string]string
	// prEvents maps prID -> events; plain persisted map (slice value, no identity).
	prEvents map[string][]PullRequestEvent

	// comments is keyed by commentID. "Dirty" table (see store_setup.go's
	// registerAllTables doc): NOT on b.registry, persisted via a DTO in
	// persistence.go, since Comment's PRid/RepoName/AfterCommitID fields are
	// tagged json:"-" (hidden from the wire API but real backend state).
	comments *store.Table[Comment]

	// commentReactions maps commentID -> reactions; plain persisted map (slice value).
	commentReactions map[string][]Reaction

	// files was previously nested (repoName -> filePath -> *File); flat table
	// keyed by the composite "repoName|filePath" string (see fileKey), with
	// filesByRepo grouping by repository. File gained a RepoName field
	// purely to carry this identity (see models.go). "Dirty" table (see
	// store_setup.go's registerAllTables doc): NOT on b.registry, persisted
	// via a DTO in persistence.go.
	files       *store.Table[File]
	filesByRepo *store.Index[File]

	// fileHistory maps repoName -> filePath -> []commitID (ordered, oldest
	// first); plain persisted map (slice value, no identity).
	fileHistory map[string]map[string][]string
	// triggers maps repoName -> triggers; plain persisted map (slice value).
	triggers      map[string][]RepositoryTrigger
	mu            *lockmetrics.RWMutex
	accountID     string
	region        string
	nextPRCounter int
}

// NewInMemoryBackend creates a new in-memory CodeCommit backend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:          store.NewRegistry(),
		repositoriesByARN: make(map[string]string),
		repoTemplateAssoc: make(map[string]map[string]struct{}),
		prApprovals:       make(map[string]map[string]string),
		prOverrides:       make(map[string]bool),
		prOverriders:      make(map[string]string),
		prEvents:          make(map[string][]PullRequestEvent),
		commentReactions:  make(map[string][]Reaction),
		fileHistory:       make(map[string]map[string][]string),
		triggers:          make(map[string][]RepositoryTrigger),
		accountID:         accountID,
		region:            region,
		mu:                lockmetrics.New("codecommit"),
	}
	registerAllTables(b)

	return b
}

// Reset clears all backend state, returning it to a pristine empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, r := range b.repositories.All() {
		r.Tags.Close()
	}

	b.registry.ResetAll()
	// The "dirty" tables (see store_setup.go's registerAllTables doc) are
	// deliberately NOT on b.registry, so each needs its own Reset() call here.
	b.prApprovalRules.Reset()
	b.comments.Reset()
	b.files.Reset()
	b.repositoriesByARN = make(map[string]string)
	b.repoTemplateAssoc = make(map[string]map[string]struct{})
	b.prApprovals = make(map[string]map[string]string)
	b.prOverrides = make(map[string]bool)
	b.prOverriders = make(map[string]string)
	b.prEvents = make(map[string][]PullRequestEvent)
	b.commentReactions = make(map[string][]Reaction)
	b.fileHistory = make(map[string]map[string][]string)
	b.triggers = make(map[string][]RepositoryTrigger)
	b.nextPRCounter = 0
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// AddRepositoryInternal seeds a Repository directly into the backend without
// going through normal validation.
func (b *InMemoryBackend) AddRepositoryInternal(r *Repository) {
	b.mu.Lock("AddRepositoryInternal")
	defer b.mu.Unlock()

	b.repositories.Put(r)
	b.repositoriesByARN[r.ARN] = r.RepositoryName
}

// AddApprovalRuleTemplateInternal seeds an ApprovalRuleTemplate directly.
func (b *InMemoryBackend) AddApprovalRuleTemplateInternal(t *ApprovalRuleTemplate) {
	b.mu.Lock("AddApprovalRuleTemplateInternal")
	defer b.mu.Unlock()

	b.approvalRuleTemplates.Put(t)
}

// AddBranchInternal seeds a Branch directly into the backend.
func (b *InMemoryBackend) AddBranchInternal(repositoryName string, br *Branch) {
	b.mu.Lock("AddBranchInternal")
	defer b.mu.Unlock()

	br.RepositoryName = repositoryName
	b.branches.Put(br)
}

// AddCommitInternal seeds a Commit directly into the backend.
func (b *InMemoryBackend) AddCommitInternal(repositoryName string, c *Commit) {
	b.mu.Lock("AddCommitInternal")
	defer b.mu.Unlock()

	c.RepositoryName = repositoryName
	b.commits.Put(c)
}

// AddPullRequestInternal seeds a PullRequest directly into the backend.
func (b *InMemoryBackend) AddPullRequestInternal(pr *PullRequest) {
	b.mu.Lock("AddPullRequestInternal")
	defer b.mu.Unlock()

	b.pullRequests.Put(pr)
}
