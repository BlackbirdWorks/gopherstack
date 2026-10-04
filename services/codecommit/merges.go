package codecommit

import (
	"fmt"
	"maps"
	"time"

	"github.com/google/uuid"
)

// MergePullRequestOptions carries the optional author/message fields of the
// squash and three-way pull request merges, plus the merging principal.
type MergePullRequestOptions struct {
	CommitMessage string
	AuthorName    string
	Email         string
	MergedBy      string
	Settings      MergeSettings
}

// MergePullRequestByFastForward merges a pull request by fast-forward.
func (b *InMemoryBackend) MergePullRequestByFastForward(
	prID, repoName, sourceCommitID string, opts MergePullRequestOptions,
) (*PullRequest, error) {
	return b.mergePullRequest(prID, repoName, sourceCommitID, mergeOptionFastForward, opts)
}

// MergePullRequestBySquash merges a pull request by squash.
func (b *InMemoryBackend) MergePullRequestBySquash(
	prID, repoName, sourceCommitID string, opts MergePullRequestOptions,
) (*PullRequest, error) {
	return b.mergePullRequest(prID, repoName, sourceCommitID, mergeOptionSquash, opts)
}

// MergePullRequestByThreeWay merges a pull request by three-way merge.
func (b *InMemoryBackend) MergePullRequestByThreeWay(
	prID, repoName, sourceCommitID string, opts MergePullRequestOptions,
) (*PullRequest, error) {
	return b.mergePullRequest(prID, repoName, sourceCommitID, mergeOptionThreeWay, opts)
}

// mergePullRequest closes the PR and, when its references resolve, moves the
// destination branch to the merge result and records MergeMetadata.
func (b *InMemoryBackend) mergePullRequest(
	prID, repoName, sourceCommitID, option string, opts MergePullRequestOptions,
) (*PullRequest, error) {
	b.mu.Lock("MergePullRequest")
	defer b.mu.Unlock()

	pr, ok := b.pullRequests.Get(prID)
	if !ok {
		return nil, fmt.Errorf("%w: pull request %s not found", ErrPullRequestNotFound, prID)
	}
	if pr.PullRequestStatus == prStatusClosed {
		return nil, fmt.Errorf("%w: pull request %s is already closed", ErrPullRequestAlreadyMerged, prID)
	}

	evals := make(map[int]*mergeEvaluation)

	for i := range pr.PullRequestTargets {
		t := &pr.PullRequestTargets[i]
		if repoName != "" && t.RepositoryName != repoName {
			continue
		}

		b.fillTargetCommits(t)

		source := sourceCommitID
		if source == "" {
			source = t.SourceCommit
		}

		if option == mergeOptionFastForward || source == "" || t.DestinationCommit == "" {
			evals[i] = nil

			continue
		}

		ev, err := b.evaluatePullRequestMerge(t, source, opts.Settings)
		if err != nil {
			return nil, err
		}

		evals[i] = ev
	}

	for i := range pr.PullRequestTargets {
		t := &pr.PullRequestTargets[i]
		if ev, has := evals[i]; has {
			t.MergeMetadata = b.applyPullRequestMerge(t, sourceCommitID, option, opts, ev)
		}
	}

	pr.PullRequestStatus = prStatusClosed
	pr.LastActivityDate = time.Now().UTC()

	return b.snapshotPullRequest(pr), nil
}

// evaluatePullRequestMerge merges a target's tips for the squash and
// three-way options; unresolved conflicts fail with ManualMergeRequired.
func (b *InMemoryBackend) evaluatePullRequestMerge(
	t *PullRequestTarget, source string, settings MergeSettings,
) (*mergeEvaluation, error) {
	ev := b.evaluateMergeLocked(t.RepositoryName, source, t.DestinationCommit, settings)
	if len(ev.unresolved) > 0 {
		return nil, fmt.Errorf("%w: %d file(s) have unresolved conflicts", ErrManualMergeRequired, len(ev.unresolved))
	}

	return ev, nil
}

// applyPullRequestMerge creates the merge commit for one target. Caller holds the lock.
func (b *InMemoryBackend) applyPullRequestMerge(
	t *PullRequestTarget, sourceCommitID, option string, opts MergePullRequestOptions, ev *mergeEvaluation,
) *MergeMetadata {
	meta := &MergeMetadata{IsMerged: true, MergeOption: option, MergedBy: opts.MergedBy}

	source := sourceCommitID
	if source == "" {
		source = t.SourceCommit
	}
	destBranch, dest := t.DestinationReference, t.DestinationCommit
	if source == "" || dest == "" {
		return meta
	}

	if option == mergeOptionFastForward {
		meta.MergeCommitID = source
		b.branches.Put(&Branch{BranchName: destBranch, CommitID: source, RepositoryName: t.RepositoryName})
		b.syncFilesToCommitLocked(t.RepositoryName, source)

		return meta
	}

	parents := []string{dest}
	message := "Merged PR using squash strategy"
	if option == mergeOptionThreeWay {
		parents = append(parents, source)
		message = "Merged PR using three-way strategy"
	}
	if opts.CommitMessage != "" {
		message = opts.CommitMessage
	}

	commit := b.newMergeCommitLocked(t.RepositoryName, parents, ev, message, opts.AuthorName, opts.Email)
	b.advanceBranchLocked(t.RepositoryName, destBranch, commit, b.treeOfLocked(t.RepositoryName, dest))
	meta.MergeCommitID = commit.CommitID

	return meta
}

// fillTargetCommits resolves a target's destination branch and current
// source, destination and merge-base commits. Caller holds the lock.
func (b *InMemoryBackend) fillTargetCommits(t *PullRequestTarget) {
	if t.DestinationReference == "" {
		if repo, ok := b.repositories.Get(t.RepositoryName); ok {
			t.DestinationReference = repo.DefaultBranch
		}
	}
	source, srcErr := b.resolveCommitSpecifier(t.RepositoryName, t.SourceReference)
	dest, destErr := b.resolveCommitSpecifier(t.RepositoryName, t.DestinationReference)
	if srcErr == nil {
		t.SourceCommit = source
	}
	if destErr == nil {
		t.DestinationCommit = dest
	}
	if srcErr == nil && destErr == nil {
		t.MergeBase = b.mergeBase(t.RepositoryName, source, dest)
	}
}

// snapshotPullRequest deep-copies pr; an open PR's targets reflect the
// current branch tips. Caller holds the lock.
func (b *InMemoryBackend) snapshotPullRequest(pr *PullRequest) *PullRequest {
	cp := copyPullRequest(pr)
	if pr.PullRequestStatus != prStatusClosed {
		for i := range cp.PullRequestTargets {
			b.fillTargetCommits(&cp.PullRequestTargets[i])
		}
	}

	return cp
}

func copyPullRequest(pr *PullRequest) *PullRequest {
	cp := *pr
	cp.PullRequestTargets = make([]PullRequestTarget, len(pr.PullRequestTargets))
	for i, t := range pr.PullRequestTargets {
		if t.MergeMetadata != nil {
			m := *t.MergeMetadata
			t.MergeMetadata = &m
		}
		cp.PullRequestTargets[i] = t
	}

	return &cp
}

// ResolveCommitSpecifier resolves a branch name or full commit ID to a
// commit ID, taking the read lock itself -- the exported counterpart of
// resolveCommitSpecifier for callers (handleGetMergeCommit) that are not
// already holding b.mu.
func (b *InMemoryBackend) ResolveCommitSpecifier(repoName, specifier string) (string, error) {
	b.mu.RLock("ResolveCommitSpecifier")
	defer b.mu.RUnlock()

	return b.resolveCommitSpecifier(repoName, specifier)
}

// resolveCommitSpecifier resolves a branch name or full commit ID to a commit
// ID. Real AWS specifiers can also be a tag or HEAD; this backend has no tag
// concept and no separate HEAD pointer, so those are out of scope. Caller
// must hold at least the read lock.
func (b *InMemoryBackend) resolveCommitSpecifier(repoName, specifier string) (string, error) {
	if specifier == "" {
		return "", fmt.Errorf("%w: commit specifier is required", ErrCommitSpecifierRequired)
	}
	if branch, ok := b.branches.Get(branchKey(repoName, specifier)); ok {
		return branch.CommitID, nil
	}
	if _, ok := b.commits.Get(commitKey(repoName, specifier)); ok {
		return specifier, nil
	}

	return "", fmt.Errorf("%w: commit specifier %s not found", ErrCommitNotFound, specifier)
}

// MergeBase returns the nearest common ancestor of two commits (a commit is
// its own ancestor), or "" when their histories are unrelated.
func (b *InMemoryBackend) MergeBase(repoName, sourceID, destID string) string {
	b.mu.RLock("MergeBase")
	defer b.mu.RUnlock()

	return b.mergeBase(repoName, sourceID, destID)
}

func (b *InMemoryBackend) mergeBase(repoName, sourceID, destID string) string {
	sourceAncestors := make(map[string]struct{})
	for queue := []string{sourceID}; len(queue) > 0; queue = queue[1:] {
		id := queue[0]
		if _, seen := sourceAncestors[id]; seen {
			continue
		}
		sourceAncestors[id] = struct{}{}
		if c, ok := b.commits.Get(commitKey(repoName, id)); ok {
			queue = append(queue, c.Parents...)
		}
	}

	visited := make(map[string]struct{})
	for queue := []string{destID}; len(queue) > 0; queue = queue[1:] {
		id := queue[0]
		if _, seen := visited[id]; seen {
			continue
		}
		visited[id] = struct{}{}
		if _, ok := sourceAncestors[id]; ok {
			return id
		}
		if c, ok := b.commits.Get(commitKey(repoName, id)); ok {
			queue = append(queue, c.Parents...)
		}
	}

	return ""
}

// MergeBranchesOptions carries the optional fields MergeBranchesBySquash and
// MergeBranchesByThreeWay accept beyond the two commit specifiers.
type MergeBranchesOptions struct {
	TargetBranch  string
	CommitMessage string
	AuthorName    string
	Email         string
	Settings      MergeSettings
}

// MergeBranchesBySquash merges source into destination as one new commit on
// top of the destination tip; the source's own commits are not parents.
func (b *InMemoryBackend) MergeBranchesBySquash(
	repoName, sourceSpecifier, destSpecifier string, opts MergeBranchesOptions,
) (*Commit, error) {
	return b.mergeBranches(repoName, sourceSpecifier, destSpecifier, opts, false)
}

// MergeBranchesByThreeWay merges source into destination as a merge commit
// with two parents, the destination tip first.
func (b *InMemoryBackend) MergeBranchesByThreeWay(
	repoName, sourceSpecifier, destSpecifier string, opts MergeBranchesOptions,
) (*Commit, error) {
	return b.mergeBranches(repoName, sourceSpecifier, destSpecifier, opts, true)
}

func (b *InMemoryBackend) mergeBranches(
	repoName, sourceSpecifier, destSpecifier string, opts MergeBranchesOptions, threeWay bool,
) (*Commit, error) {
	b.mu.Lock("MergeBranches")
	defer b.mu.Unlock()

	if !b.repositories.Has(repoName) {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	sourceID, err := b.resolveCommitSpecifier(repoName, sourceSpecifier)
	if err != nil {
		return nil, err
	}
	destID, err := b.resolveCommitSpecifier(repoName, destSpecifier)
	if err != nil {
		return nil, err
	}

	ev := b.evaluateMergeLocked(repoName, sourceID, destID, opts.Settings)
	if len(ev.unresolved) > 0 {
		return nil, fmt.Errorf("%w: %d file(s) have unresolved conflicts", ErrManualMergeRequired, len(ev.unresolved))
	}

	targetBranch := opts.TargetBranch
	if targetBranch == "" {
		targetBranch = destSpecifier
	}

	parents := []string{destID}
	message := opts.CommitMessage

	if threeWay {
		parents = append(parents, sourceID)
	}

	if message == "" {
		message = defaultMergeMessage(threeWay, sourceSpecifier, destSpecifier)
	}

	commit := b.newMergeCommitLocked(repoName, parents, ev, message, opts.AuthorName, opts.Email)
	b.advanceBranchLocked(repoName, targetBranch, commit, b.treeOfLocked(repoName, destID))

	return cloneCommit(commit), nil
}

func defaultMergeMessage(threeWay bool, source, dest string) string {
	if threeWay {
		return fmt.Sprintf("Merge %s into %s", source, dest)
	}

	return fmt.Sprintf("Squash merge of %s into %s", source, dest)
}

func cloneCommit(c *Commit) *Commit {
	cp := *c
	cp.Parents = append([]string(nil), c.Parents...)

	return &cp
}

// newMergeCommitLocked stores ev's new blobs and creates the merge commit with ev's tree.
func (b *InMemoryBackend) newMergeCommitLocked(
	repo string, parents []string, ev *mergeEvaluation, message, author, email string,
) *Commit {
	for id, content := range ev.newBlobs {
		b.storeBlobLocked(repo, id, content)
	}

	commit := &Commit{
		CommitID:       uuid.NewString(),
		TreeID:         uuid.NewString(),
		Message:        message,
		AuthorName:     author,
		AuthorEmail:    email,
		CommitterName:  author,
		CommitterEmail: email,
		RepositoryName: repo,
		Parents:        parents,
		CreatedAt:      time.Now().UTC(),
	}
	setCommitTree(commit, maps.Clone(ev.tree))
	b.commits.Put(commit)

	return commit
}

// advanceBranchLocked points branch at the merge commit and brings the file
// view and per-file history in line with its tree.
func (b *InMemoryBackend) advanceBranchLocked(repo, branch string, commit *Commit, before map[string]TreeEntry) {
	b.branches.Put(&Branch{BranchName: branch, CommitID: commit.CommitID, RepositoryName: repo})
	b.recordTreeHistoryLocked(repo, commit.CommitID, before, commit.Tree)
	b.syncFilesToTreeLocked(repo, commit.CommitID, commit.Tree)
}

// syncFilesToCommitLocked aligns the file view with a commit that carries a tree.
func (b *InMemoryBackend) syncFilesToCommitLocked(repo, commitID string) {
	if c, ok := b.commits.Get(commitKey(repo, commitID)); ok && c.HasTree {
		b.syncFilesToTreeLocked(repo, commitID, c.Tree)
	}
}

// MergeBranchesByFastForward merges branches by fast-forward: the target
// branch's tip is moved to the resolved source commit and no commit is created.
func (b *InMemoryBackend) MergeBranchesByFastForward(
	repoName, sourceRef, destinationRef, targetBranch string,
) (*Commit, error) {
	b.mu.Lock("MergeBranchesByFastForward")
	defer b.mu.Unlock()

	if !b.repositories.Has(repoName) {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	sourceCommitID, err := b.resolveCommitSpecifier(repoName, sourceRef)
	if err != nil {
		return nil, err
	}
	if _, destErr := b.resolveCommitSpecifier(repoName, destinationRef); destErr != nil {
		return nil, destErr
	}

	branch := targetBranch
	if branch == "" {
		branch = destinationRef
	}
	b.branches.Put(&Branch{
		BranchName:     branch,
		CommitID:       sourceCommitID,
		RepositoryName: repoName,
	})
	b.syncFilesToCommitLocked(repoName, sourceCommitID)

	commit, ok := b.commits.Get(commitKey(repoName, sourceCommitID))
	if !ok {
		return nil, fmt.Errorf("%w: commit %s not found", ErrCommitNotFound, sourceCommitID)
	}

	return cloneCommit(commit), nil
}
