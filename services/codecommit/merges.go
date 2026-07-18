package codecommit

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// BatchDescribeMergeConflicts describes merge conflicts between two commits.
// This is a stub implementation — it returns empty conflicts since the backend
// does not track file-level content.
func (b *InMemoryBackend) BatchDescribeMergeConflicts(
	repositoryName, destinationCommitSpecifier, sourceCommitSpecifier, _ string,
	filePaths []string,
) (*BatchDescribeMergeConflictsResult, error) {
	b.mu.RLock("BatchDescribeMergeConflicts")
	defer b.mu.RUnlock()

	if !b.repositories.Has(repositoryName) {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repositoryName)
	}

	result := &BatchDescribeMergeConflictsResult{
		DestinationCommitID: destinationCommitSpecifier,
		SourceCommitID:      sourceCommitSpecifier,
		Conflicts:           []MergeConflict{},
	}

	if len(filePaths) > 0 {
		result.Conflicts = make([]MergeConflict, 0, len(filePaths))
		for _, fp := range filePaths {
			result.Conflicts = append(result.Conflicts, MergeConflict{
				ConflictMetadata: ConflictMetadata{
					FilePath:          fp,
					NumberOfConflicts: 0,
					ContentConflict:   false,
				},
				MergeHunks: []MergeHunk{},
			})
		}
	}

	return result, nil
}

// MergePullRequestByFastForward merges a pull request by fast-forward strategy.
func (b *InMemoryBackend) MergePullRequestByFastForward(
	prID, _ /* repoName */, _ /* sourceRef */ string,
) (*PullRequest, error) {
	b.mu.Lock("MergePullRequestByFastForward")
	defer b.mu.Unlock()

	pr, ok := b.pullRequests.Get(prID)
	if !ok {
		return nil, fmt.Errorf("%w: pull request %s not found", ErrPullRequestNotFound, prID)
	}
	if pr.PullRequestStatus == prStatusMerged || pr.PullRequestStatus == prStatusClosed {
		return nil, fmt.Errorf("%w: pull request %s is already closed", ErrPullRequestAlreadyMerged, prID)
	}
	pr.PullRequestStatus = prStatusMerged
	pr.LastActivityDate = time.Now().UTC()
	cp := *pr

	return &cp, nil
}

// MergePullRequestBySquash merges a pull request by squash strategy.
func (b *InMemoryBackend) MergePullRequestBySquash(
	prID, _ /* repoName */, _ /* sourceRef */ string,
) (*PullRequest, error) {
	b.mu.Lock("MergePullRequestBySquash")
	defer b.mu.Unlock()

	pr, ok := b.pullRequests.Get(prID)
	if !ok {
		return nil, fmt.Errorf("%w: pull request %s not found", ErrPullRequestNotFound, prID)
	}
	if pr.PullRequestStatus == prStatusMerged || pr.PullRequestStatus == prStatusClosed {
		return nil, fmt.Errorf("%w: pull request %s is already closed", ErrPullRequestAlreadyMerged, prID)
	}
	pr.PullRequestStatus = prStatusMerged
	pr.LastActivityDate = time.Now().UTC()
	cp := *pr

	return &cp, nil
}

// MergePullRequestByThreeWay merges a pull request by three-way strategy.
func (b *InMemoryBackend) MergePullRequestByThreeWay(
	prID, _ /* repoName */, _ /* sourceRef */ string,
) (*PullRequest, error) {
	b.mu.Lock("MergePullRequestByThreeWay")
	defer b.mu.Unlock()

	pr, ok := b.pullRequests.Get(prID)
	if !ok {
		return nil, fmt.Errorf("%w: pull request %s not found", ErrPullRequestNotFound, prID)
	}
	if pr.PullRequestStatus == prStatusMerged || pr.PullRequestStatus == prStatusClosed {
		return nil, fmt.Errorf("%w: pull request %s is already closed", ErrPullRequestAlreadyMerged, prID)
	}
	pr.PullRequestStatus = prStatusMerged
	pr.LastActivityDate = time.Now().UTC()
	cp := *pr

	return &cp, nil
}

// MergeBranchesByFastForward merges branches by fast-forward and creates a merge commit.
func (b *InMemoryBackend) MergeBranchesByFastForward(repoName, sourceRef, destinationRef string) (*Commit, error) {
	b.mu.Lock("MergeBranchesByFastForward")
	defer b.mu.Unlock()

	if !b.repositories.Has(repoName) {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	commitID := uuid.NewString()
	treeID := uuid.NewString()
	now := time.Now().UTC()
	commit := &Commit{
		CommitID:       commitID,
		TreeID:         treeID,
		Message:        fmt.Sprintf("Merge %s into %s", sourceRef, destinationRef),
		RepositoryName: repoName,
		CreatedAt:      now,
	}
	b.commits.Put(commit)

	// Update destination branch tip
	b.branches.Put(&Branch{
		BranchName:     destinationRef,
		CommitID:       commitID,
		RepositoryName: repoName,
	})

	cp := *commit

	return &cp, nil
}

// GetMergeOptions returns the available merge options for two branches.
func (b *InMemoryBackend) GetMergeOptions(
	repoName, _ /* sourceRef */, _ /* destinationRef */ string,
) ([]string, error) {
	b.mu.RLock("GetMergeOptions")
	defer b.mu.RUnlock()

	if !b.repositories.Has(repoName) {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	return []string{"FAST_FORWARD_MERGE", "SQUASH_MERGE", "THREE_WAY_MERGE"}, nil
}

// CreateUnreferencedMergeCommit creates a new unreferenced merge commit.
func (b *InMemoryBackend) CreateUnreferencedMergeCommit(
	repoName, sourceCommitID, destinationCommitID string,
) (*Commit, error) {
	b.mu.Lock("CreateUnreferencedMergeCommit")
	defer b.mu.Unlock()

	if !b.repositories.Has(repoName) {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	commitID := uuid.NewString()
	treeID := uuid.NewString()
	now := time.Now().UTC()
	commit := &Commit{
		CommitID:       commitID,
		TreeID:         treeID,
		Message:        "Unreferenced merge commit",
		RepositoryName: repoName,
		Parents:        []string{sourceCommitID, destinationCommitID},
		CreatedAt:      now,
	}
	b.commits.Put(commit)
	cp := *commit
	cp.Parents = make([]string, len(commit.Parents))
	copy(cp.Parents, commit.Parents)

	return &cp, nil
}

// GetMergeCommit returns a commit that has both sourceCommitSpecifier and
// destinationCommitSpecifier as parents, or falls back to the most recent commit.
func (b *InMemoryBackend) GetMergeCommit(
	repoName, sourceCommitSpecifier, destinationCommitSpecifier string,
) (*Commit, error) {
	b.mu.RLock("GetMergeCommit")
	defer b.mu.RUnlock()

	if !b.repositories.Has(repoName) {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	repoCommits := b.commitsByRepo.Get(repoName)

	// Prefer a commit whose parents include both specifiers (real merge commit shape).
	for _, c := range repoCommits {
		hasSource, hasDest := false, false
		for _, p := range c.Parents {
			if p == sourceCommitSpecifier {
				hasSource = true
			}
			if p == destinationCommitSpecifier {
				hasDest = true
			}
		}
		if hasSource && hasDest {
			cp := *c
			cp.Parents = make([]string, len(c.Parents))
			copy(cp.Parents, c.Parents)

			return &cp, nil
		}
	}

	// Fallback: return the most recent commit.
	var latest *Commit
	for _, c := range repoCommits {
		if latest == nil || c.CreatedAt.After(latest.CreatedAt) {
			latest = c
		}
	}
	if latest != nil {
		cp := *latest
		cp.Parents = make([]string, len(latest.Parents))
		copy(cp.Parents, latest.Parents)

		return &cp, nil
	}

	return nil, fmt.Errorf("%w: no commits found in repository %s", ErrCommitNotFound, repoName)
}

// GetMergeConflicts returns whether there are merge conflicts (always false).
func (b *InMemoryBackend) GetMergeConflicts(
	repoName, _ /* sourceCommitSpecifier */, _ /* destCommitSpecifier */, _ /* mergeOption */ string,
) (bool, error) {
	b.mu.RLock("GetMergeConflicts")
	defer b.mu.RUnlock()

	if !b.repositories.Has(repoName) {
		return false, fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}

	return false, nil
}
