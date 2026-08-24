package codecommit

import (
	"fmt"
	"sort"
	"time"
)

// CreateBranch creates a new branch in a repository.
func (b *InMemoryBackend) CreateBranch(repositoryName, branchName, commitID string) error {
	if err := validateBranchName(branchName); err != nil {
		return err
	}

	b.mu.Lock("CreateBranch")
	defer b.mu.Unlock()

	if !b.repositories.Has(repositoryName) {
		return fmt.Errorf("%w: repository %s not found", ErrNotFound, repositoryName)
	}

	// Validate that the commitID exists in the repository.
	if !b.commits.Has(commitKey(repositoryName, commitID)) {
		return fmt.Errorf("%w: commit %s not found in repository %s", ErrCommitNotFound, commitID, repositoryName)
	}

	if b.branches.Has(branchKey(repositoryName, branchName)) {
		return fmt.Errorf("%w: branch %s already exists", ErrBranchAlreadyExists, branchName)
	}

	b.branches.Put(&Branch{
		BranchName:     branchName,
		CommitID:       commitID,
		RepositoryName: repositoryName,
	})

	return nil
}

// GetBranch returns a branch by repository and branch name.
func (b *InMemoryBackend) GetBranch(repositoryName, branchName string) (*Branch, error) {
	b.mu.RLock("GetBranch")
	defer b.mu.RUnlock()

	if !b.repositories.Has(repositoryName) {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repositoryName)
	}

	br, ok := b.branches.Get(branchKey(repositoryName, branchName))
	if !ok {
		return nil, fmt.Errorf("%w: branch %s not found", ErrBranchNotFound, branchName)
	}

	cp := *br

	return &cp, nil
}

// DeleteBranch deletes a branch from a repository.
func (b *InMemoryBackend) DeleteBranch(repositoryName, branchName string) (*Branch, error) {
	b.mu.Lock("DeleteBranch")
	defer b.mu.Unlock()

	if !b.repositories.Has(repositoryName) {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repositoryName)
	}

	br, ok := b.branches.Get(branchKey(repositoryName, branchName))
	if !ok {
		// DeleteBranch is idempotent in real AWS: DeleteBranchOutput.DeletedBranch
		// is optional (codecommit@v1.36.4 api_op_DeleteBranch.go:45), and its own
		// error switch has no BranchDoesNotExistException case, unlike GetBranch
		// (inference).
		return nil, nil //nolint:nilnil // nil branch is a meaningful "already gone" signal, not an error
	}

	cp := *br
	b.branches.Delete(branchKey(repositoryName, branchName))

	return &cp, nil
}

// ListBranches returns all branch names for a repository in sorted order.
func (b *InMemoryBackend) ListBranches(repositoryName string) ([]string, error) {
	b.mu.RLock("ListBranches")
	defer b.mu.RUnlock()

	if !b.repositories.Has(repositoryName) {
		return nil, fmt.Errorf("%w: repository %s not found", ErrNotFound, repositoryName)
	}

	group := b.branchesByRepo.Get(repositoryName)
	names := make([]string, 0, len(group))
	for _, br := range group {
		names = append(names, br.BranchName)
	}
	sort.Strings(names)

	return names, nil
}

// UpdateDefaultBranch sets the default branch for a repository.
// AWS requires the branch to exist in the repository.
func (b *InMemoryBackend) UpdateDefaultBranch(repoName, branchName string) error {
	b.mu.Lock("UpdateDefaultBranch")
	defer b.mu.Unlock()

	r, ok := b.repositories.Get(repoName)
	if !ok {
		return fmt.Errorf("%w: repository %s not found", ErrNotFound, repoName)
	}
	// Validate the branch exists.
	if branchName != "" && !b.branches.Has(branchKey(repoName, branchName)) {
		return fmt.Errorf("%w: branch %s not found in repository %s", ErrBranchNotFound, branchName, repoName)
	}
	r.DefaultBranch = branchName
	r.LastModifiedDate = time.Now().UTC()

	return nil
}
